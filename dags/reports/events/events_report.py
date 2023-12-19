from email.mime.multipart import MIMEMultipart
from billiard.pool import Pool
from multiprocessing import cpu_count
from email.mime.text import MIMEText
from typing import List
from functools import partial
from dotenv import load_dotenv
from loguru import logger
from .utils import *
from ..utils.common import get_analytics_portfolio
from volta_analytics_db_models.tabels.portfolio_v2_models import PortfolioV2Model
import datetime as dt
import numpy as np
import notifiers
import asyncio
import smtplib
import time
import sys
import os

# Get Secrets from Environment Variables

st_avg_days = 1
lt_avg_days = 14
change_threshold = 200
outlier_lt_days = 60


@logger.catch
async def process_per_location_events_data(location: PortfolioV2Model, report_date, report_datetime, api_token):
    location_banner = f"{location.location_name} - SN: {location.node_sn} - {location.facility_name} - {report_date}"

    logger.info('Processing Events Data for ' + location_banner)
    location_event_report = {
        'time': str(report_datetime),
        'location_node_id': location.location_node_id,
        'node_online': False,
        'equipment_active': True,
        'no_events': {
            'start': True,
            'start-d': True,
            'stop': True,
            'stop-u': True,
            'voltage_rise': True,
            'voltage_fall': True,
            'current_rise': True,
            'current_fall': True
        },
        'recent_start': None,
        'recent_stop': None,
        'events_trending': []
    }
    # data_end date is report_date
    data_end_date = dt.datetime.strptime(report_date, '%Y-%m-%d').date()
    # data_start date will be lt_avg_days days from start_date
    data_start_date = data_end_date - dt.timedelta(days=lt_avg_days)
    # Get Events for Location
    events_count_frame = await get_events_count_24h(location.location_node_id, data_start_date, data_end_date, api_token)
    if not events_count_frame.empty:
        # Means was online in last lt_avg_days days
        location_event_report['node_online'] = True
        # Short term average frame
        st_avg_start_date = data_end_date - dt.timedelta(days=st_avg_days)
        # Add time to st_avg_start_date
        st_avg_start_time = dt.datetime.combine(st_avg_start_date, dt.time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S')
        st_avg_count_frame = events_count_frame[events_count_frame['time'] >= st_avg_start_time]
        # Long term average frame
        lt_avg_count_frame = events_count_frame[~(events_count_frame['time'] >= st_avg_start_time)]
        # Remove time from st_avg_count_frame and lt_avg_count_frame
        st_avg_count_frame = st_avg_count_frame.drop(columns=['time', 'location_node_id'])
        lt_avg_count_frame = lt_avg_count_frame.drop(columns=['time', 'location_node_id'])
        # Get ST average of event count
        st_avg_count_frame = st_avg_count_frame.groupby(['event_type']).mean().reset_index()
        # Get LT average of event count
        lt_avg_count_frame = lt_avg_count_frame.groupby(['event_type']).mean().reset_index()
        # If st_avg_count_frame is empty, then no events in last st_avg_days days
        if st_avg_count_frame.empty:
            logger.warning('No Events in st_avg days for ' + location_banner)
            for event_type in lt_avg_count_frame['event_type']:
                location_event_report['no_events'][event_type] = False
        else:
            # If lt_avg_count_frame is empty, then no events in last lt_avg_days days
            if lt_avg_count_frame.empty:
                logger.warning('No Events in lt_avg days for ' + location_banner)
                event_matrix = st_avg_count_frame
                event_matrix = event_matrix.rename(columns={
                    'event_count': 'st_avg_count',
                })
                event_matrix['lt_avg_count'] = 0
            else:
                # Merge st_avg_count_frame and lt_avg_count_frame on event_type, if not present in lt_avg_count_frame, then lt_avg_count = 0
                event_matrix = pd.merge(st_avg_count_frame, lt_avg_count_frame, on='event_type', how='outer', suffixes=('_st_avg', '_lt_avg'))
                event_matrix = event_matrix.fillna(0)
            # Add location_node_id to event_matrix
            event_matrix['location_node_id'] = location.location_node_id
            # Rename columns
            event_matrix = event_matrix.rename(columns={
                'event_count_st_avg': 'st_avg_count',
                'event_count_lt_avg': 'lt_avg_count'
            })
            # Calculate % change in event count, where lt_avg_count is 0, make change as -1
            event_matrix['change'] = ((event_matrix['st_avg_count'] - event_matrix['lt_avg_count']) / event_matrix['lt_avg_count']) * 100
            event_matrix['change'] = event_matrix['change'].apply(lambda x: -1 if x == np.inf else x)
            # Round change, st_avg_count and lt_avg_count to 2 decimal places
            event_matrix['change'] = event_matrix['change'].round(2)
            event_matrix['st_avg_count'] = event_matrix['st_avg_count'].round(2)
            event_matrix['lt_avg_count'] = event_matrix['lt_avg_count'].round(2)

            # loop through event_type in event_matrix
            for event_type in event_matrix['event_type'].values:
                # if st_avg_count > 0 or lt_avg_count > 0, then node is online
                if event_matrix[event_matrix['event_type'] == event_type]['st_avg_count'].values[0] > 0 or \
                        event_matrix[event_matrix['event_type'] == event_type]['lt_avg_count'].values[0] > 0:
                    # mak no events False because there is at least one event
                    location_event_report['no_events'][event_type] = False
            # Only keep events with change < 0 and change > change_threshold
            event_matrix = event_matrix[(event_matrix['change'] == -1) | (event_matrix['change'] > change_threshold)]
            # Add threshold column to event_matrix
            event_matrix['threshold'] = change_threshold
            # Add event_matrix to location_event_report
            location_event_report['events_trending'] = event_matrix.to_dict('records')

    # Add exception for Workcycle nodes
    if location.work_cycle == True:
        for event_type in location_event_report['no_events'].keys():
            if event_type in ['start', 'start-d', 'stop', 'stop-u']:
                location_event_report['no_events'][event_type] = False

    # Check if there is any True in no_events or node_online is False (Only possible if events_count_frame is empty)
    # if True in location_event_report['no_events'].values() or not location_event_report['node_online']:
    if True in location_event_report['no_events'].values():
        # List type of events with no events
        no_events_list = [event_type for event_type, no_event in location_event_report['no_events'].items() if no_event]

        # First we'll check if the equipment has ran in past lt_avg_days days
        # Get 15 min data details from analytics
        node_data_daily_agg_frame = await get_daily_node_data_agg(location.location_node_id, data_start_date, data_end_date, api_token)
        if not node_data_daily_agg_frame.empty:
            location_event_report['node_online'] = True
            # If there is No row with current > i_noise, then Equipment if OFF
            if node_data_daily_agg_frame[node_data_daily_agg_frame['current'] > location.i_noise].empty:
                logger.warning('Equipment is OFF for ' + location_banner)
                location_event_report['equipment_active'] = False
        else:
            logger.warning('Equipment is OFF for ' + location_banner)
            location_event_report['equipment_active'] = False

        # Second we'll check if there are missing start stop events by comparing with 1 sec data
        if location_event_report['equipment_active'] and ('start' in no_events_list or 'stop' in no_events_list):
            # Get 1 sec data details from analytics
            sec_data_daily_agg_frame = await get_daily_1_sec_data_agg(location.location_id, data_start_date, data_end_date, api_token)
            # If sec_data_daily_agg_frame is empty, means node is offline
            if not sec_data_daily_agg_frame.empty:
                # means node was online in last lt_avg_days days
                location_event_report['node_online'] = True
                # If start or stop in no_events_list, then investigate further
                if 'start' in no_events_list:
                    start_count = sec_data_daily_agg_frame['starts'].max()
                    # If start_count > 0, modify no_events in location_event_report
                    if start_count > 0:
                        # Means there are start events in 1 sec file and node missed a start event
                        location_event_report['no_events']['start'] = True
                        # Latest time converted to date where starts > 0 in sec_data_daily_agg_frame
                        latest_start = sec_data_daily_agg_frame[sec_data_daily_agg_frame['starts'] > 0]['time'].max().date()
                        # Add latest_start to location_event_report
                        location_event_report['recent_start'] = str(latest_start)
                        # If there is a start then equipment is active
                        location_event_report['equipment_active'] = True
                    else:
                        # Means there are no start events in 1 sec file
                        location_event_report['no_events']['start'] = False
                if 'stop' in no_events_list:
                    stop_count = sec_data_daily_agg_frame['stops'].max()
                    # If stop_count > 0, modify no_events in location_event_report
                    if stop_count > 0:
                        # Means there are stop events in 1 sec file and node missed a stop event
                        location_event_report['no_events']['stop'] = True
                        # Latest time converted to date where stops > 0 in sec_data_daily_agg_frame
                        latest_stop = sec_data_daily_agg_frame[sec_data_daily_agg_frame['stops'] > 0]['time'].max().date()
                        # Add latest_stop to location_event_report
                        location_event_report['recent_stop'] = str(latest_stop)
                        # If there is a stop then equipment is active
                        location_event_report['equipment_active'] = True
                    else:
                        # Means there are no stop events in 1 sec file
                        location_event_report['no_events']['stop'] = False
            else:
                logger.warning('No 1 sec data for ' + location_banner)
                # Edge case where node is online but not sending 1-sec data
                pass
    return location_event_report


@logger.catch
def process_daily_report(location_reports, locations: List[PortfolioV2Model], report_utc_date, report_utc_datetime, api_token):
    final_event_report = {
        'time': str(report_utc_datetime),
        'report_type': 'events_report',
        'report_version': '1',
        'report_content': {
            'events_trending': {},
            'events_outliers': {},
            'no_start_events': {},
            'no_stop_events': {},
            'no_voltage_transient_events': {},
            'no_current_transient_events': {},
        }
    }
    # Create empty lists
    no_start_events_locations = []
    no_stop_events_locations = []
    no_voltage_transient_events_locations = []
    no_current_transient_events_locations = []
    events_trending_locations = []
    outlier_events = []
    # Loop through all location reports
    for location_report in location_reports:
        # Proceed only if node is online
        location = [loc for loc in locations if loc.location_node_id == location_report['location_node_id']][0]
        if location_report['node_online'] and location_report['equipment_active']:
            location_banner = f"{location.location_name} - SN: {location.node_sn} - {location.facility_name} - {report_utc_date}"
            # If no start events, add location to no_start_events_locations
            if location_report['no_events']['start']:
                logger.debug(f'No start events for {location_banner}')
                no_start_events_locations.append({
                    'location_node_id': location_report['location_node_id'],
                    'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                    'recent_start': location_report['recent_start']
                })
            # If no stop events, add location to no_stop_events_locations
            if location_report['no_events']['stop']:
                logger.debug(f'No stop events for {location_banner}')
                no_stop_events_locations.append({
                    'location_node_id': location_report['location_node_id'],
                     'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                    'recent_stop': location_report['recent_stop']
                })
            # If no voltage transient events, add location to no_voltage_transient_events_locations
            if location_report['no_events']['voltage_rise'] and location_report['no_events']['voltage_fall']:
                logger.debug(f'No voltage transient events for {location_banner}')
                no_voltage_transient_events_locations.append({
                    'location_node_id': location_report['location_node_id'],
                     'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                })
            # If no current transient events, add location to no_current_transient_events_locations
            if location_report['no_events']['current_rise'] and location_report['no_events']['current_fall']:
                logger.debug(f'No current transient events for {location_banner}')
                no_current_transient_events_locations.append({
                    'location_node_id': location_report['location_node_id'],
                   'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                })
            # Loop through events_trending
            for event_trending in location_report['events_trending']:
                # Create a dataframe
                events_trending_locations.append({
                    'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                    'event_type': event_trending['event_type'],
                    'st_avg_count': event_trending['st_avg_count'],
                    'lt_avg_count': event_trending['lt_avg_count'],
                    'change': event_trending['change'],
                    'threshold': event_trending['threshold'],
                    'location_node_id': location_report['location_node_id'],
                })
            # Get outlier events for the location
            outlier_events_df = get_outlier_events(str(report_utc_date), location_report['location_node_id'], location.np_current, outlier_lt_days, api_token)
            # loop through outlier events
            for outlier_event in outlier_events_df.to_dict('records'):
                # Create outlier events dict
                outlier_events.append({
                     'customer_id': location.location_node_id,
                    'facility_id': location.facility_id,
                    'customer_name': location.customer_name,
                    'node_sn': location.node_sn,
                    'location_name': location.location_name,
                    'facility_name': location.facility_name,
                    'event_utc_timestamp': str(outlier_event['time']),
                    'event_type': outlier_event['event_type'],
                    'outlier_type': outlier_event['outlier_type'],
                    's3_bucket_key_pair': outlier_event['s3_bucket_key_pair'],
                    'location_node_id': location_report['location_node_id'],
                })
    # Create dataframes
    no_start_events_locations_df = pd.DataFrame(no_start_events_locations)
    no_stop_events_locations_df = pd.DataFrame(no_stop_events_locations)
    no_voltage_transient_events_locations_df = pd.DataFrame(no_voltage_transient_events_locations)
    no_current_transient_events_locations_df = pd.DataFrame(no_current_transient_events_locations)
    events_trending_locations_df = pd.DataFrame(events_trending_locations)
    outlier_events_df = pd.DataFrame(outlier_events)
    # Update final report
    final_event_report['report_content']['no_start_events'] = no_start_events_locations_df.to_dict(orient='list')
    final_event_report['report_content']['no_stop_events'] = no_stop_events_locations_df.to_dict(orient='list')
    final_event_report['report_content']['no_voltage_transient_events'] = no_voltage_transient_events_locations_df.to_dict(orient='list')
    final_event_report['report_content']['no_current_transient_events'] = no_current_transient_events_locations_df.to_dict(orient='list')
    final_event_report['report_content']['events_trending'] = events_trending_locations_df.to_dict(orient='list')
    final_event_report['report_content']['outlier_events'] = outlier_events_df.to_dict(orient='list')

    # Add thresholds to final report
    final_event_report['report_content']['thresholds'] = {
        'st_avg_days': st_avg_days,
        'lt_avg_days': lt_avg_days,
        'change_threshold': change_threshold,
        'outlier_lt_days': outlier_lt_days
    }
    return final_event_report


@logger.catch
def email_events_report(final_event_report, report_receivers, report_date, email_app_pass):
    logger.info('Emailing Events Report for {}'.format(str(report_date)))
    # Create and reformat dataframes from final report
    events_trending_df = pd.DataFrame(final_event_report['report_content']['events_trending'])
    # Only keep required columns for Events report
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
        'event_type', 'lt_avg_count', 'st_avg_count', 'change', 'threshold'
    ]
    if not events_trending_df.empty:
        events_trending_df = events_trending_df[email_columns]
        # Sort by st_count descending
        events_trending_df = events_trending_df.sort_values(by='st_avg_count', ascending=False).reset_index(drop=True)
        # Change cells with change = -1 to New
        events_trending_df.loc[events_trending_df['change'] == -1, 'change'] = 'New'
    else:
        # Create empty dataframe with required columns
        events_trending_df = pd.DataFrame(columns=email_columns)
    # Rename columns for Events report
    events_trending_df = events_trending_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'event_type': 'Event Type',
            'lt_avg_count': 'LT Avg Events per Day',
            'st_avg_count': 'ST Avg Events per Day',
            'change': '% Change',
            'threshold': 'Threshold'
        }
    )

    outlier_events_df = pd.DataFrame(final_event_report['report_content']['outlier_events'])
    # Only keep required columns for Outlier events report
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
        'event_utc_timestamp', 'event_type', 'outlier_type'
    ]
    if not outlier_events_df.empty:
        outlier_events_df = outlier_events_df[email_columns]
    else:
        # Create empty dataframe with required columns
        outlier_events_df = pd.DataFrame(columns=email_columns)
    # Rename columns for Outlier events report
    outlier_events_df = outlier_events_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'event_type': 'Event Type',
            'event_utc_timestamp': 'UTC Timestamp',
            'outlier_type': 'Outlier Type'
        }
    )

    no_start_events_df = pd.DataFrame(final_event_report['report_content']['no_start_events'])
    # Only keep required columns
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
        'recent_start'
    ]
    if not no_start_events_df.empty:
        no_start_events_df = no_start_events_df[email_columns]
    else:
        # Create empty dataframe with required columns
        no_start_events_df = pd.DataFrame(columns=email_columns)
    # Rename columns
    no_start_events_df = no_start_events_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'recent_start': 'Recent Start'
        }
    )

    no_stop_events_df = pd.DataFrame(final_event_report['report_content']['no_stop_events'])
    # Only keep required columns
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
        'recent_stop'
    ]
    if not no_stop_events_df.empty:
        no_stop_events_df = no_stop_events_df[email_columns]
    else:
        # Create empty dataframe with required columns
        no_stop_events_df = pd.DataFrame(columns=email_columns)
    # Rename columns
    no_stop_events_df = no_stop_events_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'recent_stop': 'Recent Stop'
        }
    )

    no_voltage_transient_events_df = pd.DataFrame(final_event_report['report_content']['no_voltage_transient_events'])
    # Only keep required columns
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
    ]
    if not no_voltage_transient_events_df.empty:
        no_voltage_transient_events_df = no_voltage_transient_events_df[email_columns]
    else:
        # Create empty dataframe with required columns
        no_voltage_transient_events_df = pd.DataFrame(columns=email_columns)
    # Rename columns
    no_voltage_transient_events_df = no_voltage_transient_events_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
        }
    )

    no_current_transient_events_df = pd.DataFrame(final_event_report['report_content']['no_current_transient_events'])
    # Only keep required columns
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
    ]
    if not no_current_transient_events_df.empty:
        no_current_transient_events_df = no_current_transient_events_df[email_columns]
    else:
        # Create empty dataframe with required columns
        no_current_transient_events_df = pd.DataFrame(columns=email_columns)
    # Rename columns
    no_current_transient_events_df = no_current_transient_events_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
        }
    )

    # Create email HTML body
    email_body = """\
        <html>
            <head>
                <style>
                table, th, td {{border:1px solid black; border-collapse:collapse; text-align:center;}}  
                th, td {{padding: 5px;}}   
                </style>
            </head>
            <body>
                <p>
                <b>Events Trending Table:</b><br>
                {0}
                <p>
                <p>
                <b>Outlier Events:</b><br>
                {1}
                <p>
                <p>
                <b>No Start Events in last {2} Days:</b><br>
                {3}
                <p>
                <p>
                <b>No Stop Events in last {2} Days:</b><br>
                {4}
                <p>
                <p>
                <b>No Voltage Transient Events in last {2} Days:</b><br>
                {5}
                <p>
                <p>
                <b>No Current Transient Events in last {2} Days:</b><br>
                {6}
                <p>
            </body>
        </html>
    """.format(
        events_trending_df.to_html(index=False),
        outlier_events_df.to_html(index=False),
        lt_avg_days,
        no_start_events_df.to_html(index=False),
        no_stop_events_df.to_html(index=False),
        no_voltage_transient_events_df.to_html(index=False),
        no_current_transient_events_df.to_html(index=False)
    )
    # SEND EMAIL
    report_date = dt.datetime.strptime(report_date, '%Y-%m-%d').strftime('%B %d, %Y')
    sender_email = 'notifications@voltainsite.com'
    receivers = report_receivers
    subject = 'Daily Events Report for {}'.format(report_date)
    message = MIMEMultipart()
    part_1 = MIMEText(email_body, 'html')
    message.attach(part_1)
    message['From'] = sender_email
    message['To'] = ", ".join(receivers)
    message['Subject'] = subject
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(sender_email, email_app_pass)
    server.sendmail(sender_email, receivers, message.as_string())
    server.close()
    logger.success('Events Report Email Sent')
    return 0


@logger.catch
def backfill_events_report_init(avg_dates_lists, locations: List[PortfolioV2Model],api_token):
    report_utc_date, report_utc_datetime = avg_dates_lists[0], avg_dates_lists[1]
    # Create asyncio event loop
    loop = asyncio.get_event_loop()
    # Create a list of tasks
    tasks = []
    start_timer = time.time()
    task_count = 0
    for location in locations:
        task_count += 1
        tasks.append(process_per_location_events_data(location, report_utc_date, report_utc_datetime, api_token))
    # Run all tasks with async functions in it
    location_reports = loop.run_until_complete(asyncio.gather(*tasks))
    # loop.close()
    # Process daily report using location reports
    events_report = process_daily_report(location_reports, locations, report_utc_date, report_utc_datetime, api_token)
    # Post report to Analytics
    post_status = post_events_report(events_report, api_token)
    return task_count


def parse_args(args):
    debug = False
    backfill = False
    start_date = None
    end_date = None

    for i in range(1, len(args)):
        if args[i] == '-d':
            debug = True
        elif args[i] == '-b':
            backfill = True
            try:
                start_date = args[i + 1]
                end_date = args[i + 2]
            except (IndexError, ValueError):
                print("Please provide valid dates for backfill as '-b 2021-01-01 2021-01-02'")
                sys.exit(1)
        elif args[i] == '-h':
            print("Usage: python script.py [-e] [-d] [-b start_date end_date]")
            sys.exit(0)

    return debug, backfill, start_date, end_date


def process_events_report(env='staging', debug=False, backfill=False, start_date=None, end_date=None):
    try:
        if env == 'production':
            server = 'production'
        else:
            server = 'staging'

        load_dotenv(f'{os.getcwd()}/.{server}.env')
        api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
        email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))
        # Get utc datetime
        utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
        # Get log file name
        log_file_name = '{}.txt'.format(utc_datetime)
        # Get home directory
        home_dir = str(os.getenv('HOME'))
        # Log directory
        log_dir_path = '{}/.logs/jobs/{}/'.format(home_dir, 'events_report')
        # If log directory does not exist, create it
        if not os.path.exists(log_dir_path):
            os.makedirs(log_dir_path)
        # Create process logger
        process_logger = logger

        # Email on Error in Log file
        def email_log_on_error(log_filepath):
            # Read log file
            with open(log_filepath, 'r') as f:
                log_content = f.read()
                # Close file
                f.close()
            # Check if ERROR in log
            if 'ERROR' in log_content:
                params = {
                    "attachments": [log_filepath],
                    "username": "notifications@voltainsite.com",
                    "password": email_app_pass,
                    "to": "analytics-data-flow-errors@voltainsite.com",
                    "subject": "Error - Events Report",
                }
                notifier = notifiers.get_notifier("gmail")
                notifier.notify(message="Log File attached!", **params)
            return 0

        # If debug mode is enabled, configure logger accordingly
        if debug:
            process_logger = process_logger.add(
                log_dir_path + log_file_name, enqueue=True,
                format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}"
            )
            report_receivers = ['analytics-data-flow-errors@voltainsite.com']
        else:
            process_logger = process_logger.add(
                log_dir_path + log_file_name, enqueue=True,
                format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}",
                compression=email_log_on_error
            )
            report_receivers = ['analytics-reports@voltainsite.com']

        # Get Locations
        locations = get_analytics_portfolio(server_path='', analytics_api_token=api_token)

        locations = [location for location in locations if location.deployment_issue == False 
                                                            and location.current_deployment_status == 'Deployed'
                                                            and location.customer_code in ['gp']
                                                            and location.equipment_type not in ['UtilityMain']]

        # If not backfill
        if not backfill:
            # Report date will be 1 day before current UTC date
            report_utc_date = [str((dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d'))]
            # Replace time with zeros
            report_utc_datetime = [(dt.datetime.utcnow() - dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')]
            # Log number of records to process
            logger.info('Number of locations to process: {}'.format(len(locations)))
            start_timer = time.time()
            task_count = 0
            # Loop through dates
            for report_utc_date, report_utc_datetime in zip(report_utc_date, report_utc_datetime):
                # Create a list of tasks
                tasks = []
                # Create asyncio event loop, run tasks in parallel and gather results
                loop = asyncio.get_event_loop()
                for location in locations:
                    task_count += 1
                    # Create a task
                    task = loop.create_task(process_per_location_events_data(location, report_utc_date, report_utc_datetime, api_token))
                    # Add task to list
                    tasks.append(task)
                # Run all tasks with async functions in it and gather results
                location_reports = loop.run_until_complete(asyncio.gather(*tasks))
                loop.close()
                # Process daily report using location reports
                events_report = process_daily_report(location_reports, locations, report_utc_date, report_utc_datetime, api_token)
                # Post report to Analytics
                post_status = post_events_report(events_report, api_token)
                if post_status:
                    logger.success('Report posted successfully')
                    email_events_report(events_report, report_receivers, report_utc_date, email_app_pass)
            end_timer = time.time()
            logger.info('Total time taken: {} seconds'.format(round(end_timer - start_timer, 2)))
            if task_count > 0:
                # Time taken for each task
                logger.info('Time taken for each task: {} seconds'.format(round((end_timer - start_timer) / task_count, 2)))
        else:
            # Averaging date will be between start and end dates
            avg_dates_utc = [
                str((start_date + dt.timedelta(days=x)).strftime('%Y-%m-%d')) for x in range(0, (end_date - start_date).days + 1)
            ]
            # Replace time with zeros
            avg_datetimes_utc = [
                str((start_date + dt.timedelta(days=x)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')) for x in range(0, (end_date - start_date).days + 1)
            ]
            avg_dates_lists = [[x, y] for x, y in zip(avg_dates_utc, avg_datetimes_utc)]
            # Log number of records to process
            logger.info('Number of locations to process: {}'.format(len(locations)))
            logger.info('Averaging date: {}'.format(avg_dates_utc))
            logger.info('Averaging datetime: {}'.format(avg_datetimes_utc))
            start_timer = time.time()
            with Pool(cpu_count()) as pool:
                task_counts_list = pool.map(partial(backfill_events_report_init, locations_records=locations), avg_dates_lists)
            end_timer = time.time()
            logger.info('Total time taken: {} seconds'.format(round(end_timer - start_timer, 2)))
            # Get total task count
            task_count = max(task_counts_list)
            if task_count > 0:
                # Time taken for each task
                logger.info('Average CPU Time per task: {} seconds'.format(round((end_timer - start_timer) / task_count, 2)))
    except Exception as e:
        # Handle exceptions here or log them as needed
        print(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # List all arguments
    debug, backfill, start_date, end_date = parse_args(sys.argv)

    process_events_report('staging', debug, backfill, start_date, end_date)
