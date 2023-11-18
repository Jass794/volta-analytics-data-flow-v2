import json

import pandas as pd
import requests

from reports.utils.harmonics import *
from reports.utils.common import *
from dotenv import load_dotenv
from billiard.pool import Pool
from functools import partial
from loguru import logger
import datetime as dt
import notifiers
import sys
import os

lt_data_days = 90


@logger.catch
# HAT Scan for a location
def hat_scan(location_dict, report_date, report_type, api_token):
    # Log Name for this location
    log_name = str(location_dict['node_sn']) + ' - ' + location_dict['location_name'] + ' - ' + location_dict['facility_name']
    logger.info('{} HAT Scan Initiated for {}'.format(report_type.title(), log_name))

    # Configs for the scan scan duration
    st_scan_look_back_days = 15  # Number of days used by scan to find max and min for filter
    st_scan_lt_avg_days = 15  # Number of days used by scan for long term average
    st_scan_st_avg_days = 3  # short term average period

    lt_scan_look_back_days = 15  # Number of days used by scan to find max and min for filter
    lt_scan_lt_avg_days = 90  # Number of days used by scan for long term average
    lt_scan_st_avg_days = 3  # short term average period

    perform_lt_scan = True
    perform_st_scan = True

    # Change days for VFD
    if location_dict['starter'] == 'VFD':
        st_scan_lt_avg_days = 30
        st_scan_look_back_days = 30
        lt_scan_look_back_days = 30

    # data_end date is report_date
    data_end_date = dt.datetime.strptime(report_date, '%Y-%m-%d').date()
    # data_start date will be lt_avg_days days from start_date
    data_start_date = data_end_date - dt.timedelta(days=lt_data_days)

    # Extract Location Details from location_dict
    location_node_id = str(location_dict['location_node_id'])
    node_sn = int(location_dict['node_sn'])

    # Get harmonic data for location
    harmonic_dump_df = get_harmonic_data_v2(
        location_node_id, data_start_date, data_end_date, report_type.title(), api_token, location_dict['node_details']['type']
    )

    # Make frame copy
    daily_scan_frame = harmonic_dump_df.copy()

    # Get first and last date in daily_scan_frame
    first_date = daily_scan_frame['time'].min()
    last_date = daily_scan_frame['time'].max()

    # If time difference between first and last time is less than lt_avg_days days, then skip process
    if (last_date - first_date).days < lt_data_days:
        perform_lt_scan = False
        logger.warning('Not enough Harmonic Data for LT scan - {} - {}'.format(log_name, report_date))
    if (last_date - first_date).days < st_scan_lt_avg_days:
        perform_st_scan = False
        logger.warning('Not enough Harmonic Data for ST scan - {} - {}'.format(log_name, report_date))
    # If time difference between first and last time is less than lt_avg_days days, then skip process
    if daily_scan_frame.empty:
        logger.warning('Not enough Harmonic Data for scan - {} - {}'.format(log_name, report_date))
        return 0
    else:
        logger.debug('Harmonic scan from {} to {} - {}'.format(first_date, last_date, log_name))
        # Scan end day end time
        scan_end_day_end_time = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=999999)
        # Scan end day start time
        scan_end_day_start_time = scan_end_day_end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        # Only select the recent harmonics
        st_frame = harmonic_dump_df[(harmonic_dump_df['time'] == last_date)].reset_index(drop=True)
        # Sort by harmonic_freq ascending
        st_frame = st_frame.sort_values(by=['harmonic_freq'])
        # Create harmonic list from harmonic_freq columns
        sorted_harmonic_list = st_frame['harmonic_freq'].tolist()

        logger.info('Total Harmonics to Scan - {} - {}'.format(len(sorted_harmonic_list), log_name))
        # create empty df
        processed_daily_st_scan = pd.DataFrame()
        processed_daily_scan_long_term = pd.DataFrame()

        if perform_st_scan:
            # Process daily scan frame for short term
            processed_daily_st_scan = process_harmonic_data_v4(harmonic_frame=daily_scan_frame,
                                                               harmonic_list=sorted_harmonic_list,
                                                               st_avg_days=st_scan_st_avg_days,
                                                               lt_avg_days=st_scan_lt_avg_days,
                                                               max_peak_days=st_scan_look_back_days,
                                                               scan_date=scan_end_day_start_time,
                                                               scan_type='short_term',
                                                               location_dict=location_dict)
            processed_daily_st_scan = pd.DataFrame(processed_daily_st_scan)

        if perform_lt_scan:
            # Process daily scan frame for long term
            processed_daily_scan_long_term = process_harmonic_data_v4(harmonic_frame=daily_scan_frame,
                                                                      harmonic_list=sorted_harmonic_list,
                                                                      st_avg_days=lt_scan_st_avg_days,
                                                                      lt_avg_days=lt_scan_lt_avg_days,
                                                                      max_peak_days=lt_scan_look_back_days,
                                                                      scan_date=scan_end_day_start_time,
                                                                      scan_type='long_term',
                                                                      location_dict=location_dict
                                                                      )

            processed_daily_scan_long_term = pd.DataFrame(processed_daily_scan_long_term)

        processed_daily_scan = pd.concat([processed_daily_st_scan, processed_daily_scan_long_term], ignore_index=True)
        if not processed_daily_scan.empty:
            # Create post time from report_date
            post_time = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            headers = {'Authorization': 'Bearer ' + api_token}
            post_body = {
                'time': str(post_time),
                'node_sn': str(node_sn),
                'location_node_id': location_node_id,
            }
            # Hat Report type
            hat_type = '{}_hat_report_v2'.format(report_type)
            post_body[hat_type] = json.dumps(processed_daily_scan.to_dict(orient='dict'))
            # INSERT INTO STAGING DB
            # Add report to database
            logger.info('Adding scan results to staging')
            post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/daily_scans/"
            staging_response = requests.post(url=post_url, json=post_body, headers=headers)
            logger.info('Report for {} Added Staging Status: {}'.format(log_name, staging_response.status_code))
            staging_response.raise_for_status()

            # INSERT INTO PRODUCTION DB
            # Add report to database
            logger.info('Adding scan results to production')
            post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_scans/"
            prod_response = requests.post(url=post_url, json=post_body, headers=headers)
            logger.info('Report for {} Added Production Status: {}'.format(log_name, prod_response.status_code))
            prod_response.raise_for_status()
    logger.success('HAT Scan Finished for {}'.format(log_name))
    return 0


def run_hat_scan(report_type, env='staging', debug=False, ):
    if env == 'production':
        server = 'production'
    else:
        server = 'staging'

    load_dotenv(f'{os.getcwd()}/.{server}.env')
    # Get Secrets from Environment Variables
    api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
    email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))

    # Email on Error in Log file
    def email_log_on_error(log_filepath):
        # Read log file
        with open(log_filepath, 'r') as f:
            log_content = f.read()
            # Close file
            f.close()
        # Check if ERROR in log
        if 'ERROR' in log_content:
            # Get utc datetime
            utc_datetime_email = str(dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:00'))
            params = {
                "attachments": [log_filepath],
                "username": "notifications@voltainsite.com",
                "password": email_app_pass,
                "to": "analytics-data-flow-errors@voltainsite.com",
                "subject": "Error - HAT Scan ETL",
            }
            notifier = notifiers.get_notifier("gmail")
            notifier.notify(message="Log File attached!", **params)
        return 0

    # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}-{}.txt'.format(report_type, utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.logs/daily/{}/'.format(home_dir, 'hat_scan_etl')
    # If log directory does not exist, create it
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    # Create process logger
    process_logger = logger
    logs_retention = '1 week'

    # If -d in args, then debug mode, no need to email on error
    if debug:
        process_logger = process_logger.add(
            log_dir_path + log_file_name, retention=logs_retention, enqueue=True,
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}"
        )
    else:
        process_logger = process_logger.add(
            log_dir_path + log_file_name, retention=logs_retention, enqueue=True,
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}",
            compression=email_log_on_error
        )

    # Get Locations
    locations_df = get_node_df(api_token)
    if not locations_df.empty:
        hat_scan_filter = '{}_hat_report_scan'.format(report_type.lower())
        # Filter locations where HAT Report needs to run
        # If 'eq_type' does not exist, in node_configs dict then add it as 'none'

        locations_df['node_configs'] = locations_df['node_configs'].apply(lambda x: x if 'eq_type' in x else {**x, 'eq_type': 'none'})
        locations_df = locations_df[
            (locations_df.node_details.apply(lambda x: x['deploymentIssue'] == False)) &
            (locations_df.node_details.apply(lambda x: x['currentDeploymentStatus'] == 'Deployed')) &
            (locations_df.node_details.apply(lambda x: x['dataPointInterval'] == 900)) &
            (locations_df.node_configs.apply(lambda x: x['wc'] == 0)) &
            (locations_df.node_configs.apply(lambda x: x['eq_type'] not in ['dc']))
            ]

        # Convert to records
        locations_records = locations_df.to_dict('records')
    else:
        process_logger.error('No locations found')
        sys.exit(1)
        # Get UTC date
    utc_date = str((dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d'))

    with Pool(4) as pool:
        status = pool.map(partial(hat_scan, report_date=utc_date, report_type=report_type.lower(), api_token=api_token), locations_records)


if __name__ == "__main__":
    # List all arguments
    args = sys.argv
    debug = False
    if len(args) < 1:
        print("Please provide '-t Current/Voltage' for type")
        sys.exit(1)
    if '-t' in args:
        report_type = args[args.index('-t') + 1]
        if report_type not in ['Current', 'Voltage']:
            print("Report type should be either 'Current' or 'Voltage'")
            sys.exit(1)
    else:
        print("-t not in args")
        print("It should be either 'Current' or 'Voltage'")
        sys.exit(1)
    if 'production' in args:
        enviornment = 'production'
    else:
        enviornment = 'staging'
    if '-d' in args:
        debug = True

    run_hat_scan(report_type, enviornment, debug)
