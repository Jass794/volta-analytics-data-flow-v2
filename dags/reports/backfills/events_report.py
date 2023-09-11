from email.mime.multipart import MIMEMultipart
from reports.utils.events import *
from email.mime.text import MIMEText
from reports.utils.common import *
from loguru import logger
import datetime as dt
import notifiers
import smtplib
import sys
import os


# Get Secrets from Environment Variables
api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))
lt_avg_days = 14


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


@logger.catch
# Events Report
def events_report(locations_df, report_date):
    # Loop through dates
    logger.info('Events Report Date: {}'.format(report_date))
    final_events_frame = pd.DataFrame(
        columns=[
            'customer_name', 'node_sn', 'location_name', 'facility_name', 
            'event_type', 'lt_avg', 'st_count', 'change', 'threshold', 
            'location_node_id', 'facility_id', 'customer_id'
        ]
    )
    final_outlier_frame = pd.DataFrame(columns=[
        'time', 'node_sn', 'location_name', 'facility_name', 
        'event_type', 'outlier_type', 'customer_name'
    ])
    # Empty event list
    no_ss_events_list = []
    no_transient_events_list = []
    # Loop through unique location_node_id
    for location_node_id in locations_df['location_node_id'].unique():
        # Get row of location_node_id
        row = locations_df[locations_df['location_node_id'] == location_node_id]
        # Create Equipment name and location if
        node_sn = int(row['node_sn'].values[0])
        location_name = str(row['location_name'].values[0])
        facility_name = str(row['facility_name'].values[0])
        facility_id = str(row['facility_id'].values[0])
        customer_name = str(row['customer_name'].values[0])
        customer_id = str(row['customer_id'].values[0])
        np_current = float(row['np_current'].values[0])
        node_configs = row['node_configs'].values[0]
        if 'wc' not in node_configs:
            logger.warning('Skip Analysis, WC Flag not found in Node Configs, Equipment: {} - {} - {}'.format(node_sn, location_name, facility_name))
            continue
        work_cycle = int(node_configs['wc'])
        # Location_details
        location_details = {
            'customer_name': customer_name,
            'node_sn': node_sn,
            'location_name': location_name,
            'facility_name': facility_name,
            'location_node_id': location_node_id,
            'facility_id': facility_id,
            'customer_id': customer_id,
            'work_cycle': work_cycle
        }
        # Create log_name
        log_name = str(location_details['node_sn']) + ' - ' + location_details['location_name'] + ' - ' + location_details['facility_name']
        logger.info('Equipment: {}'.format(log_name))
        # Get events frame
        events_frame = get_events_df(api_token, location_node_id, report_date, lt_avg_days)
        if not events_frame.empty:
            # Slice Out Transient Events Frame
            transient_events = events_frame[events_frame['event_type'].str.contains("rise|fall")]
            # Slice Out Start Stop Events Frame
            ss_events = events_frame[~events_frame['event_type'].str.contains("rise|fall")]
            # If events_frame has no transient events, add to no_transient_events_list
            if transient_events.empty:
                no_transient_events_list.append(location_details.copy())
            # If events_frame has no start stop events, add to no_ss_events_list
            if ss_events.empty:
                if location_details['work_cycle'] == 0:
                    no_ss_events_list.append(location_details.copy())
            # If date of most recent time is not the same as end date, return empty dataframe
            if events_frame['time'].iloc[-1] != dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc):
                pass
            else:
                # Process events trend data
                email_events_frame = process_events_frame(events_frame, report_date, location_details)
                # If email_events_frame is not empty, add to final_events_frame
                if not email_events_frame.empty:
                    # Concat final_events_frame and email_events_frame
                    final_events_frame = pd.concat([final_events_frame, email_events_frame], ignore_index=True)
                    # Reset index
                    final_events_frame = final_events_frame.reset_index(drop=True)
                # Get outlier events for the location
                outlier_events_df = get_outlier_events(report_date, location_node_id, np_current, api_token)
                # If outlier_events_df is not empty, add to final_outlier_frame
                if not outlier_events_df.empty:
                    # Add columns to outlier_events_df
                    outlier_events_df['customer_name'] = customer_name
                    outlier_events_df['node_sn'] = node_sn
                    outlier_events_df['location_name'] = location_name
                    outlier_events_df['facility_name'] = facility_name
                    outlier_events_df['location_node_id'] = location_node_id
                    outlier_events_df['facility_id'] = facility_id
                    outlier_events_df['customer_id'] = customer_id

                    # Concat final_outlier_frame and outlier_events_df
                    final_outlier_frame = pd.concat([final_outlier_frame, outlier_events_df], ignore_index=True)
                    # Reset index
                    final_outlier_frame = final_outlier_frame.reset_index(drop=True)
        else:
            logger.warning('Empty Events Data for {} Days Equipment: {}'.format(lt_avg_days, str(str(node_sn) + ' - ' + location_name)))
            no_transient_events_list.append(location_details.copy())
            if location_details['work_cycle'] == 0:
                no_ss_events_list.append(location_details.copy())
    
    # Add node/equipment status to list [Offline, Online, No Start/Stops]
    no_transient_events_list = location_node_status(no_transient_events_list, report_date, api_token, lt_avg_days)
    no_ss_events_list = location_node_status(no_ss_events_list, report_date, api_token, lt_avg_days)
    if not final_outlier_frame.empty:
        final_outlier_frame = final_outlier_frame.sort_values(by=['customer_name', 'node_sn', 'location_name', 'facility_name']).reset_index(drop=True)
        # Make time column string
        final_outlier_frame['time'] = final_outlier_frame['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    final_events_frame = final_events_frame.sort_values(by='change', key=abs, ascending=False)
    # Round change to 2 decimal places
    final_events_frame['change'] = final_events_frame['change'].apply(lambda x: round(x, 2))
    final_events_frame = final_events_frame.reset_index(drop=True)
    # No events list
    no_ss_events_df = pd.DataFrame.from_records(no_ss_events_list)
    no_trensient_events_df = pd.DataFrame.from_records(no_transient_events_list)
    # Replace nan with empty string
    no_ss_events_df = no_ss_events_df.fillna('---')
    no_trensient_events_df = no_trensient_events_df.fillna('---')
    # Result dict
    result_dict = {
        'email_time': str(dt.datetime.utcnow()),
        'report_date': report_date,
        'configs': {
            'threshold_dict': threshold_dict,
            'lt_avg_days': lt_avg_days
        },
        'events_report': final_events_frame.to_dict(),
        'outlier_events': final_outlier_frame.to_dict(),
        'no_ss_events': no_ss_events_df.to_dict(),
        'no_transient_events': no_trensient_events_df.to_dict()
    }
    # Insert data into staging and production
    result = insert_events_report(result_dict, report_date, '/internal/staging', api_token)
    result = insert_events_report(result_dict, report_date, '/internal', api_token)
    logger.success('Events Report Completed')
    return result


# Email Events report
def email_events_report(processed_result):
    logger.info('Emailing Events Report')
    # Set report date
    report_date = processed_result['report_date']
    report_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    # Create Trending dataframe
    events_report_df = pd.DataFrame.from_dict(processed_result['events_report'])
    outlier_events_df = pd.DataFrame.from_dict(processed_result['outlier_events'])
    no_ss_events_df = pd.DataFrame.from_dict(processed_result['no_ss_events'])
    no_transient_events_df = pd.DataFrame.from_dict(processed_result['no_transient_events'])

    # Only keep required columns for Events report
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 
        'event_type', 'lt_avg', 'st_count', 'change', 'threshold'
    ]
    events_report_df = events_report_df[email_columns]
    # Sort by st_count descending
    events_report_df = events_report_df.sort_values(by='st_count', ascending=False).reset_index(drop=True)
    
    # Rename columns for Events report
    events_report_df = events_report_df.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'event_type': 'Event Type',
            'lt_avg': 'LT Avg',
            'st_count': 'ST Count',
            'change': '% Change',
            'threshold': 'Threshold'
        }
    )

    # Only keep required columns for Outlier events report
    email_columns = [
        'time', 'node_sn', 'location_name', 'facility_name', 
        'event_type', 'outlier_type', 'customer_name'
    ]
    outlier_events_df = outlier_events_df[email_columns]
    
    # Rename columns for Outlier events report
    outlier_events_df = outlier_events_df.rename(
        columns={
            'time': 'UTC Timestamp',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'event_type': 'Event Type',
            'outlier_type': 'Outlier Type',
            'customer_name': 'Customer'
        }
    )

    # Only keey rows with start_stop as True
    no_ss_events_df = no_ss_events_df[no_ss_events_df['start_stop'] == True]
    # Only keep required columns for No SS events report
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 'recent_start', 'recent_stop'
    ]
    no_ss_events_df = no_ss_events_df[email_columns]
    rename_columns = {
        'customer_name': 'Customer',
        'node_sn': 'Node Serial',
        'location_name': 'Equipment',
        'facility_name': 'Facility',
        'recent_start': 'Recent Start',
        'recent_stop': 'Recent Stop'
    }
    # Rename columns for email
    no_ss_events_df = no_ss_events_df.rename(columns = rename_columns)

    # Only keey rows with transients as True
    no_transient_events_df = no_transient_events_df[no_transient_events_df['transients'] == True]
    # Only keep required columns No Transient events report
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name'
    ]
    no_transient_events_df = no_transient_events_df[email_columns]
    rename_columns = {
        'customer_name': 'Customer',
        'node_sn': 'Node Serial',
        'location_name': 'Equipment',
        'facility_name': 'Facility'
    }
    # Rename columns for email
    no_transient_events_df = no_transient_events_df.rename(columns = rename_columns)

    # SEND EMAIL
    sender_email = 'notifications@voltainsite.com'
    receivers = report_receivers
    subject = 'Daily Events Report for %s' % (report_date.strftime("%B %d, %Y"))
    
    tables = """\
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
                <b>No Start-Stops in last {2} Days:</b><br>
                {3}
                <p>
                <p>
                <b>No Transients in last {2} Days:</b><br>
                {4}
                <p>
            </body>
        </html>
    """.format(
        events_report_df.to_html(index=False),
        outlier_events_df.to_html(index=False),
        lt_avg_days,
        no_ss_events_df.to_html(index=False),
        no_transient_events_df.to_html(index=False)
    )

    message = MIMEMultipart()
    part_1 = MIMEText(tables, 'html')
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
    return 'Success'


if __name__ == "__main__":
    # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}.txt'.format(utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.logs/daily/{}/'.format(home_dir, 'events_report')
    # If log directory does not exist, create it
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    # Create process logger
    process_logger = logger
    logs_retention = '1 week'

    # List all arguments
    args = sys.argv
    # If -d in args, then debug mode, no need to email on error
    if '-d' in args:
        process_logger = process_logger.add(
            log_dir_path + log_file_name, retention=logs_retention, enqueue=True, 
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}"
        )
        report_receivers = ['analytics-data-flow-errors@voltainsite.com']
    else:
        process_logger = process_logger.add(
            log_dir_path + log_file_name, retention=logs_retention, enqueue=True, 
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}",
            compression=email_log_on_error
        )
        report_receivers = ['analytics-reports@voltainsite.com']

    # Get Locations 
    locations_df = get_node_df(api_token)
    if not locations_df.empty:
        # Filter locations where trending_report_scan is true
        locations_df = locations_df[locations_df['trending_report_scan'] == True]
        # Ask for start_date and end_date
        start_date = input('Backfill start date (YYYY-MM-DD): ')
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = input('Backfill end date (YYYY-MM-DD): ')
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date()
        # Generate date list
        report_dates = generate_dates(start_date, end_date)
        for report_date in report_dates:
            event_report_dict = events_report(locations_df, report_date)
        # Email HAT Report
        email_events_report(event_report_dict)
    else:
        process_logger.error('No locations found')
        sys.exit(1)
