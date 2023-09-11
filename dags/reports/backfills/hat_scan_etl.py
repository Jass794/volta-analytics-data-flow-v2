from reports.utils.harmonics import *
from reports.utils.common import *
from multiprocessing import Pool
from functools import partial
from loguru import logger
import datetime as dt
import notifiers
import sys
import os

# Get Secrets from Environment Variables
api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))
lt_avg_days = 60


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
            "subject": "Error - Backfill HAT Scan ETL",
        }
        notifier = notifiers.get_notifier("gmail")
        notifier.notify(message="Log File attached!", **params)
    return 0


@logger.catch
# Backfill HAT Scan for a location
def backfill_hat_scan(location_dict, report_dates, report_type):
    # Log Name for this location
    log_name = str(location_dict['node_sn']) + ' - ' + location_dict['location_name'] + ' - ' + location_dict['facility_name']
    logger.info('{} Backfill Initiated for {}'.format(report_type.title(), log_name))

    # Make first date in report_dates as backfill_start_date and parse as datetime
    backfill_start_date = dt.datetime.strptime(report_dates[0], '%Y-%m-%d').date()
    # data_start date will be lt_avg_days days from backfill_start_date
    data_start_date = backfill_start_date - dt.timedelta(days=lt_avg_days)
    # data_end date last date in report_dates
    data_end_date = dt.datetime.strptime(report_dates[-1], '%Y-%m-%d').date()

    # Extract Location Details from location_dict
    location_node_id = str(location_dict['location_node_id'])
    node_sn = int(location_dict['node_sn'])

    # Get harmonic data for location
    harmonic_dump_df = get_harmonic_data(
        location_node_id, data_start_date, data_end_date, report_type.title(), api_token
    )

    # loop through report_dates
    for report_date in report_dates:
        scan_end_date = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=999999)
        scan_start_date = (scan_end_date - dt.timedelta(days=lt_avg_days)).replace(hour=0, minute=0, second=0, microsecond=0)
        daily_scan_frame = harmonic_dump_df[
            (harmonic_dump_df['time'] >= scan_start_date) &
            (harmonic_dump_df['time'] <= scan_end_date)
        ].copy()

        # Get first and last date in daily_scan_frame
        first_date = daily_scan_frame['time'].min()
        last_date = daily_scan_frame['time'].max()
        # If time difference between first and last time is less than lt_avg_days days, then skip process
        if daily_scan_frame.empty or (last_date - first_date).days < lt_avg_days:
            logger.warning('Not enough Harmonic Data for scan - {} - {}'.format(log_name, report_date))
            continue
        else:
            logger.debug('Harmonic scan from {} to {} - {}'.format(first_date, last_date, log_name))
            # Scan end day start time
            scan_end_day_start_time = scan_end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            # Scan end day end time
            scan_end_day_end_time = scan_end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            st_frame = harmonic_dump_df[
                (harmonic_dump_df['time'] >= scan_end_day_start_time) &
                (harmonic_dump_df['time'] <= scan_end_day_end_time)
            ].reset_index(drop=True)
            # Sort by harmonic_freq ascending
            st_frame = st_frame.sort_values(by=['harmonic_freq'])
            # Create harmonic list from harmonic_freq columns
            sorted_harmonic_list = st_frame['harmonic_freq'].tolist()
            logger.info('Total Harmonics to Scan - {} - {}'.format(len(sorted_harmonic_list), log_name))
            # Long term average days to Short term average days ratio
            lt_st_ratio = 6
            # Process daily scan frame
            processed_daily_scan = process_harmonic_data(daily_scan_frame, sorted_harmonic_list, lt_st_ratio, lt_avg_days)

            # Create data and configs to insert
            # Create post time from report_date
            post_time = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            headers = {'Authorization': 'Bearer ' + api_token}
            post_body = {
                'time': str(post_time),
                'node_sn': str(node_sn),
                'location_node_id': location_node_id,
            }
            # Hat Report type
            hat_type = '{}_hat_report'.format(report_type)
            post_body[hat_type] = json.dumps(processed_daily_scan)
            # INSERT INTO STAGING DB
            # Add report to database
            post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/daily_scans/"
            staging_response = requests.post(url=post_url, json=post_body, headers=headers)
            logger.info('Report for {} Added Staging Status: {}'.format(log_name, staging_response.status_code))
            staging_response.raise_for_status()

            # INSERT INTO PRODUCTION DB
            # Add report to database
            post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_scans/"
            prod_response = requests.post(url=post_url, json=post_body, headers=headers)
            logger.info('Report for {} Added Production Status: {}'.format(log_name, prod_response.status_code))
            prod_response.raise_for_status()
    logger.success('Backfill Finished for {}'.format(log_name))
    return 0


if __name__ == "__main__":
    # List all arguments
    args = sys.argv
    if len(args) < 3:
        print("Please provide '-s 2022-04-01' for start date and '-e 2022-04-15' for end date and '-t Current/Voltage' for type")
        sys.exit(1)
    if '-s' in args:
        start_date = args[args.index('-s') + 1]
        # Parse as date
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
    else:
        print("-s not in args")
        sys.exit(1)
    if '-e' in args:
        end_date = args[args.index('-e') + 1]
        # Parse as date
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date()
    else:
        print("-e not in args")
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

    # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}-{}.txt'.format(report_type, utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.backfill/{}/'.format(home_dir, 'hat_scan_etl')
    # If log directory does not exist, create it
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    # Create process logger
    process_logger = logger
    logs_retention = '1 week'

    # If -d in args, then debug mode, no need to email on error
    if '-d' in args:
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
        # Filter locations where hat_report_scan filter
        locations_df = locations_df[locations_df[hat_scan_filter] == True]
        # Convert to records
        locations_records = locations_df.to_dict('records')

    # Generate date list
    report_dates = generate_dates(start_date, end_date)

    with Pool(2) as pool:
        status = pool.map(partial(backfill_hat_scan, report_dates=report_dates, report_type=report_type.lower()), locations_records)
