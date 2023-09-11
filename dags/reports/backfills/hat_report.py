from reports.utils.harmonics import *
from reports.utils.common import *
from loguru import logger
import datetime as dt
import notifiers
import sys
import os

# Get Secrets from Environment Variables
api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))
change_impace_threshold = 50


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
            "subject": "Error - HAT Report Backfill",
        }
        notifier = notifiers.get_notifier("gmail")
        notifier.notify(message="Log File attached!", **params)
    return 0


@logger.catch
# Backfill HAT Report for a location
def backfill_hat_report(location_df, report_date, report_type):
    logger.info('{} Backfill Initiated for {}'.format(report_type.title(), report_date))
    report_hat_frame = pd.DataFrame()
    # Hat Report type
    hat_type = '{}_hat_report'.format(report_type)
    # Loop through unique location_node_id
    for location_node_id in location_df['location_node_id'].unique():
        # Get row of location_node_id
        row = location_df[location_df['location_node_id'] == location_node_id]
        # Create Equipment name and location if
        vfd_driven = bool(row['vfd_driven'].values[0])
        node_sn = int(row['node_sn'].values[0])
        location_name = str(row['location_name'].values[0])
        facility_name = str(row['facility_name'].values[0])
        facility_id = str(row['facility_id'].values[0])
        customer = str(row['customer_name'].values[0])
        customer_id = str(row['customer_id'].values[0])
        # Log Name for this location
        log_name = str(node_sn) + ' - ' + location_name + ' - ' + facility_name
        # Get HAT report df from database
        eqpt_hat_report = get_daily_scans(location_node_id, report_date, hat_type, api_token)
        logger.debug('Fetched Daily Scans for {}'.format(log_name))
        if eqpt_hat_report.empty:
            continue
        else:
            threshold = change_impace_threshold
            if vfd_driven:
                threshold = threshold * 2
            # Add equipment name and location_node_id to df
            eqpt_hat_report['node_sn'] = node_sn
            eqpt_hat_report['location_name'] = location_name
            eqpt_hat_report['facility_name'] = facility_name
            eqpt_hat_report['facility_id'] = facility_id
            eqpt_hat_report['customer_name'] = customer
            eqpt_hat_report['customer_id'] = customer_id
            eqpt_hat_report['location_node_id'] = location_node_id
            eqpt_hat_report['threshold'] = threshold
            # Re order columns position
            eqpt_hat_report = eqpt_hat_report[[
                'customer_name', 'node_sn', 'location_name', 'facility_name', 'harmonic_lf', 
                'lt_avg', 'st_avg', 'change', 'impact', 'lt_count', 'st_count', 
                'total_count', 'location_node_id', 'facility_id', 'customer_id', 'threshold'
            ]]
            # Add equipment to report_hat_frame
            report_hat_frame = pd.concat([report_hat_frame, eqpt_hat_report], ignore_index=True)
    if report_hat_frame.empty:
        logger.warning('No Daily scans found on {}'.format(report_date))
        return 0
    else:
        # Filter to reduce harmonics
        report_harmonics = apply_harmonics_filter(report_hat_frame)
        # Remove mirror harmonics from 1-2 LF
        report_harmonics = mirror_harmonics_removal(report_harmonics)
        # Get Previous hat reports
        previous_hat_reports = get_previous_reports(report_date, hat_type, api_token)
        
        if previous_hat_reports.empty:
            logger.warning('Empty previous hat report email on {}'.format(report_date))
            new_harmonics = report_harmonics
        else:
            # Remove duplicates on location_node_id and harmonics_lf
            previous_hat_reports = previous_hat_reports.drop_duplicates(subset=['location_node_id', 'harmonic_lf'])
            # Remove harmonics within tolerance using previous_hat_reports
            new_harmonics = tolerance_harmonics_removal(report_harmonics, previous_hat_reports)
            # Remove mirror harmonics in 1-2 LF range
            new_harmonics = mirror_harmonics_removal(new_harmonics)

    # Order columns in new_harmonics and report_harmonics
    ordered_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 'harmonic_lf', 
        'lt_avg', 'st_avg', 'change', 'impact', 'lt_count', 'st_count', 
        'total_count', 'location_node_id', 'facility_id', 'customer_id'
    ]
    new_harmonics = new_harmonics[ordered_columns]
    report_harmonics = report_harmonics[ordered_columns]
    # Sort new_harmonics and report_harmonics by harmonic_lf
    new_harmonics = new_harmonics.sort_values(by=['harmonic_lf'], ascending=True).reset_index(drop=True)
    report_harmonics = report_harmonics.sort_values(by=['harmonic_lf'], ascending=True).reset_index(drop=True)
    # Make node_sn, st_count, lt_count, total_count column a int
    new_harmonics['node_sn'] = new_harmonics['node_sn'].astype(int)
    new_harmonics['st_count'] = new_harmonics['st_count'].astype(int)
    new_harmonics['lt_count'] = new_harmonics['lt_count'].astype(int)
    new_harmonics['total_count'] = new_harmonics['total_count'].astype(int)
    report_harmonics['node_sn'] = report_harmonics['node_sn'].astype(int)
    report_harmonics['st_count'] = report_harmonics['st_count'].astype(int)
    report_harmonics['lt_count'] = report_harmonics['lt_count'].astype(int)
    report_harmonics['total_count'] = report_harmonics['total_count'].astype(int)
    # Insert to Staging DB
    report_contents = insert_hat_report(report_date, hat_type, new_harmonics, report_harmonics, '/internal/staging', api_token)
    # # Insert to Production DB
    report_contents = insert_hat_report(report_date, hat_type, new_harmonics, report_harmonics, '/internal', api_token)
    logger.success('HAT report for {} created\n'.format(report_date))
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
    log_dir_path = '{}/.backfill/{}/'.format(home_dir, 'hat_report')
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
    else:
        process_logger.error('No locations found')
        sys.exit(1)

    # Generate date list
    report_dates = generate_dates(start_date, end_date)
    # Loop through report dates
    for report_date in report_dates:
        backfill_hat_report(locations_df, report_date, report_type.lower())
