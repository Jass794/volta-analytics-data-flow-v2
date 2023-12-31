from email.mime.multipart import MIMEMultipart

import pandas as pd

from reports.utils.harmonics import *
from email.mime.text import MIMEText
from reports.utils.common import *
from loguru import logger
from dotenv import load_dotenv
import datetime as dt
import notifiers
import smtplib
import sys
import os


change_impace_threshold = 50





# Apply harmonic filter on hat report
def apply_harmonics_filter_v2(report_hat_frame):
    # Reset Index
    report_harmonics = report_hat_frame.reset_index(drop=True)
    # LT Avg greater than 0.01
    report_harmonics = report_harmonics[report_harmonics['lt_avg'] > 0.01]
    # Keep LF only if absolute impact or change is > 50
    report_harmonics_upper = report_harmonics[report_harmonics['change'] > 50]
    report_harmonics_lower = report_harmonics[report_harmonics['change'] < -50]

    report_harmonics_upper = report_harmonics_upper[report_harmonics_upper['st_avg'] > report_harmonics_upper['lt_harmonic_max_lf_value']]
    report_harmonics_lower = report_harmonics_lower[report_harmonics_lower['st_avg'] < report_harmonics_lower['lt_harmonic_min_lf_value']]

    report_harmonics = pd.concat([report_harmonics_upper, report_harmonics_lower], ignore_index=True)

    return report_harmonics.reset_index(drop=True)


# Remove harmonics from the harmonics_df within tolerance
def tolerance_harmonics_removal_v2(new_df, old_df):
    new_df = new_df.reset_index(drop=True)
    old_df = old_df.reset_index(drop=True)

    # Remove duplicates from old_df
    old_df = old_df.drop_duplicates(subset=['location_node_id', 'harmonic_lf'], keep='last')

    # Sort new_df by harmonic_lf
    new_df = new_df.sort_values(by='harmonic_lf').reset_index(drop=True)

    final_hat_records = []
    # Loop through each Equipment
    location_list = new_df['location_node_id'].unique()
    for location in location_list:
        equipment_new_hat_frame = new_df[new_df['location_node_id'] == location]
        equipment_old_hat_frame = old_df[old_df['location_node_id'] == location]
        # Loop through each harmonic_lf to remove next harmonics if within tolerance
        for _, row in equipment_new_hat_frame.iterrows():
            running_harmonic_lf = row['harmonic_lf']
            tolerance = get_lf_tolerance(running_harmonic_lf)
            # Find all rows with harmonic_lf in tolerance range
            old_df_tolerance = equipment_old_hat_frame[
                (equipment_old_hat_frame['harmonic_lf'] >= running_harmonic_lf - tolerance) &
                (equipment_old_hat_frame['harmonic_lf'] < (running_harmonic_lf + tolerance))
                ]
            # If old_df_tolerance is empty add row to new frame
            if old_df_tolerance.empty:
                final_hat_records.append(row)
    # Create frame from records
    final_hat_frame = pd.DataFrame(final_hat_records)
    if final_hat_frame.empty:
        return final_hat_frame
    else:
        # Sort final_hat_frame by harmonic_lf
        final_hat_frame = final_hat_frame.sort_values(by='harmonic_lf', ascending=True).reset_index(drop=True)
        # Reset index
        final_hat_frame = final_hat_frame.reset_index(drop=True)
        return final_hat_frame


@logger.catch
# HAT Report
def hat_report(location_df, report_date, report_type, api_token):
    logger.info('{} HAT Report Initiated for {}'.format(report_type.title(), report_date))
    report_hat_frame = pd.DataFrame()
    # Hat Report type
    hat_type = '{}_hat_report_v2'.format(report_type)
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
            # Add equipment name and location_node_id to df
            eqpt_hat_report['node_sn'] = node_sn
            eqpt_hat_report['location_name'] = location_name
            eqpt_hat_report['facility_name'] = facility_name
            eqpt_hat_report['facility_id'] = facility_id
            eqpt_hat_report['customer_name'] = customer
            eqpt_hat_report['customer_id'] = customer_id
            eqpt_hat_report['location_node_id'] = location_node_id
            eqpt_hat_report['starter'] = row['starter'].values[0]

            # Round harmonic_lf to 2 decimal places
            eqpt_hat_report['harmonic_lf'] = eqpt_hat_report['harmonic_lf']
            # Filter to reduce harmonics
            eqpt_hat_report = apply_harmonics_filter_v2(eqpt_hat_report)

            eqpt_hat_report = eqpt_hat_report.drop_duplicates(subset='harmonic_lf')
            # Add equipment to report_hat_frame
            report_hat_frame = pd.concat([report_hat_frame, eqpt_hat_report], ignore_index=True)
    if report_hat_frame.empty:
        logger.warning('No Daily scans found on {}'.format(report_date))
        return 0
    else:
        # Remove mirror harmonics from 1-2 LF
        report_harmonics = mirror_harmonics_removal(report_hat_frame)
        # Get Previous hat reports
        previous_hat_reports = get_previous_reports(report_date, hat_type, api_token)

        if previous_hat_reports.empty:
            logger.warning('Empty previous hat report email on {}'.format(report_date))
            new_harmonics = report_harmonics
        else:
            # Remove duplicates on location_node_id and harmonics_lf
            previous_hat_reports = previous_hat_reports.drop_duplicates(subset=['location_node_id', 'harmonic_lf'])
            # Remove harmonics within tolerance using previous_hat_reports
            new_harmonics = tolerance_harmonics_removal_v2(report_harmonics, previous_hat_reports)
            # Remove mirror harmonics in 1-2 LF range
            new_harmonics = mirror_harmonics_removal(new_harmonics)
    pd.set_option('display.max_rows', None)
    # Order columns in new_harmonics and report_harmonics
    ordered_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 'harmonic_lf',
        'lt_avg', 'st_avg', 'change', 'lt_count', 'st_count',
        'total_count', 'location_node_id', 'facility_id', 'customer_id',
        'lt_harmonic_max_lf_value', 'lt_harmonic_min_lf_value',
        'lt_harmonic_max_lf_value_date', 'lt_harmonic_min_lf_value_date', 'starter', 'scan_period_type'
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

    # Insert to Production DB
    report_contents = insert_hat_report(report_date, hat_type, new_harmonics, report_harmonics, '/internal', api_token)
    # Insert to Staging DB
    report_contents = insert_hat_report(report_date, hat_type, new_harmonics, report_harmonics, '/internal/staging', api_token)
    # Add message to report_contents
    report_contents['message'] = 'Daily Report Successfully Generated'
    logger.success('HAT report for {} created\n'.format(report_date))
    return report_contents


@logger.catch
# Email report
def email_hat_report(processed_dict, hat_type, email_app_pass):
    # If report message is empty, return
    if processed_dict['message'] == 'No Daily scans found':
        logger.error('No Daily scans found on {}'.format(processed_dict['report_date']))
    # Set report date
    report_date = dt.datetime.strptime(processed_dict['report_date'], '%Y-%m-%d').date()
    # Get new harmonics from processed_dict
    new_harmonics = pd.DataFrame(processed_dict['new_harmonics'])
    # Order by node_sn and harmonic_lf
    new_harmonics = new_harmonics.sort_values(by=['node_sn', 'harmonic_lf'], ascending=True).reset_index(drop=True)
    # Order columns for display
    ordered_columns = [
        'node_sn', 'location_name', 'facility_name', 'harmonic_lf',
        'lt_avg', 'st_avg', 'change', 'lt_count', 'st_count',
        'total_count', 'lt_harmonic_max_lf_value', 'lt_harmonic_min_lf_value',
        'lt_harmonic_max_lf_value_date', 'lt_harmonic_min_lf_value_date',
        'starter', 'scan_period_type',
        'customer_name'
    ]
    new_harmonics = new_harmonics[ordered_columns]
    # Rename location_name to Equipment
    renamed_columns = {
        'node_sn': ' Node Serial',
        'location_name': 'Equipment',
        'facility_name': 'Facility',
        'customer_name': 'Customer'
    }
    new_harmonics = new_harmonics.rename(columns=renamed_columns)

    # SEND EMAIL
    EMAIL = 'notifications@voltainsite.com'
    APP_PASSWORD = str(email_app_pass)
    RECEIVER = ['analytics-reports@voltainsite.com']

    SUBJECT = 'Daily {} HAT Report V2 for {}'.format(hat_type, report_date.strftime("%B %d, %Y"))

    logger.info(SUBJECT)
    TABLES = """\
        <html>
            <head>
                <style type="text/css">
                .column table {{border:1px solid black; border-collapse:collapse; text-align:center;}}
                .column th,.column td {{padding: 5px;text-align:center;}}
                .row {{display: flex;  margin-left:-5px;  margin-right:-5px;}}
                .column {{min-width:400px;  padding: 5px;}}
                .heading {{text-align:center;}}
                </style>
            </head>
            <body>
                <div class="row">
                <div class="column">
                <h2 class="heading">New Harmonics:</h2>
                    {0}
                </div>
                </div>
            </body>
        </html>
    """.format(
        new_harmonics.to_html(index=False)
    )

    message = MIMEMultipart()
    part_1 = MIMEText(TABLES, 'html')
    message.attach(part_1)
    message['From'] = EMAIL
    message['To'] = ", ".join(RECEIVER)
    message['Subject'] = SUBJECT

    mail_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    mail_server.ehlo()
    mail_server.login(EMAIL, APP_PASSWORD)
    mail_server.sendmail(EMAIL, RECEIVER, message.as_string())
    mail_server.close()
    return 0


def generate_hat_report(report_type, env='staging', debug=False):
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
                "subject": "Error - HAT Report",
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
    log_dir_path = f'{home_dir}/.logs/daily/hat_report_v2/'
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
    else:
        process_logger.error('No locations found')
        sys.exit(1)

    # Get UTC date
    utc_date = str((dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d'))
    hat_report_dict = hat_report(locations_df, utc_date, report_type.lower(),api_token)
    # Email HAT Report
    email_hat_report(hat_report_dict, report_type.title(), email_app_pass)


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
    if '-d' in args:
        debug=True

    generate_hat_report(report_type,debug=debug)

