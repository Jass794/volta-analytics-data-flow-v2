from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from dags.reports.utils.common import *
from dags.reports.utils.trending import *
from loguru import logger
import datetime as dt
import notifiers
import smtplib
import sys
import os




@logger.catch
# Trending Report
def trending_report(locations_df, api_token):
    # Generate utc report date
    report_date = (dt.datetime.utcnow() - dt.timedelta(days=1)).date()
    report_date = str(report_date.strftime('%Y-%m-%d'))
    # Loop through dates
    logger.info('Trending Report Date: {}'.format(report_date))
    # Final results for report date
    final_frame = pd.DataFrame(
        columns=[
            'customer_name', 'node_sn', 'location_name', 'facility_name',
            'parameter', 'lt_avg', 'st_avg', 'change', 'change_threshold',
            'absolute_threshold', 'location_node_id', 'facility_id', 'customer_id'
        ]
    )
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
        # Location_details
        location_details = {
            'node_sn': node_sn,
            'location_node_id': location_node_id,
            'facility_id': facility_id,
            'customer_id': customer_id,
            'customer_name': customer_name,
            'location_name': location_name,
            'facility_name': facility_name
        }
        # Create log_name
        log_name = str(location_details['node_sn']) + ' - ' + location_details['location_name'] + ' - ' + location_details['facility_name']
        logger.info('Equipment: {}'.format(log_name))
        # Get 60 day average frame
        avg_frame = get_trending_df(api_token, location_node_id, report_date)
        if avg_frame.empty:
            continue
        else:
            # Process the frame for trending patterns
            processed_results = process_trending_frame(avg_frame, location_details)
            # Create dataframe of final results
            processed_trend_df = pd.DataFrame.from_records(processed_results)
            if not processed_trend_df.empty:
                # Concat final_frame and processed_trend_df
                final_frame = pd.concat([final_frame, processed_trend_df], ignore_index=True)
                # Reset index
                final_frame = final_frame.reset_index(drop=True)
    # Drop row where absolute change < Threshold
    final_frame = final_frame[final_frame['change'].abs() >= final_frame['change_threshold']]
    # Sort by parameter and round change to 2 decimal places
    final_frame = final_frame.sort_values(by='change', key=abs, ascending=False).round(2)
    # Reset index
    final_frame = final_frame.reset_index(drop=True)
    # Remove any row present in previous reports
    previous_reports = get_previous_trending_reports(report_date, api_token)
    if previous_reports.empty:
        pass
    else:
        # Drop duplicates based on location_node_id and parameter
        previous_reports = previous_reports.drop_duplicates(subset=['location_node_id', 'parameter'])
        previous_reports = previous_reports.reset_index(drop=True)
        # remove rows from final_frame present in previous_reports based on location_node_id and parameter
        final_frame = final_frame[~final_frame.set_index(['location_node_id', 'parameter']).index.isin(previous_reports.set_index(['location_node_id', 'parameter']).index)].reset_index(drop=True)
        # Round lt_avg, st_avg, change to 2 decimal places
        final_frame['lt_avg'] = final_frame['lt_avg'].astype(float).round(2)
        final_frame['st_avg'] = final_frame['st_avg'].astype(float).round(2)
        final_frame['change'] = final_frame['change'].astype(float).round(2)
    # Result dict
    result_dict = {
        'email_time': str(dt.datetime.utcnow()),
        'report_date': report_date,
        'configs': {
            'threshold_dict': threshold_dict,
            'lt_avg_days': LT_AVG_DAYS,
        },
        'trending_report': final_frame.to_dict()
    }
    # Insert data into staging and production
    staging_result = insert_trending_report(result_dict, report_date, '/internal/staging', api_token)
    prod_result = insert_trending_report(result_dict, report_date, '/internal', api_token)
    logger.success('Trending Report Completed')
    return prod_result


@logger.catch
# Email Trending report
def email_trending_report(processed_result, email_app_pass):
    logger.info('Emailing Trending Report')
    # Set report date
    report_date = processed_result['report_date']
    report_date = dt.datetime.strptime(report_date, '%Y-%m-%d')

    # Create Trending dataframe
    trending_frame = pd.DataFrame.from_dict(processed_result['trending_report'])

    # Only keep required columns for email
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name',
        'parameter', 'lt_avg', 'st_avg', 'change', 'change_threshold'
    ]
    trending_frame = trending_frame[email_columns]
    # Sort by absolute change
    trending_frame = trending_frame.sort_values(by='change', key=abs, ascending=False).reset_index(drop=True)
    # Change Parameter names
    trending_frame.loc[(trending_frame.parameter == 'current'), 'parameter'] = 'Current'
    trending_frame.loc[(trending_frame.parameter == 'voltage'), 'parameter'] = 'Voltage'
    trending_frame.loc[(trending_frame.parameter == 'voltage_ll'), 'parameter'] = 'Voltage L-L'
    trending_frame.loc[(trending_frame.parameter == 'current_imbalance'), 'parameter'] = 'Current Imbalance'
    trending_frame.loc[(trending_frame.parameter == 'voltage_imbalance'), 'parameter'] = 'Voltage Imbalance'
    trending_frame.loc[(trending_frame.parameter == 'voltage_imbalance_ll'), 'parameter'] = 'Voltage Imbalance L-L'
    trending_frame.loc[(trending_frame.parameter == 'current_thd'), 'parameter'] = 'Current THD'
    trending_frame.loc[(trending_frame.parameter == 'voltage_thd_ll'), 'parameter'] = 'Voltage THD L-L'

    # Rename columns for email
    trending_frame = trending_frame.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'parameter': 'Parameter',
            'lt_avg': 'LT Avg',
            'st_avg': 'ST Avg',
            'change': '% Change',
            'change_threshold': ' % Change Threshold'
        }
    )

    # Split into increasing and decreasing frames based on % change
    increasing_frame = trending_frame[trending_frame['% Change'] > 0]
    decreasing_frame = trending_frame[trending_frame['% Change'] < 0]
    # Sort based on % Change as descending
    increasing_frame = increasing_frame.sort_values(by='% Change', ascending=False).reset_index(drop=True)
    decreasing_frame = decreasing_frame.sort_values(by='% Change', ascending=True).reset_index(drop=True)

    # SEND EMAIL
    sender_email = 'notifications@voltainsite.com'
    if '-d' in sys.argv:
        receivers = ['analytics-data-flow-errors@voltainsite.com']
    else:
        receivers = ['analytics-reports@voltainsite.com']
    subject = 'Daily Trending Report for %s' % (report_date.strftime("%B %d, %Y"))

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
                <b>Increasing Trends:</b><br>
                {0}
                <p>
                <p>
                <b>Decreasing Trends:</b><br>
                {1}
                <p>
            </body>
        </html>
    """.format(
        increasing_frame.to_html(index=False),
        decreasing_frame.to_html(index=False)
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
    logger.success('Trending Report Email Sent')
    return 0


def run_trending_report(env='staging'):
    if env == 'production':
        server = 'production'
    else:
        server = 'staging'

    load_dotenv(f'{os.getcwd()}/.{server}.env')

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
            params = {
                "attachments": [log_filepath],
                "username": "notifications@voltainsite.com",
                "password": email_app_pass,
                "to": "analytics-data-flow-errors@voltainsite.com",
                "subject": "Error - Trending Report",
            }
            notifier = notifiers.get_notifier("gmail")
            notifier.notify(message="Log File attached!", **params)
        return 0

    # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}.txt'.format(utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.logs/daily/{}/'.format(home_dir, 'trending_report')
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
    else:
        process_logger = process_logger.add(
            log_dir_path + log_file_name, retention=logs_retention, enqueue=True,
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} {message}",
            compression=email_log_on_error
        )

    # Get Locations
    locations_df = get_node_df(api_token)
    if not locations_df.empty:
        # Filter locations where Events Report needs to run
        locations_df = locations_df[
            (locations_df.node_details.apply(lambda x: x['deploymentIssue'] == False)) &
            (locations_df.node_details.apply(lambda x: x['currentDeploymentStatus'] == 'Deployed'))
            ]
        # Remove coustomer from scan
        locations_df = locations_df[
            (~locations_df['customer_code'].isin(['atha', 'lab', 'cestx', 'ice']))
        ]
        trend_report_dict = trending_report(locations_df,api_token)
        # Email HAT Report
        email_trending_report(trend_report_dict, email_app_pass)
    else:
        process_logger.error('No locations found')
        sys.exit(1)


if __name__ == "__main__":
    run_trending_report()
