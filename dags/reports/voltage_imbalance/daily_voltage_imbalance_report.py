from email.mime.multipart import MIMEMultipart
from utils.trending import *
from email.mime.text import MIMEText
from utils.common import *
from loguru import logger
import datetime as dt
import notifiers
import smtplib
import sys
import os


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
            "subject": "Error - Voltage Imbalance L-L Report",
        }
        notifier = notifiers.get_notifier("gmail")
        notifier.notify(message="Log File attached!", **params)
    return 0

@logger.catch
#Voltage Imbalance Report
def voltage_imbalance_report(locations_df):
    # Generate utc report date
    report_date = (dt.datetime.utcnow() - dt.timedelta(days=1)).date()
    report_date = str(report_date.strftime('%Y-%m-%d'))

    logger.info('Trending Report Date: {}'.format(report_date))

    final_frame = pd.DataFrame(
        columns=[
            'customer_name', 'node_sn', 'location_name', 'facility_name', 'st_avg', 
            'absolute_threshold','location_node_id', 'facility_id', 'customer_id'
        ]
    )

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
        eq_type = str(row['node_configs'].values[0]['eq_type'])
        eq_type_sub = str(row['node_configs'].values[0]['eq_type_sub'])

        # Location_details
        location_details = {
            'node_sn': node_sn,
            'location_node_id': location_node_id,
            'facility_id': facility_id,
            'customer_id': customer_id,
            'customer_name': customer_name,
            'location_name': location_name,
            'facility_name': facility_name,
            'eq_type': eq_type,
            'eq_type_sub': eq_type_sub
        }

        log_name = str(location_details['node_sn']) + ' - ' + location_details['location_name'] + ' - ' + location_details['facility_name']
        logger.info('Equipment: {}'.format(log_name))

        avg_frame = get_trending_df(api_token, location_node_id, report_date)
        if avg_frame.empty:
            continue
        else:
            imbalance_trend_results = process_imbalance_frame(avg_frame,location_details)
            imbalance_trend_df= pd.DataFrame.from_records(imbalance_trend_results)
            if not imbalance_trend_df.empty:
                # Concat final_frame and processed_trend_df
                final_frame = pd.concat([final_frame, imbalance_trend_df], ignore_index=True)
                # Reset index
                final_frame = final_frame.reset_index(drop=True)
    # Drop row where absolute change < Threshold
    final_frame = final_frame[final_frame['st_avg'].abs() >= final_frame['absolute_threshold']]
    # Sort by parameter and round change to 2 decimal places
    final_frame = final_frame.sort_values(by='st_avg', key=abs, ascending=False).round(2)
    # Reset index
    final_frame = final_frame.reset_index(drop=True)
    final_frame['st_avg'] = final_frame['st_avg'].astype(float).round(2)
    # Result dict
    result_dict = {
        'email_time': str(dt.datetime.utcnow()),
        'report_date': report_date,
        'voltage_imbalance_report': final_frame.to_dict()
    }
    # Insert data into staging and production
    staging_result = insert_voltage_imbalance_report(result_dict, report_date, '/internal/staging', api_token)
    prod_result = insert_voltage_imbalance_report(result_dict, report_date, '/internal', api_token)
    logger.success('Trending Report Completed')
    return prod_result

@logger.catch
#Email voltage imbalance report
def email_voltage_imbalance_report(processed_results):
    logger.info('Emailing Voltage Imbalance Report')

    report_date = dt.datetime.strptime(processed_results['report_date'],'%Y-%m-%d')

    voltage_imbalance_frame = pd.DataFrame.from_dict(processed_results['voltage_imbalance_report'])

    # Only keep required columns for email
    email_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 
        'st_avg', 'absolute_threshold'
    ]
    voltage_imbalance_frame = voltage_imbalance_frame[email_columns]

    # Rename columns for email
    voltage_imbalance_frame = voltage_imbalance_frame.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'st_avg': 'ST Avg',
            'absolute_threshold': 'Absolute Threshold'
        }
    )

    # SEND EMAIL
    sender_email = 'notifications@voltainsite.com'
    if '-d' in sys.argv:
        receivers = ['analytics-data-flow-errors@voltainsite.com']
    else:
        receivers = ['analytics-reports@voltainsite.com']
    subject = 'Voltage Imbalance L-L Report for %s' % (report_date.strftime("%B %d, %Y"))
    
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
                <b>Voltage Imbalance L-L:</b><br>
                {0}
                <p>
            </body>
        </html>
    """.format(
        voltage_imbalance_frame.to_html(index=False)
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
    logger.success('Voltage Imbalance Report Email Sent')
    return 0

# Insert report to database
def insert_voltage_imbalance_report(report_content, report_date, server, analytics_api_token):
    # INSERT INTO DATABASE
    if server == '/internal/staging':
        server_type = 'Staging'
    else:
        server_type = 'Production'
    # Get email notification for hat report
    notify_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/reports/voltage_imbalance_report/".format(server)
    headers = {'Authorization': 'Bearer {}'.format(analytics_api_token)}
    # Email are send a UTC day ahead of the report date
    email_time_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    query_params = {
        'given_date': str(email_time_date.date())
    }
    response = requests.get(url=notify_url, params=query_params, headers=headers)
    response.raise_for_status()
    if response.status_code == 200:
        response_dict = response.json()['content']
        time_stamp = response_dict['time']
        report_content['email_time'] =  response_dict['content']['email_time']
        # Update database with put request
        put_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/".format(server)
        put_body = {
            'time': time_stamp,
            'type': 'voltage_imbalance_report',
            'location_node_id': 'report',
            'content': json.dumps(report_content),
            'notified': True
        }
        response = requests.put(url=put_url, json=put_body, headers=headers)
        response.raise_for_status()
    elif response.status_code == 204:
        # Email are send a UTC day ahead of the report date
        email_time_date = dt.datetime.strptime(report_date, '%Y-%m-%d') + dt.timedelta(days=1)
        email_time = str(email_time_date.replace(hour=1, minute=30, second=0, microsecond=0))
        report_content['email_time'] = email_time
        # Add report to database
        post_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/".format(server)
        post_body = {
            'time': email_time,
            'type': 'voltage_imbalance_report',
            'location_node_id': 'report',
            'content': json.dumps(report_content),
            'notified': True
        }
        response = requests.post(url=post_url, json=post_body, headers=headers)
        response.raise_for_status()
    else:
        # Raise error
        raise Exception('Error Inserting to {}: {}'.format(server_type, response.status_code))
    return report_content
    
#Process nodes with absolute voltage imbalance L-L > 5% (No DC, V1, or V2)
def process_imbalance_frame(trending_df,location_details):
    ABSOLUTE_VOLTAGE_IMBALANCE_LL_THRESHOLD = 5

    processed_dict = []

    if not (location_details['eq_type'] == 'dc' or location_details['eq_type_sub'] == 'v1' or location_details['eq_type_sub'] == 'v2'):
        st_avg_days = -ST_AVG_DAYS
        st_avg = trending_df['voltage_imbalance_ll'].iloc[st_avg_days:].mean()

        calcs = {
                'facility_id': location_details['facility_id'],
                'customer_id': location_details['customer_id'],
                'node_sn': location_details['node_sn'],
                'customer_name': location_details['customer_name'],
                'location_name': location_details['location_name'],
                'facility_name': location_details['facility_name'],
                'st_avg': st_avg,
                'absolute_threshold': ABSOLUTE_VOLTAGE_IMBALANCE_LL_THRESHOLD,
                'location_node_id': location_details['location_node_id']
            }
        processed_dict.append(calcs)

    return processed_dict
    
if __name__ == "__main__":
     # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}.txt'.format(utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.logs/daily/{}/'.format(home_dir, 'voltage_imbalance_report')
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
        imbalance_report_dict = voltage_imbalance_report(locations_df)
        # Email HAT Report
        email_voltage_imbalance_report(imbalance_report_dict)
    else:
        process_logger.error('No locations found')
        sys.exit(1)
