from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

from utils.common import *
from utils.deployment import *
from loguru import logger
import datetime as dt
import notifiers
import smtplib
import sys
import os
import boto3
import io
import math
from statistics import mean
import pandas as pd
from multiprocessing import Pool
import multiprocessing
from itertools import repeat
from functools import partial
import numpy as np
import time

# load env file
load_dotenv("production.env")

# load env file
load_dotenv("production.env")

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
        params = {
            "attachments": [log_filepath],
            "username": "notifications@voltainsite.com",
            "password": email_app_pass,
            "to": "analytics-data-flow-errors@voltainsite.com",
            "subject": "Error - Deployment Report",
        }
        notifier = notifiers.get_notifier("gmail")
        notifier.notify(message="Log File attached!", **params)
    return 0


@logger.catch
# Email voltage imbalance report
def email_deployment_report(processed_results):
    logger.info('Emailing Deployment Report')

    report_date = dt.datetime.strptime(processed_results['report_date'], '%Y-%m-%d')

    voltage_imbalance_frame = pd.DataFrame.from_dict(processed_results['voltage_imbalance_report'])
    not_centered_zero_frame = pd.DataFrame.from_dict(processed_results['not_centered_zero_report'])
    # Only keep required columns for email

    email_columns = [
        'customer_name', 'facility_name', 'location_name', 'node_sn',
        'file_timestamp', 'v_noise', 'i_noise', 'avg_voltage', 'avg_current'
    ]
    not_centered_zero_frame = not_centered_zero_frame[email_columns]

    not_centered_zero_frame = not_centered_zero_frame.rename(
        columns={
            'customer_name': 'Customer',
            'node_sn': 'Node Serial',
            'location_name': 'Equipment',
            'facility_name': 'Facility',
            'file_timestamp': 'File Timestamp',
            'v_noise': 'Voltage Noise Threshold',
            'i_noise': "Current Noise Threshold",
            "avg_voltage": "Average Voltage",
            'avg_current': "Average Current"
        }
    )

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
    subject = 'Deployment Report for %s' % (report_date.strftime("%B %d, %Y"))

    tables = """
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
                <p>
                <b>Not Centered on Zero</b><br>
                {1}
                <p>
            </body>
        </html>
    """.format(
        voltage_imbalance_frame.to_html(index=False),
        not_centered_zero_frame.to_html(index=False)
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
    logger.success('Deployment Report Email Sent')
    return 0


@logger.catch
# Deployment Report
def deployment_report(locations_df):
    # Generate utc report date
    report_date = (dt.datetime.utcnow() - dt.timedelta(days=1)).date()
    report_date = str(report_date.strftime('%Y-%m-%d'))
    # Loop through dates
    logger.info('Deployment Report Date: {}'.format(report_date))

    voltage_imbalance_frame = pd.DataFrame(
        columns=[
            'customer_name', 'node_sn', 'location_name', 'facility_name', 'st_avg',
            'absolute_threshold', 'location_node_id', 'facility_id', 'customer_id'
        ]
    )
    not_centered_zero_frame = pd.DataFrame(
        columns=[
            'customer_name', 'facility_name', 'location_name', 'location_node_id', 'node_sn',
            's3_location', 'file_timestamp', 'v_noise', 'i_noise', 'avg_voltage', 'avg_current'
        ]
    )

    #locations_df = locations_df[locations_df['facility_name'] == 'Brookneal, VA']
    #locations_df = locations_df[locations_df['node_sn'] == 21187]
    node_arr = locations_df.to_dict('records')

    with Pool() as pool:
        vi_records = pool.map(partial(process_vi_row,report_date = report_date),node_arr)
        node_arr = pool.map(partial(get_recent_esa_waveform, api_token),node_arr)
        ncz_records = pool.map(get_center_of_waveform,node_arr)
        cm_records = pool.map(get_zero_crossings,node_arr)

    vi_records = [x for x in vi_records if x is not None]
    ncz_records = [x for x in ncz_records if x is not None]
    cm_records = [x for x in cm_records if x is not None]

    voltage_imbalance_frame = pd.DataFrame.from_records(vi_records)
    not_centered_zero_frame = pd.DataFrame.from_records(ncz_records)
    channel_mapping_frame = pd.DataFrame.from_records(cm_records)

    if not voltage_imbalance_frame.empty:
        # Drop row where absolute change < Threshold
        voltage_imbalance_frame = voltage_imbalance_frame[voltage_imbalance_frame['st_avg'].abs() >= voltage_imbalance_frame['absolute_threshold']]
        # Sort by parameter and round change to 2 decimal places
        voltage_imbalance_frame = voltage_imbalance_frame.sort_values(by='st_avg', key=abs, ascending=False).round(2)
        # Reset index
        voltage_imbalance_frame = voltage_imbalance_frame.reset_index(drop=True)
        voltage_imbalance_frame['st_avg'] = voltage_imbalance_frame['st_avg'].astype(float).round(2)

    if not not_centered_zero_frame.empty:
        #Drop rows where the average voltage and average current are less than noise
        not_centered_zero_frame = not_centered_zero_frame[
            (not_centered_zero_frame['avg_voltage'].abs() > not_centered_zero_frame['v_noise'])
            |
            (not_centered_zero_frame['avg_current'].abs() > not_centered_zero_frame['i_noise'])
            ]
        not_centered_zero_frame = not_centered_zero_frame.reset_index(drop=True)
        not_centered_zero_frame['avg_voltage'] = not_centered_zero_frame['avg_voltage'].astype(float).round(5)
        not_centered_zero_frame['avg_current'] = not_centered_zero_frame['avg_current'].astype(float).round(5)

    if not channel_mapping_frame.empty:
        #Keep rows that are flagged or have an avg crossing > 1/4 period
        channel_mapping_frame = channel_mapping_frame[
        #    (channel_mapping_frame['flag_a'] == True)|
        #    (channel_mapping_frame['flag_b'] == True)|
        #    (channel_mapping_frame['flag_c'] == True)|
            (channel_mapping_frame['a_avg_crossings_modia'] > (channel_mapping_frame['ia_avg_period'] / 4))|
            (channel_mapping_frame['b_avg_crossings_modib'] > (channel_mapping_frame['ib_avg_period'] / 4))|
            (channel_mapping_frame['c_avg_crossings_modic'] > (channel_mapping_frame['ic_avg_period'] / 4))
            ]
        channel_mapping_frame = channel_mapping_frame.sort_values(by=['a_avg_crossings_modia','b_avg_crossings_modib','c_avg_crossings_modic'], ascending=False)
        channel_mapping_frame = channel_mapping_frame.reset_index(drop=True)
        channel_mapping_frame['a_avg_crossings_modia'] = channel_mapping_frame['a_avg_crossings_modia'].astype(float).round(5)
        channel_mapping_frame['b_avg_crossings_modib'] = channel_mapping_frame['b_avg_crossings_modib'].astype(float).round(5)
        channel_mapping_frame['c_avg_crossings_modic'] = channel_mapping_frame['c_avg_crossings_modic'].astype(float).round(5)
        channel_mapping_frame['ia_avg_period'] = channel_mapping_frame['ia_avg_period'].astype(float).round(5)
        channel_mapping_frame['ib_avg_period'] = channel_mapping_frame['ib_avg_period'].astype(float).round(5)
        channel_mapping_frame['ic_avg_period'] = channel_mapping_frame['ic_avg_period'].astype(float).round(5)
        channel_mapping_frame['a_phase_deg'] = channel_mapping_frame['a_phase_deg'].astype(float).round(5)
        channel_mapping_frame['b_phase_deg'] = channel_mapping_frame['b_phase_deg'].astype(float).round(5)
        channel_mapping_frame['c_phase_deg'] = channel_mapping_frame['c_phase_deg'].astype(float).round(5)

    # Result dict
    result_dict = {
        'email_time': str(dt.datetime.utcnow()),
        'report_date': report_date,
        'voltage_imbalance_report': voltage_imbalance_frame.to_dict(),
        'not_centered_zero_report': not_centered_zero_frame.to_dict(),
        'channel_mapping_report': channel_mapping_frame.to_dict()
    }

    # Insert data into staging and production
    staging_result = insert_deployment_report(result_dict, report_date, '/internal/staging', api_token)
    # prod_result = insert_deployment_report(result_dict, report_date, '/internal', api_token
    return staging_result
 
def process_vi_row(row, report_date):
    node_sn = int(row['node_sn'])
    location_name = str(row['location_name'])
    facility_name = str(row['facility_name'])
    facility_id = str(row['facility_id'])
    customer_name = str(row['customer_name'])
    customer_id = str(row['customer_id'])
    eq_type = str(row['node_configs']['eq_type'])
    eq_type_sub = str(row['node_configs']['eq_type_sub'])
    i_noise = float(row['node_configs']['i_noise'])
    v_noise = float(row['node_configs']['v_noise'])
    location_node_id = str(row['location_node_id'])

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
        'eq_type_sub': eq_type_sub,
        'i_noise': i_noise,
        'v_noise': v_noise
    }
    logger.info('{} Processing Voltage Imbalance for Node: {}'.format(multiprocessing.current_process().name, node_sn))

    avg_frame = get_trending_df(api_token, location_node_id, report_date)
    imbalance_trend_results = process_voltage_imbalance_frame(avg_frame, location_details)
    return imbalance_trend_results

def get_zero_crossings(row):

    eq_type = str(row['node_configs']['eq_type'])

    if 'waveform' not in row or eq_type == 'dc':
        return None

    node_sn = int(row['node_sn'])
    location_name = str(row['location_name'])
    facility_name = str(row['facility_name'])
    customer_name = str(row['customer_name'])
    location_node_id = str(row['location_node_id'])
    file_timestamp = str(row['file_timestamp'])
    s3_location = str(row['s3_location'])
    eq_type_sub = str(row['node_configs']['eq_type_sub'])
    channel_map = str(row['channel_map'])
    vfd_driven = bool(row['vfd_driven'])
    work_cycle = bool(row['work_cycle'])
    starter = str(row['starter'])

    logger.info('{} Getting zero crossings for Node: {}'.format(multiprocessing.current_process().name,node_sn))

    waveform = row['waveform']
    waveform = pd.DataFrame().from_dict(waveform)
    waveform[['ia_moving','ib_moving','ic_moving','va_moving','vb_moving','vc_moving']] = waveform[['ia','ib','ic','va','vb','vc']].rolling(256).mean()

    waveform = waveform.dropna(thresh=8) #Drop first 255 rows where moving avg is na

    #Finds sign of each point (+1.0,0.0,-1.0)
    waveform[['ia_sign','ib_sign','ic_sign','va_sign','vb_sign','vc_sign']] = np.sign(waveform[['ia_moving','ib_moving','ic_moving','va_moving','vb_moving','vc_moving']])

    #Calculate zero crossing
    #No crossing => 0, high->low => >0 (+2), low->high => <0 (-2)
    #The value is on the index of the point just BEFORE the crossing
    waveform[['ia_crossing','ib_crossing','ic_crossing','va_crossing','vb_crossing','vc_crossing']] = waveform[['ia_sign','ib_sign','ic_sign','va_sign','vb_sign','vc_sign']].diff(-1)

    #Drop last row which is NaN due to .diff()
    waveform = waveform.iloc[:-1]

    # Phase operations

    ia_avg_period = waveform.loc[waveform['ia_crossing'] > 0, 'time'].diff().dropna().mean()
    va_avg_period = waveform.loc[waveform['va_crossing'] > 0, 'time'].diff().dropna().mean()
    ib_avg_period = waveform.loc[waveform['ib_crossing'] > 0, 'time'].diff().dropna().mean()
    vb_avg_period = waveform.loc[waveform['vb_crossing'] > 0, 'time'].diff().dropna().mean()
    ic_avg_period = waveform.loc[waveform['ic_crossing'] > 0, 'time'].diff().dropna().mean()
    vc_avg_period = waveform.loc[waveform['vc_crossing'] > 0, 'time'].diff().dropna().mean()

    ia_crossings = waveform.loc[waveform['ia_crossing'] > 0, 'time'].reset_index(drop=True)
    va_crossings = waveform.loc[waveform['va_crossing'] > 0, 'time'].reset_index(drop=True)
    ib_crossings = waveform.loc[waveform['ib_crossing'] > 0, 'time'].reset_index(drop=True)
    vb_crossings = waveform.loc[waveform['vb_crossing'] > 0, 'time'].reset_index(drop=True)
    ic_crossings = waveform.loc[waveform['ic_crossing'] > 0, 'time'].reset_index(drop=True)
    vc_crossings = waveform.loc[waveform['vc_crossing'] > 0, 'time'].reset_index(drop=True)

    #If the first current crossing happens before the first voltage crossing, drop it
    if va_crossings.size and ia_crossings.size > 1 and ia_crossings.iloc[0] < va_crossings.iloc[0]:
        ia_crossings = ia_crossings.iloc[1:]
    if vb_crossings.size and ib_crossings.size > 1 and ib_crossings.iloc[0] < vb_crossings.iloc[0]:
        ib_crossings = ib_crossings.iloc[1:]
    if vc_crossings.size and ic_crossings.size > 1 and ic_crossings.iloc[0] < vc_crossings.iloc[0]:
        ic_crossings = ic_crossings.iloc[1:]

    if vfd_driven or starter == 'VFD':
        a_crossings = ia_crossings.apply(lambda x: x - va_crossings[va_crossings < x].max())
        b_crossings = ib_crossings.apply(lambda x: x - vb_crossings[vb_crossings < x].max())
        c_crossings = ic_crossings.apply(lambda x: x - vc_crossings[vc_crossings < x].max())
    else:
        a_crossings = ia_crossings - va_crossings
        b_crossings = ib_crossings - vb_crossings
        c_crossings = ic_crossings - vc_crossings

    a_avg_crossings_modva = (a_crossings % va_avg_period).dropna().mean()
    a_avg_crossings_modia = (a_crossings % ib_avg_period).dropna().mean()
    b_avg_crossings_modvb = (b_crossings % vb_avg_period).dropna().mean()
    b_avg_crossings_modib = (b_crossings % ib_avg_period).dropna().mean()
    c_avg_crossings_modvc = (c_crossings % vc_avg_period).dropna().mean()
    c_avg_crossings_modic = (c_crossings % ic_avg_period).dropna().mean()
    
    a_phase_deg = (a_avg_crossings_modia / ia_avg_period) * 360
    b_phase_deg = (b_avg_crossings_modib / ib_avg_period) * 360
    c_phase_deg = (c_avg_crossings_modic / ic_avg_period) * 360

    flag_a = any(a_crossings > ia_avg_period) or any(a_crossings < 0)
    flag_b = any(b_crossings > ib_avg_period) or any(b_crossings < 0)
    flag_c = any(c_crossings > ic_avg_period) or any(c_crossings < 0)

    if eq_type_sub == 'v2':
        ib_avg_period = 0.0
        vb_avg_period = 0.0
        b_avg_crossings_modib = 0.0
        b_avg_crossings_modvb = 0.0
        b_phase_deg = 0.0
        flag_b = False

    if eq_type_sub == 'v1':
        ib_avg_period = 0.0
        vb_avg_period = 0.0
        b_avg_crossings_modib = 0.0
        b_avg_crossings_modvb = 0.0
        b_phase_deg = 0.0
        flag_b = False
        
        ic_avg_period = 0.0
        vc_avg_period = 0.0
        c_avg_crossings_modic = 0.0
        c_avg_crossings_modvc = 0.0
        c_phase_deg = 0.0
        flag_c = False

    # print(node_sn)
    # print(waveform)
    # print(row['file_timestamp'])

    # print(ia_avg_period)
    # print(waveform.loc[waveform['ia_crossing'] > 0])
    # print(a_crossings)
    # print(a_crossings % ia_avg_period)
    # print(a_avg_crossings_modia)

    # print(ib_avg_period)
    # print(waveform.loc[waveform['ib_crossing'] > 0])
    # print(b_crossings)
    # print(b_crossings % ib_avg_period)
    # print(b_avg_crossings_modib)

    # print(ic_avg_period)
    # print(waveform.loc[waveform['ic_crossing'] > 0])
    # print(c_crossings)
    # print(c_crossings % ic_avg_period)
    # print(c_avg_crossings_modic)

    # print(flag_a)
    # print(flag_b)
    # print(flag_c)
    ia_avg_period = nan_to_neg_one(ia_avg_period)
    ib_avg_period = nan_to_neg_one(ib_avg_period)
    ic_avg_period = nan_to_neg_one(ic_avg_period)
    a_avg_crossings_modia = nan_to_neg_one(a_avg_crossings_modia)
    b_avg_crossings_modib = nan_to_neg_one(b_avg_crossings_modib)
    c_avg_crossings_modic = nan_to_neg_one(c_avg_crossings_modic)
    a_phase_deg = nan_to_neg_one(a_phase_deg)
    b_phase_deg = nan_to_neg_one(b_phase_deg)
    c_phase_deg = nan_to_neg_one(c_phase_deg)

    return_dict = {
        'customer_name': customer_name,
        'facility_name': facility_name,            
        'location_name': location_name,
        'location_node_id': location_node_id,
        'node_sn': node_sn,
        's3_location':s3_location,
        'file_timestamp': file_timestamp,
        'channel_map': channel_map,
        'work_cycle': work_cycle,
        'vfd_driven': vfd_driven,
        'ia_avg_period': ia_avg_period,
        'ib_avg_period': ib_avg_period,
        'ic_avg_period': ic_avg_period,
        'a_avg_crossings_modia': a_avg_crossings_modia,
        'b_avg_crossings_modib': b_avg_crossings_modib,
        'c_avg_crossings_modic': c_avg_crossings_modic,
        'flag_a': flag_a,
        'flag_b': flag_b,
        'flag_c': flag_c,
        'a_phase_deg': a_phase_deg,
        'b_phase_deg': b_phase_deg,
        'c_phase_deg': c_phase_deg,
        'va_crossings': nan_to_neg_one(va_crossings.size),
        'ia_crossings': nan_to_neg_one(ia_crossings.size),
        'vb_crossings': nan_to_neg_one(vb_crossings.size),
        'ib_crossings': nan_to_neg_one(ib_crossings.size),
        'vc_crossings': nan_to_neg_one(vc_crossings.size),
        'ic_crossings':nan_to_neg_one(ic_crossings.size)
    }

    return return_dict

def get_center_of_waveform(row):
    eq_type = str(row['node_configs']['eq_type'])

    if eq_type == 'dc' or 'waveform' not in row:
        return None
    
    node_sn = int(row['node_sn'])
    location_name = str(row['location_name'])
    facility_name = str(row['facility_name'])
    customer_name = str(row['customer_name'])
    i_noise = float(row['node_configs']['i_noise'])
    v_noise = float(row['node_configs']['v_noise'])
    location_node_id = str(row['location_node_id'])
    file_timestamp = str(row['file_timestamp'])
    s3_location = str(row['s3_location'])

    product_type = 'SEL' if eq_type == '---' else 'Node'

    logger.info('{} Processing Centered on Zero for Node: {}'.format(multiprocessing.current_process().name, node_sn))

    waveform = pd.DataFrame.from_dict(row['waveform'])

    v_avg = waveform.loc[:,["va","vb","vc"]].mean().mean()
    i_avg = waveform.loc[:,["ia","ib","ic"]].mean().mean()

    return_dict = {
        'customer_name': customer_name,
        'facility_name': facility_name,            
        'location_name': location_name,
        'location_node_id': location_node_id,
        'node_sn': node_sn,
        's3_location':s3_location,
        'file_timestamp': file_timestamp,
        'v_noise': v_noise,
        'i_noise': i_noise,
        'avg_voltage': v_avg,
        'avg_current': i_avg
    }
    
    return(return_dict)


def insert_deployment_report(report_content, report_date, server, analytics_api_token):
    # INSERT INTO DATABASE
    if server == '/internal/staging':
        server_type = 'Staging'
    else:
        server_type = 'Production'

    # Get email notification for hat report
    notify_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/reports/deployment_report/".format(server)
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
        report_content['email_time'] = response_dict['content']['email_time']
        # Update database with put request
        put_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/".format(server)
        #put_url = "http://localhost:8000/internal/staging/crud/notifications/"
        put_body = {
            'time': time_stamp,
            'type': 'deployment_report',
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
        #post_url = "http://localhost:8000/internal/staging/crud/notifications/"
        post_body = {
            'time': email_time,
            'type': 'deployment_report',
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

if __name__ == "__main__":
    # Get utc datetime
    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    # Get log file name
    log_file_name = '{}.txt'.format(utc_datetime)
    # Get home directory
    home_dir = str(os.getenv('HOME'))
    # Log directory
    log_dir_path = '{}/.logs/daily/{}/'.format(home_dir, 'deployment_report')
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
        # Filter locations where Events Report needs to run
        locations_df = locations_df[
            (locations_df.node_details.apply(lambda x: x['deploymentIssue'] == False)) &
            (locations_df.node_details.apply(lambda x: x['currentDeploymentStatus'] == 'Deployed'))
            ]
        # locations_df = locations_df[locations_df['trending_report_scan'] == True]

        result_dict = deployment_report(locations_df)
        logger.success('Deployment Report Completed')
        # Email deployment Report
        #email_deployment_report(result_dict)
    else:
        process_logger.error('No locations found')
        sys.exit(1)