from .common import generate_dates
import datetime as dt
import pandas as pd
import requests
import pytz
import json
import multiprocessing
from loguru import logger
import boto3
import math
import io

LT_AVG_DAYS = 30
ST_AVG_DAYS = 5
NEW_TRENDING_BACK_DAYS = 2

# Get trending report data from analytics API
def get_trending_df(analytics_api_token, location_node_id, report_date):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=LT_AVG_DAYS)
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/app/trending/24_hr_trend/"
    headers = {'Authorization': 'Bearer ' + analytics_api_token}
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date())
    }
    # Make request to get trending report API
    response = requests.get(url, headers=headers, params=query_params)
    if response.status_code == 204:
        return pd.DataFrame()
    response.raise_for_status()
    # Receive response in JSON
    trending_dict_list = response.json()['content']
    # Convert dict to DataFrame
    trending_df = pd.DataFrame(trending_dict_list)
    # Round values to 2 decimal places
    trending_df = trending_df.round(2)
    # Reframe to required columns
    trending_df = trending_df[[
        'time', 'location_node_id', 'voltage', 'voltage_ll', 'current',
        'voltage_imbalance', 'voltage_imbalance_ll', 'current_imbalance',
        'voltage_thd_ll', 'current_thd'
    ]]
    # Parse string to datetime
    trending_df['time'] = pd.to_datetime(trending_df['time'], format='%Y-%m-%dT%H:%M:%S%z')
    # Order by time ascending and reset index
    trending_df = trending_df.sort_values(by='time').reset_index(drop=True)
    # If date of most recent time is not the same as end date, return empty dataframe
    if trending_df['time'].iloc[-1] != end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc):
        return pd.DataFrame()
    # Fill NaNs with 0
    trending_df = trending_df.fillna(0)
    # Round all some columns to 2 decimal places
    trending_df[[
        'voltage', 'voltage_ll', 'current', 'voltage_imbalance', 'voltage_imbalance_ll', 
        'current_imbalance', 'voltage_thd_ll', 'current_thd'
    ]] = trending_df[[
        'voltage', 'voltage_ll', 'current', 'voltage_imbalance', 'voltage_imbalance_ll', 
        'current_imbalance', 'voltage_thd_ll', 'current_thd'
    ]].round(2)
    return trending_df


#Process nodes with absolute voltage imbalance L-L > 5% (No DC, V1, or V2)
def process_voltage_imbalance_frame(trending_df,location_details):
    if trending_df is None or 'voltage_imbalance_ll' not in trending_df.keys():
        return None
    
    ABSOLUTE_VOLTAGE_IMBALANCE_LL_THRESHOLD = 5

    processed_dict = []
    calcs = None
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

    return calcs

def get_recent_esa_waveform(api_token,row):
    """
    Gets the most recent ON esa file for AC equipment
    Input: API token and a Row from locations_df containing information about a node
        -Minimum required fields in row:
            -node_sn
            -location_node_id
            -node_configs.eq_type
            -node_configs.eq_type_sub
    Output: Row with an additional fields: 'waveform', 'file_timestamp'
        -'waveform' is a dict containing the following keys
            -ia
            -ib
            -ic
            -va
            -vb
            -vc
    """
    node_sn = int(row['node_sn'])
    eq_type = str(row['node_configs']['eq_type'])
    location_node_id = str(row['location_node_id'])
    eq_type_sub = str(row['node_configs']['eq_type_sub'])

    if eq_type == 'dc':
        return row
    
    product_type = 'SEL' if eq_type == '---' else 'Node'
    
    logger.info('{} Getting ESA File for Node: {}'.format(multiprocessing.current_process().name,node_sn))

    url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/reports/deployment/recent_esa_ON_file/"
    #url = "http://localhost:8000/internal/staging/reports/deployment/recent_esa_ON_file/"
    headers = {'Authorization': 'Bearer {}'.format(api_token)}
    query_params = {
        'location_node_id': location_node_id,
        'product_type':product_type
    }
    response = requests.get(url=url, params=query_params, headers=headers)
    response.raise_for_status()
    if response.status_code == 200:
        response_dict = response.json()['content']

        s3_location = response_dict['s3_location']
        file_timestamp = response_dict['file_timestamp']
        channel_map = response_dict['channel_map']

        # Create sts token to read S3 files
        boto_sts = boto3.client('sts')
        sts_response = boto_sts.assume_role(
            RoleArn='arn:aws:iam::217297510618:role/assumed-role-volta-data',
            RoleSessionName='Analytics-API-Server'
        )
        # Save the details from assumed role into vars
        new_session_id = sts_response["Credentials"]["AccessKeyId"]
        new_session_key = sts_response["Credentials"]["SecretAccessKey"]
        new_session_token = sts_response["Credentials"]["SessionToken"]
        s3_assumed_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id=new_session_id,
            aws_secret_access_key=new_session_key,
            aws_session_token=new_session_token
        )
        search_bucket = s3_location.split('/')[0]
        esa_file_key = '/'.join(s3_location.split('/')[1:])
            # Read ESA from S3
        esa_object = s3_assumed_client.get_object(Bucket=search_bucket, Key=esa_file_key)
        esa_contents = io.BytesIO(esa_object['Body'].read())

        if product_type == 'Node':
            esa_data = pd.read_csv(esa_contents)
            # divide by root 3 only for non eq_type nodes
            if eq_type_sub != 'v1' and eq_type != 'dc':
                esa_data[[' Va', ' Vb', ' Vc']] = esa_data[[' Va', ' Vb', ' Vc']].div(math.sqrt(3))
            # Data Frame to Numpy Array
            va = esa_data[' Va'].to_numpy()
            vb = esa_data[' Vb'].to_numpy()
            vc = esa_data[' Vc'].to_numpy()
            ia = esa_data[' Ia'].to_numpy()
            ib = esa_data[' Ib'].to_numpy()
            ic = esa_data[' Ic'].to_numpy()
            time = esa_data['time (sec)'].to_numpy()
        else:
            esa_data = pd.read_csv(esa_contents, skiprows=1)
            print(esa_data)
            # Data Frame to Numpy Array
            va = esa_data['Vab'].to_numpy()
            vb = esa_data['Vbc'].to_numpy()
            vc = esa_data['Vca'].to_numpy()
            ia = esa_data['Ia'].to_numpy()
            ib = esa_data['Ib'].to_numpy()
            ic = esa_data['Ic'].to_numpy()
            time = esa_data['time (sec)'].to_numpy()
       

        waveform = {
            'time': time,
            'ia':ia,
            'ib':ib,
            'ic':ic,
            'va':va,
            'vb':vb,
            'vc':vc
        }

        row['waveform'] = waveform
        row['file_timestamp'] = file_timestamp
        row['s3_location'] =  s3_location
        row['channel_map'] = channel_map

    elif response.status_code == 204:
        #No content
        return row
    else:
        # Raise error
        raise Exception('Error getting recent esa on file: {}'.format(response.status_code))
    
    return row

def nan_to_neg_one(x:float):
    if math.isnan(x):
        return -1.0
    else:
        return x