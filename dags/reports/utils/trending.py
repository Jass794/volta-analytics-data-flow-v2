from .common import generate_dates
import datetime as dt
import pandas as pd
import requests
import pytz
import json


LT_AVG_DAYS = 30
ST_AVG_DAYS = 5
NEW_TRENDING_BACK_DAYS = 2
threshold_dict = {
    'absolute_threshold': {
        'voltage': 0,
        'voltage_ll': 0,
        'current': 0,
        'voltage_imbalance': 2,
        'voltage_imbalance_ll': 2,
        'current_imbalance': 2,
        'voltage_thd_ll': 2,
        'current_thd': 2,
        'voltage_gis': 2
    },
    'change_threshold_dict': {
        'voltage': 10,
        'voltage_ll': 10,
        'current': 15,
        'voltage_imbalance': 20,
        'voltage_imbalance_ll': 20,
        'current_imbalance': 20,
        'voltage_thd_ll': 20,
        'current_thd': 20,
        'voltage_gis': 15
    }
}


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
    trending_df['time'] = pd.to_datetime(trending_df['time'], format='%Y-%m-%d %H:%M:%S%z')
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


# Process trending report data
def process_trending_frame(trending_df, location_details):
    # Create processed dict
    processed_dict = []
    # Create list of columns
    columns_list = trending_df.columns.tolist()
    # Remove time, location_node_id from columns list
    columns_list.remove('time')
    columns_list.remove('location_node_id')
    # Loop through columns and calculate %change
    for parameter in columns_list:
        st_avg_days = -ST_AVG_DAYS
        st_avg = trending_df[parameter].iloc[st_avg_days:].mean()
        # If parameter is not in voltage or current, calculate %change
        if parameter not in ['voltage', 'current', 'voltage_ll']:
            # If st_svg is less than absolute threshold, continue
            if st_avg < threshold_dict['absolute_threshold'][parameter]:
                continue
        # Calculate %change
        lt_avg = trending_df[parameter].iloc[:st_avg_days].mean()
        if lt_avg == 0:
            change = 0.0
            lt_avg = 0.0
            st_avg = 0.0
        else:
            change = (st_avg - lt_avg) / lt_avg * 100
        calcs = {
            'facility_id': location_details['facility_id'],
            'customer_id': location_details['customer_id'],
            'node_sn': location_details['node_sn'],
            'customer_name': location_details['customer_name'],
            'location_name': location_details['location_name'],
            'facility_name': location_details['facility_name'],
            'parameter': parameter,
            'lt_avg': lt_avg,
            'st_avg': st_avg,
            'change': change,
            'change_threshold': threshold_dict['change_threshold_dict'][parameter],
            'absolute_threshold': threshold_dict['absolute_threshold'][parameter],
            'location_node_id': location_details['location_node_id']
        }
        processed_dict.append(calcs)
    return processed_dict


# Get previous reports
def get_previous_trending_reports(report_date, analytics_api_token):
    # Convert report_date to datetime
    report_date = dt.datetime.strptime(report_date, '%Y-%m-%d').date()
    start_date = report_date - dt.timedelta(days=NEW_TRENDING_BACK_DAYS)
    end_date = report_date - dt.timedelta(days=1)
    report_date_list = generate_dates(start_date, end_date)
    # Final df
    final_df = pd.DataFrame()
    # Loop through each date to get previous report_harmonics
    notify_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/notifications/reports/trending_report/"
    notify_headers = {'Authorization': 'Bearer ' + analytics_api_token}
    for given_date in report_date_list:
        notify_query_params = {
            'given_date': given_date
        }
        notify_response = requests.get(notify_url, headers=notify_headers, params=notify_query_params)
        notify_response.raise_for_status()
        if notify_response.status_code == 204:
            continue
        else:
            trending_report = notify_response.json()['content']['content']
            trending_report_df = pd.DataFrame(trending_report['trending_report'])
            # Add it to final df
            final_df = pd.concat([final_df, trending_report_df], ignore_index=True)
    final_df = final_df.reset_index(drop=True)
    return final_df


# Insert report to database
def insert_trending_report(report_content, report_date, server, analytics_api_token):
    # INSERT INTO DATABASE
    if server == '/internal/staging':
        server_type = 'Staging'
    else:
        server_type = 'Production'
    # Get email notification for hat report
    notify_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/reports/trending_report/".format(server)
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
            'type': 'trending_report',
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
            'type': 'trending_report',
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
