from .common import generate_dates
import datetime as dt
import pandas as pd
import requests
import pytz
import json


EVENTS_BACK_DAYS = 7
EVENTS_CHANGE_THRESHOLD = 200
OUTLIER_SCAN_DAYS = 60
threshold_dict = {
    'start': 200,
    'stop': 200,
    'start-d': 200,
    'stop-u': 200,
    'current_rise': 200,
    'current_fall': 200,
    'voltage_rise': 200,
    'voltage_fall': 200
}

# Get events timetable
def get_events_timetable(analytics_api_token, location_node_id, timezone, report_date):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=EVENTS_BACK_DAYS-1)
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/events/events_list/"
    headers = {'Authorization': 'Bearer ' + analytics_api_token}
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date()),
        'timezone': timezone,
        'event_type': 'current_rise'
    }
    # Make request to get events data report API
    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    if response.status_code == 204:
        return pd.DataFrame()
    # Receive response in JSON
    event_dict = response.json()['content']
    # Convert dict to DataFrame
    events_df = pd.DataFrame(event_dict)
    # Parse epoch milliseconds to datetime
    events_df['time'] = pd.to_datetime(events_df['time'], unit='ms')
    # Localize to timezone
    events_df['time'] = events_df['time'].dt.tz_localize(timezone)
    # Pass only required columns
    events_df = events_df[['time', 'event_type']]
    return events_df


# Process events dataframe
def process_events_table(report_date, events_df):
    # Order by time ascending
    events_df = events_df.sort_values(by='time')
    # Split time column into date and time columns
    events_df['date'] = events_df['time'].dt.strftime('%b %d, %Y')
    # Only keep date and time columns
    events_df = events_df[['date', 'time']]
    # Format time column
    events_df['time'] = events_df['time'].dt.strftime('%H:%M:%S')
    # Generate date list
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=EVENTS_BACK_DAYS-1)
    date_list = generate_dates(start_date.date(), end_date.date())
    # Create events dict
    events_dict = {}
    # Loop through dates
    for event_date in date_list:
        # List of time where date is event_date
        event_times = events_df[events_df['date'] == event_date.strftime('%b %d, %Y')]['time'].tolist()
        events_dict[str(event_date.strftime('%b %d, %Y'))] = pd.Series(event_times)
    # Convert events dict to DataFrame
    events_df = pd.DataFrame.from_dict(events_dict)
    return events_df


# Get events trending data from analytics API
def get_events_df(analytics_api_token, location_node_id, report_date, lt_avg_days):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=lt_avg_days)
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_event_count/"
    headers = {'Authorization': 'Bearer ' + analytics_api_token}
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date())
    }
    # Make request to get events data report API
    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    if response.status_code == 204:
        return pd.DataFrame()
    # Receive response in JSON
    event_dict = response.json()['content']
    # Convert dict to DataFrame
    events_df = pd.DataFrame(event_dict)
    # Parse string to datetime
    events_df['time'] = pd.to_datetime(events_df['time'], format='%Y-%m-%dT%H:%M:%S%z')
    # Order by time ascending and reset index
    events_df = events_df.sort_values(by='time').reset_index(drop=True)
    return events_df
    

# Process events data
def process_events_frame(events_df, report_date, location_details):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)
    event_details = {
        'facility_id': location_details['facility_id'],
        'customer_id': location_details['customer_id'],
        'node_sn': location_details['node_sn'],
        'customer_name': location_details['customer_name'],
        'location_name': location_details['location_name'],
        'facility_name': location_details['facility_name'],
        'location_node_id': location_details['location_node_id']
    }
    email_event_list = []
    # Loop through unique event types and calculate % change
    for event_type in events_df[events_df['time'] == end_date]['event_type'].unique():
        # If event only happend today and no lt data available, then flag it as new event
        if events_df[(events_df['time'] < end_date) & (events_df['event_type'] == event_type)].empty:
            st_count = events_df[(events_df['time'] == end_date) & (events_df['event_type'] == event_type)]['event_count'].sum()
            select_event = event_details
            select_event['event_type'] = event_type
            select_event['st_count'] = st_count
            select_event['lt_avg'] = 0
            select_event['change'] = 100000.0
            select_event['threshold'] = threshold_dict[event_type]
            email_event_list.append(select_event.copy())
        else:
            st_count = events_df[(events_df['time'] == end_date) & (events_df['event_type'] == event_type)]['event_count'].sum()
            lt_count = int(events_df[
                    (events_df['time'] < end_date) & (events_df['event_type'] == event_type)
                ]['event_count'].mean())
            
            # If lt count avg is zero, move it to 1
            if lt_count == 0:
                lt_count = 1
            # Calculate % Change
            change = (st_count - lt_count) / lt_count * 100
            # If change is above threshold, include in report
            if change > EVENTS_CHANGE_THRESHOLD:
                select_event = event_details
                select_event['event_type'] = event_type
                select_event['st_count'] = st_count
                select_event['lt_avg'] = lt_count
                select_event['change'] = change
                select_event['threshold'] = threshold_dict[event_type]
                email_event_list.append(select_event.copy())
    # if email_event_list is empty, return empty frame
    if len(email_event_list) == 0:
        return pd.DataFrame(
            columns=[
                'customer_name', 'node_sn', 'location_name', 'facility_name', 
                'event_type', 'st_count', 'lt_avg', 'change', 'threshold', 
                'location_node_id', 'facility_id', 'customer_id'
            ]
        )
    # Convert list to DataFrame
    email_event_df = pd.DataFrame(email_event_list)
    return email_event_df


# Get outlie events from analytics API
def get_outlier_events(report_date, location_node_id, np_current, analytics_api_token):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=OUTLIER_SCAN_DAYS)
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/events/utc_outlier_events/"
    headers = {'Authorization': 'Bearer ' + analytics_api_token}
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date()),
        'np_current': np_current
    }
    # Make request to get events data report API
    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    if response.status_code == 204:
        return pd.DataFrame()
    # Receive response in JSON
    event_dict = response.json()['content']
    # Convert dict to DataFrame
    outlier_df = pd.DataFrame(event_dict)
    # Parse epoch milliseconds to datetime
    outlier_df['time'] = pd.to_datetime(outlier_df['time'], unit='ms')
    # Filter out outliers on report data
    outlier_df = outlier_df[outlier_df['time'].dt.date >= end_date.date()]
    if outlier_df.empty:
        return pd.DataFrame()
    else:
    # Order by time ascending and reset index
        outlier_df = outlier_df.sort_values(by='time').reset_index(drop=True)
    return outlier_df


# Insert notification email to database
def insert_events_report(report_content, report_date, server, analytics_api_token):
    # INSERT INTO DATABASE
    if server == '/internal/staging':
        server_type = 'Staging'
    else:
        server_type = 'Production'
    # Get email notification for hat report
    notify_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/reports/events_report/".format(server)
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
            'type': 'events_report',
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
            'type': 'events_report',
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


# Add status of node for given list of locations
def location_node_status(location_list, report_date, analytics_api_token, lt_avg_days):
    new_location_list = []
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=lt_avg_days)
    # Create headers and query params
    headers = {'Authorization': 'Bearer ' + analytics_api_token}
    query_params = {
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date())
    }
    ss_url = "https://analytics-ecs-api.voltaenergy.ca/internal/app/sec_data/daily_ss_count/"
    # Loop through locations
    for location in location_list:
        query_params['location_node_id'] = location['location_node_id']
        # Query daily average trend
        ss_response = requests.get(ss_url, headers=headers, params=query_params)
        ss_response.raise_for_status()
        if ss_response.status_code == 204:
            # There should be no start atop or transient event. So set to False
            location['start_stop'] = False
            location['transients'] = False
        else:
            # Check Daily average trend
            trend_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_avg/"
            trend_response = requests.get(trend_url, headers=headers, params=query_params)
            trend_response.raise_for_status()
            if trend_response.status_code == 200:
                # It should have Transient events so set it True
                location['transients'] = True
            else:
                # Else Equipment is OFF for the period
                location['transients'] = False
            
            # Now check for start stop events
            content = ss_response.json()['content']
            if content['total_starts'] == 0 and content['total_stops'] == 0:
                location['start_stop'] = False
            else:
                location['start_stop'] = True
                location['total_starts'] = content['total_starts']
                location['total_stops'] = content['total_stops']
                # Get most recent start and stop date
                # Convert daily_ss_count dict to frame
                daily_ss_count = pd.DataFrame.from_dict(content['daily_ss_count'])
                # Sort by time descending
                daily_ss_count = daily_ss_count.sort_values(by='time', ascending=False)
                
                # Get first time where starts is not 0
                start_frame = daily_ss_count[daily_ss_count['starts'] != 0]
                if start_frame.empty:
                    location['recent_start'] = '---'
                else:
                    location['recent_start'] = start_frame['time'].iloc[0]
                
                # Get first time where stops is not 0
                stop_frame = daily_ss_count[daily_ss_count['stops'] != 0]
                if stop_frame.empty:
                    location['recent_stop'] = '---'
                else:
                    location['recent_stop'] = stop_frame['time'].iloc[0]
        # Add location to new_location_list
        new_location_list.append(location)
    return new_location_list
