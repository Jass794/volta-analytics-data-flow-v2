from .common import generate_dates
import datetime as dt
import pandas as pd
import requests
import time
import json

NEW_HARMONIC_BACK_DAYS = 2
LT_AVG_DAYS = 60
ST_AVG_DAYS = 5
FILES_PER_DAY = 96
HARMONIC_SCAN_LOWER_LF_RANGE = 0
HARMONIC_SCAN_UPPER_LF_RANGE = 2


# LF Tolerance table
def get_lf_tolerance(harmonic_freq_lf):
    if 0.0 <= harmonic_freq_lf <= 3.0:
        tolerance = 0.005
    elif 3.0 < harmonic_freq_lf <= 5.0:
        tolerance = 0.01
    elif 5.0 < harmonic_freq_lf <= 10.0:
        tolerance = 0.02
    elif 10.0 < harmonic_freq_lf <= 20.0:
        tolerance = 0.03
    elif 20.0 < harmonic_freq_lf <= 30.0:
        tolerance = 0.04
    elif 30.0 < harmonic_freq_lf <= 60.0:
        tolerance = 0.08
    elif 60.0 < harmonic_freq_lf <= 80.0:
        tolerance = 0.1
    elif harmonic_freq_lf > 80.0:
        tolerance = 0.2
    return tolerance


# Remove harmonics from the harmonics_df within tolerance
def tolerance_harmonics_removal(new_df, old_df):
    new_df = new_df.reset_index(drop=True)
    old_df = old_df.reset_index(drop=True)

    # Remove duplicates from old_df
    old_df = old_df.drop_duplicates(subset=['location_node_id', 'harmonic_lf'], keep='last')

    # Sort new_df by harmonic_lf
    new_df = new_df.sort_values(by='harmonic_lf').reset_index(drop=True)

    final_hat_columns = [
        'customer_name', 'node_sn', 'location_name', 'facility_name', 'harmonic_lf',
        'lt_avg', 'st_avg', 'change', 'impact', 'lt_count', 'st_count',
        'total_count', 'location_node_id', 'facility_id', 'customer_id'
    ]
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
    final_hat_frame = pd.DataFrame(final_hat_records, columns=final_hat_columns)
    if final_hat_frame.empty:
        return final_hat_frame
    else:
        # Sort final_hat_frame by harmonic_lf
        final_hat_frame = final_hat_frame.sort_values(by='harmonic_lf', ascending=True).reset_index(drop=True)
        # Reset index
        final_hat_frame = final_hat_frame.reset_index(drop=True)
        return final_hat_frame


# Take harmonics frame and remove all harmonics from 1 to 1.999 LF repeating in 0 to 0.999 LF
def mirror_harmonics_removal(harmonics_df):
    harmonics_df = harmonics_df.reset_index(drop=True)

    # remove index list
    remove_list = []

    # Loop through all equipments
    for location in harmonics_df['location_node_id'].unique():
        equipment_harmonics_df = harmonics_df[harmonics_df['location_node_id'] == location]
        # Loop the harmonic_lf column from 1 to 1.999 LF
        for index, row in equipment_harmonics_df[(equipment_harmonics_df['harmonic_lf'] > 1) & (equipment_harmonics_df['harmonic_lf'] < 2)].iterrows():
            # Get tolerance for harmonic_lf
            tolerance = get_lf_tolerance(row['harmonic_lf'])
            search_lf = 2 - row['harmonic_lf']
            # Slice equipment_harmonics_df for search_lf within tolerance
            search_harmonics_df = equipment_harmonics_df[
                (equipment_harmonics_df['harmonic_lf'] > search_lf - tolerance) &
                (equipment_harmonics_df['harmonic_lf'] < (search_lf + tolerance))
                ]
            if search_harmonics_df.empty:
                continue
            else:
                # Add index to remove_list
                remove_list.append(index)
    # remove the rows from the harmonics_df
    filtered_harmonics_df = harmonics_df.drop(remove_list).reset_index(drop=True).copy()
    return filtered_harmonics_df


# Get report from DB
def get_daily_scans(location_node_id, given_date, report_type, TOKEN):
    url = f"https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_scans/{report_type}/"
    headers = {'Authorization': 'Bearer ' + TOKEN}
    query_params = {
        'location_node_id': location_node_id,
        'given_date': given_date
    }

    response = requests.get(url, headers=headers, params=query_params)
    if response.status_code == 204:
        return pd.DataFrame()
    elif response.status_code == 502:
        # If 502, try again in 2 seconds
        time.sleep(2)
        response = requests.get(url, headers=headers, params=query_params)
        if response.status_code == 204:
            return pd.DataFrame()
    response.raise_for_status()
    report_dict = response.json()['content']
    # Convert report to df
    hat_report_df = pd.DataFrame(report_dict[report_type])
    return hat_report_df


# Get previous reports
def get_previous_reports(report_date, report_type, TOKEN):
    # Convert report_date to datetime
    report_date = dt.datetime.strptime(report_date, '%Y-%m-%d').date()
    start_date = report_date - dt.timedelta(days=NEW_HARMONIC_BACK_DAYS)
    # Not to include the report_date in the dates list
    report_date_list = generate_dates(start_date, (report_date - dt.timedelta(days=1)))
    # Final df
    final_df = pd.DataFrame()
    # Loop through each date to get previous report_harmonics
    notify_url = f"https://analytics-ecs-api.voltaenergy.ca/internal/crud/notifications/reports/{report_type}/"
    notify_headers = {'Authorization': 'Bearer ' + TOKEN}
    for given_date in report_date_list:
        notify_query_params = {
            'given_date': given_date
        }
        notify_response = requests.get(notify_url, headers=notify_headers, params=notify_query_params)
        notify_response.raise_for_status()
        if notify_response.status_code == 204:
            continue
        else:
            hat_report = notify_response.json()['content']['content']
            equipment_harmonics_df = pd.DataFrame(hat_report['report_harmonics'])
            # Add it to final df
            final_df = pd.concat([final_df, equipment_harmonics_df], ignore_index=True)
    final_df = final_df.reset_index(drop=True)
    return final_df


# Apply harmonic filter on hat report
def apply_harmonics_filter(report_hat_frame):
    # Reset Index
    report_harmonics = report_hat_frame.reset_index(drop=True)
    # LT Avg greater than 0.01
    report_harmonics = report_harmonics[report_harmonics['lt_avg'] > 0.01]
    # Keep LF only if absolute impact or change is > 50
    report_harmonics = report_harmonics[
        (report_harmonics['impact'].abs() > report_harmonics['threshold']) |
        (report_harmonics['change'].abs() > report_harmonics['threshold'])
        ]
    # Drop threshold column
    report_harmonics = report_harmonics.drop(columns=['threshold'])
    return report_harmonics.reset_index(drop=True)


# Get harmonics data from API
def get_harmonic_data(location_node_id, start_date, end_date, parameter, api_token):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/harmonics/harmonic_data/"
    headers = {'Authorization': 'Bearer ' + api_token}
    query_params = {
        'location_node_id': location_node_id,
        'utc_start_date': str(start_date),
        'utc_end_date': str(end_date),
        'parameter': parameter,
        'lower_lf_range': HARMONIC_SCAN_LOWER_LF_RANGE,
        'upper_lf_range': HARMONIC_SCAN_UPPER_LF_RANGE
    }
    # Create API Call for Harmonics
    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()

    if response.status_code == 204:
        harmonic_df = pd.DataFrame(columns=[
            'time', 'line_frequency', 'harmonic_freq', 'harmonic_value'
        ])
    elif response.status_code == 200:
        # Read from response
        harmonic_dict = response.json()['content']
        # Convert to pandas dataframe
        harmonic_df = pd.DataFrame.from_dict(harmonic_dict)
    else:
        raise
    # Convert harmonics frame time to datetime
    harmonic_df['time'] = pd.to_datetime(harmonic_df['time'], unit='ms')
    return harmonic_df


# Get harmonics data from API
def get_harmonic_data_v2(location_node_id, start_date, end_date, parameter, api_token, product_type):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/harmonics/harmonic_data/"
    units = 'ms'
    if product_type == 'SEL':
        units = 's'
        url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/harmonics/sel_harmonic_data/"

    headers = {'Authorization': 'Bearer ' + api_token}
    query_params = {
        'location_node_id': location_node_id,
        'utc_start_date': str(start_date),
        'utc_end_date': str(end_date),
        'parameter': parameter,
        'lower_lf_range': HARMONIC_SCAN_LOWER_LF_RANGE,
        'upper_lf_range': HARMONIC_SCAN_UPPER_LF_RANGE
    }
    # Create API Call for Harmonics
    response = requests.get(url, headers=headers, params=query_params)

    response.raise_for_status()
    if response.status_code == 204:
        harmonic_df = pd.DataFrame(columns=[
            'time', 'line_frequency', 'harmonic_freq', 'harmonic_value'
        ])
    elif response.status_code == 200:
        # Read from response
        harmonic_dict = response.json()['content']
        # Convert to pandas dataframe
        harmonic_df = pd.DataFrame.from_dict(harmonic_dict)
    else:
        raise
    # Convert harmonics frame time to datetime
    harmonic_df['time'] = pd.to_datetime(harmonic_df['time'], unit=units)

    return harmonic_df


def get_harmonic_signatures(api_token, server):
    report_url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server}/crud/v2/harmonic-signatures/'
    header = {'Authorization': f"Bearer {api_token}"}
    harmonic_signatures_df = pd.DataFrame()

    response = requests.get(url=report_url, headers=header)
    response.raise_for_status()

    if response.status_code == 200:
        harmonic_signatures_df = pd.DataFrame.from_dict(response.json()['content'])
    return harmonic_signatures_df

# Process daily scan data
def process_harmonic_data(harmonic_frame, harmonic_list, lt_st_ratio, lt_avg_days):
    # First next_harmonic_lf is from 1 LF range
    next_harmonic_lf = get_lf_tolerance(harmonic_list[0])
    # Create result dataframe
    result_frame_columns = ['harmonic_lf', 'st_avg', 'lt_avg', 'change', 'impact', 'st_count', 'lt_count', 'total_count']
    result_records = []
    # Loop through harmonic list to find impact and percent change
    for harmonic_lf in harmonic_list:
        tolerance = get_lf_tolerance(harmonic_lf)
        # If harmonics within tolerance with previous harmonic the skip
        if harmonic_lf < next_harmonic_lf:
            continue
        # Push next harmonic selection beyond current tolerance
        next_harmonic_lf = harmonic_lf + tolerance

        # Slice out harmonic frame for selected harmonic_lf
        given_harmonic_frame = harmonic_frame[
            (harmonic_frame['harmonic_freq'].ge(harmonic_lf - tolerance)) &
            (harmonic_frame['harmonic_freq'].lt(harmonic_lf + tolerance))
            ].copy()
        # Drop harmonic_freq column
        given_harmonic_frame = given_harmonic_frame.drop(columns=['harmonic_freq'])

        # Group by time and choose max if multiple harmonics are found within a tolerance
        given_harmonic_frame = given_harmonic_frame.groupby('time').max().reset_index(drop=True)
        # Total harmonics count
        total_harmonic_count = given_harmonic_frame['harmonic_value'].count()
        # Skip scan if total count is less than lt_avg_days * 96 / lt_st_ratio
        if total_harmonic_count < (lt_avg_days * 96 / lt_st_ratio):
            continue
        # ST file count
        st_file_count = -(int(total_harmonic_count / lt_st_ratio))

        # Get the short term average from last 96 rows and count occurance
        st_harmonic_average = given_harmonic_frame['harmonic_value'][st_file_count:].mean()
        st_harmonic_count = given_harmonic_frame['harmonic_value'][st_file_count:].count()

        # Get the long term average from remaining rows and count occurance
        lt_harmonic_average = given_harmonic_frame['harmonic_value'].mean()
        lt_harmonic_count = given_harmonic_frame['harmonic_value'].count()

        # Calculate %Change and Impact and total lf
        percent_change = (st_harmonic_average - lt_harmonic_average) / lt_harmonic_average * 100
        impact = percent_change * lt_harmonic_average

        # Create pandas series
        harmonic_change_record = {
            'harmonic_lf': harmonic_lf,
            'st_avg': st_harmonic_average,
            'lt_avg': lt_harmonic_average,
            'change': percent_change,
            'impact': impact,
            'st_count': st_harmonic_count,
            'lt_count': lt_harmonic_count,
            'total_count': total_harmonic_count
        }

        # Add to result records
        result_records.append(harmonic_change_record)

    # Create frame from records
    result_frame = pd.DataFrame(result_records, columns=result_frame_columns)
    # Round dataframe to 3 digits and NaN to 0.0
    result_frame = result_frame.round(3).fillna(0)
    # Convert harmonic results to dict
    result_dict = json.loads(json.dumps(result_frame.to_dict()))
    # processed_dict = {
    #     'configs': {
    #         'lt_st_ratio': lt_st_ratio,
    #     },
    #     'hat_report': result_dict
    # }
    return result_dict


# Process daily scan data
def process_harmonic_data_v4(harmonic_frame, harmonic_list, st_avg_days, lt_avg_days, max_peak_days, scan_date,  scan_type, location_dict):
    # First next_harmonic_lf is from 1 LF range
    next_harmonic_lf = get_lf_tolerance(harmonic_list[0])
    # Create result dataframe
    result_frame_columns = ['harmonic_lf', 'st_avg', 'lt_avg', 'change', 'impact', 'st_count', 'lt_count', 'total_count', 'lt_harmonic_max_lf_value', 'lt_harmonic_min_lf_value', 'scan_period_type',
                            'lt_harmonic_max_lf_value_date', 'lt_harmonic_min_lf_value_date']
    result_records = []
    # Loop through harmonic list to find impact and percent change
    for harmonic_lf in harmonic_list:
        if harmonic_lf == 1:
            continue
        tolerance = get_lf_tolerance(harmonic_lf)
        # If harmonics within tolerance with previous harmonic the skip
        if harmonic_lf < next_harmonic_lf:
            continue
        # Push next harmonic selection beyond current tolerance
        next_harmonic_lf = harmonic_lf + tolerance
        # Slice out harmonic frame for selected harmonic_lf
        given_harmonic_frame = harmonic_frame[
            (harmonic_frame['harmonic_freq'].ge(harmonic_lf - tolerance)) &
            (harmonic_frame['harmonic_freq'].lt(harmonic_lf + tolerance))
            ].copy()
        # Group by time and choose max if multiple harmonics are found within a tolerance
        given_harmonic_frame = given_harmonic_frame.sort_values(by='harmonic_value', ascending=False).drop_duplicates('time', keep='first').reset_index(drop=True)
        # Group by time and choose max if multiple harmonics are found within a tolerance
        given_harmonic_frame.index = pd.to_datetime(given_harmonic_frame['time'])
        given_harmonic_frame = given_harmonic_frame[['harmonic_value']]
        # print(given_harmonic_frame)
        # Create dates for data slicing
        st_slicing_date = scan_date - dt.timedelta(days=st_avg_days)
        lt_slicing_date = scan_date - dt.timedelta(days=lt_avg_days)

        harmonic_frame_min_date = given_harmonic_frame.index.min()
        # performing check that df have valid data
        if st_slicing_date < harmonic_frame_min_date and lt_slicing_date < harmonic_frame_min_date:
            continue
        
        # Total harmonics count
        total_harmonic_count = given_harmonic_frame[given_harmonic_frame.index >= lt_slicing_date]['harmonic_value'].count()
        # slice long term df 
        lt_harmonics_frame = given_harmonic_frame[(given_harmonic_frame.index >= lt_slicing_date) & 
                                                  (given_harmonic_frame.index < st_slicing_date)]
        
        st_harmonics_frame = given_harmonic_frame[(given_harmonic_frame.index >= st_slicing_date)]

        # Skip if st/ lt file count is less than 15/75 # todo: move this count ot variable
        if (len(st_harmonics_frame.index) < 15 or len(lt_harmonics_frame.index) < 75) and location_dict['node_details']['type'] == 'Node' :
            print(f'Skipping due to count is not desired st count {len(st_harmonics_frame.index)}, lt count {len(lt_harmonics_frame.index)}  of Harmonic LF {harmonic_lf}')
            continue
        elif location_dict['node_details']['type'] != 'Node' and len(st_harmonics_frame.index) < 5:
            print(f'Skipping due to count is not desired st SEL count {len(st_harmonics_frame.index)}, lt count {len(lt_harmonics_frame.index)}  of Harmonic LF {harmonic_lf}')
            continue
        # get the short term count and average
        st_harmonic_average = st_harmonics_frame['harmonic_value'].mean()
        st_harmonic_count = st_harmonics_frame['harmonic_value'].count()

        # Get the long term average from remaining rows and count occurrence
        lt_harmonic_average = lt_harmonics_frame['harmonic_value'].mean()
        lt_harmonic_count = lt_harmonics_frame['harmonic_value'].count()

        # Calculate % Change and Impact and total lf
        percent_change = (st_harmonic_average - lt_harmonic_average) / lt_harmonic_average * 100
        impact = percent_change * lt_harmonic_average

        # Slice the df for max/ min peak selection
        lt_strip_start_date = st_slicing_date - dt.timedelta(days=max_peak_days)
        harmonic_resampled_df = given_harmonic_frame[(given_harmonic_frame.index >= lt_strip_start_date) & 
                                                  (given_harmonic_frame.index < st_slicing_date)].resample('24H').mean().dropna()
        # print(f"first elemnt of peak selection  frame {harmonic_resampled_df.head(1)} \n last elemnt {harmonic_resampled_df.tail(1)}")

        lt_harmonic_max_lf_value = harmonic_resampled_df['harmonic_value'].max()
        lt_harmonic_min_lf_value = harmonic_resampled_df['harmonic_value'].min()
        # todo: remove commented code
        # fig = make_subplots(rows=2, cols=1, subplot_titles=[f'Ia - {harmonic_lf} change {percent_change}', 'trend'])
        #
        # # Add the raw data as scatter plot
        # fig.add_trace(go.Scatter(x=given_harmonic_frame.index, y=given_harmonic_frame['harmonic_value'], mode='lines', name=f'Raw data {harmonic_lf}'), row=1, col=1)
        #
        # # Add the raw data as scatter plot
        # fig.add_trace(go.Scatter(x=harmonic_resampled_df.index, y=harmonic_resampled_df['harmonic_value'], mode='lines', name='log trans'), row=2, col=1)
        #
        # # Add the raw data as scatter plot
        # # fig.add_trace(go.Scatter(x=trend.index, y=trend, mode='lines', name='trend'), row=2, col=1)
        # fig.show()

        # Create pandas series
        harmonic_change_record = {
            'harmonic_lf': harmonic_lf,
            'st_avg': st_harmonic_average,
            'lt_avg': lt_harmonic_average,
            'change': percent_change,
            'impact': impact,
            'st_count': st_harmonic_count,
            'lt_count': lt_harmonic_count,
            'total_count': total_harmonic_count,
            'lt_harmonic_max_lf_value': lt_harmonic_max_lf_value,
            'lt_harmonic_min_lf_value': lt_harmonic_min_lf_value,
            'lt_harmonic_max_lf_value_date': str(harmonic_resampled_df[harmonic_resampled_df['harmonic_value'] == lt_harmonic_max_lf_value].index[0].date()) + ' UTC',
            'lt_harmonic_min_lf_value_date': str(harmonic_resampled_df[harmonic_resampled_df['harmonic_value'] == lt_harmonic_min_lf_value].index[0].date()) + ' UTC',
            'scan_period_type': scan_type
        }

        # Add to result records
        result_records.append(harmonic_change_record)
    # Create frame from records
    result_frame = pd.DataFrame(result_records, columns=result_frame_columns)
    # Round dataframe to 3 digits and NaN to 0.0
    result_frame = result_frame.round(3).fillna(0)
    # Convert harmonic results to dict
    result_dict = json.loads(json.dumps(result_frame.to_dict()))
    return result_dict


# Insert report to DB
def insert_hat_report(report_date, report_type, new_harmonics, report_harmonics, server, ANALYTICS_API_TOKEN):
    # INSERT INTO DATABASE
    if server == '/internal/staging':
        server_type = 'Staging'
    else:
        server_type = 'Production'
    # Get email notification for hat report
    notify_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/reports/{}/".format(server, report_type)
    headers = {'Authorization': 'Bearer {}'.format(ANALYTICS_API_TOKEN)}
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
        report_content = {
            'email_time': response_dict['content']['email_time'],
            'report_date': report_date,
            'new_harmonics': new_harmonics.to_dict(),
            'report_harmonics': report_harmonics.to_dict()
        }
        # Update database with put request
        put_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/".format(server)

        put_body = {
            'time': time_stamp,
            'type': report_type,
            'location_node_id': 'report',
            'content': json.dumps(report_content),
            'notified': True
        }
        response = requests.put(url=put_url, json=put_body, headers=headers)
        print(response.reason)
        print(response.text)
        response.raise_for_status()
    elif response.status_code == 204:
        # Email are send a UTC day ahead of the report date
        email_time_date = dt.datetime.strptime(report_date, '%Y-%m-%d') + dt.timedelta(days=1)
        email_time = str(email_time_date.replace(hour=1, minute=30, second=0, microsecond=0))
        report_content = {
            'email_time': email_time,
            'report_date': report_date,
            'new_harmonics': new_harmonics.to_dict(),
            'report_harmonics': report_harmonics.to_dict()
        }
        # Add report to database
        post_url = "https://analytics-ecs-api.voltaenergy.ca{}/crud/notifications/".format(server)
        post_body = {
            'time': email_time,
            'type': report_type,
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
