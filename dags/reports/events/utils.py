from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger
import datetime as dt
import pandas as pd
import requests
import json


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
    else:
        tolerance = 0.3
    return tolerance


@logger.catch
def requests_http_session(
        jwt_token,
        retries=5,
        backoff_factor=1,
        status_forcelist=[502],
):
    """
    Create a requests session with retry logic
    Returns a requests session object
    """
    try:
        session = requests.Session()
    except Exception as e:
        # Get error name
        error_name = e.__class__.__name__
        logger.error(f"Failed to create requests session: {error_name}")
        raise e

    # Add JWT token to headers
    session.headers.update({"Authorization": f"Bearer {jwt_token}"})
    retry_logic = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry_logic)
    session.mount('https://', adapter)
    return session


@logger.catch
def equipment_portfolio_df(api_token):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/portfolio/"
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    response = session.get(url=url)
    if response.status_code == 204:
        return pd.DataFrame()
    response.raise_for_status()
    equipment_dict_list = response.json()['content']
    equipment_df = pd.DataFrame(equipment_dict_list)
    # Drop rows where node_details is None
    equipment_df = equipment_df.dropna(subset=['node_details'])
    # Only keep equipments that are Deployed
    equipment_df = equipment_df[
        equipment_df['node_details'].apply(lambda x: x['currentDeploymentStatus'] == 'Deployed')
    ].reset_index(drop=True)
    return equipment_df


@logger.catch
# Get harmonic data for a specific node and lf
def get_daily_avg_harmonic_data(location_node_id, start_epoch, end_epoch, harmonic_lf, parameter, api_token):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/app/harmonics/daily_harmonic_avgs/"
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    params = {
        'location_node_id': location_node_id,
        'start_epoch': start_epoch,
        'end_epoch': end_epoch,
        'search_harmonic': harmonic_lf,
        'parameter': parameter,
    }
    response = session.get(url=url, params=params)
    response.raise_for_status()
    if response.status_code == 200:
        harmonic_dict = response.json()
        # Create dataframe from dictionary
        harmonic_df = pd.DataFrame.from_dict(harmonic_dict['content']['harmonics_trend_1d'])
        # Parse time column as datetime '%Y-%m-%d %H:%M:%S%z'
        harmonic_df['time'] = pd.to_datetime(harmonic_df['time'], format='%Y-%m-%d %H:%M:%S%z')
        # Drop rows where harmonic_lf > 0.994 and harmonic_lf < 1.006
        harmonic_df = harmonic_df[
            (harmonic_df['harmonic_lf'] < 0.994) |
            (harmonic_df['harmonic_lf'] > 1.006)
            ].reset_index(drop=True)
    else:
        harmonic_df = pd.DataFrame()
    return harmonic_df


@logger.catch
async def post_daily_harmonics_avg(harmonic_dict, api_token):
    # Create post_body
    post_body = {
        "time": str(harmonic_dict['time']),
        "location_node_id": str(harmonic_dict['location_node_id']),
        "node_sn": int(harmonic_dict['node_sn']),
        "harmonic_lf": harmonic_dict['harmonic_lf'],
        "harmonic_value": harmonic_dict['harmonic_value'],
        "grouped_count": harmonic_dict['grouped_count'],
        "harmonic_elec_type": harmonic_dict['harmonic_elec_type'],
    }
    staging_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/harmonics_daily_avgs/"
    production_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/harmonics_daily_avgs/"
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    response = session.post(url=staging_url, json=post_body)
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    response = session.post(url=production_url, json=post_body)
    response.raise_for_status()
    return response.status_code


@logger.catch
async def patch_daily_harmonics_avg(harmonic_dict, api_token):
    # Create patch_body
    patch_body = {
        "time": str(harmonic_dict['time']),
        "location_node_id": str(harmonic_dict['location_node_id']),
        "node_sn": int(harmonic_dict['node_sn']),
        "harmonic_lf": harmonic_dict['harmonic_lf'],
        "harmonic_value": harmonic_dict['harmonic_value'],
        "grouped_count": harmonic_dict['grouped_count'],
        "harmonic_elec_type": harmonic_dict['harmonic_elec_type'],
    }
    staging_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/harmonics_daily_avgs/"
    production_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/harmonics_daily_avgs/"
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    response = session.patch(url=staging_url, json=patch_body)
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    response = session.patch(url=production_url, json=patch_body)
    response.raise_for_status()
    return response.status_code


@logger.catch
# Get harmonics data from API
async def get_harmonic_data(location_node_id, harmonic_avg_date, parameter, lf_range, api_token):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/harmonics/harmonic_data/"
    query_params = {
        'location_node_id': location_node_id,
        'utc_start_date': str(harmonic_avg_date),
        'utc_end_date': str(harmonic_avg_date),
        'parameter': parameter,
        'lower_lf_range': float(lf_range[0]),
        'upper_lf_range': float(lf_range[1])
    }
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    # Create API Call for Harmonics
    response = session.get(url, params=query_params)
    # print url with query params
    # logger.debug(f"API Call: {response.url}")
    if response.status_code == 422:
        # Check response for error message
        error_message = response.json()['Details']
        if 'less than Equipment Start Date' in error_message:
            logger.warning(f"No harmonics data for {harmonic_avg_date}")
            return pd.DataFrame()
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
    # If harmonic_freq in harmonic_df, rename to harmonic_lf
    if 'harmonic_freq' in harmonic_df.columns:
        harmonic_df.rename(columns={'harmonic_freq': 'harmonic_lf'}, inplace=True)
    return harmonic_df


@logger.catch
async def process_harmonic_data(daily_harmonic_data):
    # Order by harmonic_lf column in ascending order
    daily_harmonic_data = daily_harmonic_data.sort_values(by='harmonic_lf', ascending=True).reset_index(drop=True)
    # Remove line_frequency column if it exists
    if 'line_frequency' in daily_harmonic_data.columns:
        daily_harmonic_data = daily_harmonic_data.drop(columns=['line_frequency'])
    # Round values in harmonic_lf based on value returned from get_lf_tolerance
    daily_harmonic_data['harmonic_lf'] = daily_harmonic_data['harmonic_lf'].apply(lambda x: round(x, int(len(str(get_lf_tolerance(x)).split('.')[1])) + 1))
    # Resample daily on harmonic_lf and time, add new column with count of grouped items
    daily_harmonic_avg = daily_harmonic_data.groupby([pd.Grouper(key='time', freq='D'), 'harmonic_lf']).agg(
        harmonic_value=('harmonic_value', 'mean'),
        grouped_count=('harmonic_value', 'count')
    ).reset_index()
    if daily_harmonic_avg.empty:
        # Create new df from list of new records
        processed_harmonic_data = pd.DataFrame([])
        return processed_harmonic_data
    # Create list of unique harmonic_lf values
    harmonic_lf_list = daily_harmonic_avg['harmonic_lf'].unique().tolist()
    # First next_harmonic_lf is from 1 LF range
    next_harmonic_lf = get_lf_tolerance(harmonic_lf_list[0])
    last_included_harmonic_lf = next_harmonic_lf
    # List of new records for processed_harmonic_data
    new_records = []
    count = 0
    # Loop through harmonic_lf_list
    for harmonic_lf in harmonic_lf_list:
        count += 1
        tolerance = get_lf_tolerance(harmonic_lf)
        # print(f"{count}: Harmonic LF: {harmonic_lf}, Next Harmonic LF: {next_harmonic_lf}, Tolerance: {tolerance}")
        # If harmonics within tolerance with previous harmonic the skip
        if harmonic_lf < next_harmonic_lf:
            continue
        # Push next harmonic selection beyond current tolerance
        next_harmonic_lf = round(harmonic_lf + (tolerance * 2), 4)
        # Slice out harmonic frame for selected harmonic_lf
        given_harmonic_frame = daily_harmonic_avg[
            (daily_harmonic_avg['harmonic_lf'].ge(harmonic_lf - tolerance)) &
            (daily_harmonic_avg['harmonic_lf'].lt(harmonic_lf + tolerance)) &
            (daily_harmonic_avg['harmonic_lf'].gt(last_included_harmonic_lf))
            ].copy()
        if given_harmonic_frame.empty:
            continue
        new_row_dict = {
            'time': given_harmonic_frame['time'].iloc[0],
            'harmonic_lf': harmonic_lf,
            'harmonic_value': given_harmonic_frame['harmonic_value'].max(),
            'grouped_count': given_harmonic_frame['grouped_count'].sum()
        }
        new_records.append(new_row_dict)
        last_included_harmonic_lf = given_harmonic_frame['harmonic_lf'].max()
    # Create new df from list of new records
    processed_harmonic_data = pd.DataFrame(new_records)
    return processed_harmonic_data


@logger.catch
async def get_events_count_24h(location_node_id, start_date, end_date, api_token):
    event_count_frame = pd.DataFrame()
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/events/utc_events_count_24h/"
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date),
        'end_date': str(end_date),
    }
    response = session.get(url=url, params=params)
    response.raise_for_status()
    if response.status_code == 200:
        event_count_frame = pd.DataFrame(response.json()['content'])
        # Parse time column as datetime '%Y-%m-%dT%H:%M:%S%z'
        event_count_frame['time'] = pd.to_datetime(event_count_frame['time'], format='%Y-%m-%dT%H:%M:%S%z')
        # Change timestamp format to '%Y-%m-%d %H:%M:%S'
        event_count_frame['time'] = event_count_frame['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return event_count_frame


@logger.catch
# Get outlie events from analytics API
def get_outlier_events(report_date, location_node_id, np_current, outlier_lt_days, analytics_api_token):
    # Create start and end dates
    end_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
    start_date = end_date - dt.timedelta(days=outlier_lt_days)
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/reports/events/utc_outlier_events/"
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(start_date.date()),
        'end_date': str(end_date.date()),
        'np_current': np_current
    }
    # Get request session from helper
    session = requests_http_session(jwt_token=analytics_api_token)
    # Make request to get events data report API
    response = session.get(url, params=query_params)
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


@logger.catch
async def get_daily_1_sec_data_agg(location_node_id, data_start_date, data_end_date, api_token):
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_reports/1_sec_daily_agg/"
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(data_start_date),
        'end_date': str(data_end_date),
    }
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    # Make request to get events data report API
    response = session.get(url, params=query_params)
    response.raise_for_status()
    if response.status_code == 204:
        return pd.DataFrame()
    # Receive response in JSON
    data_dict = response.json()['content']
    # Convert dict to DataFrame
    data_df = pd.DataFrame(data_dict)
    # Parse time column as datetime '%Y-%m-%dT%H:%M:%S%z'
    data_df['time'] = pd.to_datetime(data_df['time'], format='%Y-%m-%dT%H:%M:%S%z')
    return data_df


@logger.catch
async def get_daily_node_data_agg(location_node_id, data_start_date, data_end_date, api_token):
    # Create URL, headers and query params
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/app/trending/24_hr_trend/"
    query_params = {
        'location_node_id': location_node_id,
        'start_date': str(data_start_date),
        'end_date': str(data_end_date),
    }
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    # Make request to get events data report API
    response = session.get(url, params=query_params)
    response.raise_for_status()
    if response.status_code == 204:
        return pd.DataFrame()
    # Receive response in JSON
    data_dict = response.json()['content']
    # Convert dict to DataFrame
    data_df = pd.DataFrame(data_dict)
    # Parse time column as datetime '%Y-%m-%dT%H:%M:%S%z'
    data_df['time'] = pd.to_datetime(data_df['time'], format='%Y-%m-%d %H:%M:%S%z')
    return data_df


@logger.catch
def post_events_report(events_report, api_token):
    staging_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/daily_reports/"
    production_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_reports/"
    # Make report_content as JSON
    post_body = {
        'time': events_report['time'],
        'report_type': events_report['report_type'],
        'report_version': int(events_report['report_version']),
        'report_content': json.dumps(events_report['report_content'])
    }
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    # Make request to get events data report API
    response = session.post(staging_url, json=post_body)
    # Get request session from helper
    session = requests_http_session(jwt_token=api_token)
    # Make request to get events data report API
    response = session.post(production_url, json=post_body)
    response.raise_for_status()
    if response.status_code == 201:
        return True
    else:
        return False
