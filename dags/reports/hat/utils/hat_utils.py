from reports.hat.models.user_input import UserInput
import sys
import re
import datetime as dt
from reports.hat.models.configs import HatConfigsModel
import pandas as pd
from loguru import logger
import json
from volta_analytics_db_models.tabels.portfolio_v2_models import PortfolioV2Model
import requests

HARMONIC_SCAN_LOWER_LF_RANGE = 0
HARMONIC_SCAN_UPPER_LF_RANGE = 2


# Get harmonics data from API
def get_harmonic_data_v2(location_node_id, start_date, end_date, parameter, api_token, product_type, search_harmonic=None):
    """It gets harmonic data from the sever

    Args:
        location_node_id (_type_): _description_
        start_date (_type_): _description_
        end_date (_type_): _description_
        parameter (_type_): _description_
        api_token (_type_): _description_
        product_type (_type_): _description_
        search_harmonic (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: pd.Datataframe
    """
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
    if search_harmonic:
        query_params = {
        'location_node_id': location_node_id,
        'utc_start_date': str(start_date),
        'utc_end_date': str(end_date),
        'parameter': parameter,
        'search_harmonic': search_harmonic,
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


def process_harmonic_data_v4(harmonic_frame, harmonic_list, st_avg_days, lt_avg_days, max_peak_days, scan_date,  scan_type, location_dict):
    """I process the harmonics which include averaging and keeping peaks

    Args:
        harmonic_frame (_type_): _description_
        harmonic_list (_type_): _description_
        st_avg_days (_type_): _description_
        lt_avg_days (_type_): _description_
        max_peak_days (_type_): _description_
        scan_date (_type_): _description_
        scan_type (_type_): _description_
        location_dict (_type_): _description_

    Returns:
        _type_: dict
    """
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
        if (len(st_harmonics_frame.index) < 15 or len(lt_harmonics_frame.index) < 75) and location_dict['product_type'] == 'Node' :
            print(f'Skipping due to count is not desired st count {len(st_harmonics_frame.index)}, lt count {len(lt_harmonics_frame.index)}  of Harmonic LF {harmonic_lf}')
            continue
    
        elif location_dict['product_type'] != 'Node' and len(st_harmonics_frame.index) < 5:
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
                                                  (given_harmonic_frame.index < st_slicing_date)].resample('12H').mean().dropna()
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


def is_valid_date_format(date_string):
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    return bool(pattern.match(date_string))

def get_user_input_from_args() -> UserInput:
    """I gets the user inputs and validate them

    Returns:
        UserInput: 
    """
    args = sys.argv

    if '-t' not in args:
        print("Please provide '-t Current/Voltage' for type")
        sys.exit(1)

    report_type_index = args.index('-t') + 1
    report_type = args[report_type_index]

    if report_type not in ['Current', 'Voltage']:
        print("Report type should be either 'Current' or 'Voltage'")
        sys.exit(1)

    environment = 'production' if 'production' in args else 'staging'
    debug = '-d' in args

    utc_date_index = args.index('-sd') + 1 if '-sd' in args else None
    utc_date = args[utc_date_index] if utc_date_index is not None else (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

    if not is_valid_date_format(utc_date):
        print("Invalid date format. Please enter a date in the format YYYY-MM-DD.")
        sys.exit(1)

    return UserInput(report_date=utc_date, environment=environment, report_type=report_type, debug=debug)


class HatScanProcessor:
    """It is responsible for harmonic amplitude trending scan
    """
    def __init__(self, report_date, location:PortfolioV2Model, hat_configs:HatConfigsModel) -> None:
        self.hat_configs = hat_configs
        self.report_date =report_date
        self.location = location
        self.loc_log_name = f"{location.node_sn} - {location.location_name} - {location.facility_name}"
        self.harmonic_result_df = pd.DataFrame() 

    def process_hat_scan(self, harmonics_df):
        
        data_first_datetime = harmonics_df['time'].min()
        data_end_datetime = harmonics_df['time'].max()
        total_data_days = (data_end_datetime - data_first_datetime).days

        perform_st_scan = True
        perform_lt_scan = True
        # If time difference between first and last time is less than lt_avg_days days, then skip process\
        if total_data_days < self.hat_configs.lt_data_days:
            perform_lt_scan = False
            logger.warning('Not enough Harmonic Data for LT scan - {}'.format(self.loc_log_name))
        if total_data_days < self.hat_configs.st_scan_lt_avg_days:
            perform_st_scan = False
            logger.warning('Not enough Harmonic Data for ST scan - {}'.format(self.loc_log_name))
         # Scan end day end time
        scan_end_day_end_time = dt.datetime.strptime(self.report_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=999999)
        # Scan end day start time
        scan_end_day_start_time = scan_end_day_end_time.replace(hour=0, minute=0, second=0, microsecond=0)

        scan_end_day_start_time = scan_end_day_start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        # Only select the recent harmonics
        st_frame = harmonics_df[(harmonics_df['time'] == data_end_datetime)].reset_index(drop=True).copy()
        # Sort by harmonic_freq ascending
        sorted_harmonic_list = st_frame.sort_values(by='harmonic_freq')['harmonic_freq'].tolist()
        logger.info('Total Harmonics to Scan - {} - {}'.format(len(sorted_harmonic_list), self.loc_log_name))
        # create empty df
        processed_daily_st_scan = pd.DataFrame()
        processed_daily_scan_long_term = pd.DataFrame()

        if perform_st_scan:
            # Process daily scan frame for short term
            processed_daily_st_scan = process_harmonic_data_v4(harmonic_frame=harmonics_df.copy(),
                                                               harmonic_list=sorted_harmonic_list,
                                                               st_avg_days=self.hat_configs.st_scan_st_avg_days,
                                                               lt_avg_days=self.hat_configs.st_scan_lt_avg_days,
                                                               max_peak_days=self.hat_configs.st_scan_look_back_days,
                                                               scan_date=scan_end_day_start_time,
                                                               scan_type='short_term',
                                                               location_dict=dict(self.location))
            processed_daily_st_scan = pd.DataFrame(processed_daily_st_scan)

        if perform_lt_scan:
            # Process daily scan frame for long term
            processed_daily_scan_long_term = process_harmonic_data_v4(harmonic_frame=harmonics_df.copy(),
                                                                      harmonic_list=sorted_harmonic_list,
                                                                      st_avg_days=self.hat_configs.lt_scan_st_avg_days,
                                                                      lt_avg_days=self.hat_configs.lt_scan_lt_avg_days,
                                                                      max_peak_days=self.hat_configs.lt_scan_look_back_days,
                                                                      scan_date=scan_end_day_start_time,
                                                                      scan_type='long_term',
                                                                      location_dict=dict(self.location))

            processed_daily_scan_long_term = pd.DataFrame(processed_daily_scan_long_term)
        self.harmonic_result_df = pd.concat([self.harmonic_result_df,processed_daily_st_scan, processed_daily_scan_long_term], ignore_index=True)
        return self.harmonic_result_df
        
    def get_result(self):
        return self.harmonic_result_df

class PostHatScanData:
    """It post data to db
    """
    def post_data(hat_scan_df, report_date, api_token,location: PortfolioV2Model, report_type, server='staging'):
        if not hat_scan_df.empty:
            log_name =  f"{location.node_sn} - {location.location_name} - {location.facility_name}"
            # Create post time from report_date
            post_time = dt.datetime.strptime(report_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            headers = {'Authorization': 'Bearer ' + api_token}
            post_body = {
                'time': str(post_time),
                'node_sn': str(location.node_sn),
                'location_node_id': location.location_node_id,
            }
            # Hat Report type
            hat_type = '{}_hat_report_v2'.format(report_type)
            post_body[hat_type] = json.dumps(hat_scan_df.to_dict(orient='dict'))
            # INSERT INTO STAGING DB
            # Add report to database
            logger.info('Adding scan results to staging')
            post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/staging/crud/daily_scans/"
            staging_response = requests.post(url=post_url, json=post_body, headers=headers)
            logger.info('Report for {} Added Staging Status: {}'.format(log_name, staging_response.status_code))
            staging_response.raise_for_status()

            # INSERT INTO PRODUCTION DB
            # Add report to database
            if server == 'production':
                post_url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/daily_scans/"
                prod_response = requests.post(url=post_url, json=post_body, headers=headers)
                logger.info('Report for {} Added Production Status: {}'.format(log_name, prod_response.status_code))
                prod_response.raise_for_status()
        
        return True
