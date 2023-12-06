from reports.hat.utils.hat_utils import get_user_input_from_args, HatScanProcessor, PostHatScanData, get_harmonic_data_v2
from reports.utils.common import get_analytics_portfolio
from dotenv import load_dotenv
from reports.hat.config.config import HatConfigs
import os
import datetime as dt
from reports.utils.harmonics import get_harmonic_signatures
from concurrent.futures import ProcessPoolExecutor as Pool
from functools import partial
from loguru import logger
import time

VERSION = 1.3

def process_harmonics(location, api_token, signature_harmonics, user_inputs):
    report_configs = HatConfigs.get_hat_configs(location=location)
    # data_end date is report_date
    data_end_date = dt.datetime.strptime(user_inputs.report_date, '%Y-%m-%d').date()
    # data_start date will be lt_avg_days days from start_date
    data_start_date = data_end_date - dt.timedelta(days=report_configs.lt_data_days)

    # Get harmonic data for location
    harmonic_dump_df = get_harmonic_data_v2(location_node_id=location.location_node_id,
                                            start_date=data_start_date,
                                            end_date=data_end_date,
                                            parameter=user_inputs.report_type,
                                            api_token=api_token,
                                            product_type=location.product_type)
    if not harmonic_dump_df.empty:
        hat_processor = HatScanProcessor(report_date=user_inputs.report_date,
                                        hat_configs=report_configs,
                                        location=location)
        # Process harmonic generic
        hat_processor.process_hat_scan(harmonics_df=harmonic_dump_df)
        # Process Signature harmonics
        if not signature_harmonics.empty:
            filtered_signature_harmonics = signature_harmonics[signature_harmonics['location_node_id'] == location.location_node_id]
            if not filtered_signature_harmonics.empty:
                signature_harmonic_list = filtered_signature_harmonics.explode('harmonic_lf')['harmonic_lf'].to_list()
                for harmonic in signature_harmonic_list:
                    logger.info(f"Scanning for Signature harmonic {harmonic} - SN: {location.node_sn}")
                    signature_harmonic_data = get_harmonic_data_v2(location_node_id=location.location_node_id,
                                                                    start_date=data_start_date,
                                                                    end_date=data_end_date,
                                                                    parameter=user_inputs.report_type,
                                                                    api_token=api_token,
                                                                    product_type=location.product_type,
                                                                    search_harmonic=harmonic)
                    
                    if not signature_harmonic_data.empty:
                        hat_processor.process_hat_scan(signature_harmonic_data)
        # Get the processed result
        hat_result = hat_processor.get_result()
        # # post the results to the DB
        if not hat_result.empty and not user_inputs.debug:
            PostHatScanData.post_data(hat_scan_df=hat_result,
                                      report_date=user_inputs.report_date,
                                      api_token=api_token,
                                      location=location,
                                      report_type=user_inputs.report_type.lower(),
                                      server=user_inputs.environment)

  
if __name__ == "__main__":
    
    user_inputs = get_user_input_from_args()
    logger.info(f"{user_inputs.report_type} Hat Scan Init for {user_inputs.report_date} - Version: {VERSION}")
    process_start_time = time.time()
    
    load_dotenv(f'{os.getcwd().replace("/dags", "")}/.{user_inputs.environment}.env')
    # Get Secrets from Environment Variables
    api_token = str(os.getenv('ANALYTICS_FILE_PROCESSORS_API_TOKEN'))
    email_app_pass = str(os.getenv("GMAIL_APP_PASSWORD"))

    locations_portfolio = get_analytics_portfolio(server_path='', analytics_api_token=api_token)

    # filter locations
    locations_portfolio = [location for location in locations_portfolio if location.deployment_issue is False and 
                          location.current_deployment_status == 'Deployed'
                          and location.eq_type != 'dc'
                          and location.work_cycle == False]
    # get the harmonic signatures we use production
    signature_harmonics = get_harmonic_signatures(api_token=api_token, server='')
    # process_harmonics(user_inputs=user_inputs, api_token=api_token, signature_harmonics=signature_harmonics,location=locations_portfolio[0])
    with Pool(4) as pool:
        status = pool.map(partial(process_harmonics,user_inputs=user_inputs, api_token=api_token, signature_harmonics=signature_harmonics), locations_portfolio)
    total_report_time =  round(time.time() -  process_start_time,2) 

    logger.info(f"{user_inputs.report_type} Hat Scan Finished For {user_inputs.report_date}  - Version: {VERSION} Time Taken: {total_report_time} sec")