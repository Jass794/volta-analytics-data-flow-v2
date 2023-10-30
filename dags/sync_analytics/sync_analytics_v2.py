"""
This script sync analytics Portfolio from the admin portfolio.

Author: Jaspreet Singh
Version: 1

Usage:
    python sync_analytics [production|staging]

Environment Files:
    - dev.env: Contains environment variables for development.
    - prod.env: Contains environment variables for production.

"""
import datetime as dt
import json
import math
import os
import sys

import notifiers
import pytz
import requests
from dotenv import load_dotenv
from loguru import logger

from data_models.portfolio_v2_models import PortfolioModelSyncAnalytics, PortalApiModel, \
    Location as PortalLocation, \
    Facility as PortalFacility, \
    NodeDetails as PortalNodeDetails, \
    NodeConfigs as AnalyticsNodeConfigs


def remove_nan_null(value):
    if value is None:
        return 0
    elif math.isnan(value):
        return 0
    else:
        return value




@logger.catch
# Get harmonic freq scan dict from metadata file
def get_harmonic_freq_scan_dict(location_node_id, meta_data_dict):
    # Check if there is any key = location_node_id
    if location_node_id in meta_data_dict.keys():
        # Get harmonic freq scan dict
        harmonic_freq_scan_dict = meta_data_dict[location_node_id]
    else:
        harmonic_freq_scan_dict = None
    return harmonic_freq_scan_dict


@logger.catch
# Load imbalance pairs
def load_imbalace_location_pairs(node_sn):
    pair = []
    if node_sn == 21044:
        pair = ['5f74cc01abdd8e19a2b2a258.0']
    elif node_sn == 21045:
        pair = ['5f74cc1babdd8e19a2b2a26a.0']
    elif node_sn == 21046:
        pair = ['5f74cbd4abdd8e19a2b2a230.0']
    elif node_sn == 21047:
        pair = ['5f74cbeeabdd8e19a2b2a246.0']
    elif node_sn == 21304:
        pair = ['63e2c0e3abdd8e287378eada.0']
    elif node_sn == 21305:
        pair = ['63e2c0e3abdd8e287378ead9.0']
    else:
        pair = []
    # Sort list if not empty
    if len(pair) > 0:
        pair.sort()
    return pair


@logger.catch
# Get customer portfolio from portal
def get_portal_portfolio(portal_api_token):
    # Create URL, headers and query param
    url = 'https://portal.voltainsite.com/api/customers'
    # Get request session from helper
    response = requests.get(url, headers=portal_api_token)
    # Query for response
    json_response = response.json()['data']
    # Create pedantic loc list
    portal_locations = [PortalApiModel(**loc) for loc in json_response]
    return portal_locations


@logger.catch
# Get latest datafile given location_node_id
def get_analytics_portfolio(server_path, analytics_api_token):
    # Create URL, headers and query params
    url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server_path}/crud/v2/portfolio/'

    # Get request session from helper
    response = requests.get(url, headers=analytics_api_token)
    response.raise_for_status()
    portfolio_list = []
    if response.status_code == 200:
        portfolio_list = response.json()['content']
        portfolio_list = [PortfolioModelSyncAnalytics(**loc) for loc in portfolio_list]
    return portfolio_list


def map_location_portal_to_analytics(portal_customer: PortalApiModel,
                                     portal_facility: PortalFacility,
                                     portal_node_details: PortalNodeDetails,
                                     portal_location: PortalLocation,
                                     portal_node_configs: AnalyticsNodeConfigs,
                                     portal_location_node_id: str,
                                     harmonics_values) -> PortfolioModelSyncAnalytics:
    np_voltage = portal_node_configs.np_voltage
    np_current = portal_node_configs.np_current
    np_hp = portal_node_configs.np_hp
    np_rpm = portal_node_configs.np_running_speed
    np_poles = portal_node_configs.np_poles
    np_rotor_bars = portal_node_configs.np_rotor_bars
    np_stator_slots = portal_node_configs.np_stator_slots

    location_name = portal_location.displayName
    node_sn = int(portal_location.locationNodeIds[portal_location_node_id])
    # Split location_node_id and if 1 int it add Field to equipment name
    if int(portal_location_node_id.split('.')[1]) == 1:
        location_name = portal_location.displayName + ' Field'
    if portal_node_details.type == 'SEL':
        np_voltage = remove_nan_null(portal_location.npVoltage)
        np_current = remove_nan_null(portal_location.npCurrent)
        np_hp = remove_nan_null(portal_location.npHp)
        np_rpm = remove_nan_null(portal_location.npRpm)
        np_poles = remove_nan_null(portal_location.npPoles)
        np_rotor_bars = remove_nan_null(portal_location.npRotorBars)

    analytics_location = PortfolioModelSyncAnalytics(
        customer_id=portal_customer.id,
        customer_code=portal_customer.code,
        facility_id=portal_facility.id,
        location_id=portal_location.id,  # todo: check again
        location_node_id=portal_location_node_id,
        node_sn=node_sn,
        secondary_node_sn=portal_location.secondaryNodeSerialNumber,
        customer_name=portal_customer.name,
        facility_name=portal_facility.name,
        location_name=location_name,
        equipment_type=portal_location.equipmentType,
        location_timezone=portal_location.timezone,
        data_start_epoch=portal_location.equipmentStartTimestamp,
        equipment_start_epoch=portal_location.equipmentStartTimestamp,
        work_cycle=bool(portal_node_configs.wc),
        np_voltage=np_voltage,
        np_current=np_current,
        np_frequency=portal_node_configs.np_frequency,
        np_hp=np_hp,
        np_rpm=int(np_rpm),
        np_poles=np_poles,
        np_rotor_bars=np_rotor_bars,
        np_stator_slots=int(np_stator_slots),
        i_noise=portal_node_configs.i_noise,
        v_noise=portal_node_configs.v_noise,
        starter=portal_location.starter or '---',
        load_application=portal_location.loadApplication or '---',
        ct_location=portal_location.ctLocation or '---',
        v_tap_location=portal_location.voltageTapLocation or '---',
        load_imbalance_pairs=load_imbalace_location_pairs(node_sn),
        harmonic_frequencies_scan=get_harmonic_freq_scan_dict(portal_location_node_id, harmonics_values),
        tr_i_max=portal_node_configs.tr_i_max,
        active_ia=portal_node_details.activeIa,
        active_ib=portal_node_details.activeIb,
        active_ic=portal_node_details.activeIc,
        active_pa=portal_node_details.activePa,
        active_pb=portal_node_details.activePb,
        active_pc=portal_node_details.activePc,
        container=portal_node_details.container,
        voltage_boundary_upper_percent=portal_node_details.voltageBoundaryUpperPercent,
        voltage_imbalance_boundary_upper_percent=portal_node_details.voltageImbalanceBoundaryUpperPercent,
        voltage_imbalance_threshold=portal_node_details.voltageImbalanceThreshold,
        voltage_thd_boundary_upper_percent=portal_node_details.voltageThdBoundaryUpperPercent,
        voltage_thd_threshold=portal_node_details.voltageThdThreshold,
        current_boundary_upper_percent=portal_node_details.currentBoundaryUpperPercent,
        current_deployment_status=portal_node_details.currentDeploymentStatus,
        current_deployment_status_update_timestamp=portal_node_details.currentDeploymentStatusUpdateTimestamp,
        current_imbalance_boundary_upper_percent=portal_node_details.voltageImbalanceBoundaryUpperPercent,
        current_imbalance_threshold=portal_node_details.currentImbalanceThreshold,
        current_thd_boundary_upper_percent=portal_node_details.currentThdBoundaryUpperPercent,
        current_thd_threshold=portal_node_details.currentThdThreshold,
        deployment_issue=portal_node_details.deploymentIssue,
        deprecation_notice=portal_node_details.deprecationNotice,
        disable_monitoring=portal_node_details.disableMonitoring,
        ip_address=portal_node_details.ipAddress,
        loss_of_phase_transient_threshold=portal_node_details.lossOfPhaseTransientThreshold,
        mac_address=portal_node_details.macAddress,
        monitor_data_point_alerts=portal_node_details.monitorDataPointAlerts,
        nameplate_valid=portal_node_details.nameplateValid,
        new_deployment_status=portal_node_details.newDeploymentStatus,
        new_deployment_status_update_timestamp=portal_node_details.currentDeploymentStatusUpdateTimestamp,
        notify_on_connect=portal_node_details.notifyOnConnect,
        notify_on_current_above_noise=portal_node_details.notifyOnCurrentAboveNoise,
        over_current_threshold=portal_node_details.overCurrentThreshold,
        over_voltage_threshold=portal_node_details.overVoltageThreshold,
        pause_notifications=portal_node_details.pauseNotifications,
        product_type=portal_node_details.type,
        under_voltage_threshold=portal_node_details.underVoltageThreshold,
        alert_library_flags=portal_location.faultLibraryAlertFlags,
        eq_type=portal_node_configs.eq_type,
        eq_type_sub=portal_node_configs.eq_type_sub,
        np_sf=portal_node_configs.np_sf)

    return analytics_location


@logger.catch
# Get latest datafile given location_node_id
def get_latest_datafile(location_node_id, server_path, analytics_api_token):
    # Create URL, headers and query params
    url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server_path}/crud/node_data/latest_header/'
    query_params = {'location_node_id': location_node_id}
    # Get request session from helper
    response = requests.get(url, params=query_params, headers=analytics_api_token)
    response.raise_for_status()
    if response.status_code == 200:
        datafile_dict = response.json()['content']
        datafile_header_dict = datafile_dict['file_headers']
    else:
        datafile_header_dict = None
    return datafile_header_dict


@logger.catch
# Get node nameplate from Portal
def get_node_configs(node_sn, portal_api_token):
    url = 'https://portal.voltainsite.com/api/nodes/{}/config'.format(str(node_sn))
    # Get request session from helper
    response = requests.get(url, headers=portal_api_token)
    response.raise_for_status()
    if response.status_code == 400:
        hdr_dict = None
    else:
        response.raise_for_status()
        if response.status_code == 200:
            hdr_dict = response.json()['data']
            # Check if dict is empty
            if hdr_dict == {}:
                hdr_dict = None
        else:
            hdr_dict = None
    return hdr_dict


@logger.catch
# Inset data to analytics
def insert_to_analytics(equipment_dict, server_path, analytics_api_token):
    # Create URL, headers and query params
    url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server_path}/crud/v2/portfolio/'
    # Get request session from
    put_response = requests.put(url, json=equipment_dict, headers=analytics_api_token)
    put_response.raise_for_status()
    # If response code is 204, Make a post request
    if put_response.status_code != 200:
        # Get request session from helper
        post_response = requests.post(url, json=equipment_dict, headers=analytics_api_token)
        post_response.raise_for_status()
        status = 'Inserted'
    else:
        status = 'Updated'
    return status


@logger.catch
# Inset data to analytics
def insert_to_analytics_archive(equipment_dict, server_path, analytics_api_token):
    # Create URL, headers and query params
    url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server_path}/crud/v2/portfolio_archive/'

    # Get request session from
    put_response = requests.put(url, json=equipment_dict, headers=analytics_api_token)
    put_response.raise_for_status()
    # If response code is 204, Make a post request
    if put_response.status_code != 200:
        # Get request session from helper
        post_response = requests.post(url, json=equipment_dict, headers=analytics_api_token)
        post_response.raise_for_status()
        status = 'Inserted'
    else:
        status = 'Updated'
    return status


@logger.catch
# Delete data from analytics
def delete_from_analytics(decommissioned_location_node_id, server_path, analytics_api_token):
    # Create URL, headers and query params
    url = f'https://analytics-ecs-api.voltaenergy.ca/internal{server_path}/crud/v2/portfolio/{decommissioned_location_node_id}/'
    # Get request session from helper
    response = requests.delete(url, headers=analytics_api_token)
    response.raise_for_status()
    if response.status_code == 200:
        status = 'Deleted'
    else:
        status = 'Not Deleted'
    return status


@logger.catch
def sync_analytics(server_path, server, portal_api_token_header, analytics_api_token_header, meta_data_dict):
    logger.debug("Get Admin Portfolio....")
    # get the portfolio from admin portal
    portal_response = get_portal_portfolio(portal_api_token_header)
    logger.debug("Get Analytics Portfolio....")
    # get the portfolio for analytics
    analytics_portfolio = get_analytics_portfolio(server_path, analytics_api_token_header)
    locations_count = 1
    location_change_list = []
    portal_mapped_locations = []
    # Here we select customer first then go to facility and then goto next individual location to process
    for portal_customer in portal_response:
        # iterate over each Facility for the Customer
        for portal_facility in portal_customer.facilities:
            # iterate over each location for the Facility
            for portal_location in portal_facility.locations:
                # iterate over all the location_node_id for selected location includes node and sub-node
                for portal_location_node_id, node_details in zip(portal_location.locationNodeIds, portal_location.nodes):
                    # Create a location name For logs
                    location_full_name = f"SN: {portal_location.locationNodeIds[portal_location_node_id]} - {portal_location.displayName} - {portal_facility.name} - {portal_customer.name}"
                    logger.info(f"Processing {location_full_name} {locations_count}")
                    # Ignore the subNodes
                    if node_details.type == 'subNode':
                        logger.warning(f"Skipped SubNode {location_full_name} {locations_count}")
                        continue
                    locations_count = locations_count + 1
                    # add to try block to if it fails to get node configs location not get deleted
                    try:
                        # get the node configs from the latest data file
                        node_configs = get_latest_datafile(portal_location_node_id, server_path, analytics_api_token_header)

                        if node_details.currentDeploymentStatus in ['Pre-Deployment', 'Deployed'] and node_configs is None and node_details.type != 'SEL':
                            logger.info('.....Calling Node config for headers because data file configs are null')
                            # Get node configs from Portal API
                            node_configs = get_node_configs(portal_location.locationNodeIds[portal_location_node_id], portal_api_token_header)

                    except Exception as e:
                        logger.warning(f'Failed to fetch node configs from: {e}')
                        node_configs = None
                    # Create pydantic node configs model
                    node_configs = AnalyticsNodeConfigs(**node_configs) if node_configs else AnalyticsNodeConfigs()
                    # Create location according to the analytics location
                    portal_analytics_mapped = map_location_portal_to_analytics(portal_customer,
                                                                               portal_facility,
                                                                               node_details,
                                                                               portal_location,
                                                                               node_configs,
                                                                               portal_location_node_id,
                                                                               meta_data_dict)

                    portal_mapped_locations.append(portal_analytics_mapped)
                    # compare the location with analytics
                    if portal_analytics_mapped not in analytics_portfolio:
                        present_values = []
                        updated_values = []
                        # Here we are finding the differences between the old data and updated data
                        for analytics_location in analytics_portfolio:
                            if portal_analytics_mapped.node_sn == analytics_location.node_sn:
                                analytics_location_dict = analytics_location.dict()
                                for key, value in portal_analytics_mapped.dict().items():
                                    if value != analytics_location_dict[key]:
                                        updated_values.append({key: value})
                                        present_values.append({key: analytics_location_dict[key]})

                        if updated_values:
                            logger.info(f'{portal_analytics_mapped.node_sn} present values {present_values}')
                            logger.info(f'{portal_analytics_mapped.node_sn} new values {updated_values}')
                        else:
                            updated_values = [portal_analytics_mapped.dict()]
                            logger.info(f'{portal_analytics_mapped.node_sn} adding it as a new location....')

                        location_change_list.append({
                            "node_sn": portal_analytics_mapped.node_sn,
                            "present_values": present_values,
                            "updated_values": updated_values,
                        })
                        insert_status = insert_to_analytics(portal_analytics_mapped.dict(), server_path, analytics_api_token_header)
                        logger.success(f'.....{server} - {insert_status} - {portal_analytics_mapped.node_sn}')
    logger.info(f"{len(location_change_list)} location updated/added....")
    # get the updated locations
    analytics_portfolio = get_analytics_portfolio(server_path, analytics_api_token_header)
    # check if there are location to prevent unnecessary deletion
    delete_locations_list = []
    if portal_mapped_locations:
        # Create a list of locations to delete from Analytics
        delete_locations_list = [location for location in analytics_portfolio if location not in portal_mapped_locations]
        # added a filter to stop accidental deletion of location
        if len(delete_locations_list) < 20:
            for location in delete_locations_list:
                # delete the locations from analytics
                status = delete_from_analytics(location.location_node_id, server_path, analytics_api_token_header)
                logger.success('.....Deleting... {} - {} - {}'.format(server, status, location.node_sn))
                # add the deleted locations to archive portfolio
                status = insert_to_analytics_archive(location.dict(), server_path, analytics_api_token_header)
                logger.success('.....Archive Insert {} - {} - {}'.format(server, status, location.node_sn))
        else:
            logger.error("error occur because delete count is not legit.. Please verify it")

    portal_logs = f"Total location Update {len(location_change_list)}\n Changes = {location_change_list}\n Deleted locations = {delete_locations_list}"
    logger.info(portal_logs)


def sync_analytics_wrapper(environment):
    if environment == 'staging':
        server_path = '/staging'
        server = 'staging'
    else:
        server_path = ''
        server = 'production'
    # load the env file

    load_dotenv(f'{os.getcwd()}/.{server}.env')

    # Create headers for api calls
    portal_api_token_header = {'Authorization': f"Bearer {os.getenv('PORTAL_ADMIN_API_TOKEN')}"}
    analytics_api_token_header = {'Authorization': f"Bearer {os.getenv('ANALYTICS_SYNC_PORTAL_API_TOKEN')}"}

    utc_datetime = str(dt.datetime.utcnow().strftime('%Y%m%d_%H%M00'))
    log_file_name = '{}.txt'.format(utc_datetime)

    # create log dir path
    log_dir_path = f'{os.getcwd()}/.logs/sync-analytics/{server}/'

    # Email on Error in Log file
    def email_log_on_error(log_filepath):
        utc_minutes = int(dt.datetime.now(pytz.utc).strftime('%M'))
        # Notify of Error every 15 mins
        if utc_minutes % 15 == 0:
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
                    "password": os.getenv('GMAIL_APP_PASSWORD'),
                    "to": "analytics-data-flow-errors@voltainsite.com",
                    "subject": "[{}] Error Syncing Analytics - {}".format(server.title(), utc_datetime_email),
                }
                notifier = notifiers.get_notifier("gmail")
                notifier.notify(message="Log File attached!".format(server.title()), **params)
        return 0

    # Create directories if they do not exist
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)

    process_logger = logger.add(
        log_dir_path + log_file_name, enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {function}:{line} | {message}",
        compression=email_log_on_error
    )

    # Read Harmonic Frequency JSON file
    with open(f'./harmonic_freq_scan_meta.json', 'r') as f:
        meta_data_dict = json.load(f)
        # Close file
        f.close()

    sync_analytics(server_path, server, portal_api_token_header, analytics_api_token_header, meta_data_dict)


if __name__ == '__main__':

    # Create argument list
    args = sys.argv[1:]
    # If first argument is not from ['staging', 'production'], exit
    if args[0] not in ['staging', 'production']:
        print('Invalid argument. Please use staging or production as first argument.')
        sys.exit()

    environment = args[0]

    sync_analytics_wrapper(environment)
