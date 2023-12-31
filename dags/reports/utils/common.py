import datetime as dt
import pandas as pd
import requests


# Get list of nodes to run current hat report
def get_node_df(ANALYTICS_API_TOKEN):
    url = "https://analytics-ecs-api.voltaenergy.ca/internal/crud/portfolio/"
    headers = {'Authorization': 'Bearer ' + ANALYTICS_API_TOKEN}

    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        return pd.DataFrame()
    response.raise_for_status()
    equipment_dict_list = response.json()['content']
    equipment_df = pd.DataFrame(equipment_dict_list)

    # Drop rows where node_details is None
    equipment_df = equipment_df.dropna(subset=['node_details'])
    # Remove rows where deployment status is not "Deployed"
    equipment_df = equipment_df[equipment_df['node_details'].apply(lambda x: x['currentDeploymentStatus'] == 'Deployed')].reset_index(drop=True)
    return equipment_df


# Generate list of dates to backfill in ascending order
def generate_dates(start_date, end_date):
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(str(d))
        d += dt.timedelta(days=1)
    return dates

