import pandas as pd
import numpy as np
import awswrangler as wr
import time
import json

def get_user_data(user_dict):
    """
    Extract user tag info (for now) and return list
    
    Args:
        user_dict (dict): Dictionary received after parsing input args
        
    Returns:
        user_tags (list): List of tags chose by the user
        user_categories (list): List of wellness wheel categories chosen
    
    Example user dict would be:
    
    user_dict = {"pid": "0987654321",
    "category_tags": [
      {
        "id": 1,
        "name": "Empowerment",
        "category": {
          "id": 4,
          "name": "Mental Health and Wellness"
        }
      },
      {
        "id": 2,
        "name": "International Students",
        "category": {
          "id": 8,
          "name": "Communities and Social"
        }
      },
      {
        "id": 3,
        "name": "International Students",
        "category": {
          "id": 8,
          "name": "Communities and Social"
        }
      }
    ]}
    """
    
    user_tags = []
    user_categories = []
    for tag_object in user_dict['category_tags']:
        user_tags.append(tag_object['name'].strip())
        user_categories.append(tag_object['category']['name'].strip())
    
    # {'user_id': user_id, 'tags': ['cricket', 'bollywood', 'career', 'arts/culture-&-fun']}
    return user_tags, user_categories



def get_events_data(data_source, workgroup="primary", 
                    start_date=None, end_date=None, horizon_days=14):

    """
    Retrieve events data within a specified time horizon.
    
    Args:
        data_source (str): Athena data source to query from.
        workgroup (str): Athena workgroup to use (default: "primary").
        start_date (str): Start date of the time horizon (default: None).
        end_date (str): End date of the time horizon (default: None).
        horizon_days (int): Number of days to look ahead (default: 14).
        
    Returns:
        pandas.DataFrame: A DataFrame containing events data.
    """
    curr_millis_utc = int(time.time()*1000)
    horizon_millis = horizon_days * 86400 * 1000
    end_millis_utc = curr_millis_utc + horizon_millis
    query = f"""SELECT * FROM "tockify" where "start_millis" >= {curr_millis_utc} and "start_millis" < {end_millis_utc}"""
    events_df = wr.athena.read_sql_query(
                        sql = query,
                        database = "events_data",
                        data_source = data_source,
                        workgroup = workgroup,
                        ctas_approach = False,
        )
    
    # events_df = events_df.drop_duplicates()
    
    events_df['tags'] = events_df['tags'].apply(lambda x: [item.strip().lower() for item in x.strip('[]').split(',')])
    # events_df = events_df.loc[events_df['calname'] == 'ucenevents']
    
    events_df = events_df.sort_values(by='tags', na_position='last')
    # events_df = events_df.drop_duplicates(subset=['summary', 'start_millis'])

    return events_df

def get_resource_data(data_source, workgroup='primary'):
    query = f"""SELECT * FROM "asset_asset" """
    resource_df = wr.athena.read_sql_query(
                        sql = query,
                        database = "willo_dev",
                        data_source = data_source,
                        workgroup = workgroup,
                        ctas_approach = False,
        )
    
    return resource_df