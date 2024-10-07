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
    user_categorytags = []
    user_categories = []
    
    for tag_object in user_dict['category_tags']:
        user_categorytags.append(tag_object['id'])
        user_categories.append(tag_object['category']['id'])
    
    return user_categorytags, user_categories



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
    
    def get_tockify_data():
        query = f"""
        SELECT *
        FROM "tockify"
        WHERE "start_millis" >= {curr_millis_utc}
          AND "start_millis" < {end_millis_utc}
          AND "date" = (SELECT MAX("date") FROM "tockify")
        """
        events_df = wr.athena.read_sql_query(
            sql=query,
            database="events_data",
            data_source=data_source,
            workgroup=workgroup,
            ctas_approach=False,
        )
        
        events_df['id'] = 'tockify_'+events_df['calname']+'_'+events_df['uid']+'_'+events_df['start_millis'].astype(str)
    
        events_df['tags'] = events_df['tags'].apply(lambda x: [item.strip().lower() for item in x.strip('[]').split(',')])
        events_df = events_df.loc[events_df['calname'] != 'ucsdwc']
        # events_df = events_df.loc[events_df.date == events_df.date.max()]

        events_df = events_df.sort_values(by='tags', na_position='last')

        # if summary, start_millis together then repeat events might appear often
        events_df = events_df.drop_duplicates(subset=['summary']) #, 'start_millis'])

        return events_df
    
    def get_trumba_data():
        query = f"""
        SELECT *
        FROM "trumba_data"
        """
        
        events_df = wr.athena.read_sql_query(
            sql=query,
            database="events_data",
            data_source="datalake-athena",
            workgroup="primary",
            ctas_approach=False,
        )
        
        events_df['startdatetime'] = pd.to_datetime(events_df['startdatetime'])
        events_df['start_millis'] = pd.to_datetime(events_df.startdatetime)
        events_df['start_millis'] = events_df['start_millis'].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='shift_forward')
        events_df['start_millis'] = events_df['start_millis'].astype('int64') // 10**6
        
        events_df['enddatetime'] = pd.to_datetime(events_df['enddatetime'])
        events_df['end_millis'] = pd.to_datetime(events_df.startdatetime)
        events_df['end_millis'] = events_df['end_millis'].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='shift_forward')
        events_df['end_millis'] = events_df['end_millis'].astype('int64') // 10**6
        
        events_df = events_df[events_df['start_millis'] >= curr_millis_utc]
        events_df = events_df[events_df['end_millis'] <= end_millis_utc]
        # events_df['calname'] = events_df.template.str.lower().apply(lambda x: "".join(x.split()))
        events_df['calname'] = 'ucsandiegohealth'
        events_df['id'] = "trumba_"+events_df['calname']+"_"+events_df.eventid.astype(str)
        events_df['tags'] = ['']*len(events_df)
        events_df = events_df.rename({'title': 'summary'}, axis=1)
        
        events_df = events_df.sort_values(by='tags', na_position='last')

        # if summary, start_millis together then repeat events might appear often
        events_df = events_df.drop_duplicates(subset=['summary']) #, 'start_millis'])
        
        return events_df
    
    common = ['calname', 'tags', 'summary',
          'end_millis', 'description',
          'start_millis', 'id']

    tockify_events = get_tockify_data()
    trumba_events = get_trumba_data()

    events_df = pd.concat([tockify_events[common], trumba_events[common]])
   
#     query = f"""SELECT * FROM "tockify" where "start_millis" >= {curr_millis_utc} and "start_millis" < {end_millis_utc} and date = MAX(date)"""
#     events_df = wr.athena.read_sql_query(
#                         sql = query,
#                         database = "events_data",
#                         data_source = data_source,
#                         workgroup = workgroup,
#                         ctas_approach = False,
#         )
    
    # events_df = events_df.drop_duplicates()
    
    return events_df


def create_tags_from_app(data_source="datalake-athena", workgroup='primary'):
    asset_category = get_asset_data("asset_category")
    asset_category = asset_category.rename(columns={
        'name': 'category_name',
        'id': 'category_id',
    })
    
    asset_categorytag = get_asset_data("asset_categorytag")
    
    tags_from_app = pd.merge(asset_categorytag, asset_category[['category_id', 'category_name']], on='category_id', how='left')
    tags_from_app.dropna(subset=['category_id'], inplace=True)
    tags_from_app.sort_values('category_id', inplace=True)
    
    return tags_from_app


def get_asset_data(table_name, data_source="datalake-athena", workgroup='primary'):
    asset_data = wr.athena.read_sql_query(
        sql=f'SELECT * FROM {table_name}',
        database="willo_dev",
        data_source="datalake-athena",
        workgroup='primary',
        ctas_approach=False,
    )
    asset_data = asset_data.loc[
        asset_data.jobprocesseddate == asset_data.jobprocesseddate.max(), 
        # ['id','name']
    ]
    return asset_data


def get_category_names(data_source="datalake-athena", workgroup='primary'):
    asset_category = get_asset_data('asset_category')
    category_name_id = dict(zip(asset_category['id'], asset_category['name']))
    return category_name_id

def get_resource_space(data_source="datalake-athena", workgroup='primary'):
#     # SQL join category_id, categorytag_id, asset_id, tag_id ON asset_id
#   sql_query = """
#       SELECT DISTINCT
#           c.category_id,
#           c.asset_id,
#           t.tag_id,
#           ct.categorytag_id
#       FROM
#           asset_asset_category c
#       JOIN
#           asset_asset_tags t
#       ON
#           c.asset_id = t.asset_id
#       JOIN
#           asset_asset_category_tag ct
#       ON
#           c.asset_id = ct.asset_id
#   """

    ct = get_asset_data("asset_asset_category_tag")
    c = get_asset_data("asset_asset_category")
    t = get_asset_data("asset_asset_tags")

    search_space = pd.merge(ct, c, on="asset_id")
    search_space = pd.merge(search_space, t, on="asset_id")
    search_space = search_space[['id','asset_id', 'categorytag_id', 'category_id']]
    return search_space