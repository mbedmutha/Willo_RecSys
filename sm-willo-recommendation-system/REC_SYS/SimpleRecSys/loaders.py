import pandas as pd
import numpy as np
import awswrangler as wr
import time
import json
# import os

# os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

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
		user_tags.append(tag_object['id'])
		user_categories.append(tag_object['category']['id'])
	
	# {'user_id': user_id, 'tags': ['cricket', 'bollywood', 'career', 'arts/culture-&-fun']}
	return user_tags, user_categories



def get_events_data(data_source="datalake-athena", workgroup="primary", 
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

def get_resource_tags_categories(data_source="datalake-athena", workgroup='primary'):
	# query = f"""SELECT * FROM "asset_asset" """
	# resource_df = wr.athena.read_sql_query(
	#                     sql = query,
	#                     database = "willo_dev",
	#                     data_source = data_source,
	#                     workgroup = workgroup,
	#                     ctas_approach = False,
	#     )
	
	asset_category = wr.athena.read_sql_query(
	                    sql=f'SELECT * FROM "asset_asset_category"',
	                    database="willo_dev",
	                    data_source="datalake-athena",
	                    workgroup=workgroup,
	                    ctas_approach=False,
	    )
	asset_category = asset_category.drop(columns={'jobprocesseddate'}).drop_duplicates(subset=['id'])
	
	asset_tags = wr.athena.read_sql_query(
	                    sql=f'SELECT * FROM "asset_asset_tags"',
	                    database="willo_dev",
	                    data_source="datalake-athena",
	                    workgroup='primary',
	                    ctas_approach=False,
	    )
	asset_tags = asset_tags.drop(columns={'jobprocesseddate'}).drop_duplicates(subset=['id'])
	
	search_space = asset_category.merge(asset_tags, on='asset_id')[['category_id', 'asset_id', 'tag_id']].drop_duplicates()
	# del asset_tags
	# del asset_category
	search_space = None
	print("Got get_resource_tags_categories")
	return search_space