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

def get_category_names(data_source="datalake-athena", workgroup='primary'):
	asset_category = wr.athena.read_sql_query(
		sql=f'SELECT * FROM "asset_category"',
		database="willo_dev",
		data_source="datalake-athena",
		workgroup='primary',
		ctas_approach=False,
	)
	asset_category = asset_category.loc[
		asset_category.jobprocesseddate == asset_category.jobprocesseddate.max(), 
		['id','name']
	]
	category_name_id = dict(zip(asset_category['id'], asset_category['name']))
	return category_name_id

def get_resource_space(data_source="datalake-athena", workgroup='primary'):
	# SQL join category_id, categorytag_id, asset_id, tag_id ON asset_id
	sql_query = """
		SELECT DISTINCT
			c.category_id,
			c.asset_id,
			t.tag_id,
			ct.categorytag_id
		FROM
			asset_asset_category c
		JOIN
			asset_asset_tags t
		ON
			c.asset_id = t.asset_id
		JOIN
			asset_asset_category_tag ct
		ON
			c.asset_id = ct.asset_id
	"""

	# Execute the query and read the results into a DataFrame
	search_space = wr.athena.read_sql_query(
		sql=sql_query,
		database="willo_dev",
		data_source="datalake-athena",
		workgroup=workgroup,
		ctas_approach=False
	)
	return search_space