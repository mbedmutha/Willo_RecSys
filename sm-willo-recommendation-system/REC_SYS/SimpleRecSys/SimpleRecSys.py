import pandas as pd
import numpy as np
import awswrangler as wr
import time

from loaders import get_events_data, get_user_data, get_resource_data

class SimpleRecSys:
	"""
	Basic model for recommendations; barebones
	"""
	def __init__(self, athena_data_source="datalake-athena"):
		
		"""
		Initialize the SimpleRecSys class.
		"""
		self.events_df = get_events_data(data_source=athena_data_source)
		self.resources_df = get_resources_data(data_source=athena_data_source)

	def _match_events(self):
		
		"""
		Perform matching between user data and events data.
		Algorithm : Cosine Similarity (TF-IDF)

		FUTURE: Implement real matching
		"""
		
		num_events = len(self.events_df)

		# If not enough events are found, broaden horizon
		if num_events < 3:
			self.events_df = get_events_data(data_source=athena_data_source, horizon_days=365)


		# Randomized event shortlist

		# TODO: actual matching once user/tag info is available in datalake
		self.matched_events = self.events_df.sample(3)
		

	def rank_events(self):
		
		"""
		FUTURE: Will rank mateched events
		"""
		pass
	
	def match_resources(self):
		
		"""
		FUTURE: Will match resources
		"""
		pass

	def rank_resources(self):
		
		"""
		FUTURE: Will rank matched resources
		"""
		pass
	

	def predict(self, user_dict):
		"""
		Predict top events for a given user.
		
		Args:
			user_dict (dict): A dict describing user preferences

		** future: dict could be replaced with user_id for enabling batched mode 
			
		Returns:
			dict: A dictionary containing -
				list of event_ids for events under "happening_this_week"
				list of dicts for resources under "recommended_categories"
		"""
		
		self.user_tags, self.user_categories = get_user_data(user_dict)
		
		out_dict = {}
		out_dict['happening_this_week'] = [
			"tockify_ucenevents_4921_1727373600000",
			"tockify_ucsdwc_40_1720033200000",
			"tockify_ucenevents_5043_1719680400000",
			"tockify_ucsdwc_49_1719433800000",
			"tockify_ucenevents_5059_1727283600000"
		]

		out_dict['recommended_categories'] = [
			{
				"category": {"name": "Physical Health", "id": 4},
				"services": [42,43,51]
			},
			{
				"category": {"name": "Basic Needs", "id": 1},
				"services": [3, 11, 10]
			},
			{
				"category": {"name": "Academic","id": 6},
				"services": [53, 20, 23]
			}
		]

		return out_dict