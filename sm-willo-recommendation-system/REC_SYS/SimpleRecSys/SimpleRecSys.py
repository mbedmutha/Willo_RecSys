import pandas as pd
import numpy as np
import awswrangler as wr
import time

from loaders import get_events_data, get_user_data, get_resource_tags_categories
from utils import create_resource_output

class SimpleRecSys:
	"""
	Basic model for recommendations; barebones
	"""
	def __init__(self, athena_data_source="datalake-athena"):
		
		"""
		Initialize the SimpleRecSys class.
		"""
		self.athena_data_source = athena_data_source
		self.events_df = None #get_events_data(data_source=athena_data_source)
		self.resource_space = get_resource_tags_categories(data_source=athena_data_source)

	def _match_events(self):
		
		"""
		Perform matching between user data and events data.
		Algorithm : Cosine Similarity (TF-IDF)

		FUTURE: Implement real matching
		"""
		
		num_events = len(self.events_df)

		# If not enough events are found, broaden horizon
		if num_events < 3:
			self.events_df = get_events_data(data_source=self.athena_data_source, horizon_days=365)


		# Randomized event shortlist

		# TODO: actual matching once user/tag info is available in datalake
		self.matched_events = self.events_df.sample(3)
		

	def rank_events(self):
		
		"""
		FUTURE: Will rank mateched events
		"""
		pass
	
	def _match_resources(self):
		
		"""
		FUTURE: Will match resources
		"""
		try:
			self.matched_items = self.resource_space.loc[self.resource_space.category_id.isin(self.user_categories)].copy()
			self.matched_items = self.matched_items.loc[self.matched_items.tag_id.isin(self.user_tags)]
		except:
			print("Execption")
			
	def rank_resources(self):
		
		"""
		FUTURE: Will rank matched resources
		"""
		try:
			pred_resources = self.matched_items
		except:
			pred_resources = self.resource_space.sample(3)
		
		print("Using pred_resource as", pred_resources.head())
		out = {}
		cat_choice = self.user_categories
		if len(self.user_categories) == 0:
			cat_choice = list(pred_resources.category_id.values)

		for cat_id in cat_choice:
			cat_df = pred_resources.loc[pred_resources.category_id == cat_id]
			if len(cat_df) > 0:
				out[int(cat_id)] = list(cat_df.sample(min(3, len(cat_df)))[['asset_id', 'category_id']].drop_duplicates()['asset_id'].values)
				out[int(cat_id)] = [int(x) for x in out[cat_id]]
			else:
				cat_resources = self.resource_space.loc[self.resource_space.category_id == cat_id].sample(3)
				out[int(cat_id)] = list(cat_resources['asset_id'].values)
				out[int(cat_id)] = [int(x) for x in out[cat_id]]
				# cat_resources[['asset_id', 'category_id']].drop_duplicates()
				# if category is missing in our matcheddata, but user interested in category?	
		
		return out
	

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
		self._match_resources()
		print("Matched entries: ", self.matched_items)
		resource_dict = self.rank_resources()
		resource_out_list = create_resource_output(resource_dict)
		
		out_dict = {}
		out_dict['happening_this_week'] = [
			"tockify_ucenevents_4921_1727373600000",
			"tockify_ucsdwc_40_1720033200000",
			"tockify_ucenevents_5043_1719680400000",
			"tockify_ucsdwc_49_1719433800000",
			"tockify_ucenevents_5059_1727283600000"
		]

		out_dict['recommended_categories'] = resource_out_list
		# [
		# 	{
		# 		"category": {"name": "Physical Health", "id": 4},
		# 		"services": [42,43,51]
		# 	},
		# 	{
		# 		"category": {"name": "Basic Needs", "id": 1},
		# 		"services": [3, 11, 10]
		# 	},
		# 	{
		# 		"category": {"name": "Academic","id": 6},
		# 		"services": [53, 20, 23]
		# 	}
		# ]
		print(user_dict)
		print(out_dict)
		
		return out_dict