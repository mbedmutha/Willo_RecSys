import pandas as pd
import numpy as np
import awswrangler as wr
import time
import json

from loaders import get_events_data, get_user_data, get_resource_space, get_category_names
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
		self.resource_space = get_resource_space(data_source=self.athena_data_source)
		self.category_names = get_category_names(data_source=self.athena_data_source)

	def _match_events(self):
		
		"""
		Perform matching between user data and events data.
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
		Candidate Generation Stage for Resource Recommendations
		(presently only one way of generating candidates)
		
		FUTURE: Once data starts coming in collaborative filtering will generate\
		other batch of candidates as well.
		
		Generates matches based on user preferences (categorytags and categories)
		
		Assumes self.user_categories and self.user_categorytags have been created
		
		Creates class variable matched_items that is a subset of the resource\
		space chosen based on selected categorytags and categories
		"""
		# User selected categories
		if (len(self.user_categories) > 0):
			self.matched_items = self.resource_space.loc[
				self.resource_space.category_id.isin(self.user_categories)
			].copy()
			# Selected category tags too
			if (len(self.user_categorytags) > 0):
				self.matched_items = self.matched_items.loc[
					self.matched_items.categorytag_id.isin(self.user_categorytags)
				]
				
		# Can there ever be a case where categories are selected but not tags?
		# Or vice versa?
		
		# User did not select categories themselves
		else:
			print(f"No matches found")
			self.matched_items = None

			
	def rank_resources(self):
		
		"""
		Candidate Ranking Stage for Resource Recommendarions
		
		Assumes match_resources() is already executed
		
		If no matches found, it will randomly select three samples\
		under each category (can be user defined or randomly selected)
		
		Args: None
		
		Returns:
			dict: A dictionary containing with category_id as the key\
				and correspinding asset_id's as values
				
		NOTE: always ensure values being added to the dict have dtype int
		"""
		
		# Check if matched_items is not None and has non-zero length
		if self.matched_items is not None and not self.matched_items.empty:
			pred_resources = self.matched_items
			print(f"Found {len(pred_resources)} valid matching resources")
		else:
			# Create pred_resources if it doesn't exist
			pred_resources = pd.DataFrame()  # Initialize as an empty DataFrame
		
		# f non zero number of matches are found
		if not pred_resources.empty:
			cat_choice = pd.Series(pred_resources.category_id.unique()).tolist()
		# If user had not selected anything in the first place
		elif self.user_categories is None or len(self.user_categories) == 0:
			cat_choice = pd.Series(self.resource_space.category_id.unique()).sample(n=3).tolist()
		# If zero matches were found, but user had selected specific categories at least
		else:
			cat_choice = list(self.user_categories)
		print("Chosen category_id's", cat_choice)
	
		out = {}

		# For each category chosen for the user
		for cat_id in cat_choice:
			
			# If not matches exist, initialize with empth
			if (pred_resources.empty) or (len(pred_resources)==0):
				cat_resources = pd.DataFrame()
			# If match found, identify possible resources for given category
			else:
				cat_resources = pred_resources.loc[pred_resources.category_id == cat_id]
			
			if (len(cat_resources) == 0) or (cat_resources.empty):
				cat_resources = self.resource_space.loc[
					self.resource_space.category_id == cat_id
				]
			print(f"Selecting from {len(cat_resources)} items for category_id {cat_id}")
				
			# To ensure same output always set random state, else remove param
			out[int(cat_id)] = pd.Series(cat_resources['asset_id'].unique()).sample(
				n=min(3, len(cat_resources['asset_id'].unique())),
				# random_state=1
			).tolist()
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
		
		self.user_categorytags, self.user_categories = get_user_data(user_dict)
		self._match_resources()
		print(f"Matched entries resulted in {len(self.matched_items)} items")
		resource_dict = self.rank_resources()
		resource_out_list = create_resource_output(resource_dict = resource_dict, 
												   category_names = self.category_names)
		
		out_dict = {}
		out_dict['happening_this_week'] = [
			"tockify_ucenevents_4921_1727373600000",
			"tockify_ucsdwc_40_1720033200000",
			"tockify_ucenevents_5043_1719680400000",
			"tockify_ucsdwc_49_1719433800000",
			"tockify_ucenevents_5059_1727283600000"
		]

		out_dict['recommended_categories'] = resource_out_list

		print(user_dict)
		print(out_dict)
		
		return out_dict