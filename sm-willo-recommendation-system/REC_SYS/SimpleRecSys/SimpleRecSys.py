import pandas as pd
import numpy as np
import awswrangler as wr
import time
import json
import boto3

from loaders import get_events_data, get_user_data, get_resource_space, get_category_names, create_tags_from_app
from utils import create_resource_output

class SimpleRecSys:
    """
    Basic model for recommendations; barebones
    """
    def __init__(self, athena_data_source="datalake-athena", reset_freq_seconds = 43200):
        
        """
        Initialize the SimpleRecSys class.
        """
        self.athena_data_source = athena_data_source
        
        # Replace with S3 paths later
        # self.tockify_tags_path = "./tockify_tags.txt"
        # self.tockify_mapping_path = "./tockify_mapping.json"
        
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'chi-willo-sagemaker-output-data'
        
        self.calname_apptag_map_path = "model-metadata/calname_apptag_map.json"
        self.calname_appcat_map_path = "model-metadata/calname_appcat_map.json"
        self.caltag_apptag_map_path = "model-metadata/caltag_apptag_map.json"
        
        self.initialization_time = time.time()
        self.reset_frequency_seconds = 43200 # 12 hours
        self.next_update_unix_seconds = self.initialization_time + self.reset_frequency_seconds
        
        # self.events_df = get_events_data(data_source=self.athena_data_source)
        # self.resource_space = get_resource_space(data_source=self.athena_data_source)
        # self.category_names = get_category_names(data_source=self.athena_data_source)
        
        self.tags_from_app = create_tags_from_app()
        
        self.reset_event_info()
        self.reset_resource_info()
        
    # Basic Code to reset/sync with database
    def reset_event_info(self):
        # Read events
        self.events_df = get_events_data(data_source=self.athena_data_source, horizon_days=100)
        
        # Ensure tags_from_app exists
        if not hasattr(self, 'tags_from_app'):
            self.tags_from_app = create_tags_from_app()
            
        # Load mapping info
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.caltag_apptag_map_path)
        content = response['Body'].read().decode('utf-8')
        self.event_user_map = json.loads(content)
        
        # Verify tags
#         invalid_tags=[]
#         invalid_entries = {}
        available_tags = self.tags_from_app['name'].to_list()

#         for center_tag, values in self.event_user_map.items():
#             tags = values
#             invalid_tags = [tag for tag in tags if tag not in available_tags and tag not in self.taglist]

#             if invalid_tags:    
#                 invalid_entries[center_tag] = invalid_tags
                
        default_student_tags = [
            "Peer Support"
        ]
    
        # Replace tag_name by tag_id
        for key in self.event_user_map:
            tags = self.event_user_map[key]
            tags += default_student_tags
            for i, tag in enumerate(tags):
                if tag in available_tags:
                    tag_id = self.tags_from_app[self.tags_from_app['name'] == tag]['id'].iloc[0]
                    tags[i] = tag_id
                self.event_user_map[key] = [x for x in tags if type(x) != str]
                
        # default_event_tags = [
        #     "Price-Center-Student-Union", "Student-Center",
        #     "The-Basement", "The-Loft"
        # ]        
        # default_event_tags = [x.lower() for x in default_event_tags]

        asset_tags_ids = []
        lowercase_keys = {i.lower(): i for i in self.event_user_map.keys()}

        for index, row in self.events_df.iterrows():
            flag = True
            new_tags = []
            if row['tags']== ['']:
                # for tag in default_event_tags:
                #     new_tags.extend(self.event_user_map.get(lowercase_keys[tag]))
                #     flag = False
                # # print(row['summary'], new_tags)
                asset_tags_ids.append(new_tags)
                continue
            for tag in row['tags']:
                if (tag in lowercase_keys.keys()) and (self.event_user_map.get(lowercase_keys[tag]) != []):
                    new_tags.extend(self.event_user_map.get(lowercase_keys[tag]))
                    flag = False
            asset_tags_ids.append(new_tags)
        self.events_df['asset_tags_ids'] = asset_tags_ids
        
        # # Their categories
        eventtags_cat = []
        for tags in self.events_df['asset_tags_ids']:
            if tags == []:
                cats = set([2, 3, 7])
            try:
                cats = set([self.tags_from_app[self.tags_from_app['id']== tag]['category_id'].iloc[0] for tag in tags])
            except:
                cat = set([])
            eventtags_cat.append(cats)

        self.events_df['eventtags_cat'] = eventtags_cat
        # Please note that the combined list of appended tags for all keys is as it is, and not a set - for frequency purposes

    def reset_resource_info(self):
        self.resource_space = get_resource_space(data_source=self.athena_data_source)
        self.category_names = get_category_names(data_source=self.athena_data_source)
    
    # Utils for event matching
    def calculate_overlap(self, user_categorytags, event_tags):
        """Calculates the overlap between two lists of tags."""
        return len(set(user_categorytags) & set(event_tags))

    def calculate_category_overlap(self, user_categories, event_categories):
        """Calculates the overlap between two lists of categories."""
        return len(set(user_categories) & set(event_categories))

    def calculate_combined_score(self, tag_overlap, category_overlap, w_tags=2, w_cats=1):
        """Combines tag and category overlap into a single score."""
        return tag_overlap * w_tags + category_overlap * w_cats

    # Main event recommendations
    def recommend_events(self, w_tags=5, w_cats=1):
        """
        Recommends top 5 events based on tag and category overlap.
        Assumes asset_tags_ids and eventtags_cat already in self.events_df
        """
        
        if not hasattr(self, 'tags_from_app'):
            self.tags_from_app = create_tags_from_app()

        if self.user_categorytags and self.user_categories:
            usertags = self.user_categorytags
            usertags_cat = self.user_categories
        else:
            print("Returning 5 random events")
            return self.events_df.sample(n=min(5, len(self.events_df)))['id'].to_list()
        
        # if len(usertags) == 0 or len(usertags_cat):
        #     return self.events_df.sample(5)['id'].to_list()

        event_scores = []
        for index, row in self.events_df.iterrows():
            event_tags = row['asset_tags_ids']
            event_cats = row['eventtags_cat']

            tag_overlap = self.calculate_overlap(usertags, event_tags)
            category_overlap = self.calculate_category_overlap(usertags_cat, event_cats)
            combined_score = self.calculate_combined_score(tag_overlap, category_overlap, w_tags, w_cats)

            event_scores.append((index, combined_score))

        event_scores.sort(key=lambda x: x[1], reverse=True)
        print(f"Found {len(event_scores)} events returned 5")
        top_5_events = self.events_df.loc[[x[0] for x in event_scores[:5]]]        

        return top_5_events['id'].to_list()
    
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
            self.matched_items = pd.DataFrame()

            
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
        
        if time.time() > self.next_update_unix_seconds:
            self.reset_event_info()
            self.reset_resource_info()
            self.initialization_time = time.time()
            self.next_update_unix_seconds = self.initialization_time + self.reset_frequency_seconds
        
        self.user_categorytags, self.user_categories = get_user_data(user_dict)
                                         
        # Generate resources recommendations
        self._match_resources()
        try:
            print(f"Matched entries resulted in {len(self.matched_items)} items")
        except:
            print(f"No matches found")
            
        resource_dict = self.rank_resources()
        resource_out_list = create_resource_output(resource_dict = resource_dict, 
                                                   category_names = self.category_names)
        # Generate event recommendations
        event_out = self.recommend_events()
        
                                         
                                         
        out_dict = {}
        out_dict['happening_this_week'] = event_out
        
        # [
        #     "tockify_ucenevents_4921_1727373600000",
        #     "tockify_ucsdwc_40_1720033200000",
        #     "tockify_ucenevents_5043_1719680400000",
        #     "tockify_ucsdwc_49_1719433800000",
        #     "tockify_ucenevents_5059_1727283600000"
        # ]

        out_dict['recommended_categories'] = resource_out_list

        print(user_dict)
        print(out_dict)
        
        return out_dict
    
    
##### ##### ##### ##### #####
#         with open(self.tockify_mapping_path, 'r') as json_file:
#             self.event_user_map = json.load(json_file)
            
        # with open(self.tockify_tags_path, 'r') as f:
        #     taglist = f.readlines()
        #     self.taglist = [tagname.strip('\n') for tagname in taglist]
        
#         # Verify tags
#         invalid_tags=[]
#         invalid_entries = {}
#         available_tags = self.tags_from_app['name'].to_list()

#         for center_tag, values in self.event_user_map.items():
#             tags = values
#             invalid_tags = [tag for tag in tags if tag not in available_tags and tag not in self.taglist]

#             if invalid_tags:    
#                 invalid_entries[center_tag] = invalid_tags
                
#         # Replace tag_name by tag_id
#         for key in self.event_user_map:
#             tags = self.event_user_map[key]
#             for i, tag in enumerate(tags):            
#                 if tag in available_tags:
#                     tag_id = self.tags_from_app[self.tags_from_app['name'] == tag]['id'].iloc[0]
#                     tags[i] = tag_id

#                 self.event_user_map[key] = [x for x in tags if type(x) != str]
##### ##### ##### ##### #####
                    
##### ##### ##### ##### #####    
#         # Make combined tags list for unicenter tags list in events_df
#         asset_tags_ids = []
#         for index, row in self.events_df.iterrows():
#             flag = True
#             new_tags = []
#             if len(row['tags'])==0:
#                 if row['calname'] == 'oasisevents':
#                     new_tags.append('Academic Support')
#             for tag in row['tags']:
#                 lowercase_keys = {i.lower(): i for i in self.event_user_map.keys()}
#                 if (tag in lowercase_keys.keys()) and (self.event_user_map.get(lowercase_keys[tag]) != []):
#                     new_tags.extend(self.event_user_map.get(lowercase_keys[tag]))
#                     flag = False
#             asset_tags_ids.append(sorted(new_tags))

#         self.events_df['asset_tags_ids'] = asset_tags_ids

#         # Their categories
#         eventtags_cat = []
#         for tags in self.events_df['asset_tags_ids']:
#             cats = set([self.tags_from_app[self.tags_from_app['id']== tag]['category_id'].iloc[0] for tag in tags])
#             eventtags_cat.append(cats)

#         self.events_df['eventtags_cat'] = eventtags_cat
#         # Please note that the combined list of appended tags for all keys is as it is, and not a set - for frequency purposes
##### ##### ##### ##### #####        

## Supporting Code

#   def _match_events(self):        
#       """
#       Perform matching between user data and events data.
#       FUTURE: Implement real matching
#       """     
#       num_events = len(self.events_df)

#       # If not enough events are found, broaden horizon
#       if num_events < 3:
#           self.events_df = get_events_data(data_source=self.athena_data_source, horizon_days=365)


#       # Randomized event shortlist

#       # TODO: actual matching once user/tag info is available in datalake
#       self.matched_events = self.events_df.sample(3)
        

#   def rank_events(self):
        
#       """
#       FUTURE: Will rank mateched events
#       """
#       pass
