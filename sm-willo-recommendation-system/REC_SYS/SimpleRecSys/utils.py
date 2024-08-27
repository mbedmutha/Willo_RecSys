import pandas as pd
import awswrangler as wr


def generate_pseudo_tags(df, tagset):
    """
    df is event df or resource df; tagset is superset of all tags a user can choose
    this is when tags for resource are empty
    past experiments with spacy and bert were not very effective but still worth integration

    PLACEHOLDER: still experimenting alternatives
    """
    
    # empty tags
    empty_idx = df.loc[df.tags.apply(lambda x: '' in x)].index
    
    for i in idx:
        print(df.loc[i]['calname'])
        # add logic based on calname
        
    # also see experiments with spacy/BERT - especially useful for fusion
        
    return df

def create_resource_output(resource_dict, category_names, workgroup='primary'):
    out_list = []
    for cat_id in list(resource_dict.keys()):
        temp_dict = {}
        temp_dict['category'] = {
            "name" : str(category_names[cat_id]),
            "id" : int(cat_id)
        }
        temp_dict['services'] = resource_dict[cat_id]
        out_list.append(temp_dict)
        
    return out_list