import pandas as pd

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