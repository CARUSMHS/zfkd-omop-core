import os
import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_care_site_table(inzidenzort_mapping):
    care_site_query = """
        select distinct
        Bundesland_Nr                                  as care_site_id
        ,Bundesland                                     as care_site_name
        ,NULL                                           as place_of_service_concept_id
        ,NULL                                           as location_id
        ,NULL                                           as care_site_source_value
        ,NULL                                           as place_of_service_source_value

        from inzidenzort_mapping
        order by Bundesland_Nr
    """

    care_site = ps.sqldf(care_site_query)

    ## db not null constraints
    # required: provider_id
    care_site = hp.dropna_columns(care_site, ['care_site_id'], "care_site.py", "care_site" )

    # db import
    imp.df_import(care_site,'care_site')

if __name__ == "__main__":
    # logging
    hp.init_logger()
    
    # stage import
    stage_df = ['inzidenzort_mapping']
    stage_dataframes = imp.stage_import(stage_df)
    inzidenzort_mapping = stage_dataframes['inzidenzort_mapping'] 
    
    create_care_site_table(inzidenzort_mapping)