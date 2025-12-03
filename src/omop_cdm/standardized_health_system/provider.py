import os
import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_provider_table(tumor, inzidenzort_mapping):
    
	provider_query = """
		select distinct
		t.inzidenzort_com                               as provider_id
		,Landkreis	                                    as provider_name
		,NULL                                           as npi
		,NULL                                           as dea
		,NULL                                           as specialty_concept_id
		,t.register_id_fk                               as care_site_id
		,NULL                                           as year_of_birth
		,NULL                                           as gender_concept_id
		,NULL                                           as provider_source_value
		,NULL                                           as specialty_source_value
		,NULL                                           as specialty_source_concept_id
		,NULL                                           as gender_source_value
		,NULL                                           as gender_source_concept_id

		from inzidenzort_mapping
		inner join tumor t
		on t.inzidenzort = Landkreis_Nr
		where Landkreis is not null
	"""

	provider = ps.sqldf(provider_query)

	## db not null constraints
	# required: provider_id
	provider = hp.dropna_columns(provider, ['provider_id'], "provider.py", "provider" )

	# db import
	imp.df_import(provider,'provider')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    tumor = imp_dataframes['tumor']
    
    # stage import
    stage_df = ['inzidenzort_mapping']
    stage_dataframes = imp.stage_import(stage_df)
    inzidenzort_mapping = stage_dataframes['inzidenzort_mapping'] 
    
    create_provider_table(tumor, inzidenzort_mapping)