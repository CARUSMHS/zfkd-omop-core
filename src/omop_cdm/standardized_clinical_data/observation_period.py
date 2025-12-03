import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import exporter as ex
from utils import helper as hp

    
def create_observation_period_table(person, tumor):

    # use column observation_end_date from stage table person
    query = """
        select row_number() over (order by t.diagnosedatum)    as observation_period_id 
        ,p.person_id                                           as person_id
        ,min(t.diagnosedatum)                                  as observation_period_start_date
        ,p.observation_end_date                                as observation_period_end_date
        ,44814725                                              as period_type_concept_id

        from person p
        inner join tumor t
        on p.person_source_value=t.pat_id
        group by p.person_id
        """

    observation_period = ps.sqldf(query)

    ## db not null constraints
    # required: observation_period_id, person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id
    observation_period = hp.dropna_columns(observation_period, ['observation_period_start_date', 'observation_period_end_date'], "observation_period.py", "observation_period")

    #db import
    imp.df_import(observation_period,'observation_period')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    tumor = imp_dataframes['tumor']
    
    # stage import
    stage_df = ['person']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    
    create_observation_period_table(person, tumor)