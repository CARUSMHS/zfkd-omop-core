import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_death_table(patient, person, mappings):
    # use column death_date from stage table person
    # icd 10 / snomed [1:1]
    query="""
        select
        p.person_id
        ,p.death_date                                     as death_date
        ,NULL                                             as death_datetime
        ,32815                                            as death_type_concept_id
        ,m.target_concept_id                              as cause_concept_id
        ,pa.todesursache_grundleiden                      as cause_source_value
        ,NULL                                             as cause_source_concept_id 

        from patient pa
        inner join person p
        on p.person_source_value=pa.pat_id
        left join mappings m
        on pa.todesursache_grundleiden=m.source_code
        and m.source_vocabulary_id = 'ICD10GM'
        and m.target_vocabulary_id = 'SNOMED'
        and m.target_concept_class='Disorder'
    """

    death = ps.sqldf(query)

    ## db not null constraints
    # required: death_date
    death = hp.dropna_columns(death, ['death_date'], "death.py", "death" )

    # db import
    imp.df_import(death,'death')


if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    patient = imp_dataframes['patient']

    # stage import
    stage_df = [
        'person',
        'mappings']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    mappings = stage_dataframes['mappings'] 
    
    create_death_table(patient, person, mappings)
    
