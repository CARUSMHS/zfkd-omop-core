from utils import importer  as imp
from utils import helper as hp
from utils import exporter as ex
import pandasql as ps
import pandas as pd

def create_episode_table(tumor, fe, person, concept, condition_occurrence):
    # Implementation of episodes
    # related to the tumor evolvement

    # episode of disease
    condition_occurrence_t = hp.disease_episode_prep(condition_occurrence, person, tumor, concept)

    disease_query = """
        select 
        c.person_id
        ,32533                              as episode_concept_id
        ,c.condition_start_date             as episode_start_date
        ,NULL                               as episode_start_datetime
        ,c.condition_end_date               as episode_end_date
        ,NULL                               as episode_end_datetime
        ,NULL                               as episode_parent_id
        ,c.episode_number
        ,c.condition_concept_id             as episode_object_concept_id
        ,32546                              as episode_type_concept_id
        ,c.condition_source_value           as episode_source_value
        ,c.condition_source_concept_id      as episode_source_concept_id
        
        from condition_occurrence_t c
        inner join concept co
        on c.condition_concept_id = co.concept_id
        and co.vocabulary_id = 'ICDO3'
        and co.concept_class_id = 'ICDO Condition'

    """  
    disease_episode = ps.sqldf(disease_query)
        
    # episode - disease dynamic
    disease_dynamic = hp.get_disease_dynamic(fe, person, concept)

    dynamic_query="""
        select
        c.person_id
        ,d.concept_id_disease_dynamic       as episode_concept_id         
        ,d.start_date                       as episode_start_date
        ,NULL                               as episode_start_datetime
        ,d.end_date                         as episode_end_date
        ,NULL                               as episode_end_datetime
        ,NULL                               as episode_parent_id 
        ,d.episode_number
        ,c.condition_concept_id             as episode_object_concept_id
        ,32546                              as episode_type_concept_id
        ,c.condition_source_value           as episode_source_value
        ,c.condition_source_concept_id      as episode_source_concept_id
    
        from disease_dynamic d
        inner join condition_occurrence_t c
        on d.pat_id=c.person_source_value
        and d.tumor_id_fk=c.tumor_id    
    """

    dynamic_episode = ps.sqldf(dynamic_query)

    # disease extent
    disease_extent = hp.get_disease_extent(tumor, fe, person, concept)

    extent_query="""
        select
        c.person_id
        ,e.concept_id_disease_extent        as episode_concept_id         
        ,e.start_date                       as episode_start_date
        ,NULL                               as episode_start_datetime
        ,e.end_date                         as episode_end_date
        ,NULL                               as episode_end_datetime
        ,NULL                               as episode_parent_id 
        ,e.episode_number                   as episode_number
        ,c.condition_concept_id             as episode_object_concept_id
        ,32546                              as episode_type_concept_id
        ,c.condition_source_value           as episode_source_value
        ,c.condition_source_concept_id      as episode_source_concept_id
    
        from disease_extent e
        inner join condition_occurrence_t c
        on e.pat_id=c.person_source_value
        and e.tumor_id_fk=c.tumor_id    
    """

    extent_episode = ps.sqldf(extent_query)


    # Implementation of episodes
    # related to the treatment of a tumor 

    # treatment regimen - prep
    regimen = ex.db_export('regimen_ingredients')
    drug_tmp = ex.db_export('temp_drug_exposure')

    # merging operations
    regimen_prep = regimen.merge(drug_tmp, 
                            left_on=['person_id','drug_era_id'],
                            right_on=['person_id','drug_exposure_id'],
                            how='inner')

    # sorting
    regimen_prep = regimen_prep.sort_values(by=['person_id', 'regimen_start_date', 'Tumor_ID_FK'])

    # counter assigned to 'person_id', 'Tumor_ID_FK', 'hemonc_concept_id'
    regimen_prep = regimen_prep[regimen_prep['hemonc_concept_id'].notna()]
    distinct_regimen = regimen_prep[['person_id', 'Tumor_ID_FK', 'hemonc_concept_id']].drop_duplicates()
    distinct_regimen['regimen_number']= (distinct_regimen.groupby(['person_id', 'Tumor_ID_FK']).cumcount() + 1)

    # merging operations
    regimen_prep = regimen_prep.merge(distinct_regimen[['person_id', 'Tumor_ID_FK', 'hemonc_concept_id', 'regimen_number']], on=['person_id', 'Tumor_ID_FK', 'hemonc_concept_id'], how='left')


    regimen_query="""
        select distinct
        r.person_id
        ,32531                              as episode_concept_id         
        ,r.regimen_start_date               as episode_start_date
        ,NULL                               as episode_start_datetime
        ,r.regimen_end_date                 as episode_end_date
        ,NULL                               as episode_end_datetime
        ,NULL                               as episode_parent_id 
        ,r.regimen_number                   as episode_number
        ,r.hemonc_concept_id                as episode_object_concept_id
        ,32546                              as episode_type_concept_id
        ,c.condition_source_value           as episode_source_value
        ,c.condition_source_concept_id      as episode_source_concept_id
    
        from regimen_prep r
        
        left join condition_occurrence_t c
        on c.person_id=r.person_id
        and c.tumor_id=r.tumor_id_fk  
    """

    regimen_episode = ps.sqldf(regimen_query)

    # drug treatment

    # sorting
    drug_tmp = drug_tmp.sort_values(by=['person_id', 'drug_concept_id', 'drug_exposure_start_date', 'Tumor_ID_FK'])
    # counter assigned to episode_number
    drug_tmp['drug_number'] = drug_tmp.groupby(['person_id', 'Tumor_ID_FK']).cumcount() + 1

    drug_query="""
        select
        d.person_id
        ,32941                              as episode_concept_id
        ,d.drug_exposure_start_date         as episode_start_date
        ,NULL                               as episode_start_datetime
        ,d.drug_exposure_end_date           as episode_end_date
        ,NULL                               as episode_end_datetime
        ,NULL                               as episode_parent_id
        ,d.drug_number                      as episode_number
        ,d.drug_concept_id                  as episode_object_concept_id
        ,32546                              as episode_type_concept_id
        ,c.condition_source_value           as episode_source_value
        ,c.condition_source_concept_id      as episode_source_concept_id
    
        from drug_tmp d
        
        left join condition_occurrence_t c
        on c.person_id = d.person_id
        and c.tumor_id = d.tumor_id_fk
    """

    drug_episode = ps.sqldf(drug_query)
    
    # delete temporary table
    imp.delete_table('temp_drug_exposure')


    # concatenate all dfs related to measurement domain + db not null constraints
    episode_merge = pd.concat([disease_episode,dynamic_episode,extent_episode,regimen_episode,drug_episode], ignore_index=True, axis=0)

    ## db not null constraints
    # required: episode_id, person_id, episode_concept_id, episode_start_date, episode_object_concept_id, episode_type_concept_id
    episode_merge = hp.dropna_columns(episode_merge, ['person_id', 'episode_concept_id', 'episode_start_date', 'episode_object_concept_id'], "episode.py", "episode_merge" )

    # #add unique occurrence_id
    episode = hp.add_occurrence(episode_merge, 'episode')

    # db import
    imp.df_import(episode,'episode')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    tumor = imp_dataframes['tumor']
    fe = imp_dataframes['fe']
    
    # stage import
    stage_df = [
        'person',
        'concept',
        'condition_occurrence']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    concept = stage_dataframes['concept'] 
    condition_occurrence = stage_dataframes['condition_occurrence']
    
    create_episode_table(tumor, fe, person, concept, condition_occurrence)