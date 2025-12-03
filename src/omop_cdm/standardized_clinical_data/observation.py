import os
import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp
    
def create_observation_table(patient, tumor, systemtherapie, person, mappings, meta):
    # systemic therapy type / SNOMED
    systemtherapie = hp.calculate_date(systemtherapie, tumor, 'Beginn_SYST', 'Anzahl_Tage_Diagnose_SYST')
    sysytype_query = """
        select p.person_id
        ,m.concept_id                                               as observation_concept_id
        ,s.Beginn_SYST                                              as observation_date
        ,NULL                                                       as observation_datetime
        ,32534                                                      as observation_type_concept_id
        ,NULL                                                       as value_as_number
        ,NULL                                                       as value_as_string
        ,NULL                                                       as value_as_concept_id
        ,NULL                                                       as qualifier_concept_id
        ,NULL                                                       as unit_concept_id
        ,t.inzidenzort_com                                          as provider_id
        ,s.visit                                                    as visit_occurrence_id
        ,NULL                                                       as visit_detail_id
        ,s.Therapieart                                              as observation_source_value
        ,NULL                                                       as observation_source_concept_id
        ,NULL                                                       as unit_source_value
        ,NULL                                                       as qualifier_source_value
        ,NULL                                                       as value_source_value
        -- observation_event_id wird weiter unten gemapped
        ,1147127                                                    as obs_event_field_concept_id
        ,t.icdo3
        
        from systemtherapie s
        inner join person p
        on p.person_source_value=s.pat_id
        inner join tumor t
        on s.pat_id = t.pat_id
        and s.Tumor_Id_FK=t.Tumor_id
        inner join meta m
        on s.Therapieart=m.source_code
        and m.element = 'Therapieart'
        and m."conceptDomain"='Observation'
        
        where t.icdo3 is not null --constraints verletzung möglich
    """

    systype_obs = ps.sqldf(sysytype_query)


    # Morphology / ICDO3/SNOMED
    morph_query = """
        select p.person_id
        ,m.target_concept_id                                        as observation_concept_id
        ,t.Diagnosedatum                                            as observation_date
        ,NULL                                                       as observation_datetime
        ,32534                                                      as observation_type_concept_id
        ,NULL                                                       as value_as_number
        ,NULL                                                       as value_as_string
        ,NULL                                                       as value_as_concept_id
        ,NULL                                                       as qualifier_concept_id
        ,NULL                                                       as unit_concept_id
        ,t.inzidenzort_com                                          as provider_id
        ,t.visit                                                    as visit_occurrence_id
        ,NULL                                                       as visit_detail_id
        ,t.Primaertumor_Morphologie_ICD_O                           as observation_source_value
        ,m.source_concept_id                                        as observation_source_concept_id
        ,NULL                                                       as unit_source_value
        ,NULL                                                       as qualifier_source_value
        ,NULL                                                       as value_source_value
        -- observation_event_id wird weiter unten gemapped
        ,1147127                                                    as obs_event_field_concept_id
        ,t.icdo3
        
        from tumor t
        inner join person p
        on p.person_source_value=t.pat_id
        inner join mappings m
        on t.Primaertumor_Morphologie_ICD_O=m.source_code
        and m.target_domain_id='Observation' 
        and (m.target_vocabulary_id='ICDO3' or m.target_vocabulary_id='SNOMED')
    """

    morph_obs = ps.sqldf(morph_query)

    # Topography / SNOMED
    topo_query = """
        select p.person_id
        ,m.target_concept_id                                        as observation_concept_id
        ,t.diagnosedatum                                            as observation_date
        ,NULL                                                       as observation_datetime
        ,32534                                                      as observation_type_concept_id
        ,NULL                                                       as value_as_number
        ,NULL                                                       as value_as_string
        ,NULL                                                       as value_as_concept_id
        ,NULL                                                       as qualifier_concept_id
        ,NULL                                                       as unit_concept_id
        ,t.inzidenzort_com                                          as provider_id
        ,t.visit                                                    as visit_occurrence_id
        ,NULL                                                       as visit_detail_id
        ,t.Primaertumor_Topographie_ICD_O                           as observation_source_value
        ,m.source_concept_id                                        as observation_source_concept_id
        ,NULL                                                       as unit_source_value
        ,NULL                                                       as qualifier_source_value
        ,NULL                                                       as value_source_value
        -- observation_event_id wird weiter unten gemapped
        ,1147127                                                    as obs_event_field_concept_id
        ,t.icdo3
        
        from tumor t
        inner join person p
        on p.person_source_value=t.pat_id
        inner join mappings m
        on t.Primaertumor_Topographie_ICD_O=m.source_code
        and m.target_domain_id='Spec Anatomic Site' 
        and m.source_vocabulary_id='ICDO3'
        and m.target_vocabulary_id='SNOMED'
    """

    topo_obs = ps.sqldf(topo_query)

    # concatenate all dfs related to measurement domain + db not null constraints
    observation_merge = pd.concat([systype_obs,morph_obs,topo_obs], ignore_index=True).reset_index(drop=True)

    ## db not null constraints
    # required: observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id
    observation_merge = hp.dropna_columns(observation_merge, ['observation_concept_id', 'observation_date'], "observation.py", "observation_merge" )

    # add unique occurrence_id
    observation_temp = observation_merge.drop_duplicates()

    #temporary db import
    imp.df_import(observation_temp,'temp_observation')

    # db configuration to enable script execution on Postgres database
    connection_params = imp.load_config('database')
    sql_path = os.path.join("src/sql/observation_onc_extension.sql")
    # for debugging
    # sql_path = os.path.join("sql/observation_onc_extension.sql")
    imp.run_sql_file(sql_path,connection_params)
    
    # delete temporary table
    imp.delete_table('temp_observation')
    imp.delete_table('temp_condition_occurrence') # not needed anymore
    imp.delete_table('mappings') 

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    patient = imp_dataframes['patient']
    tumor = imp_dataframes['tumor']
    systemtherapie = imp_dataframes['systemtherapie']

    # stage import
    stage_df = [
        'person',
        'mappings',
        'meta']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    mappings = stage_dataframes['mappings']
    meta = stage_dataframes['meta'] 
    
    create_observation_table(patient, tumor, systemtherapie, person, mappings, meta)