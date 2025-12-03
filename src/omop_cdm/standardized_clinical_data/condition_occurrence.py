import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp
from utils import exporter as ex


def create_condition_occurrence_table(patient, tumor, person, mappings):
    
    # icd10 - snomed [1:1]
    icd_query= """
        select p.person_id
        ,m.target_concept_id                                               as condition_concept_id
        ,t.diagnosedatum                                                   as condition_start_date
        ,NULL                                                              as condition_start_datetime
        ,p.observation_end_date                                            as condition_end_date
        ,NULL                                                              as condition_end_datetime
        ,32879                                                             as condition_type_concept_id
        ,32902                                                             as condition_status_concept_id
        ,NULL                                                              as stop_reason
        ,t.inzidenzort_com                                                 as provider_id 
        ,t.visit                                                           as visit_occurrence_id
        ,NULL                                                              as visit_detail_id
        ,t.primaertumor_icd                                                as condition_source_value
        ,m.source_concept_id                                               as condition_source_concept_id
        ,NULL                                                              as condition_status_source_value
        ,t.icdo3

        from tumor t
        inner join person p
        on p.person_source_value=t.pat_id
        inner join mappings m
        on t.primaertumor_icd=m.source_code
        and m.source_vocabulary_id = 'ICD10GM'
        and m.target_vocabulary_id = 'SNOMED'
        and m.target_domain_id = 'Condition'
        
        where substr(t.primaertumor_icd,1,3) <> 'C80' -- oncology data assessement issue
    """

    icd_condition = ps.sqldf(icd_query)

    #icdo3 / snomed [if not available icdo3] [1:1]

    icdo_query = """
        select p.person_id
        ,m.target_concept_id                                               as condition_concept_id
        ,t.diagnosedatum                                                   as condition_start_date
        ,NULL                                                              as condition_start_datetime
        ,p.observation_end_date                                            as condition_end_date
        ,NULL                                                              as condition_end_datetime
        ,32879                                                             as condition_type_concept_id
        ,32902                                                             as condition_status_concept_id
        ,NULL                                                              as stop_reason
        ,t.inzidenzort_com                                                 as provider_id 
        ,t.visit                                                           as visit_occurrence_id
        ,NULL                                                              as visit_detail_id
        ,t.icdo3                                                           as condition_source_value
        ,m.source_concept_id                                               as condition_source_concept_id
        ,NULL                                                              as condition_status_source_value
        ,t.icdo3

        from tumor t
        inner join person p
        on p.person_source_value=t.pat_id
        inner join mappings m
        on t.icdo3=m.source_code
        and m.source_vocabulary_id = 'ICDO3'
        and m.source_domain_id = 'Condition'
    """

    icdo_condition = ps.sqldf(icdo_query)

    # concatenate all df related to the condition domain
    condition_occurrence_temp = pd.concat([icd_condition,icdo_condition], ignore_index=True)

    ## db not null constraints
    # required: condition_occurence_id, person_id, condition_concept_id, condition_start_date, condition_type_concept_id
    condition_occurrence_temp = hp.dropna_columns(condition_occurrence_temp, ['condition_concept_id', 'condition_start_date'], "condition_occurrence.py", "condition_occurrence_temp")

    #add unique occurrence_id
    condition_occurrence_temp = hp.add_occurrence(condition_occurrence_temp, 'condition_occurrence')
    condition_occurrence = condition_occurrence_temp.drop(['icdo3'], axis=1)

    # stage import
    ex.stage_export(condition_occurrence,'condition_occurrence')

    ## db import
    imp.df_import(condition_occurrence,'condition_occurrence')
    imp.df_import(condition_occurrence_temp,'temp_condition_occurrence')
    return condition_occurrence

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    patient = imp_dataframes['patient']
    tumor = imp_dataframes['tumor']

    # stage import
    stage_df = [
        'person',
        'mappings']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    mappings = stage_dataframes['mappings'] 
    
    create_condition_occurrence_table(patient, tumor, person, mappings)