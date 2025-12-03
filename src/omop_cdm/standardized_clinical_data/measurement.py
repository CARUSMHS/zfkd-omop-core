import os
import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_measurement_table(patient, tumor, fe, op, person, meta, mappings):
    ## side location / cancer modifier
    side_query = """
                select p.person_id
                ,m.concept_id                                       as measurement_concept_id
                ,t.diagnosedatum                                    as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,NULL                                               as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,t.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,t.seitenlokalisation                               as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from tumor t
                inner join person p
                on p.person_source_value=t.pat_id
                inner join meta m
                on t.seitenlokalisation = m.source_code
                and m.element = 'Seitenlokalisation'
    """

    side_measure = ps.sqldf(side_query)

    ## grading / cancer modifier
    grad_query = """
                select p.person_id
                ,m.concept_id                                       as measurement_concept_id
                ,t.diagnosedatum                                    as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,NULL                                               as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,t.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,t.primaertumor_grading                             as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from tumor t
                inner join person p
                on p.person_source_value=t.pat_id
                inner join meta m
                on t.primaertumor_grading=m.source_code
                and m.element = 'Grading'
    """

    grad_measure = ps.sqldf(grad_query)

    ## clincal uicc / cancer modifier
    cuicc_query = """
                select p.person_id
                ,m.concept_id                                       as measurement_concept_id
                ,t.diagnosedatum                                    as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,NULL                                               as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,t.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,t.cTNM_UICC_Stadium                                as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from tumor t
                inner join person p
                on p.person_source_value=t.patient_id_fk
                inner join meta m
                on t.cTNM_UICC_Stadium=m.source_code
                and m.element = 'Clinical_UICC'
    """

    cuicc_measure = ps.sqldf(cuicc_query)

    ## pathological uicc  / cancer modifier
    puicc_query = """
                select p.person_id
                ,m.concept_id                                       as measurement_concept_id
                ,t.diagnosedatum                                    as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,NULL                                               as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,t.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,t.pTNM_UICC_Stadium                                as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from tumor t
                inner join person p
                on p.person_source_value=t.pat_id
                --and t.register_id_fk=pa.register_id_fk
                inner join meta m
                on t.pTNM_UICC_Stadium=m.source_code
                and m.element = 'AJCC_UICC_Stage_Group_path'
    """

    puicc_measure = ps.sqldf(puicc_query)

    ## Diagnostic accuracy - implemented as Meas Value (Diagnostic Confirmation - 35918797 - NAACCR)
    dacc_query = """
                select p.person_id
                ,35918797                                           as measurement_concept_id
                ,t.diagnosedatum                                    as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,m.concept_id                                       as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,t.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,t.diagnosesicherung                                as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from tumor t
                inner join person p
                on p.person_source_value=t.pat_id
                inner join meta m
                on t.diagnosesicherung=m.source_code
                and m.element = 'Diagnosesicherung'
    """
    dacc_measure = ps.sqldf(dacc_query)

    # metastasis / Cancer Modifier
    met_query = """
                select p.person_id
                ,35918797                                           as measurement_concept_id
                ,f.datum_folgeereignis                              as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,m.concept_id                                       as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,f.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,f.menge_fm                                         as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                        as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from fe f
                inner join person p
                on p.person_source_value=f.pat_id
                inner join meta m
                on f.menge_fm=m.source_code
                and m.element = 'Lokalisation_der_Fernmetastase'
                left join tumor t
                on t.pat_id = f.pat_id 
                and t.tumor_id = f.tumor_id_fk
    """

    met_measure = ps.sqldf(met_query)
    
    
    # residual status / Cancer Modifier
    residual_query = """
                select p.person_id
                ,m.concept_id                                       as measurement_concept_id
                ,o.datum_op                                         as measurement_date
                ,NULL                                               as measurement_datetime
                ,NULL                                               as measurement_time
                ,32534                                              as measurement_type_concept_id
                ,NULL                                               as operator_concept_id
                ,NULL                                               as value_as_number
                ,NULL                                               as value_as_concept_id
                ,NULL                                               as unit_concept_id
                ,NULL                                               as range_low
                ,NULL                                               as range_high
                ,t.inzidenzort_com                                  as provider_id
                ,o.visit                                            as visit_occurrence_id
                ,NULL                                               as visit_detail_id
                ,o.Beurteilung_Residualstatus                       as measurement_source_value
                ,NULL                                               as measurement_source_concept_id
                ,NULL                                               as unit_source_value
                ,NULL                                               as unit_source_concept_id
                ,NULL                                               as value_source_value
                --,c.condition_occurrence_id                          as measurement_event_id
                ,1147127                                            as meas_event_field_concept_id
                ,t.icdo3

                from op o
                inner join person p
                on p.person_source_value=o.pat_id
                inner join meta m
                on o.Beurteilung_Residualstatus=m.source_code
                and m.element = 'Residualstatus'
                inner join tumor t
                on t.pat_id = o.pat_id 
                and t.tumor_id = o.tumor_id_fk
    """
    residualstatus_measure = ps.sqldf(residual_query)

    # concatenate all dfs related to measurement domain + db not null constraints
    measurement_merge = pd.concat([side_measure,grad_measure,cuicc_measure, puicc_measure, dacc_measure, met_measure, residualstatus_measure], ignore_index=True).reset_index(drop=True)

    ## db not null constraints
    # required: measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id
    measurement_merge = hp.dropna_columns(measurement_merge, ['measurement_concept_id', 'measurement_date'], "measurement.py", "measurement_merge" )

    # #add unique occurrence_id
    #measurement_merge.reset_index(inplace=True)
    #measurement_temp = hp.add_occurrence(measurement_merge, 'measurement')
    measurement_temp = measurement_merge.drop_duplicates()

    #temporary db import
    imp.df_import(measurement_temp,'temp_measurement')

    # db configuration to enable script execution on Postgres database
    connection_params = imp.load_config('database')
    sql_path = os.path.join("src/sql/measurement_onc_extension.sql")
    # for debugging
    # sql_path = os.path.join("sql/measurement_onc_extension.sql")
    imp.run_sql_file(sql_path,connection_params)
    
    # delete temporary table
    imp.delete_table('temp_measurement')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    patient = imp_dataframes['patient']
    tumor = imp_dataframes['tumor']
    fe = imp_dataframes['fe']
    op = imp_dataframes['op']
    
    # stage import
    stage_df = [
        'person',
        'meta',
        'mappings']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    meta = stage_dataframes['meta'] 
    mappings = stage_dataframes['mappings']
    
    
    create_measurement_table(patient, tumor, fe, op, person, meta, mappings)

