import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import exporter as ex
from utils import helper as hp

def create_visit_occurrence_table(patient, tumor, op, strahlentherapie, systemtherapie, fe, person):
    # Tumor Event
    t_query = """
            select
            t.visit                                           as visit_occurrence_id
            ,p.person_id
            ,9202                                             as visit_concept_id
            ,t.diagnosedatum                                  as visit_start_date
            ,NULL                                             as visit_start_datetime
            ,p.observation_end_date                           as visit_end_date
            ,NULL                                             as visit_end_datetime
            ,32879                                            as visit_type_concept_id
            ,inzidenzort_com                                  as provider_id 
            ,t.register_id_fk                                 as care_site_id
            ,'Diagnosis'                                      as visit_source_value
            ,NULL                                             as visit_source_concept_id
            ,NULL                                             as admitted_from_concept_id
            ,NULL                                             as admitted_from_source_value
            ,NULL                                             as discharged_to_concept_id
            ,NULL                                             as discharged_to_source_value
            ,NULL                                             as preceding_visit_occurrence_id

            from tumor t
            inner join person p
            on p.person_source_value=t.pat_id
            inner join patient pa
            on t.pat_id=pa.pat_id
    """

    tumor_event = ps.sqldf(t_query)

    # Surgery Event
    # preprocessing
    op = hp.calculate_date(op, tumor, 'Datum_OP', 'Anzahl_Tage_Diagnose_OP')

    o_query = """
            select distinct
            o.visit                                           as visit_occurrence_id
            ,p.person_id
            ,9202                                             as visit_concept_id
            ,o.datum_op                                       as visit_start_date
            ,NULL                                             as visit_start_datetime
            ,o.datum_op                                       as visit_end_date
            ,NULL                                             as visit_end_datetime
            ,32879                                            as visit_type_concept_id
            ,inzidenzort_com                                  as provider_id 
            ,o.register_id_fk                                 as care_site_id
            ,'Surgery'                                        as visit_source_value
            ,NULL                                             as visit_source_concept_id
            ,NULL                                             as admitted_from_concept_id
            ,NULL                                             as admitted_from_source_value
            ,NULL                                             as discharged_to_concept_id
            ,NULL                                             as discharged_to_source_value
            ,NULL                                             as preceding_visit_occurrence_id

            from op o
            inner join person p
            on p.person_source_value=o.pat_id
            inner join patient pa
            on p.person_source_value=pa.pat_id
            inner join tumor t
            on o.pat_id=t.pat_id
            and o.Tumor_id_FK=t.Tumor_id
    """

    op_event = ps.sqldf(o_query)


    # Radiation event
    # preprocessing
    strahlentherapie = hp.calculate_date(strahlentherapie, tumor, 'Beginn_Bestrahlung', 'Anzahl_Tage_Diagnose_ST')
    strahlentherapie = hp.calculate_end_date(strahlentherapie, 'Beginn_Bestrahlung', 'Anzahl_Tage_ST', 'visit_end_date')

    ra_query = """
            select
            s.visit                                           as visit_occurrence_id
            ,p.person_id
            ,9202                                             as visit_concept_id
            ,s.beginn_bestrahlung                             as visit_start_date
            ,NULL                                             as visit_start_datetime
            ,s.visit_end_date                                 as visit_end_date
            ,NULL                                             as visit_end_datetime
            ,32879                                            as visit_type_concept_id
            ,inzidenzort_com                                  as provider_id 
            ,s.register_id_fk                                 as care_site_id
            ,'Radiation'                                      as visit_source_value
            ,NULL                                             as visit_source_concept_id
            ,NULL                                             as admitted_from_concept_id
            ,NULL                                             as admitted_from_source_value
            ,NULL                                             as discharged_to_concept_id
            ,NULL                                             as discharged_to_source_value
            ,NULL                                             as preceding_visit_occurrence_id

            from strahlentherapie s
            inner join person p
            on p.person_source_value=s.pat_id
            inner join patient pa
            on p.person_source_value=pa.pat_id
            inner join tumor t
            on s.pat_id=t.pat_id
            and s.Tumor_id_FK=t.Tumor_id
    """

    ra_event = ps.sqldf(ra_query)

    # systemic events
    # preprocessing
    systemtherapie = hp.calculate_date(systemtherapie, tumor, 'Beginn_SYST', 'Anzahl_Tage_Diagnose_SYST')
    systemtherapie = hp.calculate_end_date(systemtherapie, 'Beginn_SYST', 'Anzahl_Tage_SYST', 'visit_end_date')

    sy_query = """
            select distinct
            sy.visit                                          as visit_occurrence_id
            ,p.person_id
            ,9202                                             as visit_concept_id
            ,sy.beginn_syst                                   as visit_start_date
            ,NULL                                             as visit_start_datetime
            ,sy.visit_end_date                                as visit_end_date
            ,NULL                                             as visit_end_datetime
            ,32879                                            as visit_type_concept_id
            ,inzidenzort_com                                  as provider_id 
            ,sy.register_id_fk                                as care_site_id
            ,'Systemic'                                       as visit_source_value
            ,NULL                                             as visit_source_concept_id
            ,NULL                                             as admitted_from_concept_id
            ,NULL                                             as admitted_from_source_value
            ,NULL                                             as discharged_to_concept_id
            ,NULL                                             as discharged_to_source_value
            ,NULL                                             as preceding_visit_occurrence_id

            from systemtherapie sy
            inner join person p
            on p.person_source_value=sy.pat_id
            inner join patient pa
            on p.person_source_value=pa.pat_id
            inner join tumor t
            on sy.pat_id=t.pat_id
            and sy.Tumor_id_FK=t.Tumor_id
    """

    sy_event = ps.sqldf(sy_query)

    # status event
    stat_query = """
            select
            f.visit                                           as visit_occurrence_id
            ,p.person_id
            ,9202                                             as visit_concept_id
            ,f.datum_folgeereignis                            as visit_start_date
            ,NULL                                             as visit_start_datetime
            ,f.datum_folgeereignis                            as visit_end_date
            ,NULL                                             as visit_end_datetime
            ,32879                                            as visit_type_concept_id
            ,inzidenzort_com                                  as provider_id 
            ,f.register_id_fk                                 as care_site_id
            ,'Status'                                         as visit_source_value
            ,NULL                                             as visit_source_concept_id
            ,NULL                                             as admitted_from_concept_id
            ,NULL                                             as admitted_from_source_value
            ,NULL                                             as discharged_to_concept_id
            ,NULL                                             as discharged_to_source_value
            ,NULL                                             as preceding_visit_occurrence_id

            from fe f
            inner join person p
            on p.person_source_value=f.pat_id
            inner join patient pa
            on p.person_source_value=pa.pat_id
            inner join tumor t
            on f.pat_id=t.pat_id
            and f.Tumor_id_FK=t.Tumor_id
    """

    stat_event = ps.sqldf(stat_query)


    visit_occurrence = pd.concat([tumor_event,op_event,ra_event,sy_event,stat_event], ignore_index=True)

    ## db not null constraints
    # required: visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_end_date, visit_type_concept_id
    visit_occurrence = hp.dropna_columns(visit_occurrence, ['visit_start_date', 'visit_end_date'], "visit_occurrence.py", "visit_occurrence" )

    # stage import
    ex.stage_export(visit_occurrence,'visit_occurrence')

    # db import
    imp.df_import(visit_occurrence,'visit_occurrence')
    return visit_occurrence

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
    strahlentherapie = imp_dataframes['strahlentherapie']
    op = imp_dataframes['op']
    fe = imp_dataframes['fe']
    
    # stage import
    stage_df = ['person']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    
    create_visit_occurrence_table(patient, tumor, op, strahlentherapie, systemtherapie, fe, person)
 