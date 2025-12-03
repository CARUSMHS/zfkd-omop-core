import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_procedure_occurrence_table(tumor, op, systemtherapie, strahlentherapie, person, mappings, meta, visit_occurrence):
    # ops / snomed map [1:n]
    # preprocessing
    op = hp.calculate_date(op, tumor, 'Datum_OP', 'Anzahl_Tage_Diagnose_OP')
    op['quantity'] = op['ID'].map(op['ID'].value_counts())
    ops_query = """
            select p.person_id
            ,m.target_concept_id                                         as procedure_concept_id
            ,o.datum_op                                                  as procedure_date
            ,NULL                                                        as procedure_datetime
            ,o.datum_op                                                  as procedure_end_date
            ,NULL                                                        as procedure_end_datetime
            ,32879                                                       as procedure_type_concept_id
            ,NULL                                                        as modifier_concept_id
            ,o.quantity
            ,t.inzidenzort_com                                           as provider_id
            ,o.visit                                                     as visit_occurrence_id
            ,NULL                                                        as visit_detail_id
            ,o.ops                                                       as procedure_source_value
            ,m.source_concept_id                                         as procedure_source_concept_id
            ,NULL                                                        as modifier_source_value 

            from op o
            inner join mappings m
            on o.ops = m.source_code
            and m.source_vocabulary_id = 'OPS'
            and m.source_domain_id = 'Procedure'
            inner join person p
            on o.pat_id = p.person_source_value  
            inner join tumor t
            on o.pat_id = t.pat_id
            and o.tumor_id_fk = t.tumor_id        
    """

    ops_procedure = ps.sqldf(ops_query)

    # surgery intention / snomed [1:1]
    opint_query = """
            select distinct p.person_id
            ,m.concept_id                                                as procedure_concept_id
            ,o.datum_op                                                  as procedure_date
            ,NULL                                                        as procedure_datetime
            ,o.datum_op                                                  as procedure_end_date
            ,NULL                                                        as procedure_end_datetime
            ,32879                                                       as procedure_type_concept_id
            ,NULL                                                        as modifier_concept_id
            ,o.quantity
            ,t.inzidenzort_com                                           as provider_id
            ,o.visit                                                     as visit_occurrence_id
            ,NULL                                                        as visit_detail_id
            ,o.intention                                                 as procedure_source_value
            ,NULL                                                        as procedure_source_concept_id
            ,NULL                                                        as modifier_source_value 

            from op o
            inner join meta m
            on o.intention = m.source_code
            and m.element = 'Intention_der_Operation'
            inner join person p
            on o.pat_id = p.person_source_value   
            inner join tumor t
            on o.pat_id = t.pat_id
            and o.tumor_id_fk = t.tumor_id        
    """

    opint_procedure = ps.sqldf(opint_query)


    # type of therapy / snomed [1:1]
    # preprocessing
    systemtherapie = hp.calculate_date(systemtherapie, tumor, 'Beginn_SYST', 'Anzahl_Tage_Diagnose_SYST')
    systemtherapie['quantity'] = systemtherapie['ID'].map(systemtherapie['ID'].value_counts())

    type_query = """
            select distinct p.person_id
            ,m.concept_id                                                as procedure_concept_id
            ,sy.beginn_syst                                              as procedure_date
            ,NULL                                                        as procedure_datetime
            ,v.visit_end_date                                            as procedure_end_date
            ,NULL                                                        as procedure_end_datetime
            ,32879                                                       as procedure_type_concept_id
            ,NULL                                                        as modifier_concept_id
            ,sy.quantity
            ,t.inzidenzort_com                                           as provider_id
            ,sy.visit                                                    as visit_occurrence_id
            ,NULL                                                        as visit_detail_id
            ,sy.therapieart                                              as procedure_source_value
            ,NULL                                                        as procedure_source_concept_id
            ,NULL                                                        as modifier_source_value 

            from systemtherapie sy
            inner join meta m
            on sy.therapieart = m.source_code
            and m.element = 'Therapieart'
            and m."table" = 'Systemische_Behandlung'
            inner join person p
            on sy.pat_id = p.person_source_value 
            inner join visit_occurrence v
            on sy.visit = v.visit_occurrence_id     
            inner join tumor t
            on sy.pat_id = t.pat_id   
            and sy.tumor_id_fk = t.tumor_id
    """

    type_procedure = ps.sqldf(type_query)

    # aaplication radiation / snomed [1:1]
    # preprocessing
    strahlentherapie = hp.calculate_date(strahlentherapie, tumor, 'Beginn_Bestrahlung', 'Anzahl_Tage_Diagnose_ST')
    strahlentherapie['quantity'] = strahlentherapie['ID'].map(strahlentherapie['ID'].value_counts())

    applispecifi_query = """
            select distinct p.person_id
            ,m.concept_id                                                as procedure_concept_id
            ,str.beginn_bestrahlung                                      as procedure_date
            ,NULL                                                        as procedure_datetime
            ,v.visit_end_date                                            as procedure_end_date
            ,NULL                                                        as procedure_end_datetime
            ,32879                                                       as procedure_type_concept_id
            ,NULL                                                        as modifier_concept_id
            ,str.quantity
            ,t.inzidenzort_com                                           as provider_id
            ,str.visit                                                   as visit_occurrence_id
            ,NULL                                                        as visit_detail_id
            ,str.application_clean                                       as procedure_source_value
            ,NULL                                                        as procedure_source_concept_id
            ,NULL                                                        as modifier_source_value 

            from strahlentherapie str
            inner join meta m
            on str.application_clean = m.source_code
            and m.element = 'Therapieart'
            and m."table" = 'Bestrahlung'
            inner join person p
            on str.pat_id = p.person_source_value 
            inner join visit_occurrence v
            on str.visit = v.visit_occurrence_id      
            inner join tumor t
            on str.pat_id = t.pat_id   
            and str.tumor_id_fk = t.tumor_id   
    """

    applicationspeci_procedure = ps.sqldf(applispecifi_query)
    
    appli_query = """
           select distinct p.person_id
           ,m.concept_id                                                as procedure_concept_id
           ,str.beginn_bestrahlung                                      as procedure_date
           ,NULL                                                        as procedure_datetime
           ,v.visit_end_date                                            as procedure_end_date
           ,NULL                                                        as procedure_end_datetime
           ,32879                                                       as procedure_type_concept_id
           ,NULL                                                        as modifier_concept_id
           ,str.quantity
           ,t.inzidenzort_com                                           as provider_id
           ,str.visit                                                   as visit_occurrence_id
           ,NULL                                                        as visit_detail_id
           ,str.Applikationsart                                         as procedure_source_value
           ,NULL                                                        as procedure_source_concept_id
           ,NULL                                                        as modifier_source_value 

            from strahlentherapie str
            inner join meta m
            on substr(str.Applikationsart,1,1) = m.source_code
            and m.element = 'Therapieart'
            and m."table" = 'Bestrahlung'
            inner join person p
            on str.pat_id = p.person_source_value 
            inner join visit_occurrence v
            on str.visit = v.visit_occurrence_id      
            inner join tumor t
            on str.pat_id = t.pat_id   
            and str.tumor_id_fk = t.tumor_id   
    """

    application_procedure = ps.sqldf(appli_query)

    # concatenate all df related to the procedure domain
    procedure_occurrence_mer = pd.concat([ops_procedure,opint_procedure,type_procedure, applicationspeci_procedure, application_procedure], ignore_index=True)

    ## db not null constraints
    # required: procedure_occurence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id
    procedure_occurrence_mer = hp.dropna_columns(procedure_occurrence_mer, ['procedure_concept_id', 'procedure_date'], "procedure_occurrence.py", "procedure_occurrence_mer" )

    #add unique occurrence_id
    procedure_occurrence = hp.add_occurrence(procedure_occurrence_mer, 'procedure_occurrence')

    #db import
    imp.df_import(procedure_occurrence,'procedure_occurrence')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    tumor = imp_dataframes['tumor']
    systemtherapie = imp_dataframes['systemtherapie']
    strahlentherapie = imp_dataframes['strahlentherapie']
    op = imp_dataframes['op']

    # stage import
    stage_df = [
        'person',
        'mappings',
        'meta',
        'visit_occurrence']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    mappings = stage_dataframes['mappings']
    meta = stage_dataframes['meta']
    visit_occurrence = stage_dataframes['visit_occurrence']
    
    create_procedure_occurrence_table(tumor, op, systemtherapie, strahlentherapie, person, mappings, meta, visit_occurrence)