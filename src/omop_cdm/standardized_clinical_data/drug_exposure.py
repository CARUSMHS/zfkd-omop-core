import pandas as pd
import pandasql as ps
from utils import importer as imp
from utils import helper as hp

def create_drug_exposure_table(tumor, systemtherapie, person, mappings):
    # preprocessing
    hp.map_substances_to_ATC(systemtherapie, mappings)
    systemtherapie = hp.calculate_end_date(systemtherapie, 'Beginn_SYST', 'Anzahl_Tage_SYST', 'drug_exposure_end_date')

    # ATC + Substance / RxNorm [1:1]
    atc_query = """
        select row_number() over (order by sy.syst_id)      as drug_exposure_id
        ,p.person_id
        ,m.target_concept_id                                as drug_concept_id
        ,sy.beginn_syst                                     as drug_exposure_start_date
        ,NULL                                               as drug_exposure_start_datetime
        ,sy.drug_exposure_end_date  
        ,NULL                                               as drug_exposure_end_datetime
        ,NULL                                               as verbatim_end_date
        ,32879                                              as drug_type_concept_id
        ,NULL                                               as stop_reason
        ,NULL                                               as refills
        ,NULL                                               as quantity
        ,sy.anzahl_tage_syst                                as days_supply
        ,NULL                                               as sig
        ,NULL                                               as route_concept_id
        ,NULL                                               as lot_number
        ,t.inzidenzort_com                                  as provider_id
        ,sy.visit                                           as visit_occurrence_id
        ,NULL                                               as visit_detail_id
        ,sy.substances_atc                                  as drug_source_value
        ,m.source_concept_id                                as drug_source_concept_id
        ,NULL                                               as route_source_value
        ,NULL                                               as dose_unit_source_value
        ,sy.tumor_id_fk

        from systemtherapie sy
        inner join person p
        on sy.pat_id = p.person_source_value
        inner join mappings m
        on sy.substances_atc = m.source_code
        and m.source_vocabulary_id='ATC'
        and m.source_domain_id='Drug'
        inner join tumor t
        on sy.pat_id=t.pat_id
        and sy.tumor_id_fk=t.tumor_id
        
        """

    atc_drug = ps.sqldf(atc_query)

    ## db not null constraints
    # required: drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id
    atc_drug = hp.dropna_columns(atc_drug, ['drug_concept_id', 'drug_exposure_start_date', 'drug_exposure_end_date'], "drug_exposure.py", "atc_drug" )

    # temporary db import  needed for episode module (during episode load, tmp_drug data is deleted)
    imp.df_import(atc_drug,'temp_drug_exposure')

    atc_drug = atc_drug.drop(['Tumor_ID_FK'], axis=1)
    # db import
    imp.df_import(atc_drug,'drug_exposure')

if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    tumor = imp_dataframes['tumor']
    systemtherapie = imp_dataframes['systemtherapie']
    
    # stage import
    stage_df = [
        'person',
        'mappings']
    stage_dataframes = imp.stage_import(stage_df)
    person = stage_dataframes['person']
    mappings = stage_dataframes['mappings'] 

    create_drug_exposure_table(tumor, systemtherapie, person, mappings)