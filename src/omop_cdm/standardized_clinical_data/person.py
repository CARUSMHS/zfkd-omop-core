import pandas as pd
import pandasql as ps
from utils import importer  as imp
from utils import exporter as ex
from utils import helper as hp

def calculate_birth_year(patient, tumor):
    patient['Geburtsjahr'] = pd.to_datetime(patient['Geburtsdatum']).dt.year
    
    # ai-care specific
    if "Alter_bei_Diagnose" in tumor.columns:
        tumor_min = tumor.groupby(['pat_id']).agg({
            'Diagnosedatum': 'min',
            'Alter_bei_Diagnose': 'min'
        }).reset_index()
        patient = patient.merge(tumor_min, on=['pat_id'], how='left')
        patient['Diagnosejahr'] = pd.to_datetime(patient['Diagnosedatum']).dt.year
        mask = patient['Geburtsjahr'].isna() & patient['Diagnosejahr'].notna() & patient['Alter_bei_Diagnose'].notna()
        patient.loc[mask, 'Geburtsjahr'] = patient['Diagnosejahr'] - patient['Alter_bei_Diagnose']
        patient = patient.drop(columns=['Diagnosedatum', 'Diagnosejahr', 'Alter_bei_Diagnose'])
    
    patient = patient.dropna(subset=['Geburtsjahr'])
    patient['Geburtsjahr'] = patient['Geburtsjahr'].astype(int)
    return patient

def create_person_table(patient, tumor, meta):
    # preprocessing
    patient = calculate_birth_year(patient, tumor)

    query = """
        select row_number() over (order by p.patient_id)    as person_id 
        ,m.concept_id		                                as gender_concept_id
        ,p.Geburtsjahr                                      as year_of_birth
        ,strftime('%m',p.Geburtsdatum)                      as month_of_birth
        ,strftime('%d',p.Geburtsdatum)                      as day_of_birth
        ,NULL                                               as birth_datetime
        ,0                                                  as race_concept_id
	    ,0                                                  as ethnicity_concept_id
        ,NULL                                               as location_id
	    ,prov.inzidenzort_com                               as provider_id 
	    ,prov.register_id_fk                                as care_site_id 
	    ,p.pat_id                                           as person_source_value
	    ,p.geschlecht                                       as gender_source_value
	    ,NULL                                               as gender_source_concept_id
	    ,NULL                                               as race_source_value
	    ,NULL                                               as race_source_concept_id
	    ,NULL                                               as ethnicity_source_value
	    ,NULL                                               as ethnicity_source_concept_id

        from patient p
        inner join meta m
        on p.Geschlecht = m.source_code
        and m.conceptDomain='Gender'
        
        left join (
            select * from (
                select *,
                    ROW_NUMBER() over (
                        PARTITION BY pat_id
                        ORDER BY diagnosedatum desc
                    ) as rn
                from tumor
            ) vd
            where rn = 1
        ) prov on p.pat_id = prov.pat_id
    """
    person = ps.sqldf(query)

    ## db not null constraints
    # required: person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id
    person = hp.dropna_columns(person, ['gender_concept_id', 'year_of_birth'], "person.py", "person" )

    # db import
    imp.df_import(person,'person')
    return person

def export_person_into_stage (person, patient, tumor, op, strahlentherapie, systemtherapie, fe):
    # add columns death_date and observation_end_date to stage table person 
    # columns neccessary for omop table death and observation_period (end_date)
    person = hp.create_death_date(person, patient, tumor)
    person = hp.create_observation_end_date(person, patient, tumor, op, strahlentherapie, systemtherapie, fe)

    # stage export
    ex.stage_export(person,'person')
    return person


if __name__ == "__main__":
    # logging
    hp.init_logger()

    # source import
    imp_dataframes = imp.source_import() 
    for name, df in imp_dataframes.items():
        globals()[name] = df
    patient = imp_dataframes['patient']
    tumor = imp_dataframes['tumor']
    op = imp_dataframes['op']
    strahlentherapie = imp_dataframes['strahlentherapie']
    systemtherapie = imp_dataframes['systemtherapie']
    fe = imp_dataframes['fe']
    
    # stage import
    stage_df = ['meta']
    stage_dataframes = imp.stage_import(stage_df)
    meta = stage_dataframes['meta'] 
    
    person = create_person_table(patient, tumor, meta)
    export_person_into_stage(person, patient, tumor, op, strahlentherapie, systemtherapie, fe)
    