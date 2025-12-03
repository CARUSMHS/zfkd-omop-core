import json
import pandas as pd
import logging
import numpy as np
import warnings
from dateutil.relativedelta import relativedelta

def init_logger(mode='a'):
    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler("info.log", mode=mode),
        ]
    )

def add_occurrence(df, suffix):
    """
    Add a column with row numbers to a DataFrame.
    
    Parameters:
    df: source DataFrame.
    suffix: respective OMOP Domain for the new column
    
    Returns:
    df:  DataFrame with new column suffix + '_occurrence_id' --> sorted.
    """
   
    # insert df_name_ + occurrence_id
    df[suffix + '_id'] = df.index + 1

    # Reorder the columns so that the new column is the first one
    cols = [suffix + '_id'] + [col for col in df.columns if col != suffix + '_id']
    df = df[cols]

    return df

def dropna_columns(df, columns, class_name, table_name):
    logger = logging.getLogger(class_name)
    logger.info(f'Number of entries in the {table_name} table before drop_columns: {len(df)}')
    for i, column in enumerate(columns):
        df = df.dropna(subset=columns[i]) 
        logger.info(f'Number of entries in the {table_name} table after dropna() on {columns[:i+1]}: {len(df)}')
    return df

# column neccessary for omop table death
def create_death_date(person, patient, tumor):
    death_patient = patient[patient['Verstorben'] == 'J'][['pat_id', 'Datum_Vitalstatus']]
    death_patient = death_patient.rename(columns={'pat_id': 'person_source_value', 'Datum_Vitalstatus': 'death_date'})
    person = person.merge(death_patient, on=['person_source_value'], how='left')
    person['death_date'] = pd.to_datetime(person['death_date'])
    # if no death date available, use Diagnosedatum + Anzahl_Tage_Diagnose_Tod
    tumor_min = tumor.groupby(['pat_id']).agg({
    'Diagnosedatum': 'min',
    'Anzahl_Tage_Diagnose_Tod': 'max'
    }).reset_index()
    tumor_min = tumor_min.rename(columns={'pat_id': 'person_source_value'})
    person = person.merge(tumor_min, on=['person_source_value'])
    mask = person['death_date'].isna()
    person.loc[mask, 'death_date'] = pd.to_datetime(person['Diagnosedatum']) + pd.to_timedelta(person['Anzahl_Tage_Diagnose_Tod'], unit='D')
    person = person.drop(columns=['Diagnosedatum', 'Anzahl_Tage_Diagnose_Tod'])
    return person

def calculate_date(df, tumor_df, date_col, duration_col):
    diagnosedatum_pro_tumor = tumor_df[['pat_id', 'Tumor_ID', 'Diagnosedatum']]
    diagnosedatum_pro_tumor = diagnosedatum_pro_tumor.rename(columns={'Tumor_ID': 'Tumor_ID_FK'})
    df = df.merge(diagnosedatum_pro_tumor, on=['pat_id', 'Tumor_ID_FK'])
    mask = df[date_col].isna()
    df.loc[mask, date_col] = pd.to_datetime(df['Diagnosedatum']) + pd.to_timedelta(df[duration_col], unit='D')
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

def calculate_end_date(df, date_col, duration_col, end_date):
    # if no duration available fill with 0
    df[duration_col] = df[duration_col].fillna(0)
    df[end_date] = pd.to_datetime(df[date_col]) + pd.to_timedelta(df[duration_col], unit='d')
    return df

def get_max_date(df, date_col, duration_col, new_col, person_df):
    # keep only patients where not all date_col == NaN
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    valid_patients = df.groupby(['pat_id']).filter(lambda x: not x[date_col].isna().all())
    if not duration_col:
        df_datum_max = valid_patients.groupby(['pat_id']).agg({
            date_col: 'max',
        }).reset_index()
        df_datum_max = df_datum_max.rename(columns={'pat_id': 'person_source_value'})
        person_df = person_df.merge(df_datum_max, on=['person_source_value'], how='left')
        return person_df
    else:
        idx = valid_patients.groupby(['pat_id'])[date_col].idxmax()
        df_datum_max = valid_patients.loc[idx, ['pat_id', date_col, duration_col]]
        df_datum_max = calculate_end_date(df_datum_max, date_col, duration_col, new_col)
        df_datum_max = df_datum_max.drop(columns=[date_col, duration_col])
        df_datum_max = df_datum_max.rename(columns={'pat_id': 'person_source_value'})
        person_df = person_df.merge(df_datum_max, on=['person_source_value'], how='left')
        return person_df

# column neccessary for omop table observation_period (end_date)
def create_observation_end_date(person, patient, tumor, op, strahlentherapie, systemtherapie, fe):
    # calculate NA dates
    op = calculate_date(op, tumor, 'Datum_OP', 'Anzahl_Tage_Diagnose_OP')
    strahlentherapie = calculate_date(strahlentherapie, tumor, 'Beginn_Bestrahlung', 'Anzahl_Tage_Diagnose_ST')
    systemtherapie = calculate_date(systemtherapie, tumor, 'Beginn_SYST', 'Anzahl_Tage_Diagnose_SYST')
    
    # max dates
    person = get_max_date(tumor, 'Diagnosedatum', None, 'Diagnosedatum', person)
    person = get_max_date(op, 'Datum_OP', None, 'Datum_OP', person)
    person = get_max_date(strahlentherapie, 'Beginn_Bestrahlung', 'Anzahl_Tage_ST', 'Datum_Strahlentherapie', person)
    person = get_max_date(systemtherapie, 'Beginn_SYST', 'Anzahl_Tage_SYST', 'Datum_Systemtherapie', person)
    person = get_max_date(fe, 'Datum_Folgeereignis', None, 'Datum_Folgeereignis', person)
    
    # fill observation_end_date
    person['observation_end_date'] = person['death_date']
    mask_not_death = person['observation_end_date'].isna()
    # step 1: calculate last observation_date
    person.loc[mask_not_death, 'observation_end_date_calculated'] = person.loc[mask_not_death, ['Diagnosedatum', 'Datum_OP', 'Datum_Strahlentherapie', 'Datum_Systemtherapie', 'Datum_Folgeereignis']].max(axis=1)
    person = person.drop(columns=['Diagnosedatum', 'Datum_OP', 'Datum_Strahlentherapie', 'Datum_Systemtherapie', 'Datum_Folgeereignis'])
    # step 2: get Datum_Vitalstatus (a) or calculate if no date available, use Diagnosedatum + Anzahl_Tage_Diagnose_Zensierung (b)
    # step 2a
    patient = patient[['pat_id', 'Datum_Vitalstatus']]
    patient = patient.rename(columns={'pat_id': 'person_source_value', 'Datum_Vitalstatus': 'observation_end_date_available'})
    person = person.merge(patient, on=['person_source_value'])
    person['observation_end_date_available'] = pd.to_datetime(person['observation_end_date_available'])
    # step 2b
    tumor_min = tumor.groupby(['pat_id']).agg({
    'Diagnosedatum': 'min',
    'Anzahl_Monate_Diagnose_Zensierung': 'max'
    }).reset_index()
    tumor_min = tumor_min.rename(columns={'pat_id': 'person_source_value'})
    person = person.merge(tumor_min, on=['person_source_value'])
    mask = person['observation_end_date_available'].isna()
    person['Anzahl_Monate_Diagnose_Zensierung'] = person['Anzahl_Monate_Diagnose_Zensierung'].fillna(0)
    person.loc[mask, 'observation_end_date_available'] = person.loc[mask].apply(lambda row: pd.to_datetime(row['Diagnosedatum']) + relativedelta(months=int(row['Anzahl_Monate_Diagnose_Zensierung'])),axis=1)
    person = person.drop(columns=['Diagnosedatum', 'Anzahl_Monate_Diagnose_Zensierung'])
    # step 3: choose biggest observation date
    person.loc[mask_not_death, 'observation_end_date'] = person.loc[mask_not_death, ['observation_end_date_calculated', 'observation_end_date_available']].max(axis=1)
    person = person.drop(columns=['observation_end_date_calculated', 'observation_end_date_available'])
    return person

# map substances to ATC_code
def map_substances_to_ATC(df_systemtherapie, df_mappings):
    df_mappings = df_mappings[df_mappings['source_vocabulary_id'] == 'ATC']
    
    # 1. df_mappings['source_code']
    source_codes_set = set(df_mappings['source_code'].str.upper().values)
    df_systemtherapie['substances_atc'] = df_systemtherapie['substances'].str.upper()
    df_systemtherapie['substances_atc'] = df_systemtherapie['substances_atc'].where(df_systemtherapie['substances_atc'].isin(source_codes_set), np.nan)
    
    # 2.df_mappings['source_description']
    mapping_dict = df_mappings.set_index(df_mappings['source_description'].str.split(';').str[0].str.upper())['source_code'].str.upper().to_dict()
    mask = df_systemtherapie['substances_atc'].isna()
    df_systemtherapie.loc[mask, 'substances_atc'] = df_systemtherapie.loc[mask, 'substances'].str.upper().map(mapping_dict)
    
    # 3. df_mappings['source_description'] -> substances + 'e'
    df_systemtherapie['substances_e'] = df_systemtherapie['substances'].astype(str) + 'e'
    mask = df_systemtherapie['substances_atc'].isna()
    df_systemtherapie.loc[mask, 'substances_atc'] = df_systemtherapie.loc[mask, 'substances_e'].str.upper().map(mapping_dict)
    
    # 4.df_mappings['source_description'] -> substring of substances
    def map_substance(value, complete):
        if isinstance(value, str):
            value_lower = value.lower()
            if complete:
                matches = [mapping_dict[key] for key in mapping_dict if key.lower() in value_lower]
            else: 
                matches = [mapping_dict[key] for key in mapping_dict if key[:-1].lower() in value_lower]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                return np.nan
        return np.nan
    
    mask = df_systemtherapie['substances_atc'].isna()
    df_systemtherapie.loc[mask, 'substances_atc'] = df_systemtherapie.loc[mask, 'substances'].apply(map_substance, args=(True,))
    
    # 5. df_mappings['source_description'] -> substring of substances + 'e'
    mask = df_systemtherapie['substances_atc'].isna()
    df_systemtherapie.loc[mask, 'substances_atc'] = df_systemtherapie.loc[mask, 'substances'].apply(map_substance, args=(False,))

    return df_systemtherapie
    

def date_convert(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns in the DataFrame that are in the 'YYYYMMDD' format to date.

    """
    for column in df.columns:
        if 'date' in column:
            df[column] = pd.to_datetime(df[column], format='%Y%m%d')         
    return df

def get_episode_interval(df, date_col, disease_col, df_stage_person):
     # min and max date for each tumor: helps to distinguish tumors
    df_min_date = df.groupby('Tumor_ID_FK').agg({date_col: 'min'}).reset_index()
    df_min_date = df_min_date.rename(columns={date_col: 'Min_Datum'})
    df = df.merge(df_min_date, on=['Tumor_ID_FK'])
    df_max_date = df.groupby('Tumor_ID_FK').agg({date_col: 'max'}).reset_index()
    df_max_date = df_max_date.rename(columns={date_col: 'Max_Datum'})
    df = df.merge(df_max_date, on=['Tumor_ID_FK'])
    
    # calculate start_date and end_date for each row
    # shift(-1): nextline in df
    df['start_date'] = df[date_col]
    df['end_date'] = df[date_col].shift(-1) - pd.Timedelta(days=1)
    mask = df[date_col] == df['Max_Datum']
    df.loc[mask, 'end_date'] = df[date_col]
    
    # aggregation: group same disease_types for each tumor (only if they follow each other)
    # status helps to identify canges in disease_type or tumorID
    df['start_date_status'] = df[disease_col] != df[disease_col].shift(+1)
    mask = df[date_col] == df['Min_Datum']
    df.loc[mask, 'start_date_status']  = True
    df['end_date_status'] = df[disease_col] != df[disease_col].shift(-1)
    mask = df[date_col] == df['Max_Datum']
    df.loc[mask, 'end_date_status'] = True

    # drop irrelevant rows and fill end_date
    df = df[~((df['start_date_status'] == False) & (df['end_date_status'] == False))]
    mask = df['end_date_status'] == False
    df.loc[mask, 'end_date'] = df['end_date'].shift(-1)
    df = df[df['start_date_status'] != False]
    
    # for each tumor: replace last end_date by observation_end_date of patient
    person = df_stage_person[["person_source_value", "observation_end_date"]] 
    person = person.rename(columns={'person_source_value': 'pat_id'})
    df = df.merge(person, on=['pat_id'], how='left')
    mask = df["Max_Datum"] == df["end_date"]  
    df.loc[mask, "end_date"] = df["observation_end_date"]
    
    # number per episode for each tumor
    df["episode_number"] = df.groupby(['pat_id', 'Tumor_ID_FK']).cumcount() + 1
    
    df = df.drop(columns=['Max_Datum', 'Min_Datum', 'start_date_status', 'end_date_status', 'observation_end_date'])
    return df

def get_disease_dynamic(df_fe, stage_person, stage_concept):
    rename_dict = {
        'V': 'Complete Remission',
        'R': 'Complete Remission',
        'T': 'Partial Remission',
        'P': 'Progression',
        'K': 'Stable Disease',
        'B': 'Stable Disease',
    }
    
    df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'] = df_fe['Gesamtbeurteilung_Tumorstatus'].replace(rename_dict).replace(['X', 'U', 'D', 'Y'], np.nan)
    # calculate Gesamtbeurteilung_Tumorstatus_disease_dynamic if NaN (+ 20.055 mappings)
    conditions = [
        (df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'].isna()) & 
        (df_fe['Verlauf_Lokaler_Tumorstatus'] == 'K') & (df_fe['Verlauf_Tumorstatus_Lymphknoten'] == 'K') & (df_fe['Verlauf_Tumorstatus_Fernmetastasen'] == 'K'),

        (df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'].isna()) & 
        (df_fe['Verlauf_Lokaler_Tumorstatus'] == 'T') & (df_fe['Verlauf_Tumorstatus_Lymphknoten'] == 'T') & (df_fe['Verlauf_Tumorstatus_Fernmetastasen'] == 'T'),

        (df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'].isna()) & 
        (df_fe['Verlauf_Lokaler_Tumorstatus'] == 'N') & (df_fe['Verlauf_Tumorstatus_Lymphknoten'] == 'N') & (df_fe['Verlauf_Tumorstatus_Fernmetastasen'] == 'N')
        ]
    values = ['Complete Remission', 'Partial Remission', 'Stable Disease']
    df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'] = np.select(conditions, values, default=df_fe['Gesamtbeurteilung_Tumorstatus_disease_dynamic'])
    
    df_fe['Datum_Folgeereignis'] = pd.to_datetime(df_fe['Datum_Folgeereignis'])
    # sort values: important for next line calculation
    df_fe = df_fe.sort_values(by=['Tumor_ID_FK', 'Datum_Folgeereignis']).dropna(subset=['Gesamtbeurteilung_Tumorstatus_disease_dynamic', 'Datum_Folgeereignis'])
    df_fe = df_fe.drop_duplicates(subset=["Tumor_ID_FK", "pat_id", "Gesamtbeurteilung_Tumorstatus_disease_dynamic", "Datum_Folgeereignis"], keep='first')
    
    # add concept_id
    stage_concept = stage_concept[stage_concept['concept_class_id'] == 'Disease Dynamic']
    df_fe["concept_id_disease_dynamic"] = df_fe["Gesamtbeurteilung_Tumorstatus_disease_dynamic"].map(stage_concept.set_index("concept_name")["concept_id"])
    df_fe = df_fe.dropna(subset=['concept_id_disease_dynamic'])
   
    return get_episode_interval(df_fe, 'Datum_Folgeereignis', 'Gesamtbeurteilung_Tumorstatus_disease_dynamic', stage_person)

# information: only cancer entities in 'disease_extent_classification.json' have been considered, feel free to add new cancer entities
# information: so far only entities that can be classified by TNM have been considered
def get_disease_extent(df_tumor, df_fe, stage_person, stage_concept):
    warnings.simplefilter(action='ignore')
    
    # Step 1: find valid ICD codes
    with open("src/utils/disease_extent_classification.json", "r") as file:
        classification_json = json.load(file)
    # for debugging
    # with open("utils/disease_extent_classification.json", "r") as file:
    #     classification_json = json.load(file)
    entities_tnm = classification_json.get("TNM")
    
    df_tumor["Primaertumor_ICD"] = df_tumor["Primaertumor_ICD"].astype(str).str.upper().str.split('.').str[0]
    df_tumor_icd = df_tumor[["pat_id", "Tumor_ID", "Primaertumor_ICD"]]
    df_tumor_icd = df_tumor_icd.rename(columns={'Tumor_ID': 'Tumor_ID_FK'})
    df_fe = df_fe.merge(df_tumor_icd, on=['pat_id', 'Tumor_ID_FK'])

    df_fe= df_fe[df_fe["Primaertumor_ICD"].isin(entities_tnm)]
    df_tumor = df_tumor[df_tumor["Primaertumor_ICD"].isin(entities_tnm)]
    
    # Step 2: data preprocessing: replace useless values
    
    def check_tnm(value, valid_list):
        if pd.isna(value):
            return np.nan 
        matches = [item for item in valid_list if item in value]
        if len(matches) == 1:
            return matches[0]
        else:
            return np.nan

    # List of valid characters
    valid_list_t = ["0", "1", "2", "3", "4"]
    valid_list_n = ["0", "1", "2", "3"]
    valid_list_m = ["0", "1"]
    # TNM 
    convert_col_tumor = ['pTNM_T', 'cTNM_T', 'pTNM_N', 'cTNM_N', 'pTNM_M', 'cTNM_M']
    convert_col_fe = ['Folgeereignis_TNM_T', 'Folgeereignis_TNM_N', 'Folgeereignis_TNM_M']
    df_tumor[convert_col_tumor] = df_tumor[convert_col_tumor].astype(pd.StringDtype())
    df_fe[convert_col_fe] = df_fe[convert_col_fe].astype(pd.StringDtype())
    # special case for T: is
    df_tumor['pTNM_T'] = df_tumor['pTNM_T'].apply(lambda x: "1" if pd.notna(x) and 'is' in x else x)
    df_tumor['cTNM_T'] = df_tumor['cTNM_T'].apply(lambda x: "1" if pd.notna(x) and 'is' in x else x)
    df_fe['Folgeereignis_TNM_T'] = df_fe['Folgeereignis_TNM_T'].apply(lambda x: "1" if pd.notna(x) and 'is' in x else x)
    
    df_tumor['pTNM_T'] = df_tumor['pTNM_T'].apply(lambda x: check_tnm(x, valid_list_t))
    df_tumor['cTNM_T'] = df_tumor['cTNM_T'].apply(lambda x: check_tnm(x, valid_list_t))
    df_tumor['pTNM_N'] = df_tumor['pTNM_N'].apply(lambda x: check_tnm(x, valid_list_n))
    df_tumor['cTNM_N'] = df_tumor['cTNM_N'].apply(lambda x: check_tnm(x, valid_list_n))
    df_tumor['pTNM_M'] = df_tumor['pTNM_M'].apply(lambda x: check_tnm(x, valid_list_m))
    df_tumor['cTNM_M'] = df_tumor['cTNM_M'].apply(lambda x: check_tnm(x, valid_list_m))
    df_fe['Folgeereignis_TNM_T'] = df_fe['Folgeereignis_TNM_T'].apply(lambda x: check_tnm(x, valid_list_t))
    df_fe['Folgeereignis_TNM_N'] = df_fe['Folgeereignis_TNM_N'].apply(lambda x: check_tnm(x, valid_list_n))
    df_fe['Folgeereignis_TNM_M'] = df_fe['Folgeereignis_TNM_M'].apply(lambda x: check_tnm(x, valid_list_m))
    
    # Verlauf df_Fe
    verlauf_all = ["K", "T", "P", "N", "R"]
    df_fe['Verlauf_Tumorstatus_Fernmetastasen'] = df_fe['Verlauf_Tumorstatus_Fernmetastasen'].where(df_fe['Verlauf_Tumorstatus_Fernmetastasen'].isin(verlauf_all), np.nan)
    df_fe['Verlauf_Tumorstatus_Lymphknoten'] = df_fe['Verlauf_Tumorstatus_Lymphknoten'].where(df_fe['Verlauf_Tumorstatus_Lymphknoten'].isin(verlauf_all), np.nan)
    df_fe['Verlauf_Lokaler_Tumorstatus'] = df_fe['Verlauf_Lokaler_Tumorstatus'].where(df_fe['Verlauf_Lokaler_Tumorstatus'].isin(verlauf_all), np.nan)
    
    # Primaertumor_LK_befallen and Primaertumor_LK_untersucht: must be numeric
    if not pd.api.types.is_numeric_dtype(df_tumor["Primaertumor_LK_befallen"]):
        df_tumor["Primaertumor_LK_befallen"] = pd.to_numeric(df_tumor["Primaertumor_LK_befallen"], errors='coerce')
    if not pd.api.types.is_numeric_dtype(df_tumor["Primaertumor_LK_untersucht"]):
        df_tumor["Primaertumor_LK_untersucht"] = pd.to_numeric(df_tumor["Primaertumor_LK_untersucht"], errors='coerce')
    
    # 3 new columns: T(0/1), N(0/1), M(0/1)
    # Step 3: df_tumor (pTNM > cTNM > primärdiagnose_menge_fm/ primaertumor_lk_befallen)
    def calculate_M_tumor(row):
        if pd.notna(row['pTNM_M']):
            return row['pTNM_M']
        elif pd.notna(row['cTNM_M']):
            return row['cTNM_M']
        elif pd.notna(row['Primaerdiagnose_Menge_FM']): 
            return "1"
        else: 
            return np.nan
        
    df_tumor['M'] = df_tumor.apply(calculate_M_tumor, axis=1)
    
    def calculate_N_tumor(row):
        if pd.notna(row['pTNM_N']):
            return row['pTNM_N']
        elif pd.notna(row['cTNM_N']):
            return row['cTNM_N']
        elif pd.notna(row['Primaertumor_LK_untersucht']) and pd.notna(row['Primaertumor_LK_befallen']) and row['Primaertumor_LK_untersucht'] >= row['Primaertumor_LK_befallen']:
            if row['Primaertumor_LK_befallen'] == 0:
                return "0"
            else:
                return "1"
        else:
            return np.nan

    df_tumor['N'] = df_tumor.apply(calculate_N_tumor, axis=1)

    def calculate_T_tumor(row):
        if pd.notna(row['pTNM_T']):
            return row['pTNM_T']
        elif pd.notna(row['cTNM_T']):
            return row['cTNM_T']
        else:
            return np.nan
        
    df_tumor['T'] = df_tumor.apply(calculate_T_tumor, axis=1)
     
    # Step 4: df_fe (TNM > tumorstatus_metastasen/lymphknoten/lokal > primärdiagnose_menge_fm)
    def calculate_M_fe(row):
        if pd.notna(row['Folgeereignis_TNM_M']):
            return row['Folgeereignis_TNM_M']
        elif pd.notna(row['Verlauf_Tumorstatus_Fernmetastasen']):
            if row['Verlauf_Tumorstatus_Fernmetastasen'] == "K":
                return "0"
            else:
                return "1"
        elif pd.notna(row['Menge_FM']): 
            return "1"
        else: 
            return np.nan
        
    df_fe['M'] = df_fe.apply(calculate_M_fe, axis=1)
    
    def calculate_N_fe(row):
        if pd.notna(row['Folgeereignis_TNM_N']):
            return row['Folgeereignis_TNM_N']
        elif pd.notna(row['Verlauf_Tumorstatus_Lymphknoten']):
            if row['Verlauf_Tumorstatus_Lymphknoten'] == "K":
                return "0"
            else:
                return "1"
        else: 
            return np.nan
        
    df_fe['N'] = df_fe.apply(calculate_N_fe, axis=1)
    
    def calculate_T_fe(row):
        if pd.notna(row['Folgeereignis_TNM_T']):
            return row['Folgeereignis_TNM_T']
        elif pd.notna(row['Verlauf_Lokaler_Tumorstatus']):
            if row['Verlauf_Lokaler_Tumorstatus'] == "K":
                return "0"
            else:
                return "1"
        else: 
            return np.nan
        
    df_fe['T'] = df_fe.apply(calculate_T_fe, axis=1)
    
    # Step 5: new col: disease extent:
    tnm_col = ['T', 'N', 'M']
    # df_tumor
    for col in tnm_col:
        if not pd.api.types.is_numeric_dtype(df_tumor[col]):
            df_tumor[col] = pd.to_numeric(df_tumor[col], errors='coerce')
    
    # df_fe
    for col in tnm_col:
        if not pd.api.types.is_numeric_dtype(df_fe[col]):
            df_fe[col] = pd.to_numeric(df_fe[col], errors='coerce')

    # special cases: T == 4 and N == 0 and M == 0 -> find extra information in disease_extent_classification.json
    entities_T4_invasive = classification_json.get("T4_N0_M0_invasive_disease")
    entities_T4_nan = classification_json.get("T4_N0_M0_nan")
    
    def assign_disease(row):
        global num_nan_t4
        global num_inv_disease_t4
        if pd.notna(row['M']) and row['M'] >= 1.0:
            return 'Metastatic Disease'
        elif pd.notna(row['N']) and row['N'] >= 1.0:
            return 'Invasive Disease'
        elif pd.notna(row['T']) and row['T'] == 4.0 and row['Primaertumor_ICD'] in entities_T4_invasive:
            return 'Invasive Disease'
        elif pd.notna(row['T']) and row['T'] == 4.0 and row['Primaertumor_ICD'] in entities_T4_nan:
            return np.nan
        elif pd.notna(row['T']) and row['T'] >= 1.0:
            return 'Confined Disease'
        else:
            return np.nan

    df_tumor['disease_extent'] = df_tumor.apply(assign_disease, axis=1)
    df_fe['disease_extent'] = df_fe.apply(assign_disease, axis=1)
    
    # concat tumor and fe df
    df_tumor['date'] = pd.to_datetime(df_tumor['Diagnosedatum'])
    df_fe['date'] = pd.to_datetime(df_fe['Datum_Folgeereignis'])
    df_tumor_concat = df_tumor[['pat_id', 'Tumor_ID', 'date', 'disease_extent']].rename(columns={'Tumor_ID': 'Tumor_ID_FK'})
    df_fe_concat = df_fe[['pat_id', 'Tumor_ID_FK', 'date', 'disease_extent']]
    df_concat = pd.concat([df_tumor_concat, df_fe_concat])
    
    # sort values: important for next line calculation
    df_concat = df_concat.sort_values(by=['Tumor_ID_FK', 'date']).dropna(subset=['disease_extent', 'date'])
    df_concat = df_concat.drop_duplicates(subset=["Tumor_ID_FK", "pat_id", "disease_extent", "date"], keep='first')
    
    # add concept_id
    stage_concept = stage_concept[stage_concept['concept_class_id'] == 'Disease Extent']
    df_concat["concept_id_disease_extent"] = df_concat["disease_extent"].map(stage_concept.set_index("concept_name")["concept_id"])
    df_concat = df_concat.dropna(subset=['concept_id_disease_extent'])
    
    return get_episode_interval(df_concat, 'date', 'disease_extent', stage_person)


def disease_episode_prep(stage_condition, stage_person, imp_tumor, stage_concept):
    """
    add a counter to icd03 conditions of a tumor patient
    """
    # merges condition df and icdo3 concept from concept df
    merge1 = stage_condition.merge(stage_concept[stage_concept['vocabulary_id'] == 'ICDO3'], 
                             left_on='condition_concept_id', 
                             right_on='concept_id', 
                             how='inner')
    
    # sorting
    merge1 = merge1.sort_values(by=['person_id', 'condition_concept_id', 'condition_start_date'])
    
    # further merging operations
    merge2 = merge1.merge(stage_person, on='person_id', how='inner')
    merge = merge2.merge(imp_tumor, 
                         left_on=['person_source_value','condition_source_value','visit_occurrence_id'],
                         right_on=['pat_id','icdo3','visit'],
                         how='inner')
    
    # counter assigned to episode_number
    merge['episode_number'] = merge.groupby(['person_id', 'Tumor_ID']).cumcount() + 1
    
    return merge
