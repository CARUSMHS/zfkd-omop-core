import os
import json
import glob
import re
import psycopg2
import pandas as pd
import sqlalchemy as sq
from sqlalchemy import text
from typing import List
import logging

# -> normal python run: /workspaces/zfkd-omop-core
# -> debugging: /workspaces/zfkd-omop-core/src

def load_config(section=None):
    """loading confuguration information for this etl-worker"""
    
    config_file = os.path.join("config.json")
    # for debugging
    # config_file = os.path.join("/workspaces/zfkd-omop-core/config.json")
    
    with open(config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)
    if section:
        return config.get(section, {})
    
    return config

def csv_importer(*args,**kwargs):
        """
        Read all .csv data of a folder 
        """ 
        folder_path = load_config('path_zfkd_data')
        possible_files = [
            'patient.csv', 'tumor.csv', 'fe.csv', 'op.csv', 'systemtherapie.csv',
            'strahlentherapie.csv', 'modul_mamma.csv' 'systemtherapie_with_substance_service_variable.csv', 
        ]
        
        dataframes = {}
        for filename in os.listdir(folder_path):
            if filename in possible_files:
                if filename.endswith('.csv'):
                    file_path = os.path.join(folder_path, filename)
                    df = pd.read_csv(file_path, low_memory=False)
                    # naming
                    df_name = filename[:-4].lower()  # delete '.csv'
                    if 'fe' in df_name or 'folgeereignis' in df_name:
                        dataframes['fe'] = df  
                    else:
                        dataframes[df_name] = df 

        return dataframes

def stage_import(selected_files: List[str] = None) -> List[pd.DataFrame]:
    """
    reads all CSV-Data or selectes CSV-Data from stage-folder.

    Args:
        selected_files (List[str]): An optional list of filenames (without .csv) to be loaded. If None, all CSV files will be loaded.

    Returns:
        List[pd.DataFrame]: A list of DataFrames representing the loaded CSV files
    """
    stage_folder = os.path.join('src/stage')
    # for debugging
    # stage_folder = os.path.join('stage')
    
    # loading process
    if selected_files is None:
        file_pattern = os.path.join(stage_folder, '*.csv')
        csv_files = glob.glob(file_pattern)
    else:
        # only necessary files
        csv_files = [os.path.join(stage_folder, f"{file}.csv") for file in selected_files]

    # load CSV data als DataFrames
    dataframes = {}
    for file in csv_files:
        file_name = os.path.splitext(os.path.basename(file))[0]
        
        ## ohdsi metadata upload
        if file_name in ['concept', 'concept_ancestor', 'concept_relationship', 'domain', 'vocabulary', 'concept_class',
                         'concept_relationship', 'relationship', 'concept_synonym', 'inzidenzort_mapping']:
            with open(file, 'r') as f:
                first_line = f.readline()
                sep = ';' if ';' in first_line else '\t' if '\t' in first_line else ','
            dataframes[file_name] = pd.read_csv(file, sep=sep, quotechar='"', low_memory=False)
        else:
            dataframes[file_name] = pd.read_csv(file, low_memory=False)
    return dataframes

# visit Key
def source_import(*args, **kwargs):
    """
    1. Add a new column 'visit' with a unique key to all the provided DataFrames.
    2. Read OPS codes and substances as comma-separated values.
    """
    dataframes = csv_importer()

    # valid Diagnosedatum required!
    if "tumor" in dataframes:
        dft = dataframes["tumor"]
        parsed_dates = pd.to_datetime(dft['Diagnosedatum'], errors='coerce')
        dft = dft[parsed_dates.notna()]
        dataframes["tumor"] = dft
        
    # uniqe key
    visit_number = 1
    
    for name, df in dataframes.items():
        
        # create unique pat_id key
        if ('Patient_ID_FK' in df.columns or 'Patient_ID' in df.columns) and 'Register_ID_FK' in df.columns:
            if 'Patient_ID_FK' in df.columns:
                df['pat_id'] = df['Patient_ID_FK'].astype(str) + '_' + df['Register_ID_FK'].astype(str)
            else:
                df['pat_id'] = df['Patient_ID'].astype(str) + '_' + df['Register_ID_FK'].astype(str)
        
        if name == 'patient': 
            continue
        # calculate amount of rows 
        num_rows = len(df)
        df['visit'] = list(range(visit_number, visit_number + num_rows))
        visit_number += num_rows
        
        # unique id within each imported df (except person)
        df['ID'] = range(1, len(df) + 1)
    
    
        # key value -> null values not allowed
        df = df.dropna(subset=['pat_id'])
    
    # add icdo3
    if "tumor" in dataframes:
        dft = dataframes["tumor"]
        dft["icdo3"] = (
            dft["Primaertumor_Morphologie_ICD_O"]
            + "-"
            + dft["Primaertumor_Topographie_ICD_O"]
        )
        
        # unique provider_id
        stage_df = ['inzidenzort_mapping']
        stage_dataframes = stage_import(stage_df)
        inzidenzort_mapping = stage_dataframes['inzidenzort_mapping'] 

        # replace Na and invalid inzidenzorte
        # dft['Inzidenzort'] = dft['Inzidenzort'].apply(lambda x: str(x)[:4]).astype(int) # uicc hamburg specifica: convert to int and keep first 4 digits
        dft['Inzidenzort'] = dft['Inzidenzort'].where(dft['Inzidenzort'].isin(inzidenzort_mapping['Landkreis_Nr']), 17000.0).fillna(17000.0)
        dft["Inzidenzort_int"] = dft["Inzidenzort"].astype(float).astype(int) # zfill doesn't like float
        
        dft["inzidenzort_com"] = (
        dft["Inzidenzort_int"].astype(str) +
        dft["Register_ID_FK"].astype(str).apply(lambda x: x.zfill(2))
        )

        dft["inzidenzort_com"] = dft["inzidenzort_com"].astype(int) # change object dtype to int
        
        dataframes["tumor"] = dft
        
    # add cleaned radiation application 
    if 'strahlentherapie' in dataframes:
        dfst = dataframes['strahlentherapie']
        # read application until first ;
        dfst['application_clean'] = dfst['Applikationsspezifikation'].str.split(';').str[0]
        # Saving in DataFrame Dictionary
        dataframes['strahlentherapie'] = dfst

    # split substances
    if 'systemtherapie' in dataframes:
        dfs = dataframes['systemtherapie']
        
        # ai-care specific
        if 'systemtherapie_with_substance_service_variable' in dataframes:
            df_systemtherapie_service = dataframes['systemtherapie_with_substance_service_variable']
            df_systemtherapie_service['unique_id'] = (df_systemtherapie_service['SYST_ID'].astype(str) + '_' + df_systemtherapie_service['Register_ID_FK'].astype(str))
            dfs['unique_id'] = (dfs['SYST_ID'].astype(str) + '_' + dfs['Register_ID_FK'].astype(str))
            dfs = dfs.merge(df_systemtherapie_service[['unique_id', 'Substanzen_extracted']], on='unique_id', how='left')
            # split substances
            dfs['substances_raw'] = dfs['Substanzen_extracted'].str.split(';')
            dfs = dfs.drop(columns=['Substanzen_extracted', 'unique_id'])
        else:
            # split substances
            dfs['substances_raw'] = dfs['Substanzen'].str.split(';')
            
        dfs = dfs.explode('substances_raw')
        #remove versions 
        dfs['substances_raw'] = dfs['substances_raw'].str.replace(r'\s+', '', regex=True)
        dfs['substances'] = dfs['substances_raw'].apply(lambda value: re.sub(r'\d{4}$', '', value).strip() if isinstance(value, str) else value)
        # Saving in DataFrame Dictionary
        dataframes['systemtherapie'] = dfs

    # split surgery ops 
    if 'op' in dataframes:
        dfo = dataframes['op']
        # split OPS
        dfo['ops'] = dfo['Menge_OPS_code'].str.split(';')
        dfo = dfo.explode('ops')
        # Saving in DataFrame Dictionary
        dataframes['op'] = dfo

    for name, df in dataframes.items():
        logger = logging.getLogger(f"{name}.py")
        logger.info(f'Number of entries in the {name} table (source ZfKD): {len(df)}')
    return dataframes  

def run_sql_file(sql_file_path, connection_params):
    """
    Reads a specific SQL file and executes it directly on the PostgreSQL database.
    
    Args:
    - sql_file_path: The path to the SQL file to execute.
    - connection_params: Dictionary containing connection parameters for the PostgreSQL database from load_config().
    """

    # Connection to PostgreSQL-DB
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cursor:
            # read SQL-Data 
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql = file.read()
                cursor.execute(sql)
        # save 
        conn.commit()


def df_import(df, table_name):
    """
    Creates a table in the PostgreSQL database from a Pandas DataFrame

    :param df: The Pandas DataFrame to be saved in the database
    :param table_name: The name of the table to be created.
    """
    connection_params = load_config('database')  
    
    # database connection with SQLAlchemy
    engine = sq.create_engine(f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['dbname']}")
    
    # truncate table to avoid ddl error
    with engine.connect() as connection:
        result = connection.execute(
                                text("""
                                    SELECT EXISTS (
                                    SELECT 1 
                                    FROM information_schema.tables 
                                    WHERE table_name = :table_name
                                    AND table_schema = 'cdm' 
                                    ) AS table_exists;
                                    """), {"table_name": table_name}
                                    )
        exists = result.fetchone()[0]

        if exists:
            connection.execute(text(f'TRUNCATE TABLE cdm.{table_name} CASCADE;'))
            connection.commit()

        # save DataFrame in Database
        df.to_sql(table_name, con=engine, schema='cdm', index=False, if_exists='append')  # avoid ddl conflict, has to be set to append (otherwise you overwrite column type specification)

        logger = logging.getLogger(f"{table_name}.py")
        logger.info(f'Number of entries in the {table_name} table (omop): {len(df)}')


def delete_table(table_name):
    """
    This function deletes a specified table from the database.
    """
    # load connection params
    connections_params = load_config('database')
    
    try:
        with psycopg2.connect(**connections_params) as conn:
            with conn.cursor() as cur:
                sql = f"DROP TABLE IF EXISTS cdm.{table_name} CASCADE;"  # deleting with dependencies
                cur.execute(sql)
                conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass

    
    
