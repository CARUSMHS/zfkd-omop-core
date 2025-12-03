import os
import pandas as pd
import psycopg2 
import utils.importer as imp
import logging


def stage_export(df: pd.DataFrame, filename: str):
    
    # path to stage
    stage_folder = os.path.join('src/stage')
    # for debugging
    # stage_folder = os.path.join('stage')
    
    # check if stage is there
    os.makedirs(stage_folder, exist_ok=True)
    
    # complete paths to csv data
    file_path = os.path.join(stage_folder, f"{filename}.csv")
    
    # save 
    df.to_csv(file_path, index=False)
    
    logger = logging.getLogger(f"{filename}.py")
    logger.info(f'Number of entries in the {filename} table (stage): {len(df)}')

def db_export(table_name: str = None, sql_query: str = None) -> pd.DataFrame:
    
    # db connection
    connections_params = imp.load_config('database')
    
    if sql_query is not None:
        with open(sql_query, 'r', encoding='utf-8') as file:
            sql_query_str = file.read()
    else:
        sql_query_str = f"SELECT * FROM cdm.{table_name};"
    
    with psycopg2.connect(**connections_params) as conn:
        df = pd.read_sql(sql_query_str, conn)

    return df

def execute_sql(statement):
    
    try:
        # db connection
        connections_params = imp.load_config('database')
        conn = psycopg2.connect(**connections_params)
        cur = conn.cursor()

        # execute statement
        cur.execute(statement)

        # check if the query returns data (i.e., SELECT or WITH ... SELECT)
        if cur.description:
            # extract column names
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=colnames)
            cur.close()
            conn.close()
            return df
        else:
            # for INSERT, UPDATE, etc.
            conn.commit()
            cur.close()
            conn.close()
            return None

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Execution error: {e}")
        return None



