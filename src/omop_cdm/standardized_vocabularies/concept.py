import pandas as pd
import psycopg2
from utils import importer as imp
from utils import helper as hp
from utils import exporter as ex

def import_concept(concept):
    
    # preprocessing
    concept = hp.date_convert(concept)
    concept[['concept_name','vocabulary_id']] = concept[['concept_name','vocabulary_id']].fillna("None")
    ex.stage_export(concept,'concept_prep')

    connections_params = imp.load_config('database')
    with psycopg2.connect(**connections_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE cdm.concept;")
                with open('src//stage//concept_prep.csv', 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                     """COPY cdm.concept FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER true)""",
                    f
              )

    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    # stage import 
    stage_df = ['concept']
    stage_dataframes = imp.stage_import(stage_df)
    concept = stage_dataframes['concept']
    
    import_concept(concept)