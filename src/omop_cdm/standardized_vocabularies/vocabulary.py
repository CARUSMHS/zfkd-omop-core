import pandas as pd
import psycopg2
from utils import importer as imp
from utils import helper as hp

def import_vocabulary():
    
    connections_params = imp.load_config('database')
    with psycopg2.connect(**connections_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE cdm.vocabulary;")
                with open('src//stage//vocabulary.csv', 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                     """COPY cdm.vocabulary FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER true)""",
                    f
              )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    import_vocabulary()