import pandas as pd
import psycopg2
from utils import importer as imp

def import_domain():   
         
    connections_params = imp.load_config('database')
    with psycopg2.connect(**connections_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE cdm.domain;")
                with open('src//stage//domain.csv', 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                     """COPY cdm.domain FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER true)""",
                    f
              )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    import_domain()