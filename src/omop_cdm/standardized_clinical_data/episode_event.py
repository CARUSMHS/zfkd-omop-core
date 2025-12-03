import psycopg2
import utils.importer as imp
import utils.helper as hp

# create episode_event table - runs on Postgres DB
def create_episode_event_table():
    
    connections_params = imp.load_config('database')
    with psycopg2.connect(**connections_params) as conn:
        with conn.cursor() as cur:

            sql = """
                INSERT INTO cdm.episode_event
                SELECT 
                    e.episode_id,
                    c.condition_occurrence_id,
                    1147127 -- condition_occurrence_id
                FROM cdm.episode e
                INNER JOIN cdm.condition_occurrence c
                ON e.person_id = c.person_id 
                AND c.condition_start_date BETWEEN e.episode_start_date AND e.episode_end_date

                UNION ALL
                
                SELECT 
                    e.episode_id,
                    m.measurement_id,
                    1147138 -- measurement.measurement_id
                FROM cdm.episode e
                INNER JOIN cdm.measurement m
                ON e.person_id = m.person_id
                AND m.measurement_date BETWEEN e.episode_start_date AND e.episode_end_date

                UNION ALL

                SELECT
                    e.episode_id,
                    p.procedure_occurrence_id,
                    1147082 -- procedure_occurrence.procedure_occurrence_id
                FROM cdm.episode e
                INNER JOIN cdm.procedure_occurrence p
                ON e.person_id = p.person_id
                AND p.procedure_datetime BETWEEN e.episode_start_date AND e.episode_end_date

            UNION ALL
            
            SELECT 
                e.episode_id,
                d.drug_exposure_id,
                1147094 -- drug_exposure.drug_exposure_id
            FROM cdm.episode e
            INNER JOIN cdm.drug_exposure d
            ON e.person_id = d.person_id
            AND d.drug_exposure_start_date BETWEEN e.episode_start_date AND e.episode_end_date

            UNION ALL

            SELECT 
                e.episode_id,
                o.observation_id,
                1147165 -- observation.observation_id
            FROM cdm.episode e
            INNER JOIN cdm.observation o
            ON e.person_id = o.person_id
            AND o.observation_date BETWEEN e.episode_start_date AND e.episode_end_date;
            """


            try:
                cur.execute(sql)
                affected_rows = cur.rowcount
                conn.commit()
            
            except Exception as e:
                conn.rollback()  
            
            
if __name__ == "__main__":
    # logging
    hp.init_logger()
    
    create_episode_event_table()