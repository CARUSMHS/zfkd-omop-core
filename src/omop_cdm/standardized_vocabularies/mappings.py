import pandas as pd
import psycopg2
from utils import importer as imp
from utils import exporter as exp



def import_mappings():

    connections_params = imp.load_config('database')
    with psycopg2.connect(**connections_params) as conn:
            with conn.cursor() as cursor:
                mappings_view= """
                create or replace view cdm.mappings as 
                    select c.concept_code AS source_code,
                    c.concept_id as source_concept_id,
                    c.concept_name as source_description,
                    c.vocabulary_id as source_vocabulary_id,
                    c.domain_id as source_domain_id,
                    c.concept_class_id as source_concept_class_id,
                    c1.concept_id as target_concept_id,
                    c1.concept_name as target_description,
                    c1.vocabulary_id as target_vocabulary_id,
                    c1.domain_id as target_domain_id,
                    c1.concept_class_id as target_concept_class,
                    c1.invalid_reason as target_invalid_reason,
                    c1.standard_concept as target_standard_concept
                from cdm.concept c
                join cdm.concept_relationship cr 
                    on c.concept_id = cr.concept_id_1 
                    and cr.invalid_reason is NULL 
                    and lower(cr.relationship_id::text) = 'maps to'::text
                join cdm.concept c1 
                    on cr.concept_id_2 = c1.concept_id 
                    and c1.invalid_reason is null
                    """
                cursor.execute(mappings_view)         
                conn.commit()
    
    # bring it to the stage
    mappings = exp.db_export('mappings')
    exp.stage_export(mappings, 'mappings')

    
    

if __name__ == "__main__":
    import_mappings()