with cte as (
    select
         'episode' as table_name,
         e.episode_source_value as source_value,
         e.episode_source_concept_id as source_concept_id,
         e.episode_concept_id as concept_id,
         COUNT(*) as record_count,
         case when e.episode_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
     from cdm.episode e
     group by e.episode_source_value, e.episode_concept_id, e.episode_source_concept_id

     union all

    --   -- episode - HemOnc
     select
         'episode' as table_name,
         e.episode_source_value as source_value,
         e.episode_source_concept_id as source_concept_id,
         e.episode_object_concept_id as concept_id,
         COUNT(*) as record_count,
        case when e.episode_object_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
     from cdm.episode e
     inner join cdm.concept c  
     on e.episode_object_concept_id = c.concept_id
     where c.vocabulary_id = 'HemOnc'
     group by e.episode_source_value, e.episode_object_concept_id, e.episode_source_concept_id

)
select
    mo.table_name,
    mo.source_value,
    cc.vocabulary_id as source_vocabulary_id,
    cc.domain_id as source_domain_id,
    mo.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id,
    c.concept_class_id,
    mo.record_count,
    mo.mapping_status
from cte mo
left join cdm.concept c on mo.concept_id = c.concept_id
left join cdm.concept cc on mo.source_concept_id = cc.concept_id
order by
    mo.mapping_status DESC,
    mo.table_name,
    mo.record_count DESC;