with mapping_overview as (
    -- condition_occurrence
    select
        'condition_occurrence' as table_name,
        co.condition_source_value as source_value,
        null as source_vocabulary_id,
        co.condition_source_concept_id as source_concept_id,
        co.condition_concept_id as concept_id,
        COUNT(*) as record_count,
        case when co.condition_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.condition_occurrence co
    group by co.condition_source_value, co.condition_concept_id, co.condition_source_concept_id

    union all

    -- drug_exposure
    select
        'drug_exposure' as table_name,
        de.drug_source_value as source_value,
        null as source_vocabulary_id,
        de.drug_source_concept_id as source_concept_id,
        de.drug_concept_id as concept_id,
        COUNT(*) as record_count,
        case when de.drug_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.drug_exposure de
    group by de.drug_source_value, de.drug_concept_id, de.drug_source_concept_id

    union all

    -- procedure_occurrence
    select
        'procedure_occurrence' as table_name,
        po.procedure_source_value as source_value,
        null as source_vocabulary_id,
        po.procedure_source_concept_id as source_concept_id,
        po.procedure_concept_id as concept_id,
        COUNT(*) as record_count,
        case when po.procedure_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.procedure_occurrence po
    group by po.procedure_source_value, po.procedure_concept_id, po.procedure_source_concept_id

    union all

    -- procedure occurrence - custom [intention of surgery]
    select
        'procedure_occurrence' as table_name,
        po.procedure_source_value as source_value,
        'custom - intention of surgery' as source_vocabulary_id,
        null as source_concept_id,
        po.procedure_concept_id as concept_id,
        COUNT(*) as record_count,
        case when po.procedure_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.procedure_occurrence po
    inner join cdm.meta m
    on po.procedure_source_value = m.source_code
    where m.element = 'Intention_der_Operation'
    group by po.procedure_source_value, po.procedure_concept_id

      union all

    -- procedure occurrence - custom [Type of therapy]
    select
        'procedure_occurrence' as table_name,
        po.procedure_source_value as source_value,
        'custom - Type of therapy (systemic)' as source_vocabulary_id,
        null as source_concept_id,
        po.procedure_concept_id as concept_id,
        COUNT(*) as record_count,
        case when po.procedure_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.procedure_occurrence po
    inner join cdm.meta m
    on po.procedure_source_value = m.source_code
    where m.element = 'Therapieart'
      and m."table" = 'Systemische_Behandlung'
    group by po.procedure_source_value, po.procedure_concept_id

    union all

    -- procedure occurrence - custom [application radiation]
    select
        'procedure_occurrence' as table_name,
        po.procedure_source_value as source_value,
        'custom - application radiation' as source_vocabulary_id,
        null as source_concept_id,
        po.procedure_concept_id as concept_id,
        COUNT(*) as record_count,
        case when po.procedure_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.procedure_occurrence po
    inner join cdm.meta m
    on po.procedure_source_value = m.source_code
    where m.element = 'Therapieart'
        and m."table" = 'Bestrahlung'
    group by po.procedure_source_value, po.procedure_concept_id

    union all

    -- measurement
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        null as source_vocabulary_id,
        m.measurement_source_concept_id as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    group by m.measurement_source_value, m.measurement_concept_id, m.measurement_source_concept_id

    union all

    -- measurement - custom [side localization]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - side localization' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Seitenlokalisation'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [Grading]
    select
        'measurement' as table_name,
         m.measurement_source_value as source_value,
        'custom - Grading' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Grading'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [Clinical UICC]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - Clinical UICC' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Clinical_UICC'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [AJCC UICC Stage Group]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - AJCC UICC Stage Group' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'AJCC_UICC_Stage_Group'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [diagnostic accuracy]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - diagnostic accuracy' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Diagnosesicherung'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [metastasis localization]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - metastasis localization' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Lokalisation_der_Fernmetastase'
    group by m.measurement_source_value, m.measurement_concept_id

    union all

    -- measurement - custom [residual status]
    select
        'measurement' as table_name,
        m.measurement_source_value as source_value,
        'custom - Residualstatus (surgery)' as source_vocabulary_id,
        null as source_concept_id,
        m.measurement_concept_id as concept_id,
        COUNT(*) as record_count,
        case when m.measurement_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.measurement m
    inner join cdm.meta me
    on m.measurement_source_value = me.source_code
    where me.element = 'Residualstatus'
    group by m.measurement_source_value, m.measurement_concept_id
    
    union all

    -- observation
    select
        'observation' as table_name,
        o.observation_source_value as source_value,
        null as source_vocabulary_id,
        o.observation_source_concept_id as source_concept_id,
        o.observation_concept_id as concept_id,
        COUNT(*) as record_count,
        case when o.observation_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.observation o
    group by o.observation_source_value, o.observation_concept_id, o.observation_source_concept_id

    union all

    -- observation - custom (Type of Therapy (Watch an Wait, Active Surveillance))
    select
        'observation' as table_name,
        o.observation_source_value as source_value,
        'custom - Type of Therapy (systemic)' as source_vocabulary_id,
        null as source_concept_id,
        o.observation_concept_id as concept_id,
        COUNT(*) as record_count,
        case when o.observation_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.observation o
    inner join cdm.meta m
    on o.observation_source_value = m.source_code
    where m.element = 'Therapieart'
        and m."conceptDomain"='Observation'
    group by o.observation_source_value, o.observation_concept_id

    -- union all

    -- -- episode
    -- select
    --     'episode' as table_name,
    --     e.episode_source_value as source_value,
    --     e.episode_source_concept_id as source_concept_id,
    --     e.episode_concept_id as concept_id,
    --     COUNT(*) as record_count,
    --     case when e.episode_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    -- from cdm.episode e
    -- group by e.episode_source_value, e.episode_concept_id, e.episode_source_concept_id

    -- union all

    --   -- episode - HemOnc
    -- select
    --     'episode' as table_name,
    --     e.episode_source_value as source_value,
    --     e.episode_source_concept_id as source_concept_id,
    --     e.episode_object_concept_id as concept_id,
    --     COUNT(*) as record_count,
    --     case when e.episode_object_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    -- from cdm.episode e
    -- group by e.episode_source_value, e.episode_object_concept_id, e.episode_source_concept_id

    union all

    -- person: gender
    select
        'person' as table_name,
        p.gender_source_value as source_value,
        null as source_vocabulary_id,
        p.gender_source_concept_id as source_concept_id,
        p.gender_concept_id as concept_id,
        COUNT(*) as record_count,
        case when p.gender_concept_id = 0 then 'Unmapped' else 'Mapped' end as mapping_status
    from cdm.person p
    group by p.gender_concept_id, p.gender_source_value, p.gender_source_concept_id
)

select
    mo.table_name,
    mo.source_value,
    case when source_vocabulary_id is null then coalesce(cc.vocabulary_id,c.vocabulary_id) else source_vocabulary_id end as source_vocabulary_id,
    cc.domain_id as source_domain_id,
    mo.concept_id,
    c.concept_name,
    c.concept_code,
    c.vocabulary_id,
    c.domain_id,
    c.concept_class_id,
    mo.record_count,
    mo.mapping_status
from mapping_overview mo
left join cdm.concept c on mo.concept_id = c.concept_id
left join cdm.concept cc on mo.source_concept_id = cc.concept_id
where mo.record_count > 0
order by
    mo.mapping_status DESC,
    mo.table_name,
    mo.record_count DESC;
