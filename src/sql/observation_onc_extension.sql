  Truncate table cdm.observation;
  INSERT INTO cdm.observation
  select distinct row_number() over (order by o.observation_date) as observation_id
  ,cast(o.person_id as int) person_id
  ,cast(o.observation_concept_id as int) observation_concept_id
  ,cast(o.observation_date as date) as observation_date
  ,cast(o.observation_datetime as timestamp) as observation_datetime
  ,cast(o.observation_type_concept_id as int) observation_type_concept_id
  ,cast(o.value_as_number as int) value_as_number 
  ,cast(o.value_as_string as varchar(60)) value_as_string
  ,cast(o.value_as_concept_id as int) value_as_concept_id
  ,cast(o.qualifier_concept_id as int) qualifier_concept_id 
  ,cast(o.unit_concept_id as int) unit_concept_id 
  ,cast(o.provider_id as int) provider_id
  ,cast(o.visit_occurrence_id as int) visit_occurrence_id
  ,cast(o.visit_detail_id as int) visit_detail_id
  ,o.observation_source_value
  ,cast(o.observation_source_concept_id as int) observation_source_concept_id
  ,o.unit_source_value
  ,o.qualifier_source_value
  ,o.value_source_value
  ,sub.condition_occurrence_id as measurement_event_id
  ,cast(o.obs_event_field_concept_id as int) obs_event_field_concept_id
  
  from cdm.temp_observation o
  left join (select distinct c.condition_occurrence_id, c.person_id, c.icdo3,m.source_vocabulary_id, m.source_concept_id
                             from cdm.temp_condition_occurrence c
                             inner join cdm.mappings AS m ON c.condition_concept_id::int = m.target_concept_id
							 and m.target_vocabulary_id = 'ICDO3'
  							 and m.source_domain_id = 'Condition'
  							 ) sub
  on sub.person_id::int=o.person_id::int
  and sub.icdo3=o.icdo3 