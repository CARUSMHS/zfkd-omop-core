  Truncate table cdm.measurement;
  INSERT INTO cdm.measurement
  select distinct row_number() over (order by m.measurement_date) as measurment_id
  ,m.person_id
  ,m.measurement_concept_id
  ,cast(m.measurement_date as date) as measurement_date
  ,cast(m.measurement_datetime as timestamp) as measurement_datetime
  ,m.measurement_time
  ,m.measurement_type_concept_id
  ,cast(m.operator_concept_id as int) operator_concept_id
  ,cast(m.value_as_number as int) value_as_number 
  ,cast(m.value_as_concept_id as int) value_as_concept_id 
  ,cast(m.unit_concept_id as int) unit_concept_id 
  ,cast(m.range_low as int) range_low 
  ,cast(m.range_high as int) range_high 
  ,m.provider_id provider_id 
  ,m.visit_occurrence_id
  ,cast(m.visit_detail_id as int) visit_detail_id
  ,m.measurement_source_value
  ,cast(m.measurement_source_concept_id as int) measurement_source_concept_id
  ,m.unit_source_value
  ,0 as unit_source_concept_id
  ,m.value_source_value
  ,sub.condition_occurrence_id as measurement_event_id
  ,m.meas_event_field_concept_id
  
  from cdm.temp_measurement m
  left join (select distinct c.condition_occurrence_id, c.person_id, c.icdo3,m.source_vocabulary_id, m.source_concept_id
                             from cdm.temp_condition_occurrence c
                             inner join cdm.mappings AS m ON c.condition_concept_id::int = m.target_concept_id
							 and m.target_vocabulary_id = 'ICDO3'
  							 and m.source_domain_id = 'Condition'
  							 ) sub
  on sub.person_id::int=m.person_id 
  and sub.icdo3=m.icdo3 