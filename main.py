import sys
import os
from tqdm import tqdm
import glob

utils_path = os.path.join(os.path.dirname(__file__), 'src')  
if utils_path not in sys.path:
    sys.path.append(utils_path)

# utils
import utils.importer as imp
import utils.helper as hp
import utils.xml_parser as xp

# mapping scripts
import omop_cdm.standardized_vocabularies.concept as concept
import omop_cdm.standardized_vocabularies.concept_ancestor as concept_ancestor
import omop_cdm.standardized_vocabularies.concept_relationship as concept_relationship
import omop_cdm.standardized_vocabularies.concept_synonym as concept_synonym
import omop_cdm.standardized_vocabularies.concept_class as concept_class 
import omop_cdm.standardized_vocabularies.domain as domain
import omop_cdm.standardized_vocabularies.relationship as relationship
import omop_cdm.standardized_vocabularies.vocabulary as vocabulary
import omop_cdm.standardized_vocabularies.mappings as mappings

# add-on scripts
import omop_cdm.standardized_health_system.provider as provider
import omop_cdm.standardized_health_system.care_site as care_site

# omop cdm scripts
import omop_cdm.standardized_clinical_data.person as person 
import omop_cdm.standardized_clinical_data.observation_period as observation_period
import omop_cdm.standardized_clinical_data.death as death
import omop_cdm.standardized_clinical_data.visit_occurrence as visit_occurrence
import omop_cdm.standardized_clinical_data.condition_occurrence as condition_occurrence
import omop_cdm.standardized_clinical_data.drug_exposure as drug_exposure
import omop_cdm.standardized_clinical_data.procedure_occurrence as procedure_occurrence
import omop_cdm.standardized_clinical_data.measurement as measurement  
import omop_cdm.standardized_clinical_data.observation as observation
import omop_cdm.standardized_clinical_data.episode as episode
import omop_cdm.standardized_clinical_data.episode_event as episode_event

# (0) prepare environment
# logging
hp.init_logger('w')

steps = 20
progress = tqdm(total=steps, desc="Progress", ncols=80,  bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} • Elapsed: {elapsed}")
print("")

def update_progress_bar(progress, description):
    progress.set_description(description)
    progress.update(1)
    print("")

# DB preparation
# delete constraints
connections_params = imp.load_config('database')
sql_path_delete = os.path.join('src/sql/delete_constraints.sql')
imp.run_sql_file(sql_path_delete, connections_params)

# DB preparation
# execucte ddl 
sql_path_ddl = os.path.join("src/sql/cdm_ddl_5.4.sql")
imp.run_sql_file(sql_path_ddl,connections_params)

# (1) metadata import (stage + db)
required_mapping_tables = ['meta', 'concept', 'concept_ancestor', 'concept_relationship', 'concept_synonym', 'concept_class', 'domain', 'relationship', 'vocabulary', 'inzidenzort_mapping']
stage_dataframes = imp.stage_import()
missing = [key for key in required_mapping_tables if key not in stage_dataframes]
if missing:
    print("Program aborted due missing mapping tables:", ", ".join(missing))
    sys.exit(1)
else:
    meta_stage = stage_dataframes['meta']
    concept_stage = stage_dataframes['concept']
    inzidenzort_mapping_stage = stage_dataframes['inzidenzort_mapping']

update_progress_bar(progress, "Done: Metadata imported")

concept.import_concept(concept_stage)
concept_ancestor.import_concept_ancestor()
concept_relationship.import_concept_relationship()
concept_synonym.import_concept_synonym()
concept_class.import_concept_class()
domain.import_domain()
relationship.import_relationship()
vocabulary.import_vocabulary()
mappings.import_mappings()

# load mapping data from stage and export custom meta data to db
mappings_ = ['mappings']
mappings_stage = imp.stage_import(mappings_)
mappings_stage = mappings_stage['mappings']
imp.df_import(meta_stage, 'meta')

update_progress_bar(progress, "Done: Metadata in DB")

# (2) data source import
if imp.load_config('xml_input'):
    xp.parse_file(
        glob.glob(os.path.join(imp.load_config('iam_path_zfkd_data'), "*.xml"))[0]
    )

required_input_tables = ['patient', 'tumor', 'op', 'systemtherapie', 'strahlentherapie', 'fe']
imp_dataframes = imp.source_import()
missing = [key for key in required_input_tables if key not in imp_dataframes]
if missing:
    print("Program aborted due missing input tables:", ", ".join(missing))
    sys.exit(1)
else:
    patient = imp_dataframes['patient']
    tumor = imp_dataframes['tumor']
    op = imp_dataframes['op']
    systemtherapie = imp_dataframes['systemtherapie']
    strahlentherapie = imp_dataframes['strahlentherapie']
    fe = imp_dataframes['fe']

update_progress_bar(progress, "Done: Input data imported")

# (3) create add-On tables (provider, care_site)
provider.create_provider_table(tumor, inzidenzort_mapping_stage)
care_site.create_care_site_table(inzidenzort_mapping_stage)
update_progress_bar(progress, "Done: Provider and Care_site Table in DB")

# (4) create OMOP common data model tables 
person_cmd = person.create_person_table(patient, tumor, meta_stage)
update_progress_bar(progress, "Done: Person Table in DB")

# creates death date (if person is death) and observation end date + uploads person table in stage folder
person_stage = person.export_person_into_stage(person_cmd, patient, tumor, op, strahlentherapie, systemtherapie, fe)
update_progress_bar(progress, "Done: Person Table in stage")

observation_period.create_observation_period_table(person_stage, tumor)
update_progress_bar(progress, "Done: Observation_period Table in DB")

death.create_death_table(patient, person_stage, mappings_stage)
update_progress_bar(progress, "Done: Death Table in DB")

visit_occurrence_stage = visit_occurrence.create_visit_occurrence_table(patient, tumor, op, strahlentherapie, systemtherapie, fe, person_stage)
update_progress_bar(progress, "Done: Visit_occurrence Table in DB")

condition_occurrence_stage = condition_occurrence.create_condition_occurrence_table(patient, tumor, person_stage, mappings_stage)
update_progress_bar(progress, "Done: Condition_occurrence Table in DB")

drug_exposure.create_drug_exposure_table(tumor, systemtherapie, person_stage, mappings_stage)
update_progress_bar(progress, "Done: Drug_exposure Table in DB")

procedure_occurrence.create_procedure_occurrence_table(tumor, op, systemtherapie, strahlentherapie, person_stage, mappings_stage, meta_stage, visit_occurrence_stage)
update_progress_bar(progress, "Done: Procedure_occurrence Table in DB")

measurement.create_measurement_table(patient, tumor, fe, op, person_stage, meta_stage, mappings_stage)
update_progress_bar(progress, "Done: Measurement Table in DB")

observation.create_observation_table(patient, tumor, systemtherapie, person_stage, mappings_stage, meta_stage)
update_progress_bar(progress, "Done: Observation Table in DB")

sql_path_oncoFinder = os.path.join("/workspaces/zfkd_omop/src/sql/onco_regimen_finder.sql")
imp.run_sql_file(sql_path_oncoFinder, connections_params)
update_progress_bar(progress, "Done: Onco_regimen_finder executed in DB")

episode.create_episode_table(tumor, fe, person_stage, concept_stage, condition_occurrence_stage)
update_progress_bar(progress, "Done: Episode Table in DB")

episode_event.create_episode_event_table()
update_progress_bar(progress, "Done: Episode_Event Table in DB")

# (5) check constraints
sql_path_pks = os.path.join("src/sql/cdm_pks_5.4.sql")
imp.run_sql_file(sql_path_pks, connections_params)
update_progress_bar(progress, "Done: Primary Keys added in DB")

sql_path_constraints = os.path.join("src/sql/cdm_constraints_5.4.sql")
imp.run_sql_file(sql_path_constraints, connections_params)
update_progress_bar(progress, "Done: contraints added in DB")

sql_path_indices = os.path.join("src/sql/cdm_indices_5.4.sql")
imp.run_sql_file(sql_path_indices, connections_params)
update_progress_bar(progress, "Done: indices added in DB")

progress.close()