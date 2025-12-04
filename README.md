# ETL Pipeline for German Cancer Registry Data: Transforming ZfKD Format to OMOP CDM (Version 5.4)

## :bookmark: Table of Contents
- [Paper](#page_facing_up-Paper)
- [Repository Overview](#bar_chart-Repository-Overview)
- [Setup and Configuration](#gear-Setup-and-Configuration)
- [OMOP Implementation Specifics](#hammer_and_wrench-OMOP-Implementation-Specifics)
- [Directory Structure](#file_folder-Directory-Structure)
- [Required data sources for socioeconomic information](#earth_africa-Required-data-sources-for-socioeconomic-information)
- [References](#books-References)
- [License](#scroll-License)

<br><br>
## :page_facing_up: Paper
Titel: *From National to International: Evaluation of OMOP CDM Mapping for German Cancer Registry Data Using a Socioeconomic Use Case*

- This repository contains the ETL (Extract, Transform, Load) pipeline for transforming German Cancer Registry data from the ZfKD format into the OMOP Common Data Model (CDM).  
- The analytical/statistical part of the study is maintained in a separate repository: 

If you use this work, please cite our paper as:


<br><br>
## :bar_chart: Repository Overview
This project provides an Extract–Transform–Load (ETL) pipeline for transforming data from the German Centre for Cancer Registry Data (ZfKD) into the international Observational Medical Outcomes Partnership Common Data Model (OMOP CDM).
The goal is to convert population-based oncology registry data into a structured and interoperable format - enabling its use in research, analysis, and international OHDSI projects.

**Background**:
The ZfKD provides epidemiological and clinical cancer registry data collected as part of Germany’s national cancer registration process.
To make these data interoperable and analytically usable in an international context (e.g., within the OHDSI network), a transformation into the OMOP CDM is required.

This project automates that process. The transformation pipeline starts from the ZfKD XML format (see [Confluence-Instanz der Plattform § 65c](https://plattform65c.atlassian.net/wiki/spaces/UMK/pages/15532570/XML-Schema)) and performs the mapping to OMOP CDM structure.


<br><br>
## :gear: Setup and Configuration
Before running the analysis, please complete the following preparation steps to set up the environment and required data.

### Preparation
1. ZfKD XML data must be converted to CSV to serve as input for our ETL pipeline. If your Data is in ZfKD XML format, change in config.json param "xml_input" to true. Optionally, the input csv tables can be preprocessed using the pipeline by Germer et al. (see references). Recommended use for suitable data quality!
2. Specify the path to the input ZfKD data file(s) in config.json (param: "path_zfkd_data"). The following files are required: 

ZfKD.xml or 

patient.csv, tumor.csv, op.csv, systemtherapie.csv, strahlentherapie.csv, and fe.csv. Columns in these tables need to be comma-separated.
3. The OMOP CDM will be stored in a PostgreSQL database. Therefore, you first need to create a PostgreSQL database. The connection parameters are defined in the config.json file.
> Note: Before running the analysis, update the values in config.json to match your database settings and source folder of ZfKD data.
4. Metadata from Athena, required for mapping progress, is regularly updated and stored in the University of Hamburg’s data repository (Jasmin Carus, & Mareile Beernink. (2025). ZfKD | OMOP CDM - Metadata Cataloge (Version 2) [Data set]. http://doi.org/10.25592/uhhfdm.17924). Please download the metadata from the University of Hamburg’s data repository and place the files in the src/stage folder.<br>**Required files**: concept.csv, concept_ancestor.csv, concept_class.csv, concept_relationship.csv, concept_synonym.csv, domain.csv, meta.csv, relationship.csv, vocabulary.csv
> Note: Since the metadata files are large, it is not recommended to include them in your GitHub repository. Also, make sure to regularly check the University of Hamburg’s data repository for updated metadata.

### Run the Analysis
This repository includes a development container to simplify setup and ensure a consistent environment. You can use it to run the project without manually installing dependencies.

Once the preparation steps are completed, run the analysis with the following command:
```python
python main.py 
```
> In addition to tracking the progress in the terminal output, a info.log file records the number of entries per table. This allows you to monitor how many entries were successfully mapped.

### Running Time

For runtime estimation: During development and testing, we used cancer data from the [AI-CARE project](https://ai-care-cancer.de/) as well as the [Universitäres Cancer Center Hamburg (UCC Hamburg)](https://www.uke.de/kliniken-institute/zentren/universit%C3%A4res-cancer-center-hamburg-(ucch)/index.html). 

Data volume in AI-CARE tables: 
- Patients: 1,055,118
- Tumors: 1,092,214
- Surgeries: 597,293
- Radiotherapy: 539,415
- Systemic therapy: 873,565
- Follow-up events: 1,518,227

Runtime Environment: 
- Operating System / Kernel: Linux hostname 5.4.0-216-generic x86_64 GNU/Linux
- CPU: 12 cores, 12 threads (Common KVM processor) 
- Memory: 98 GiB total, 88 GiB available 
- Python Version: Python 3.11.14
- Notable Python Packages: Data analysis (pandas==2.3.3, numpy==2.3.4, pandasql==0.7.3), Visualization (matplotlib==3.10.7, seaborn==0.13.2, plotly==6.4.0), Database / SQL (SQLAlchemy==2.0.44, psycopg2==2.9.11)
  
Runtime: ~2 hours

Data volume in UCC Hamburg tables: 
- Patients: 1083
- Tumors: 1051
- Surgeries: 520
- Radiotherapy: 1055
- Systemic therapy: 1231
- Follow-up events: 1481

Runtime Environment:
- Operating System / Kernel: Linux 6.8.0-87-generic x86_64 GNU/Linux
- CPU: 
- Memory: 503 Gi total, 73 Gi available

Runtime: ~26min

<br><br>
## :hammer_and_wrench: OMOP Implementation Specifics
The ETL pipeline provided in this repository covers the following OMOP tables, based on CDM Version 5.4 (as of November 15, 2025):
- **Standardized clinical data**: Person, Observation_period, Death, Visit_occurrence, Condition_occurrence, Drug_exposure, Procedure_occurrence, Measurement, Observation, Episode, Episode_event
- **Standardized health system**: Care_site, Provider
- **Standardized vocabularies**: Concept, Vocabulary, Domain, Concept_class, Concept_synonym, Concept_relationship, Relationship, Concept_ancestor

<img width="755" height="412" alt="image" src="https://github.com/user-attachments/assets/56439986-5912-461a-ba9b-9e0242e5f98e" />

Source: [Github of OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/)

**Please note**: 

1. Since detailed information about the treatment locations was not available during the development of the pipeline, the Care_site table is populated with the federal state of the treatment location. The Provider_id is constructed as follows:
`provider_id = <district_id> + "_" + <federal_state>`
- <district_id> = person's place of residence (in german: Kreis-ID e.g. 1001 for Flensburg, 9572 for Erlangen-Höchstadt)
- <federal_state> = place of treatment
2. For disease extents in the Episode table, only cancer entities listed in utils/disease_extent_classification.json have been considered. You may add additional cancer entities as needed. Currently, disease extents are only defined for entities that can be classified using the TNM system (as of November 15, 2025).

> The detailed mapping of ZfKD columns to the corresponding OMOP tables and columns is documented in the Excel file information/ZfKD_to_OMOP.xlsx.


<br><br>
## :file_folder: Directory Structure
Get csv from data sources (see below in section "Required data sources")
   
(*) need to be added
```text
├── archiv
    ├── 2509_GMDS_analysis.ipynb
    └── mapping_overview.ipynb
├── information
    └── ZfKD_to_OMOP.xlsx
├── src
    ├── data (*)
        ├── zfkd.xml (*) (if xml format is input, else not needed)
        ├── fe.csv (*) (if csv format is input, else created during runtime)
        ├── op.csv (*) (if csv format is input, else created during runtime)
        ├── patient.csv (*) (if csv format is input, else created during runtime)
        ├── strahlentherapie.csv (*) (if csv format is input, else created during runtime)
        ├── systemtherapie.csv (*) (if csv format is input, else created during runtime)
        └── tumor.csv (*) (if csv format is input, else created during runtime)
    ├── omop_cdm
        ├── standardized_clinical_data
            ├── condition_occurrence.py
            ├── death.py
            ├── drug_exposure.py
            ├── episode_event.py
            ├── episode.py
            ├── measurement.py
            ├── observation_period.py
            ├── observation.py
            ├── person.py
            ├── procedure_occurrence.py
            └── visit_occurrence.py
        ├── standardized_health_system
            ├── care_site.py
            └── provider.py
        └── standardized_vocabularies
            ├── concept_ancestor.py
            ├── concept_class.py
            ├── concept_relationship.py
            ├── concept_synonym.py
            ├── concept.py
            ├── domain.py
            ├── mappings.py
            ├── relationship.py
            └── vocabulary.py
    ├── sql
        ├── cdm_constraints_5.4.sql
        ├── cdm_ddl_5.4.sql
        ├── cdm_indices_5.4.sql
        ├── cdm_pks_5.4.sql
        ├── delete_constraints.sql
        ├── episodic_modelling.sql
        ├── mapping_overview.sql
        ├── measurement_onc_extension.sql
        ├── observation_onc_extension.sql
        └── onco_regimen_finder.sql
    ├── stage
        ├── concept_ancestor.csv (*)
        ├── concept_class.csv (*)
        ├── concept_prep.csv (created during runtime)
        ├── concept_relationship.csv (*)
        ├── concept_synonym.csv (*)
        ├── concept.csv (*)
        ├── condition_occurrence.csv (created during runtime)
        ├── domain.csv (*)
        ├── inzidenzort_mapping.csv
        ├── mappings.csv (created during runtuime)
        ├── meta.csv (*)
        ├── person.csv (created during runtime)
        ├── relationship.csv (*)
        ├── visit_occurrence.csv (created during runtime)
        └── vocabulary.csv (*)
    └── utils
        ├── disease_extent_classification.json
        ├── exporter.py
        ├── helper.py
        ├── importer.py
        ├── xml_mapper.json.py
        └── xml_parser.py 
├── config.json
├── info.log (created during runtime)
├── main.py
├── README.md
└── requirements.txt
```


<br><br>
## :earth_africa: Required data sources
Metadata from Athena, required for mapping progress, is regularly updated and stored in the University of Hamburg’s data repository.
> Jasmin Carus, & Mareile Beernink. (2025). ZfKD | OMOP CDM - Metadata Cataloge (Version 2) [Data set]. http://doi.org/10.25592/uhhfdm.17924)

Please download the metadata from the University of Hamburg’s data repository and place the files in the src/stage folder.

**Required files**: concept.csv, concept_ancestor.csv, concept_class.csv, concept_relationship.csv, concept_synonym.csv, domain.csv, meta.csv, relationship.csv, vocabulary.csv

<br><br>
## :books: References
In the process of transforming the cancer registry data from XML to CSV and harmonizing it using the pipeline by Germer et al., we acknowledge the following publication:
> ** Germer, Sebastian and Sauerberg, Markus and Johanns, Ole and Rudolph, Christiane and Gundler, Christopher and Meisegeier, Stefan and Abnaof, Khalid and Kim-Wanner, Soo-Zin and Krauß, Anna and Langholz, Manuela and Luttmann, Sabine and Rath, Natalie and Rausch, Katharina and Katalinic, Alexander and Handels, Heinz and Nennecke, Alice and Kusche, Henrik and Kusche, Henrik.**
> *Harmonizing Regional Cancer Registry Data to Facilitate Germany-wide Epidemiological Analyses.*
> Available at SSRN: https://ssrn.com/abstract=5722854 or http://dx.doi.org/10.2139/ssrn.5722854

<br><br>
## :scroll: License
This project is licensed under the Apache License, Version 2.0.

A full copy of the license can be found in the file [licence.txt](./licence.txt)
or at http://www.apache.org/licenses/LICENSE-2.0.

If you use this software in scientific work, please cite the associated publication as:
<!-- TODO -->

