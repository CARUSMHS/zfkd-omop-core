--### COHORT BUILD ########
-- Step 1

DROP TABLE IF  EXISTS cdm.cancer_cohort;

DROP TABLE IF EXISTS cdm.cancer_regimen;

CREATE TABLE cdm.cancer_regimen (
       concept_name varchar,
       drug_era_id bigint,
       person_id bigint not null,
       rn bigint,
       drug_concept_id bigint,
       ingredient_start_date date not null,
       ingredient_end_date date
)
;

CREATE TABLE cdm.cancer_cohort (
       concept_name varchar,
       drug_era_id bigint,
       person_id bigint not null,
       rn bigint,
       drug_concept_id bigint,
       ingredient_start_date date not null,
       ingredient_end_date date
)
;


-- Jasmin 250211: Drug_Era is replaced with drug_exposure table 
/*with CTE_second as (
select
       lower(c.concept_name) as concept_name,
       de.drug_era_id,
       de.person_id,
       de.drug_concept_id,
       de.drug_era_start_date as ingredient_start_date,
       de.drug_era_end_date as ingredient_end_date
from cdm.drug_era de */
with CTE_second as (
select
       lower(c.concept_name) as concept_name,
       de.drug_exposure_id as drug_era_id,
       de.person_id,
       de.drug_concept_id,
       de.drug_exposure_start_date as ingredient_start_date,
       de.drug_exposure_end_date as ingredient_end_date
from cdm.drug_exposure de
inner join cdm.concept_ancestor ca on ca.descendant_concept_id = de.drug_concept_id
inner join cdm.concept c on c.concept_id = ca.ancestor_concept_id
    where c.concept_id in (
          select descendant_concept_id as drug_concept_id from cdm.concept_ancestor ca1
          where ancestor_concept_id in (21601386) /* Drug concept_id  */
)
and c.concept_class_id = 'Ingredient'
)
insert into cdm.cancer_cohort
select cs.concept_name,
       cs.drug_era_id,
       cs.person_id ,
       c2.rn,
       cs.drug_concept_id,
       cs.ingredient_start_date,
       cs.ingredient_end_date
from CTE_second cs
inner join (select distinct person_id, row_number()over(order by person_id) rn from (SELECT distinct person_id FROM CTE_second) cs) c2 on c2.person_id = cs.person_id
;


insert into  cdm.cancer_regimen
select *
from cdm.cancer_cohort;


--######### REGIMEN CALC #####
-- Step 2


DROP TABLE if exists cdm.cancer_regimen_tmp;

WITH add_groups AS (
  SELECT r1.person_id, r1.drug_era_id, r1.concept_name, r1.ingredient_start_date, min(r2.ingredient_start_date) as ingredient_start_date_new
  FROM cdm.cancer_regimen r1
  LEFT JOIN cdm.cancer_regimen r2 on r1.person_id = r2.person_id and
                                                     r2.ingredient_start_date <= r1.ingredient_start_date and
                                                     r2.ingredient_start_date >= r1.ingredient_start_date + interval '30 day'
  GROUP BY r1.person_id, r1.drug_era_id, r1.concept_name, r1.ingredient_start_date
),
regimens AS (
  SELECT person_id, ingredient_start_date_new,
         MAX(CASE WHEN ingredient_start_date = ingredient_start_date_new THEN 1 ELSE 0 END) as contains_original_ingredient
  FROM add_groups g
  GROUP BY ingredient_start_date_new, person_id
),
regimens_to_keep AS (
SELECT rs.person_id, gs.drug_era_id, gs.concept_name, rs.ingredient_start_date_new as ingredient_start_date
FROM regimens rs
LEFT JOIN add_groups gs on rs.person_id = gs.person_id and rs.ingredient_start_date_new = gs.ingredient_start_date_new
WHERE contains_original_ingredient > 0
),
updated_table AS (
SELECT * FROM regimens_to_keep
UNION
SELECT person_id, drug_era_id, concept_name, ingredient_start_date
FROM cdm.cancer_regimen WHERE drug_era_id NOT IN (SELECT drug_era_id FROM regimens_to_keep)
)
SELECT person_id, drug_era_id, concept_name, ingredient_start_date
INTO cdm.cancer_regimen_tmp
FROM updated_table;

DROP TABLE if exists cdm.cancer_regimen;

CREATE TABLE cdm.cancer_regimen (
       person_id bigint not null,
       drug_era_id bigint,
       concept_name varchar,
       ingredient_start_date date not null
); --DISTKEY(person_id) SORTKEY(person_id, ingredient_start_date);

INSERT INTO  cdm.cancer_regimen
SELECT * FROM cdm.cancer_regimen_tmp;


DROP TABLE if exists cdm.cancer_regimen_tmp;


--######### REGIMEN Voc #####
DROP TABLE IF EXISTS cdm.regimen_voc;
with CTE as (
select c1.concept_name as reg_name, 
		 string_agg(lower(c2.concept_name), ',' order by lower(c2.concept_name) asc) as combo_name, 
		 c1.concept_id
from cdm.concept_relationship 
join cdm.concept c1 on c1.concept_id=concept_id_1 
join cdm.concept c2 on c2.concept_id=concept_id_2
		where case when c1.vocabulary_id='HemOnc' and relationship_id='Has cytotoxic chemo' then 1 -- Jasmin 250211: added
					when c1.vocabulary_id='HemOnc' and relationship_id='Has supportive med' then 1 -- Jasmin 250211: added
					when c1.vocabulary_id='HemOnc' and relationship_id='Has antineoplastic' then 1 
		else 0 end = 1 
group by c1.concept_name,c1.concept_id
--order by c1.concept_name
),
CTE_second as (
select c.*, (case when lower(reg_name) = replace(combo_name,',',' and ') then 0
			 else row_number() over (partition by combo_name order by length(c.reg_name)) end ) as rank
from CTE c
--order by rank desc
),
CTE_third as (
select *,min(rank) over (partition by combo_name) as min
from CTE_second 
),
CTE_fourth as (
select ct.reg_name, ct.combo_name, ct.concept_id 
from CTE_third ct
where rank = min
)
select * 
into cdm.regimen_voc
from CTE_fourth;


--########## REGIMEN INGREDIENTS #############
-- Step 4

drop table if exists  cdm.regimen_ingredients;

with cte as (
select r.person_id, r.ingredient_start_date as regimen_start_date,
        STRING_AGG(lower(r.concept_name), ',' ORDER BY lower(r.concept_name)) as regimen
from cdm.cancer_regimen r
group by r.person_id, r.ingredient_start_date
)
select cte.person_id, orig.drug_era_id, i.concept_name as ingredient, i.ingredient_start_date, i.ingredient_end_date,
        cast(cte.regimen as varchar(2000)) as regimen, vt.concept_id as hemonc_concept_id, vt.reg_name, cte.regimen_start_date, max(i.ingredient_end_date) over (partition by cte.regimen_start_date, cte.person_id) as regimen_end_date
into cdm.regimen_ingredients
from cdm.cancer_regimen orig
left join cte on cte.person_id = orig.person_id and cte.regimen_start_date = orig.ingredient_start_date
left join cdm.cancer_cohort i on i.person_id = orig.person_id and i.drug_era_id = orig.drug_era_id
left join cdm.regimen_voc vt on cte.regimen = vt.combo_name
order by cte.person_id, regimen_start_date

