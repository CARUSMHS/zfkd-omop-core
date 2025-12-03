CREATE OR REPLACE PROCEDURE public.pg_delete_all_cdm_constraints()
 LANGUAGE plpgsql
AS $procedure$
declare
sqlstatement text;
sqlstatement2 text;
begin

-- delete all Constraints and with 'CASCADE' all associated FK'S
for sqlstatement in 
SELECT 'ALTER TABLE cdm.'||table_name||' DROP CONSTRAINT '||constraint_name||' CASCADE;'
--select table_name ,constraint_name,*
from information_schema.constraint_table_usage
where table_schema ='cdm' 
and constraint_name like 'xpk%'
order by constraint_name loop/**/
if sqlstatement is not NULL
then 
	execute sqlstatement;
end if;
end loop ;

-- delte all fruther indices
for sqlstatement2 in select --*
'DROP INDEX ' || string_agg(i.indexrelid::regclass::text, ', ' ORDER  BY n.nspname, i.indrelid::regclass::text, cl.relname) AS drop_cmd
FROM   pg_index i
JOIN   pg_class cl ON cl.oid = i.indexrelid
JOIN   pg_namespace n ON n.oid = cl.relnamespace
LEFT   JOIN pg_constraint co ON co.conindid = i.indexrelid
WHERE   n.nspname = 'cdm'
AND    co.conindid IS NULL LOOP
if sqlstatement2 is not NULL
then 
	execute sqlstatement2;
end if;
end loop ;

end;
$procedure$
;

-- execute funcion
call pg_delete_all_cdm_constraints();