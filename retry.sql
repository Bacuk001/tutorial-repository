with falied_tasks as (select * from batch_job_execution where status <> 'COMPLETED' and exit_code<>'COMPLETED'),
item_id_row as ( select * from batch_job_execution_params where job_execution_id in (select job_execution_id from falied_tasks group by job_execution_id) and key_name = 'itemId'),
ttt as (select e.job_execution_id, p.string_val, e.status, e.exit_code  from batch_job_execution_params as p inner join batch_job_execution as e on p.job_execution_id =e.job_execution_id where string_val in (SElect string_val from item_id_row)),
status as (select distinct 
	s.string_val as item_id, 
	(select max(job_execution_id) as job_execution_id from ttt as v where v.status ='FAILED'  AND v.string_val = s.string_val  group by v.string_val)  as FAILED,
	(select max(job_execution_id) as job_execution_id from ttt as v where v.exit_code ='COMPLETED' AND v.string_val = s.string_val group by v.string_val) as COMPLETED
  from ttt as s order by string_val)
select * from status where COMPLETED is null AND FAILED is not null
