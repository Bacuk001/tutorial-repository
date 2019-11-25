select p.job_execution_id, p.string_val as task_tpe, k.*, cont.* from batch_job_execution_params as p 
	inner join batch_job_execution as k on k.job_execution_id = p.job_execution_id
	inner join batch_job_execution_context as cont on cont.job_execution_id = p.job_execution_id
where p.job_execution_id in (select job_execution_id from batch_job_execution_params where string_val ='4006168') and p.key_name ='jobType' 
