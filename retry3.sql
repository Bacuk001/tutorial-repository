with execution_params as ( select job_type, max(job_execution_id) as job_execution_id, item_id,
               (select string_val from BATCH_JOB_EXECUTION_PARAMS as x where x.job_execution_id = job_execution_id and key_name='sessionId' limit 1) as session_id 
               from (select (select string_val from BATCH_JOB_EXECUTION_PARAMS where k.job_execution_id = job_execution_id and key_name='jobType') as job_type, 
                   (select string_val from BATCH_JOB_EXECUTION_PARAMS where k.job_execution_id = job_execution_id and key_name='itemId') as item_id, 
                         job_execution_id
                         from BATCH_JOB_EXECUTION_PARAMS k ) group by job_type, item_id )
SELECT inst.job_instance_id, inst.job_name, 
                        execution.JOB_EXECUTION_ID, execution.START_TIME, execution.END_TIME, execution.STATUS, execution.EXIT_CODE, 
                        execution.EXIT_MESSAGE, execution.CREATE_TIME, execution.LAST_UPDATED, execution.VERSION, execution.JOB_CONFIGURATION_LOCATION, 
                        cont.SERIALIZED_CONTEXT, cont.SHORT_CONTEXT, 
                        item_id, job_type, session_id 
               FROM BATCH_JOB_EXECUTION AS execution 
                        INNER JOIN BATCH_JOB_EXECUTION_CONTEXT AS cont ON execution.job_execution_id = cont.job_execution_id
                        INNER JOIN BATCH_JOB_INSTANCE AS inst ON execution.job_instance_id = inst.job_instance_id 
                        INNER JOIN execution_params AS params ON  execution.job_execution_id = params.job_execution_id 
                         WHERE (SELECT count(*) FROM BATCH_JOB_EXECUTION where job_execution_id in 
                               (select job_execution_id from BATCH_JOB_EXECUTION_PARAMS k WHERE k.key_name='itemId'AND k.string_val in 
                               (select string_val from BATCH_JOB_EXECUTION_PARAMS WHERE job_execution_id = execution.job_execution_id and key_name = 'itemId')) 
                                   AND (STATUS = 'COMPLETED' or EXIT_CODE = 'COMPLETED')) = 0
                         AND ((execution.STATUS = 'STARTING' AND execution.STATUS ='STARTED' AND execution.STATUS = 'UNKNOWN' AND  session_id <> '24b2b06c-3df6-4c83-8fda-4d8c3719e982') 
                           OR execution.STATUS = 'FAILED' AND execution.EXIT_CODE <> 'COMPLETED') 
        		