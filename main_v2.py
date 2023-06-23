import datetime
import os
import time
from google.api_core import exceptions as gexceptions

def generate_ddl_queries(project_id, tables_dict, restricted_tables_dict):
    ddl_queries = []
    for dataset_name, tables in tables_dict.items():
        for table_name in tables:
            ddl_query = """
                CREATE OR REPLACE TABLE {dataset_name}.{table_name} AS SELECT * FROM EXTERNAL_QUERY("{project_id}.asia-south1.{dataset_name}", "SELECT * FROM {table_name}");
                """.format(dataset_name=dataset_name, table_name=table_name, project_id=project_id)
            ddl_queries.append((ddl_query, dataset_name, table_name))

    for dataset_name, tables in restricted_tables_dict.items():
        for table_name, column_names in tables.items():
            query = "SELECT {cols} FROM {table_name}".format(cols=", ".join(column_names), table_name=table_name)
            ddl_query = """
                CREATE OR REPLACE TABLE {dataset_name}.{table_name} AS SELECT * FROM EXTERNAL_QUERY("{project_id}.asia-south1.{dataset_name}", "{query}");
                """.format(dataset_name=dataset_name, table_name=table_name, query=query, project_id=project_id)
            ddl_queries.append((ddl_query, dataset_name, table_name))
    return ddl_queries

def execute_ddl_queries(bq_client, ddl_queries, query_jobs_info=None):
    if not query_jobs_info:
        query_jobs_info = dict()
    count = 0
    for ddl_query, dataset_name, table_name in ddl_queries:
        query_job = bq_client.query(
                    ddl_query,
                    location="asia-south1",
                    job_id_prefix="replace_table_{}_{}_".format(dataset_name, table_name),
                )
        print("[{}] Replacing Table {}.{} in job: {}".format(datetime.datetime.now(), dataset_name, table_name, query_job.job_id))
        query_jobs_info[query_job.job_id] = (ddl_query, dataset_name, table_name)
        count += 1
        if count % 50 == 0:
            time.sleep(5) # QuickFix: Sleeping 5 seconds for every 50 queries to avoid reaching 100 concurrency limit
    return query_jobs_info

def get_missing_query_jobs(bq_client, query_jobs_info):
    missing_jobs = []
    missing_job_ids = []
    for job_id, (ddl_query, dataset_name, table_name) in query_jobs_info.items():
        try:
            job = bq_client.get_job(job_id, location='asia-south1')
            print("[{}] {}.{}: {}".format(datetime.datetime.now(), dataset_name, table_name, 'RUNNING' if job.running() else 'DONE'))
        except gexceptions.NotFound:
            print("[{}] Missing Job {}".format(datetime.datetime.now(), job_id))
            missing_jobs.append((ddl_query, dataset_name, table_name))
            missing_job_ids.append(job_id)
    for job_id in missing_job_ids:
        query_jobs_info.pop(job_id)
    return missing_jobs

def execute_and_verify_ddl_queries(bq_client, ddl_queries, verify=False):
    start_time = str(datetime.datetime.utcnow())
    query_jobs_info = dict()
    while len(ddl_queries) != 0:
        query_jobs_info = execute_ddl_queries(bq_client, ddl_queries, query_jobs_info)
        if verify:
            ddl_queries = get_missing_query_jobs(bq_client, query_jobs_info)
        else:
            ddl_queries = []
    end_time = str(datetime.datetime.utcnow())
    return start_time, end_time, query_jobs_info

def await_job_completion(bq_client, job_id):
    job = bq_client.get_job(job_id, location='asia-south1')
    while not job.done():
        time.sleep(5)

def execute_queries_sequentially(bq_client, ddl_queries):
    query_jobs_info = dict()
    for ddl_query in ddl_queries:
        try:
            query_jobs_info = execute_ddl_queries(bq_client, [ddl_query], {})
            await_job_completion(bq_client, list(query_jobs_info.keys())[0])
            print_query_summary(bq_client, query_jobs_info)
        except Exception as e:
            print(e)
            print("[FAIL] BQ Sync failed for query {}".format(ddl_query))

def replace_tables(bq_client, project_id, tables_dict, restricted_tables_dict):
    ddl_queries = generate_ddl_queries(project_id, tables_dict, restricted_tables_dict)
    return execute_and_verify_ddl_queries(bq_client, ddl_queries)

def replace_derived_tables(bq_client, queries):
    ddl_queries = [(q[1], 'bq_views', q[0]) for q in queries]
    return execute_queries_sequentially(bq_client, ddl_queries)

def show_progress_message(running_jobs):
    running_job_names = [j.job_id for j in running_jobs]
    table_names = ['_'.join(j.job_id.split('_')[2:-1]) for j in running_jobs]
    message = '[{}]'.format(datetime.datetime.now())
    display_table_names = []
    for table_name in table_names:
        if sum(len(t) for t in display_table_names) + len(display_table_names)*2 + len(table_name) < 120:
            display_table_names.append(table_name)
        else:
            break
    remaining = len(table_names) - len(display_table_names)
    message = message + " Replacing: "+ ', '.join(display_table_names)
    if remaining > 0:
        message += " & {} others".format(remaining)
    return message

def running_queries_count(bq_client, start_time_in_iso_format, end_time_in_iso_format, _show_progress=True):
    start_time = datetime.datetime.fromisoformat(start_time_in_iso_format)
    end_time = datetime.datetime.fromisoformat(end_time_in_iso_format)
    running_jobs = [j for j in bq_client.list_jobs(
            min_creation_time=start_time, max_creation_time=end_time
        ) if not j.done()]
    if _show_progress:
        message = show_progress_message(running_jobs)
        print("\r"+message, end='')
    return len(running_jobs)

def print_query_summary(bq_client, query_jobs_info):
    for job_id, (ddl_query, dataset_name, table_name) in query_jobs_info.items():
        try:
            job = bq_client.get_job(job_id, location='asia-south1')
            print("[{}] {}.{}: Billed for {} GB. Duration: {}".format(
                datetime.datetime.now(), dataset_name, table_name, 
                 job.total_bytes_billed / 1024 / 1024 / 1024,
                 job.ended - job.created
                )
            )
        except gexceptions.NotFound:
            print("[{}] Missing Job {}".format(datetime.datetime.now(), job_id))
        except Exception as e:
            try:
                print("Exception {} in Job {}".format(job.exception(), job_id))
            except:
                print("Exception {} in Job {}".format(e, job_id))

def is_time_between(start, end):
    import datetime
    start_hour, start_min = [int(i) for i in start.split(":")]
    end_hour, end_min = [int(i) for i in end.split(":")]
    dt = datetime.datetime.now()
    if (start_hour, start_min) < (end_hour, end_min):
        is_right_time = datetime.time(start_hour, start_min) < dt.time() < datetime.time(end_hour, end_min)
    else:
        is_right_time = datetime.time(start_hour, start_min) < dt.time() or dt.time() < datetime.time(end_hour, end_min)
    return is_right_time

def ignore_errors(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print("Exceptions: ", e)

def retry(func, *args, **kwargs):
    errors = True
    while errors:
        try:
            errors = False
            return func(*args, **kwargs)
        except Exception as e:
            errors = True
            print("Exceptions: ", e)
            time.sleep(5)


def retry_replace_bigquery_tables(bq_client, project_id, tables_dict, restricted_tables_dict):
    start_time, end_time, query_jobs_info = retry(replace_tables, bq_client, project_id, tables_dict, restricted_tables_dict)
    _running_queries = retry(running_queries_count, bq_client, start_time, end_time)
    while _running_queries != 0:
        time.sleep(60)
        _running_queries = retry(running_queries_count, bq_client, start_time, end_time)
    print_query_summary(bq_client, query_jobs_info)

def retry_replace_bq_derived_tables(bq_client, queries):
    retry(replace_derived_tables, bq_client, queries)


IB_COM_ENDPOINT = "https://ibcom.ibhubs.co/hooks/oab1kkxywbnbmk3ccp7sqqf4xc"
IB_COM_CHANNEL = "loki_and_data_analytics"
IB_COM_USERNAME = "ibcom_bot"

def send_ib_com_message(message):
    import json
    import requests
    payload = {
        "channel": IB_COM_CHANNEL,
        "username": IB_COM_USERNAME,
        "text": message
    }
    response = requests.post(
        IB_COM_ENDPOINT,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )
    return response


def restart_replication_task(client, task_arn):
    client.start_replication_task(
        ReplicationTaskArn=task_arn,
        StartReplicationTaskType='reload-target',
    )

def resume_replication_task(client, task_arn):
    client.start_replication_task(
        ReplicationTaskArn=task_arn,
        StartReplicationTaskType='resume-processing',
    )

def stop_replication_task(client, task_arn):
    client.stop_replication_task(
        ReplicationTaskArn=task_arn
    )

def modify_replication_task(client, task_arn, table_mappings):
    import json
    response = client.modify_replication_task(
        ReplicationTaskArn=task_arn,
        TableMappings=json.dumps(table_mappings),
    )
    
def get_replication_status_and_stop_reason(client, task_arn):
    """
    stop_reason = None => running
    stop_reason = MANUALLY_STOPPED => stopped manually
    stop_reason = FULL_LOAD_ONLY_FINISHED => task completed successfully
    """
    response = client.describe_replication_tasks(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    task_arn,
                ],
            },
        ]
    )
    task_status = response['ReplicationTasks'][0]['Status']
    task_stats = response['ReplicationTasks'][0]['ReplicationTaskStats']
    print("{} {}".format(datetime.datetime.now(), task_stats))
    print("{} {}".format(datetime.datetime.now(), task_status))
    stop_reason = None
    if task_status == "stopped":
        stop_reason = response['ReplicationTasks'][0].get('StopReason', "MANUALLY_STOPPED")
        print(stop_reason)
    return task_status, stop_reason

def get_replication_status(client, task_arn):
    task_status, _ = get_replication_status_and_stop_reason(client, task_arn)
    return task_status

def retry_data_replication(client, migration_task_arn, max_duration_mins=20, max_replication_retries=3):
    retry(restart_replication_task, client, migration_task_arn)
    time.sleep(300)
    replication_status = 'failed'
    replication_retries = 1
    while replication_retries < max_replication_retries:
        start_time = datetime.datetime.now()
        while replication_status == 'failed':
            while retry(get_replication_status, client, migration_task_arn) == 'running':
                now = datetime.datetime.now()
                if (now - start_time).total_seconds() > (max_duration_mins * 60) and replication_retries < max_replication_retries:
                    stop_replication_task(client, migration_task_arn)
                time.sleep(300)
            replication_status = retry(get_replication_status, client, migration_task_arn)
            if replication_status == 'failed':
                retry(resume_replication_task, client, migration_task_arn)
                time.sleep(300)
            elif replication_status in ('stopped', 'stopping'):
                break
        while retry(get_replication_status, client, migration_task_arn) == 'stopping':
            time.sleep(60)
        replication_status, stop_reason = retry(get_replication_status_and_stop_reason, client, migration_task_arn)
        if replication_status == 'stopped' and stop_reason == 'FULL_LOAD_ONLY_FINISHED':
            break
        replication_retries += 1

def await_replication_task(client, task_arn):
    while retry(get_replication_status, client, task_arn) == 'running':
            time.sleep(300)

def get_sql_instance_status(service, project_id, instance_id):
    response =  service.instances().get(
        project=project_id, instance=instance_id
    ).execute()
    if response['state'] == 'RUNNABLE':
        if response['settings']['activationPolicy'] == 'ALWAYS':
            status = "RUNNING"
        elif response['settings']['activationPolicy'] == 'NEVER':
            status = "STOPPED"
        else:
            print("Unknown status", response['settings']['activationPolicy'])
            status = "RETRY"
    elif response['state'] == 'MAINTENANCE':
        status = "RETRY"
    else:
        status = "RETRY"
    return status

def _start_cloud_sql(service, project_id, instance_id):
    service.instances().patch(
        project=project_id, instance=instance_id,
        body={"settings": {"activationPolicy": "ALWAYS"}}
    ).execute()

def _stop_cloud_sql(service, project_id, instance_id):
    service.instances().patch(
        project=project_id, instance=instance_id,
        body={"settings": {"activationPolicy": "NEVER"}}
    ).execute()

