import time
from env_config import env_details, derived_queries_details, load_credentials_and_clients
from main_v2 import (
    is_time_between, send_ib_com_message, 
    retry_replace_bigquery_tables, retry_replace_bq_derived_tables, 
    retry_data_replication, retry,
    get_sql_instance_status, _start_cloud_sql, _stop_cloud_sql
)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync RDS to BigQuery.')
    parser.add_argument('--now', action='store_true', default=False,
                        help='Start sync immediately')
    args = parser.parse_args()
    import boto3
    session = boto3.Session(region_name='ap-south-1')
    client = session.client('dms')
    load_credentials_and_clients()

    current_environments = [
        'ibcom@nxtwave',
        'prod@nxtwave',
        'dev@nxtwave',
    ]

    while True:
        done_environments = set()
        done_derived_queries = set()
        for env in current_environments:

            # Wait for the time-slot to proceed to next steps
            if not args.now:
                print("Waiting for the right time...")
                if env_details[env].get('right_time'):
                    while not is_time_between(*env_details[env].get('right_time')):
                        time.sleep(300)

            # Start GCP SQL instance if not always running
            if not env_details[env]['always_on_sql_instance']:
                print("Starting Cloud SQL Instance...")
                retry(_start_cloud_sql, env_details[env]['service'], env_details[env]['project_id'], env_details[env]['sql_instance_id'])
                while retry(get_sql_instance_status, env_details[env]['service'], env_details[env]['project_id'], env_details[env]['sql_instance_id']) != "RUNNING":
                    time.sleep(60)
                print(retry(get_sql_instance_status, env_details[env]['service'], env_details[env]['project_id'], env_details[env]['sql_instance_id']))
                print("Started Cloud SQL Instance.")

            # Start DMS Replication if it is not continuous syncing type
            if not env_details[env]['continuous_replication']:
                print("Starting DMS Replication Task...")
                retry_data_replication(client, env_details[env]['dms_arn'])

            # Replace BigQuery tables from GCP SQL instance
            print("Replacing BigQuery Tables...")
            retry_replace_bigquery_tables(env_details[env]['bq_client'], env_details[env]['project_id'], env_details[env]['tables'], env_details[env]['restricted_tables'])
            tables_count = sum(len(v) for v in env_details[env]['tables'].values())
            restricted_tables_count = sum(len(v) for v in env_details[env]['restricted_tables'].values())
            send_ib_com_message("{} tables sync completed ({} + {})".format(env_details[env]['msg_name'], tables_count, restricted_tables_count))

            # Stop GCP SQL instance if not always running type
            if not env_details[env]['always_on_sql_instance']:
                print("Stopping Cloud SQL Instance...")
                retry(_stop_cloud_sql, env_details[env]['service'], env_details[env]['project_id'], env_details[env]['sql_instance_id'])

            done_environments.add(env)


            for dq in derived_queries_details:
                if dq['dq_id'] not in done_derived_queries and done_environments > set(dq['depends']):
                    retry_replace_bq_derived_tables(
                        env_details[dq['bq_client_env']]['bq_client'],
                        dq['queries']
                    )
                    done_derived_queries.add(dq['dq_id'])
                    send_ib_com_message("Updated {} Derived tables {}".format(dq['name'], len(dq['queries'])))

        print("Waiting around 15 hours ...")
        time.sleep(60000)
