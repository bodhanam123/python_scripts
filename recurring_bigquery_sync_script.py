import datetime
import time
from env_config import env_details, derived_queries_details, load_credentials_and_clients
from main_v2 import (
    is_time_between, send_ib_com_message, 
    retry_replace_bigquery_tables, retry_replace_bq_derived_tables, 
    retry_data_replication, retry,
    get_sql_instance_status, _start_cloud_sql, _stop_cloud_sql
)



def wait_until(dt):
    """
    Waits until specified datetime using time.sleep()
    """
    now = datetime.datetime.now()
    if now < dt:
        time.sleep((dt - now).total_seconds())


def generate_timestamps(start_time, end_time, delta, date):
    """
    Generates a list of timestamps in the given time range
    """
    timestamps = []
    start_dt = datetime.datetime.combine(date, datetime.datetime.strptime(start_time, "%H:%M").time())
    end_dt = datetime.datetime.combine(date, datetime.datetime.strptime(end_time, "%H:%M").time())
    if end_dt < start_dt:
        end_dt += datetime.timedelta(days=1)
    current_dt = start_dt
    while current_dt <= end_dt:
        timestamps.append(current_dt)
        current_dt += delta
    return timestamps


def get_next_datetime_in_range(start_time, end_time, delta):
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    timestamps = generate_timestamps(start_time, end_time, delta, today) + generate_timestamps(start_time, end_time, delta, tomorrow)
    now = datetime.datetime.now()
    timestamps.sort()
    for dt in timestamps:
        if now < dt:
            return dt



def run_derived_queries(dq_id):
    for dq in derived_queries_details:
        if dq['dq_id'] == dq_id:
            retry_replace_bq_derived_tables(
                env_details[dq['bq_client_env']]['bq_client'], 
                dq['queries']
            )
            # send_ib_com_message("@durga.gummalla Updated Derived tables {}".format(len(dq['queries'])))



def sync_recurring_tables(env):
    print("Replacing BigQuery Tables...")
    retry_replace_bigquery_tables(env_details[env]['bq_client'], env_details[env]['project_id'], env_details[env]['tables'], env_details[env]['restricted_tables'])
    tables_count = sum(len(v) for v in env_details[env]['tables'].values())
    restricted_tables_count = sum(len(v) for v in env_details[env]['restricted_tables'].values())
    # send_ib_com_message("@durga.gummalla {} tables sync completed ({} + {})".format(env_details[env]['msg_name'], tables_count, restricted_tables_count))



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync RDS to BigQuery.')
    parser.add_argument('--now', action='store_true', default=False,
                        help='Start sync immediately')
    args = parser.parse_args()
    load_credentials_and_clients()
    env = 'recurring@ibuild'

    right_time = ["10:00", "22:30"]
    start_time, end_time = right_time
    delta = datetime.timedelta(minutes=30)
    first = True
    while True:
        scheduled_dt = get_next_datetime_in_range(start_time, end_time, delta)
        print("Next Schedule {}".format(scheduled_dt))
        if args.now and first:
            first = False
        else:
            wait_until(scheduled_dt)
        sync_recurring_tables(env)
        run_derived_queries(2)
        # send_ib_com_message("Synced Taskflow Tables")
