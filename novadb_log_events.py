import json
import os
import sqlite3
import statsd
from contextlib import closing

# set up statsd
stats = statsd.StatsClient('localhost', 8125)
account_name = os.getenv("MONITORING_GCS_ACCOUNT");
namespace = os.getenv("MONITORING_GCS_NAMESPACE");
tenant = os.getenv("MONITORING_TENANT");
role = os.getenv("MONITORING_ROLE");
role_instance = os.getenv("MONITORING_ROLE_INSTANCE");

metric_identifier = {
    "Account": account_name,
    "Namespace": namespace,
}

def create_connection(database_file):
    return sqlite3.connect(database_file)

def create_table(connection):
    with closing(connection.cursor()) as cursor:
        query = """CREATE TABLE IF NOT EXISTS LogEvent (
            event_product text,
            event_category text,
            event_type text,
            event_date text,
            PRIMARY KEY(event_product, event_category, event_type)
        ); """
        cursor.execute(query)

def upsert(connection, event_product, event_category, event_type, event_date):
    success = False
    retry_attempt = 0
    max_retry_attempts = 5

    while not success and retry_attempt < max_retry_attempts:
        try:
            with closing(connection.cursor()) as cursor:
                query = """INSERT INTO LogEvent(event_product, event_category, event_type, event_date)
                VALUES (?, ?, ?, ?) ON CONFLICT(event_product, event_category, event_type) DO UPDATE set event_date=?"""
                values = (event_product, event_category, event_type, event_date, event_date)
                cursor.execute(query, values)
                connection.commit()
                success = True
        except sqlite3.Error as e:
            print("Error occurred while replacing log event {0}, {1}, {2}, {3}: {4}. Retrying".format(event_product, event_category, event_type, event_date, str(e)))
            connection.rollback()
            success = False
            retry_attempt += 1
    
    if retry_attempt == max_retry_attempts:
        print("Emitting metrics for upsert error")
        emit_metrics(event_product, event_category, event_type, event_date)

def emit_metrics(event_product, event_category, event_type, event_date):
    # statsd
    dims = metric_identifier.copy()
    dims['Metric'] = "UpsertLogEventError"
    dims['Dims'] = {
        'Tenant': tenant,
        'EventProduct': event_product,
        'EventCategory': event_category,
        'EventType': event_type,
        'EventDate': event_date,
        "Role": role,
        "RoleInstance": role_instance,
        "Service": "cassandra",
    }

    try:
        stats.gauge(json.dumps(dims), 1)
    except Exception as e:
        print("Error emitting metrics: " + str(e))

    # Dump metrics into local filesystem too
    try:
        with open('/var/log/log_parse_error_metrics_new.json', 'w+') as f:
            f.write(json.dumps(dims))
            f.close()
            os.rename(f.name, '/var/log/log_parse_error_metrics.json')
    except Exception as e:
        print("Error writing out metrics to local file system: " + str(e))

def init():
    database_file = r"/var/lib/cassandra/nova/novautil.db"

    connection = create_connection(database_file)
    create_table(connection)

    return connection

def main():
    with closing(init()) as conn:
        print("Log events stored in Nova DB:")

        query = '''SELECT * FROM LogEvent'''
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            output = cursor.fetchall()
            for row in output:
                print(row)

if __name__ == '__main__':

    main()
