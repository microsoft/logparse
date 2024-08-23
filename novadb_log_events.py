# Copyright (c) Microsoft Corporation. All rights reserved.

# Utility methods to deal with log events in DB.

import json
import os
import shutil
import sqlite3
import statsd
import time
import traceback

from contextlib import closing

# set up statsd
stats = statsd.StatsClient('localhost', 8125)
account_name = os.getenv("MONITORING_MDM_ACCOUNT");
namespace = os.getenv("MONITORING_MDM_NAMESPACE");
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
            attributes text,
            PRIMARY KEY(event_product, event_category, event_type)
        ); """
        cursor.execute(query)

def upsert_events(connection, events_map):
    '''
    Values of the events_map are upserted into the DB as part of a single transaction,
    and committed at the end, to minimize disk I/O.
    '''
    if len(events_map) == 0:
        return
        
    success = False
    retry_attempt = 0
    max_retry_attempts = 5
    while not success and retry_attempt < max_retry_attempts:
        with closing(connection.cursor()) as cursor:
            try:
                for event in events_map.values():
                    attributes = get_attributes(event)
                    upsert(connection, cursor, event["event_product"], event["event_category"], event["event_type"], event["date"], attributes)       
                
                connection.commit()
                success = True

            except sqlite3.Error as e:
                print("Error occurred while comitting log events {0}. Retrying: {1}".format(str(events_map), str(e)))
                connection.rollback()
                success = False
                retry_attempt += 1
                if retry_attempt == max_retry_attempts:
                    print("Emitting metrics for commit error")
                    emit_commit_error_metrics()
                else:
                    time.sleep(1)

def get_attributes(event):
    attributes = {}

    try:
        if event["event_category"] == 'startup':
            if event["event_type"] == 'ip_address_conflict':
                attributes = {"endpoint": event["endpoint"]}

        if event["event_category"] == 'tombstone':
            if event["event_type"] == 'warning_threshold_exceeded':
                attributes = {"tombstoned_cells": event["tombstoned_cells"], "keyspace": event["keyspace"], "table": event["table"]}

            if event["event_type"] == 'error_threshold_exceeded':
                attributes = {"live_cells": event['live_cells'], "tombstoned_cells": event['tombstoned_cells'], "keyspace": event['keyspace'], "table": event['table'], "key": event['key'], "requested_columns": event['requested_columns'], slice_start: event['slice_start'], "slice_end": event['slice_end'], "deletion_info": event['deletion_info']}
    except:
        ex = traceback.format_exc()
        print("get_attributes: exception encountered - " + ex)
        
    return json.dumps(attributes)

def upsert(connection, cursor, event_product, event_category, event_type, event_date, attributes):
    query = """INSERT INTO LogEvent(event_product, event_category, event_type, event_date)
    VALUES (?, ?, ?, ?) ON CONFLICT(event_product, event_category, event_type) DO UPDATE set event_date=?, attributes=?"""
    values = (event_product, event_category, event_type, event_date, event_date, attributes)
 
    cursor.execute(query, values)

def emit_commit_error_metrics():
    dims = metric_identifier.copy()
    dims['Metric'] = "CommitLogEventsError"
    dims['Dims'] = {
        'Tenant': tenant,
        "Role": role,
        "RoleInstance": role_instance,
        "Service": "cassandra",
    }

    emit_metrics(dims)

    emit_metrics_to_file(dims, '/var/log/logevents_commit_error_metrics_new.json', '/var/log/logevents_commit_error_metrics.json')

def emit_metrics(dims):
    jsonDims = None
    try:
        jsonDims = json.dumps(dims)
        stats.gauge(jsonDims, 1)
    except Exception as e:
        print("Error emitting metrics " + jsonDims + ": " + str(e))

def emit_metrics_to_file(dims, src, dst):
    # Dump metrics into local filesystem
    try:
        # Make a copy of destination file if it already exists
        if os.path.exists(dst):
            shutil.copyfile(dst, src)
        
        # Append to it and rename it as the destination file
        with open(src, 'a+') as f:
            f.write(json.dumps(dims))
            f.close()
            os.rename(f.name, dst)

    except Exception as e:
        print("Error writing out metrics to file "+ dst +": " + str(e))

def init():
    database_file = r"/var/lib/cassandra/nova/logevents.db"

    connection = create_connection(database_file)
    connection.execute('pragma journal_mode=wal')

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
