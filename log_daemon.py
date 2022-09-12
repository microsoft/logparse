#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Parses cassandra log files and stores them in sqlite DB.

import datetime
import novadb_log_events
import os
import re
import sqlite3
import systemlog

from contextlib import closing
from fluent import sender
from os import path
from pygtail import Pygtail

# set up fluent
logger = sender.FluentSender('nova', port=25234)

log_file = '/var/log/cassandra/system.log'

try:
    with closing(novadb_log_events.init()) as connection:
        with closing(connection.cursor()) as cursor:
            while True:
                if path.exists(log_file):
                    log = Pygtail(log_file)
                    success = False
                    retry_attempt = 0
                    max_retry_attempts = 5
                    start_time = None
                    end_time = None
                    while not success and retry_attempt < max_retry_attempts:
                        try:
                            for event in systemlog.parse_log(log):
                                event_date = event['date']
                                if start_time is None:
                                    start_time = str(event_date)
                                end_time = str(event_date)
                                event_date_timestamp = int(datetime.datetime.timestamp(event_date))
                                del event['date']
                                # send to fluentd
                                # saw errors in the mdsd.err logs so not sure
                                # try int so we don't have nano secs
                                logger.emit_with_time('cassandra', event_date_timestamp, event)
                                
                                novadb_log_events.upsert(connection, cursor, event["event_product"], event["event_category"], event["event_type"], str(event_date))       
                        
                            connection.commit()
                            success = True

                        except sqlite3.Error as e:
                            print("Error occurred while comitting log events between {0} and {1}. Retrying: {2}".format(start_time, end_time, str(e)))
                            connection.rollback()
                            success = False
                            retry_attempt += 1
                            if retry_attempt == max_retry_attempts:
                                print("Emitting metrics for commit error")
                                emit_commit_error_metrics()
                            else:
                                time.sleep(1)

finally:
    print("Log parsing stopped")