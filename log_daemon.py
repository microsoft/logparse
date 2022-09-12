#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Parses cassandra log files and stores them in sqlite DB.

import datetime
import novadb_log_events
import os
import re
import sqlite3
import systemlog
import time

from contextlib import closing
from fluent import sender
from os import path
from pygtail import Pygtail

# set up fluent
logger = sender.FluentSender('nova', port=25234)

log_file = '/var/log/cassandra/system.log'

try:
    with closing(novadb_log_events.init()) as connection:
            while True:
                if path.exists(log_file):
                    lines = Pygtail(log_file)
                    success = False
                    retry_attempt = 0
                    max_retry_attempts = 5
                    while not success and retry_attempt < max_retry_attempts:
                        with closing(connection.cursor()) as cursor:
                            start_time_events = None
                            end_time_events = None
                            try:
                                for event in systemlog.parse_log(lines):
                                    event_date = event['date']
                                    # send to fluentd
                                    if retry_attempt == 0:
                                        # saw errors in the mdsd.err logs so not sure
                                        # try int so we don't have nano secs
                                        event_date_timestamp = int(datetime.datetime.timestamp(event_date))
                                        del event['date']
                                        logger.emit_with_time('cassandra', event_date_timestamp, event)

                                    if start_time_events is None:
                                        start_time_events = str(event_date)
                                    end_time_events = str(event_date)  
                                    novadb_log_events.upsert(connection, cursor, event["event_product"], event["event_category"], event["event_type"], str(event_date))       
                            
                                connection.commit()
                                success = True

                            except sqlite3.Error as e:
                                print("Error occurred while comitting log events between {0} and {1}. Retrying: {2}".format(start_time_events, end_time_events, str(e)))
                                connection.rollback()
                                success = False
                                retry_attempt += 1
                                if retry_attempt == max_retry_attempts:
                                    print("Emitting metrics for commit error")
                                    novadb_log_events.emit_commit_error_metrics()
                                else:
                                    time.sleep(1)

finally:
    print("Log parsing stopped")