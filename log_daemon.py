#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Parses cassandra log files and stores them in sqlite DB.

import datetime
import novadb_log_events
import os
import systemlog

from contextlib import closing
from fluent import sender
from os import path
from pygtail import Pygtail

# set up fluent
logger = sender.FluentSender('nova', port=25234)

log_file = '/var/log/cassandra/system.log'

try:
    with closing(novadb_log_events.init()) as conn:
        while True:
            if path.exists(log_file):
                log = Pygtail(log_file)
                for event in systemlog.parse_log(log):
                    event_date = event['date']
                    event_date_timestamp = int(datetime.datetime.timestamp(event_date))
                    del event['date']
                    # send to fluentd
                    # saw errors in the mdsd.err logs so not sure
                    # try int so we don't have nano secs
                    logger.emit_with_time('cassandra', event_date_timestamp, event)
                    novadb_log_events.upsert(conn, event["event_product"], event["event_category"], event["event_type"], str(event_date))
finally:
    print("Log parsing stopped")
