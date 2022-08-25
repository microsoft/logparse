#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Parses cassandra log files and stores them in sqlite DB.

import datetime
import novadb_log_events
import os
import systemlog

from contextlib import closing
from os import path
from pygtail import Pygtail

try:
    with closing(novadb_log_events.init()) as conn:
        while True:
            if path.exists('/var/log/cassandra/debug.log'):
                log = Pygtail('/var/log/cassandra/debug.log')
                for event in systemlog.parse_log(log):
                    novadb_log_events.upsert(conn, event["event_product"], event["event_category"], event["event_type"], event['date'])
finally:
    print("Log parsing stopped")
