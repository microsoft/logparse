#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Processes cassandra log, emits it to Geneva, and stores log events in durable storage.

import datetime
import novadb_log_events
import systemlog

from contextlib import closing
from fluent import sender
from os import path
from pygtail import Pygtail

# set up fluent
logger = sender.FluentSender('nova', port=25234, nanosecond_precision=True)

log_file = '/var/log/cassandra/system.log'

try:
    with closing(novadb_log_events.init()) as connection:
        while True:
            if path.exists(log_file):
                lines = Pygtail(log_file) # Fetch log lines

            events = dict()
            for parsed_logline_map in systemlog.parse_log(lines): # Processes the log lines, and outputs it as a map of fields
                # Emit the parsed line to Geneva
                timestamp = datetime.datetime.timestamp(parsed_logline_map["date"])
                parsed_logline_map["date"] = str(parsed_logline_map["date"]) # If not converted to string, fluentd throws a serialization error for datetime object
                logger.emit_with_time('cassandra', timestamp, parsed_logline_map)

                # Add the parsed line to a map, which will be iterated over later, and stored persistently in the DB
                if parsed_logline_map['event_type'] != 'unknown':
                    key = "{0}:{1}:{2}".format(parsed_logline_map["event_product"], parsed_logline_map["event_category"], parsed_logline_map["event_type"])
                    events[key] = parsed_logline_map

            novadb_log_events.upsert_events(connection, events)
finally:
    print("Log parsing stopped")