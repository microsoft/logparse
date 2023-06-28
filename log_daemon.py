#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Processes cassandra log, emits it to Geneva, and stores log events in durable storage.

import datetime
import novadb_log_events
import systemlog
import time

from contextlib import closing
from fluent import sender
from os import path
from os import environ
from pygtail import Pygtail


# Set up fluent
logger = sender.FluentSender('nova', port=25234, nanosecond_precision=True)

log_file = '/var/log/cassandra/system.log'
sleep_time = 5 # Sleep time in seconds
address = environ.get('ADDRESS')

try:
    with closing(novadb_log_events.init()) as connection:
        while True:
            if not path.exists(log_file):
                print("Log file {0} does not exist. Going to sleep for {1} seconds".format(log_file, sleep_time))
                time.sleep(sleep_time)
                continue 
                
            lines = Pygtail(log_file) # Fetch log lines

            events = dict()
            num_lines = 0
            for parsed_line in systemlog.parse_log(lines): # Processes each log line, and outputs it as a map of fields
                num_lines += 1
                # Emit the parsed log to Geneva
                timestamp = datetime.datetime.timestamp(parsed_line["date"])
                parsed_line["date"] = str(parsed_line["date"]) # If not converted to string, fluentd throws a serialization error for datetime object
                parsed_line["address"] = address
                logger.emit_with_time('cassandra', timestamp, parsed_line)

                # Add the parsed log to a map, which will be iterated over later, and stored persistently in the DB
                if parsed_line['event_type'] != 'unknown':
                    key = "{0}:{1}:{2}".format(parsed_line["event_product"], parsed_line["event_category"], parsed_line["event_type"])
                    events[key] = parsed_line

            if num_lines == 0:
                # This is to keep Pygtail from consuming >99% CPU
                time.sleep(1)
                continue

            novadb_log_events.upsert_events(connection, events)
finally:
    print("Log parsing stopped")