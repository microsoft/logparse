#!/home/azureuser/cassandra-logparse/.venv/bin/python3

# Copyright (c) Microsoft Corporation. All rights reserved.

# Parses cassandra log files and emits them to fluentd, statsd, and sidecar

import json
from fluent import sender
from os import path
import os
import sys
from pygtail import Pygtail
import systemlog
import time
import statsd
from queue import Queue
from threading import Thread
import datetime

# set up fluent
logger = sender.FluentSender('nova', port=25234)

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

# set up Java
q = Queue()
if len(sys.argv) > 1:
    PIPE = sys.argv[1]
else:
    print("Usage: %s <pipe>" % sys.argv[0])
    sys.exit(1)

try:
    os.mkfifo(PIPE)
except OSError as e:
    print("Failed to create FIFO: %s" % e)

cancel = False


# use a background thread to write to the pipe
# since it's blocking
def writeToPipe():
    with open(PIPE, 'w') as fifo:
        flush_time = time.time()
        # process until canceled and queue is empty
        while not (cancel and q.empty()):
            data = q.get()
            if not data.get('terminate', False):
                fifo.write(json.dumps(data) + "\n")
                # flush every 10s
                if (flush_time + 10 < time.time()) or (q.empty()):
                    fifo.flush()
                    flush_time = time.time()
            q.task_done()


t = Thread(target=writeToPipe)
t.daemon = True
t.start()

try:
    while True:
        if path.exists('/var/log/cassandra/system.log'):
            log = Pygtail('/var/log/cassandra/system.log')
            for event in systemlog.parse_log(log):
                date = int(datetime.datetime.timestamp(event['date']))
                del event['date']
                # send to fluentd
                # saw errors in the mdsd.err logs so not sure
                # try int so we don't have nano secs
                logger.emit_with_time('cassandra', date, event)

                if event['event_type'] != 'unknown':
                    # write to Java
                    d = dict({
                        'event_product': event['event_product'],
                        'event_type': event['event_type'],
                        'event_category': event['event_category'],
                        'event_date': date
                    })
                    q.put(d)

        # sleep for the next round
        time.sleep(10)  # 10s

# on exit
finally:
    cancel = True
    # wake up thread
    q.put(dict({'terminate': 'True'}))
    # there is a way to wait until tall items are procesed
    # but we want to have a timeout since systemd will get antsy
    # so use join on thread instead of the queue
    t.join(60)

    # close fluentd
    logger.close()
