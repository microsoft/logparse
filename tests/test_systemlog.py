from datetime import datetime
from unittest import TestCase

import systemlog


class Test(TestCase):

    LOG = "INFO  [NonPeriodicTasks:1] 2023-11-07 16:10:19,707 SSTable.java:111 - Deleting sstable: XXX"
    EXCEPTION_LOG = """ERROR [epollEventLoopGroup-5-3] 2023-11-07 16:10:42,645 NoSpamLogger.java:98 - Failed notifying listeners
        java.lang.NullPointerException: null
        ......"""

    def test_parse_log(self):
        logs = Test.LOG + "\n" + Test.LOG
        lines = logs.split("\n")
        output = list(systemlog.parse_log(lines))
        assert len(output) == 2
        self._verify_log(output[0])
        self._verify_log(output[1])

    def test_parse_log_with_exception(self):
        logs = Test.EXCEPTION_LOG + "\n" + Test.LOG
        lines = logs.split("\n")
        output = list(systemlog.parse_log(lines))
        assert len(output) == 2
        self._verify_exception_log(output[0])
        self._verify_log(output[1])

    def test_parse_log_with_exception_in_last_line(self):
        logs = Test.LOG + "\n" + Test.EXCEPTION_LOG
        lines = logs.split("\n")
        output = list(systemlog.parse_log(lines))
        assert len(output) == 2
        self._verify_log(output[0])
        self._verify_exception_log(output[1])

    def _verify_log(self, fields):
        assert fields["level"] == "INFO"
        assert fields["thread_name"] == "NonPeriodicTasks"
        assert fields["thread_id"] == "1"
        assert fields["date"] == datetime(2023, 11, 7, 16, 10, 19, 707000)
        assert fields["source_file"] == "SSTable.java"
        assert fields["source_line"] == "111"
        assert fields["message"] == "Deleting sstable: XXX"
        assert fields["event_product"] == "unknown"
        assert fields["event_category"] == "unknown"
        assert fields["event_type"] == "unknown"

    def _verify_exception_log(self, fields):
        assert fields["level"] == "ERROR"
        assert fields["thread_name"] == "epollEventLoopGroup-5"
        assert fields["thread_id"] == "3"
        assert fields["date"] == datetime(2023, 11, 7, 16, 10, 42, 645000)
        assert fields["source_file"] == "NoSpamLogger.java"
        assert fields["source_line"] == "98"
        assert fields["message"] == "Failed notifying listeners"
        assert fields["event_product"] == "unknown"
        assert fields["event_category"] == "unknown"
        assert fields["event_type"] == "unknown"
        assert fields["exception"] == "        java.lang.NullPointerException: null        ......"