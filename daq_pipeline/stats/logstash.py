import logging

_logger = logging.getLogger('LogstashStats')


class LogstashStats(object):

    def __init__(self, logstash_address, stats_send_interval):
        self.logstash_address = logstash_address
        self.stats_send_interval = stats_send_interval

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def add(self, data, metadata):
        pass
