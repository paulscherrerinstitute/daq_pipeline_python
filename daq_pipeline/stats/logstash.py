import logging
from time import time

_logger = logging.getLogger('LogstashStats')


class LogstashStats(object):

    def __init__(self, logstash_address, stats_send_interval):
        self.logstash_address = logstash_address
        self.stats_send_interval = stats_send_interval

        self.start_time = None
        self.n_messages = 0

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def add(self, data, metadata):

        if self.start_time is None:
            self.start_time = time()

        self.n_messages += 1
        current_time = time()

        if current_time-self.start_time > self.stats_send_interval:

            _logger.info("Processing rate: %s Hz", self.n_messages/self.stats_send_interval)

            self.n_messages = 0
            self.start_time = current_time
