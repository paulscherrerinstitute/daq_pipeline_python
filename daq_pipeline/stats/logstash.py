import logging
from collections import defaultdict
from time import time

_logger = logging.getLogger('LogstashStats')


class LogstashStats(object):

    def __init__(self, logstash_address, stats_send_interval):
        self.logstash_address = logstash_address
        self.stats_send_interval = stats_send_interval

        self.stats_cache = defaultdict(lambda: [0, 0])

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

    def _add_stats_to_cache(self, stats_data):

        for stats_name, value in stats_data.items():
            field = self.stats_cache[stats_name]

            # Holds the sum of the values.
            field[0] += value
            # Holds the number of values in the first field.
            field[1] += 1

    def _get_stats_from_cache(self, time_delta=None):

        result = {}

        for stat_name, values in self.stats_cache.items():
            value_sum = values[0]
            n_elements_in_sum = values[1]

            result[stat_name] = value_sum/n_elements_in_sum

        if "iteration" in self.stats_cache:

            result["duty_cycle_utilization"] = result["iteration"] / 0.01

            if time_delta is not None:
                n_values = self.stats_cache["iteration"][1]
                result["rep_rate"] = n_values / time_delta

        self.stats_cache.clear()

        return result

    def add(self, stats_data):

        self._add_stats_to_cache(stats_data)

        if self.start_time is None:
            self.start_time = time()

        current_time = time()
        delta = current_time - self.start_time

        if delta > self.stats_send_interval:

            _logger.info("Stats: %s", self._get_stats_from_cache(delta))

            self.start_time = current_time
