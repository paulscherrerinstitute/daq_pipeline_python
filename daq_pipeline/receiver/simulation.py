import logging
from random import randint
from time import time, sleep

import numpy

from daq_pipeline import config

_logger = logging.getLogger('SimulatedReceiver')


class SimulatedReceiver(object):
    def __init__(self, device_name, channels_metadata, target_read_time=0.03):
        self.device_name = device_name
        self.channels_metadata = channels_metadata
        self.target_read_time = target_read_time

        _logger.info("Simulating device_name=%s with channels_metadata=%s", self.device_name, self.channels_metadata)

        self.current_pulse_id = int(time())

    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, traceback):
        pass

    def _generate_data(self, shape, type):

        raw_data = numpy.zeros(shape=shape, dtype=type)

        random_value = randint(0, 100)
        raw_data += random_value

        return raw_data.tobytes()

    def get_data(self):
        start_time = time()

        self.current_pulse_id += 1
        pulse_id = self.current_pulse_id

        data = []

        for channel_metadata in self.channels_metadata:
            channel_name = channel_metadata["name"]
            shape = channel_metadata["shape"]
            type = channel_metadata["type"]
            encoding = channel_metadata["encoding"]
            compression = channel_metadata["compression"]

            raw_data = self._generate_data(shape, type)

            data.append(
                (self.device_name,
                 pulse_id % config.CHANNEL_DATA_PARTITION_MODULO,
                 channel_name,
                 pulse_id,
                 raw_data,
                 type,
                 shape,
                 encoding,
                 compression)
            )

        delta = (time() - start_time) - self.target_read_time

        if delta < 0:
            sleep(-delta)

        return data
