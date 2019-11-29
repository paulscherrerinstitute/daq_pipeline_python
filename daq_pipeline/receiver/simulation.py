import logging
from random import randint
from time import time, sleep

import numpy

from daq_pipeline import config
from daq_pipeline.receiver.utils import get_pulse_id_mod

_logger = logging.getLogger('SimulatedReceiver')


class SimulatedReceiver(object):
    def __init__(self, device_name, channels_metadata, target_read_time=0.001):
        self.device_name = device_name
        self.channels_metadata = channels_metadata
        self.target_read_time = target_read_time

        _logger.info("Simulating device_name=%s with channels_metadata=%s", self.device_name, self.channels_metadata)

        self.current_pulse_id = int(time())
        self.iter_counter = 1
        self.start_time = 0
        self.cache = {}

    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, traceback):
        pass

    def _generate_data(self, shape, type):

        if (tuple(shape), type) not in self.cache:

            if type == "string":
                return "this is just a random string" \
                       "this is just a random string" \
                       "this is just a random string".encode()

            # Downgrade images to arrays.
            if len(shape) > 1:
                shape = shape[0]

            raw_data = numpy.zeros(shape=shape, dtype=type)

            random_value = randint(0, 100)
            raw_data += random_value

            self.cache[(tuple(shape), type)] = raw_data.tobytes()

        return self.cache[(tuple(shape), type)]

    def get_data(self):

        if self.iter_counter == 1:
            self.start_time = time()

        self.current_pulse_id += 1
        pulse_id = self.current_pulse_id
        pulse_id_modulo = pulse_id // config.CHANNEL_DATA_PARTITION_MODULO

        _logger.debug("Generating source device_name=%s pulse_id_modulo=%s pulse_id=%s",
                      self.device_name, pulse_id_modulo, pulse_id)

        data = []
        channels = []
        n_data_bytes = 0

        for channel_metadata in self.channels_metadata:

            channel_name = channel_metadata["name"]
            shape = channel_metadata.get("shape", [1])
            type = channel_metadata.get("type", "float64")
            encoding = channel_metadata.get("encoding", "little")
            compression = channel_metadata.get("compression", None)

            raw_data = self._generate_data(shape, type)

            n_bytes = len(raw_data)
            pulse_id_modulo = pulse_id // get_pulse_id_mod(n_bytes)

            data.append(
                (channel_name,
                 pulse_id_modulo,
                 pulse_id,
                 raw_data,
                 type,
                 shape,
                 encoding,
                 compression)
            )

            channels.append(channel_name)
            n_data_bytes += n_bytes

        delta = (time() - self.start_time) - (self.target_read_time * self.iter_counter)

        self.iter_counter %= 100
        self.iter_counter += 1

        if delta < 0:
            sleep(-delta)

        return data, {"pulse_id": pulse_id,
                      "channels": channels,
                      "n_data_bytes": n_data_bytes}
