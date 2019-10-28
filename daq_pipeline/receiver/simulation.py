import logging

_logger = logging.getLogger('SimulatedReceiver')


class SimulatedReceiver(object):
    def __init__(self, device_name, channels_metadata):
        self.device_name = device_name
        self.channels_metadata = channels_metadata

        _logger.info("Simulating device_name=%s with channels_metadata=%s", self.device_name, self.channels_metadata)

    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, traceback):
        pass

    def get_data(self):
        pulse_id = int(time())

        shape = [2048, 2048]
        type = "uint16"
        encoding = "little"
        compression = None

        pulse_id = pulse_id + 1

        raw_data = numpy.random.rand(*shape)
        raw_data *= 100
        raw_data = raw_data.astype(type).tobytes()

        data = (self.device_name, pulse_id % 10000000, channel_name, pulse_id, raw_data, type, shape, encoding, compression)

        return data