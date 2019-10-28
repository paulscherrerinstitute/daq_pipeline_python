import logging

_logger = logging.getLogger('BsreadReceiver')


class BsreadReceiver(object):

    def __init__(self, source_address, connection_mode, receive_timeout):
        self.source_address = source_address
        self.connection_mode = connection_mode
        self.receive_timeout = receive_timeout

        _logger.info("Instantiating with source_address=%s and connection_mode=$s with receive_timeout=%s",
                     self.source_address, self.connection_mode, self.receive_timeout)

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        _logger.debug("Connecting to bsread stream %s", self.source_address)
        pass

    def close(self):
        _logger.debug("Closing bsread stream %s.", self.source_address)
        pass

    def get_data(self):
        pass
