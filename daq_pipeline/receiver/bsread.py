import logging

_logger = logging.getLogger('BsreadReceiver')

class BsreadReceiver(object):

    def __init__(self, source_address, connection_mode, receive_timeout):
        self.source_address = source_address
        self.connection_mode = connection_mode
        self.receive_timeout = receive_timeout

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def get_data(self):
        from time import sleep
        sleep(1)

        return 12345, {'meta':'data'}
