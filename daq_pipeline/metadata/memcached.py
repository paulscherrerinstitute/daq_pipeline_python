import logging

_logger = logging.getLogger('MemcachedMetadata')


class MemcachedMetadata(object):

    def __init__(self, memcached_address, metadata_send_modulo):
        self.memcached_address = memcached_address
        self.metadata_send_modulo = metadata_send_modulo

        _logger.info("memcached_address=%s, metadata_send_modulo=%s",
                     self.memcached_address, self.metadata_send_modulo)

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def add(self, metadata):
        _logger.debug("Adding metadata=%s", metadata)
