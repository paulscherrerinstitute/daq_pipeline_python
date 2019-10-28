import logging

_logger = logging.getLogger('CassandraStore')


class CassandraStore(object):

    def __init__(self, nodes_addresses, batch_size):
        self.nodes_addresses = nodes_addresses
        self.batch_size = batch_size

        _logger.info("Instantiating with nodes_addresses=%s and batch_size=%s",
                     self.nodes_addresses, self.batch_size)

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        _logger.debug("Connecting to %s", self.nodes_addresses)
        pass

    def close(self):
        _logger.debug("Disconnect from Cassandra")
        pass

    def save(data):
        pass