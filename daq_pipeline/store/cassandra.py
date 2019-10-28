import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

_logger = logging.getLogger('CassandraStore')


INSERT_STATEMENT = """
INSERT INTO bsread_data.channel_data
    (device_name, pulse_id_mod, channel_name, pulse_id, data, type, shape, encoding, compression)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class CassandraStore(object):

    def __init__(self, nodes_addresses, batch_size):
        self.nodes_addresses = nodes_addresses
        self.batch_size = batch_size

        _logger.info("Instantiating with nodes_addresses=%s and batch_size=%s",
                     self.nodes_addresses, self.batch_size)

        _logger.debug("Using INSERT_STATEMENT: %s", INSERT_STATEMENT)

        self.cluster = None
        self.session = None
        self.prep_insert_statement = None

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        _logger.debug("Connecting to %s", self.nodes_addresses)

        self.cluster = Cluster(self.nodes_addresses)
        self.session = self.cluster.connect()

        self.prep_insert_statement = self.session.prepare(INSERT_STATEMENT)
        self.prep_insert_statement.consistency_level = ConsistencyLevel.ANY

    def close(self):
        _logger.debug("Disconnect from Cassandra")

        if self.session:
            self.session.shutdown()

        if self.cluster:
            self.cluster.shutdown()

        self.cluster = None
        self.session = None
        self.prep_insert_statement = None

    def save(data):
        pass
