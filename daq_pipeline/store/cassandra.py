import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session, TokenAwarePolicy
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import PreparedStatement, BatchStatement

_logger = logging.getLogger('CassandraStore')


INSERT_STATEMENT = """
INSERT INTO bsread_data.channel_data
    (device_name, pulse_id_mod, channel_name, pulse_id, data, type, shape, encoding, compression)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class NoBatchSaveProvider(object):
    def save(self, session: Session, prep_insert_statement: PreparedStatement, data):
        batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)

        for channel_data in data:
            batch.add(prep_insert_statement, channel_data)

        session.execute(batch)


class BatchSaveProvider(object):
    def __init__(self, batch_size):
        self.batch_size = batch_size

    def save(self, session, prep_insert_statement, data):
        raise NotImplementedError()

        # batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
        # batch.add(prep_insert_statement, data)
        # session.execute(batch)


class CassandraStore(object):

    def __init__(self, nodes_addresses, batch_size):
        self.nodes_addresses = nodes_addresses
        self.batch_size = batch_size

        _logger.info("Instantiating with nodes_addresses=%s and batch_size=%s",
                     self.nodes_addresses, self.batch_size)

        _logger.debug("Using INSERT_STATEMENT: %s", INSERT_STATEMENT)

        if self.batch_size < 2:
            self.save_provider = NoBatchSaveProvider()
        else:
            self.save_provider = BatchSaveProvider(self.batch_size)

        self.cluster = None
        self.session = None
        self.prep_insert_statement = None

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        _logger.debug("Connecting to %s", self.nodes_addresses)

        load_balancing_policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())
        self.cluster = Cluster(self.nodes_addresses,
                               load_balancing_policy=load_balancing_policy)
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

    def save(self, data):
        _logger.debug("Saving data to cassandra")
        self.save_provider.save(self.session,
                                self.prep_insert_statement,
                                data)
