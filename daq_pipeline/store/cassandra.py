import logging
from collections import deque, defaultdict
from time import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session, TokenAwarePolicy
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import PreparedStatement, BatchStatement, BatchType

_logger = logging.getLogger('CassandraStore')

INSERT_STATEMENT = """
INSERT INTO bsread_data.channel_data
    (channel_name, pulse_id_mod, pulse_id, data, type, shape, encoding, compression)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?)
"""


class NoBatchSaveProvider(object):

    def __init__(self):
        self.future_cache = set()

    def save(self, session: Session, prep_insert_statement: PreparedStatement, data):

        if data is None:
            raise ValueError("Cannot save None data to Cassandra.")

        def success_insert(results, future, pulse_id, channel_name):
            self.future_cache.remove(future)

            _logger.debug("Inserted pulse_id=%s for channel_name=%s", pulse_id, channel_name)

        def failed_insert(e, future, pulse_id, channel_name):
            self.future_cache.remove(future)

            _logger.error("ERRRO IN %s. %s", channel_name, e)

        for pulse_data in data:
            future = session.execute_async(prep_insert_statement, pulse_data)
            future.add_callbacks(callback=success_insert, callback_args=(future, 1, pulse_data[0]),
                                 errback=failed_insert, errback_args=(future, 1, pulse_data[0]))

            self.future_cache.add(future)

        return len(self.future_cache)


class BatchSaveProvider(object):

    def __init__(self):
        self.future_cache = set()
        self.data_cache = defaultdict(list)
        self.cache_counter = 0

    def save(self, session: Session, prep_insert_statement: PreparedStatement, data):

        if data is None:
            raise ValueError("Cannot save None data to Cassandra.")

        for channel_data in data:
            channel_name = channel_data[0]
            self.data_cache[channel_name].append(channel_data)

        self.cache_counter += 1

        if self.cache_counter < 100:
            return len(self.future_cache)

        def success_insert(results, future, pulse_id, channel_name):
            self.future_cache.remove(future)

            _logger.debug("Inserted pulse_id=%s for channel_name=%s", pulse_id, channel_name)

        def failed_insert(e, future, pulse_id, channel_name):
            self.future_cache.remove(future)

            _logger.error("ERRRO IN %s. %s", channel_name, e)

        for channel_name, data in self.data_cache.items():
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            for pulse_data in data:
                batch.add(prep_insert_statement, pulse_data)

            future = session.execute_async(batch)
            future.add_callbacks(callback=success_insert, callback_args=(future, 1, channel_name),
                                 errback=failed_insert, errback_args=(future, 1, channel_name))

            self.future_cache.add(future)

        self.data_cache.clear()
        self.cache_counter = 0

        return len(self.future_cache)


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
        return self.save_provider.save(self.session,
                                       self.prep_insert_statement,
                                       data)
