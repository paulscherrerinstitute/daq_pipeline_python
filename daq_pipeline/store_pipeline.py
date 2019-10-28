import cassandra
import logging
import argparse

_logger = logging.getLogger('store_pipeline')

# Just above the minimum normal operational frequency.
DEFAULT_RECEIVE_TIMEOUT = 1.1
CASSANDRA_CLUSTER_ADDRESSES = ['sf-nube-12', 'sf-nube-13', 'sf-nube-14']
MEMCACHED_ADDRESS = 'sf-nube-11'
LOGSTASH_ADDRESS = 'http://logstash.psi.ch'

DEFAULT_ZMQ_MODE = 'pull'
DEFAULT_BATCH_SIZE = 1
DEFAULT_METADATA_SEND_MODULO = 100
# Interval in seconds.
DEFAULT_STATS_SEND_INTERVAL = 5


class BsreadReceiver(object):
    _logger = logging.getLogger('BsreadReceiver')

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


class CassandraStore(object):
    _logger = logging.getLogger('CassandraStore')

    def __init__(self, nodes_addresses, batch_size):
        self.nodes_addresses = nodes_addresses
        self.batch_size = batch_size

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def save(data):
        print(data)


class MemcachedMetadataSender(object):
    _logger = logging.getLogger('MemcachedMetadataSender')

    def __init__(self, memcached_address, metadata_send_modulo):
        self.memcached_address = memcached_address
        self.metadata_send_modulo = metadata_send_modulo

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()
    
    def connect(self):
        pass

    def close(self):
        pass

    def add(self, metadata):
        pass


class LogstashStatsSender(object):
    _logger = logging.getLogger('LogstashStatsSender')

    def __init__(self, logstash_address, stats_send_interval): 
        self.logstash_address = logstash_address
        self.stats_send_interval = stats_send_interval

    def __enter__(self):
        self.connect()

    def __exit__(self, ex_type, ex_value, traceback):
        self.close()

    def connect(self):
        pass

    def close(self):
        pass

    def add(self, data, metadata):
        pass

def store_pipeline(data_receiver, 
                   data_store, 
                   metadata_sender,
                   stats_sender):

    with data_receiver, data_store, metadata_sender, stats_sender:
        while True:

            data, metadata = data_receiver.get_data() 

            # In case of receive timeout, data and metadata is None.
            if data is None:
                cassandra_store.save(data)
                metadata_sender.add(metadata)

            stats_sender.add(data, metadata)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('source', type=str, 
                        help='Stream address in format "tcp://host:port".')

    parser.add_argument(
        '--mode', choices=['pull', 'sub'], 
        default=DEFAULT_ZMQ_MODE,
        help='DEFAULT=%s : ZMQ connection mode.' % DEFAULT_ZMQ_MODE)

    parser.add_argument(
        '--batch_size', type=int, 
        default=DEFAULT_BATCH_SIZE,
        help='DEFAULT=%s (in pulse_id): ' % DEFAULT_BATCH_SIZE +
        'N pulse_ids to batch for each insert.'
    )

    parser.add_argument(
        '--metadata_send_modulo', type=int, 
        default=DEFAULT_METADATA_SEND_MODULO, 
        help='DEFAULT=%s (in pulse_id) : ' % DEFAULT_METADATA_SEND_MODULO +
        'Send metadata update every N pulse_ids.')

    parser.add_argument(
        '--stats_send_interval', type=int,
        default=DEFAULT_STATS_SEND_INTERVAL, 
        help='DEFAULT=%s (in seconds) : ' % DEFAULT_STATS_SEND_INTERVAL + 
             'Send statistics every N seconds.'
    )

    args = parser.parse_args()

    source_address = args.source
    connection_mode = args.mode
    batch_size = args.batch_size
    metadata_send_modulo = args.metadata_send_modulo
    stats_send_interval = args.stats_send_interval

    _logger.info('Starting store_pipeline with arguments: %s', args)
    
    try:

        receive_timeout = DEFAULT_RECEIVE_TIMEOUT
        _logger.debug('receive_timeout=%s', receive_timeout)
        bsread_receiver = BsreadReceiver(source_address,
                                         connection_mode, 
                                         receive_timeout)
      
        # How many channel data inserts to batch in a single write.
        nodes_addresses = CASSANDRA_CLUSTER_ADDRESSES
        _logger.debug('nodes_addresses=%s', nodes_addresses)
        cassandra_store = CassandraStore(nodes_addresses, batch_size)

        # Metadata will be sent when pulse_id % metadata_send_modulo == 0.
        memcached_address = MEMCACHED_ADDRESS 
        _logger.debug('memcached_address=%s', memcached_address)
        memcached_meta_sender = MemcachedMetadataSender(memcached_address, 
                                                        metadata_send_modulo)
     
        # Send interval is in seconds.
        logstash_address = LOGSTASH_ADDRESS
        _logger.debug('logstash_address=%s', logstash_address)
        logstash_stats_sender = LogstashStatsSender(logstash_address, 
                                                    stats_send_interval)

        store_pipeline(data_receiver=bsread_receiver,
                       data_store=cassandra_store,
                       metadata_sender=memcached_meta_sender,
                       stats_sender=logstash_stats_sender)
    
    except KeyboardInterrupt:
        _logger.info('Store pipeline %s interupted (SIGINT)', source_address)

    except Exception as e:
        _logger.exception('Store pipeline %s stopped', source_address)


if __name__ == '__main__':
    main()
