import logging
import argparse
import os

from daq_pipeline import config
from daq_pipeline.metadata.memcached import MemcachedMetadata
from daq_pipeline.pipeline.store import store_pipeline
from daq_pipeline.receiver.bsread import BsreadReceiver
from daq_pipeline.stats.logstash import LogstashStats
from daq_pipeline.store.cassandra import CassandraStore


_logger = logging.getLogger('start_store_pipeline')


def main():
    parser = argparse.ArgumentParser()

    source = os.getenv("SOURCE", config.DEFAULT_SOURCE)
    parser.add_argument('--source', type=str,
                        default=source,
                        help='DEFAULT=%s: ' % source +
                             'Stream address in format "tcp://host:port"')

    zmq_mode = os.getenv("ZMQ_MODE", config.DEFAULT_ZMQ_MODE)
    parser.add_argument(
        '--mode', choices=['pull', 'sub'],
        default=zmq_mode,
        help='DEFAULT=%s : ZMQ connection mode.' % zmq_mode)

    batch_size = os.getenv("BATCH_SIZE", config.DEFAULT_BATCH_SIZE)
    parser.add_argument(
        '--batch_size', type=int,
        default=batch_size,
        help='DEFAULT=%s (in pulse_id): ' % batch_size +
             'N pulse_ids to batch for each insert.'
    )

    meta_send_modulo = os.getenv("METADATA_SEND_MODULO", config.DEFAULT_METADATA_SEND_MODULO)
    parser.add_argument(
        '--metadata_send_modulo', type=int,
        default=meta_send_modulo,
        help='DEFAULT=%s (in pulse_id) : ' % meta_send_modulo +
             'Send metadata update every N pulse_ids.')

    stats_send_interval = os.getenv("STATS_SEND_INTERVAL", config.DEFAULT_STATS_SEND_INTERVAL)
    parser.add_argument(
        '--stats_send_interval', type=int,
        default=stats_send_interval,
        help='DEFAULT=%s (in seconds) : ' % stats_send_interval +
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

        receive_timeout = config.DEFAULT_RECEIVE_TIMEOUT
        _logger.debug('receive_timeout=%s', receive_timeout)
        bsread_receiver = BsreadReceiver(source_address,
                                         connection_mode,
                                         receive_timeout)

        # How many channel data inserts to batch in a single write.
        nodes_addresses = config.CASSANDRA_CLUSTER_ADDRESSES
        _logger.debug('nodes_addresses=%s', nodes_addresses)
        cassandra_store = CassandraStore(nodes_addresses, batch_size)

        # Metadata will be sent when pulse_id % metadata_send_modulo == 0.
        memcached_address = config.MEMCACHED_ADDRESS
        _logger.debug('memcached_address=%s', memcached_address)
        memcached_meta_sender = MemcachedMetadata(memcached_address,
                                                  metadata_send_modulo)

        # Send interval is in seconds.
        logstash_address = config.LOGSTASH_ADDRESS
        _logger.debug('logstash_address=%s', logstash_address)
        logstash_stats_sender = LogstashStats(logstash_address,
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
