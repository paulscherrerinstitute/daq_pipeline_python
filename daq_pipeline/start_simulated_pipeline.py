import json
import logging
import argparse
import os

from daq_pipeline import config
from daq_pipeline.metadata.memcached import MemcachedMetadata
from daq_pipeline.pipeline.simulation import simulated_pipeline
from daq_pipeline.receiver.simulation import SimulatedReceiver
from daq_pipeline.stats.logstash import LogstashStats
from daq_pipeline.store.cassandra import CassandraStore

_logger = logging.getLogger('start_store_pipeline')


def main():
    parser = argparse.ArgumentParser()

    device_name = os.getenv("DEVICE_NAME")
    parser.add_argument('--device_name', type=str, default=device_name,
                        help='Simulated device name')

    source_file = os.getenv("SOURCE_FILE", "/sources.json")
    parser.add_argument('--source_file', type=str, default=source_file,
                        help='Simulation sources file')

    read_time = os.getenv("READ_TIME", 0.001)
    parser.add_argument('--read_time', type=float, default=read_time,
                        help='Simulation sources file')

    iteration_time = os.getenv("ITERATION_TIME", 0.01)
    parser.add_argument('--iteration_time', type=float, default=iteration_time,
                        help='Simulation sources file')

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

    device_name = args.device_name
    source_file = args.source_file
    read_time = args.read_time
    iteration_time = args.iteration_time

    batch_size = args.batch_size
    metadata_send_modulo = args.metadata_send_modulo
    stats_send_interval = args.stats_send_interval

    logging.getLogger("MemcachedMetadata").setLevel(logging.DEBUG)
    logging.getLogger("CassandraStore").setLevel(logging.DEBUG)

    _logger.info('Starting simulated_pipeline with arguments: %s', args)

    try:
        with open(source_file, 'r') as input_file:
            sources = json.load(input_file)

        if device_name not in sources:
            raise ValueError("device_name=%s not found in sources file." % device_name)

        channels_metadata = sources[device_name]
        simulated_receiver = SimulatedReceiver(device_name, channels_metadata, read_time)

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

        simulated_pipeline(data_receiver=simulated_receiver,
                           data_store=cassandra_store,
                           metadata_sender=memcached_meta_sender,
                           stats_sender=logstash_stats_sender,
                           target_iteration_time=iteration_time)

    except KeyboardInterrupt:
        _logger.info('Simulated pipeline %s interupted (SIGINT)', device_name)

    except Exception as e:
        _logger.exception('Simulated pipeline %s stopped', device_name)


if __name__ == '__main__':
    main()
