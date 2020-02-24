CASSANDRA_CLUSTER_ADDRESSES = ['sf-nube-12', 'sf-nube-13']
MEMCACHED_ADDRESS = 'sf-nube-11'
LOGSTASH_ADDRESS = 'http://logstash.psi.ch'

# Just above the minimum normal operational frequency.
DEFAULT_RECEIVE_TIMEOUT = 1.1

DEFAULT_SOURCE = "tcp://localhost:8888"
DEFAULT_ZMQ_MODE = 'pull'
DEFAULT_BATCH_SIZE = 1
DEFAULT_METADATA_SEND_MODULO = 100
# Interval in seconds.
DEFAULT_STATS_SEND_INTERVAL = 5

CHANNEL_DATA_PARTITION_MODULO = 1000000
