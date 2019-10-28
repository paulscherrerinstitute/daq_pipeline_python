import argparse
import random
import string
from time import time

import numpy
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cqlengine.query import BatchQuery
from cassandra.query import BatchStatement

INSERT_STATEMENT = """
INSERT INTO channel_data
    (device_name, pulse_id_mod, channel_name, pulse_id, data, type, shape, encoding, compression)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def start(device_name, batch_modulo):

    cluster = Cluster(['172.26.120.72', '172.26.120.73', '172.26.120.74'])
    session = cluster.connect('bsread_data')

    insert_statement = session.prepare(INSERT_STATEMENT)
    insert_statement.consistency_level = ConsistencyLevel.ANY

    channel_name = device_name + ":" + ''.join(random.sample(string.ascii_lowercase, 10))
    pulse_id = int(time())

    shape = [2048, 2048]
    type = "uint16"
    encoding = "little"
    compression = None

    total_time = 0
    max_time = float("-inf")
    min_time = float("inf")

    batch = BatchQuery()
    insert_modulo = 1
    start_time = time()

    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
    insert_batch_index = 0

    while True:

        insert_batch_index += 1
        insert_batch_index %= batch_modulo

        data = None

        if batch_modulo == 1:
            session.execute(insert_statement, None)
            continue


        if i % insert_modulo == 0:
            delta_time = time() - start_time
            session.execute(batch)

            total_time += delta_time

            if i > 10:
                max_time = max(max_time, delta_time)
                min_time = min(min_time, delta_time)

            print("Iteration %s, average insert time %s, total %s, min %s, max %s" %
                  (i, total_time / i, total_time, min_time, max_time))

            start_time = time()


        pulse_id = pulse_id + 1

        raw_data = numpy.random.rand(*shape)
        raw_data *= 100
        raw_data = raw_data.astype(type).tobytes()

        data = (device_name, pulse_id % 10000000, channel_name, pulse_id, raw_data, type, shape, encoding, compression)
        batch.add(insert_statement, data)



def main():
    parser = argparse.ArgumentParser(description='SF DAQ simulated store pipeline')
    parser.add_argument('-d', '--device', default=''.join(random.sample(string.ascii_lowercase, 20)),
                        help="Name of the device.")
    parser.add_argument('-r', '--rate', default=100, help="Simulated stream rate (in Hz).")
    parser.add_argument('-b', '--batch', default=100, help="How many inserts to batch together.")
    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")
    arguments = parser.parse_args()

    start()


if __name__ == "__main__":
    main()
