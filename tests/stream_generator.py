import argparse
import json
import logging
import os

import numpy
from bsread import Sender

_logger = logging.getLogger('start_store_pipeline')


def get_generator_function(type, shape):
    if type is None:
        type = "float64"

    if type == "string":
        return lambda x: "string"

    if type == "bool":
        return lambda x: True

    return lambda x: numpy.zeros(shape=shape, dtype=type)


def main():
    parser = argparse.ArgumentParser()

    device_name = os.getenv("DEVICE_NAME")
    parser.add_argument('--device_name', type=str, default=device_name,
                        help='Simulated device name')

    source_file = os.getenv("SOURCE_FILE", "/sources.json")
    parser.add_argument('--source_file', type=str, default=source_file,
                        help='Simulation sources file')

    port = os.getenv("PORT", "/sources.json")
    parser.add_argument('--port', type=int, default=port,
                        help='Simulation sources file')

    args = parser.parse_args()

    device_name = args.device_name
    source_file = args.source_file
    port = args.read_time

    with open(source_file, 'r') as input_file:
        sources = json.load(input_file)

    if device_name not in sources:
        raise ValueError("device_name=%s not found in sources file." % device_name)

    try:
        channels_metadata = sources[device_name]

        sender = Sender(port=port)

        for channel in channels_metadata:
            sender.add_channel({"name": channel["name"],
                                "function": get_generator_function(channel["type"], channel["shape"]),
                                "metadata": {"type": channel["type"],
                                             "shape": channel["shape"]}})

        sender.generate_stream()

    except KeyboardInterrupt:
        _logger.info('Generator %s interupted (SIGINT)', device_name)

    except Exception as e:
        _logger.exception('Generator %s stopped', device_name)


if __name__ == '__main__':
    main()
