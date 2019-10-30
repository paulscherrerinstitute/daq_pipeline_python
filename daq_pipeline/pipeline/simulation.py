import logging
from time import time, sleep

_logger = logging.getLogger('simulated_pipeline')


def simulated_pipeline(data_receiver,
                       data_store,
                       metadata_sender,
                       stats_sender,
                       target_iteration_time):

    with data_receiver, data_store, metadata_sender, stats_sender:

        iter_counter = 1
        start_time = None

        _logger.debug("Starting pipeline")

        while True:

            if iter_counter == 1:
                start_time = time()

            data, metadata = data_receiver.get_data()
            _logger.debug("Received pulse_id=%s", metadata["pulse_id"])

            # In case of receive timeout, data and metadata is None.
            if data is not None:
                data_store.save(data)
                metadata_sender.add(metadata)

            stats_sender.add(data, metadata)

            delta = (time() - start_time) - (target_iteration_time * iter_counter)

            iter_counter %= 100
            iter_counter += 1

            if delta < 0:
                sleep(-delta)
