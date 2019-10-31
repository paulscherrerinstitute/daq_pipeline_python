import logging
from time import time, sleep

from daq_pipeline.stats.utils import start_timer, get_timer_delta

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

        # Some sensible initial values not to screw with the statistics calculation.
        stats_iteration_time = 0.01
        stats_save_time = 0.01
        stats_add_metadata_time = 0.01

        while True:

            start_timer("iteration")

            if iter_counter == 1:
                start_time = time()

            start_timer("get_data")
            data, metadata = data_receiver.get_data()
            stats_get_data_time = get_timer_delta("get_data")

            _logger.debug("Received pulse_id=%s", metadata["pulse_id"])

            # In case of receive timeout, data and metadata is None.
            if data is not None:
                start_time("save")
                data_store.save(data)
                stats_save_time = get_timer_delta("save")

                start_time("add_metadata")
                metadata_sender.add(metadata)
                stats_add_metadata_time = get_timer_delta("add_metadata")

            stats_sender.add({
                "iteration": stats_iteration_time,
                "get_data": stats_get_data_time,
                "save": stats_save_time,
                "add_metadata": stats_add_metadata_time,
            })

            delta = (time() - start_time) - (target_iteration_time * iter_counter)

            iter_counter %= 100
            iter_counter += 1

            stats_iteration_time = get_timer_delta("iteration")

            if delta < 0:
                sleep(-delta)
