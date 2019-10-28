import logging

_logger = logging.getLogger('store_pipeline')


def store_pipeline(data_receiver,
                   data_store,
                   metadata_sender,
                   stats_sender):

    with data_receiver, data_store, metadata_sender, stats_sender:

        while True:

            data, metadata = data_receiver.get_data()

            # In case of receive timeout, data and metadata is None.
            if data is None:
                data_store.save(data)
                metadata_sender.add(metadata)

            stats_sender.add(data, metadata)
