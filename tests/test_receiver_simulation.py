import unittest
from time import time

from daq_pipeline.receiver.simulation import SimulatedReceiver


class SimulatedReceiverTest(unittest.TestCase):
    def test_generated_data(self):
        device_name = "simulated_device"
        target_read_time = 0.01

        channels_metadata = [
            {"name": "scalar_1", "shape": [1], "type": "uint32", "encoding": "little", "compression": None},
            {"name": "scalar_2", "shape": [1], "type": "uint64", "encoding": "big", "compression": "Whateves"},
            {"name": "array_1", "shape": [10], "type": "uint16", "encoding": "big", "compression": None},
            {"name": "array_2", "shape": [10], "type": "uint32", "encoding": "big", "compression": None},
        ]

        receiver = SimulatedReceiver(device_name, channels_metadata, target_read_time)

        start_time = time()

        n_iterations = 100
        for _ in range(n_iterations):
            data = receiver.get_data()

        delta = (time() - start_time) - (n_iterations * target_read_time)
        self.assertTrue(delta < 0.01, "We are loosing more then 1 pulse / second @ 100Hz.")

        pulse_id = None
        modulo_id = None

        for i_channel in range(len(channels_metadata)):
            if pulse_id is None:
                pulse_id = data[i_channel][3]
                modulo_id = data[i_channel][1]

            self.assertEqual(device_name, data[i_channel][0])
            self.assertEqual(modulo_id, data[i_channel][1])
            self.assertEqual(channels_metadata[i_channel]["name"], data[i_channel][2], "channel name missmatch")
            self.assertEqual(pulse_id, data[i_channel][3])
            self.assertEqual(channels_metadata[i_channel]["type"], data[i_channel][5], "channel name missmatch")
            self.assertEqual(channels_metadata[i_channel]["shape"], data[i_channel][6], "channel name missmatch")
            self.assertEqual(channels_metadata[i_channel]["encoding"], data[i_channel][7], "channel name missmatch")
            self.assertEqual(channels_metadata[i_channel]["compression"], data[i_channel][8], "channel name missmatch")

            channel_data = data[i_channel][4]
            self.assertTrue(isinstance(channel_data, bytes))
