import unittest

from daq_pipeline.stats.logstash import LogstashStats


class StatsLogstashTest(unittest.TestCase):

    def test_logstash_statistics_calulcation(self):
        stash = LogstashStats(None, None)

        stats = {
            "stat1": 10,
            "stat2": 20,
            "stat3": 30,
            "iteration": 0
        }

        for i in range(10):
            stash._add_stats_to_cache(stats)

        stats = stash._get_stats_from_cache(2)

        self.assertEqual(stats["stat1"], 10.0)
        self.assertEqual(stats["stat2"], 20.0)
        self.assertEqual(stats["stat3"], 30.0)
        self.assertEqual(stats["rep_rate"], 5)

        self.assertDictEqual({}, stash._get_stats_from_cache())
        self.assertDictEqual({}, stash._get_stats_from_cache(1))
