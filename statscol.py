"""
    counters
"""

class StatsCollector(object):

    def __init__(self, logger):
        self._stats = {}
        self.logger = logger

    def get_stats(self):
        return self._stats

    def set_stats(self, stats):
        self._stats = stats

    def clear_stats(self):
        self._stats.clear()

    def get_value(self, key, default=None):
        return self._stats.get(key, default)

    def set_value(self, key, value):
        self._stats[key] = value

    def inc_value(self, key, count=1, start=0):
        d = self._stats
        d[key] = d.setdefault(key, start) + count

    def max_value(self, key, value):
        self._stats[key] = max(self._stats.setdefault(key, value), value)

    def min_value(self, key, value):
        self._stats[key] = min(self._stats.setdefault(key, value), value)

    def _print(self):
        self.logger.info("spiderInfo: %s" % self._stats)

class MStatsCollector(object):

    def __init__(self, logger):
        self.mstats = {}
        self.logger = logger

    def persist_stats(self, id, stats):
        self.mstats[id] = stats

    def set_value(self, id, key, value):
        if id not in self.mstats:
            self.mstats[id] = StatsCollector(self.logger)
        self.mstats[id].set_value(key, value)

    def _print(self, id):
        if id not in self.mstats:
            return
        self.mstats[id]._print()

