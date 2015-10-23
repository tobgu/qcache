from collections import deque
import json


def encode_deque(obj):
    if isinstance(obj, deque):
        return list(obj)

    raise TypeError(repr(obj) + " is not JSON serializable")


class Statistics(object):
    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.reset()

    def inc(self, stat_name, count=1):
        if stat_name not in self.stats:
            self.stats[stat_name] = 0

        self.stats[stat_name] += count

    def append(self, stat_name, value):
        if stat_name not in self.stats:
            self.stats[stat_name] = deque(maxlen=self.buffer_size)

        self.stats[stat_name].append(value)

    def reset(self):
        self.stats = {}

    def to_json(self):
        return json.dumps(self.stats, default=encode_deque)