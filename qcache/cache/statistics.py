from collections import deque
from typing import Iterable, Dict, Any
import time


class Statistics:
    stats: Dict[str, Any]

    def __init__(self, buffer_size: int) -> None:
        self.buffer_size = buffer_size
        self.reset()

    def inc(self, stat_name: str, count: int=1):
        if stat_name not in self.stats:
            self.stats[stat_name] = 0

        self.stats[stat_name] += count

    def append(self, stat_name: str, value: float):
        if stat_name not in self.stats:
            self.stats[stat_name] = deque(maxlen=self.buffer_size)

        self.stats[stat_name].append(value)

    def extend(self, stat_name: str, values: Iterable[float]):
        if stat_name not in self.stats:
            self.stats[stat_name] = deque(maxlen=self.buffer_size)

        self.stats[stat_name].extend(values)

    def reset(self, timestamp: float=None):
        if timestamp is None:
            timestamp = time.time()
        self.stats = {'since': timestamp,
                      'statistics_buffer_size': self.buffer_size}

    def snapshot(self) -> dict:
        """
        Create a statistics snapshot. This will reset the statistics.
        """
        snapshot = self.stats.copy()
        for k, v in snapshot.items():
            if isinstance(v, deque):
                snapshot[k] = list(v)

        timestamp = time.time()
        snapshot['statistics_duration'] = timestamp - snapshot['since']
        del snapshot['since']
        self.reset()
        return snapshot
