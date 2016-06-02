from __future__ import absolute_import

from decimal import Decimal


class BackoffTimer(object):
    """
    This is a timer that is smart about backing off exponentially when there are problems
    
    Internally, the intervals are tracked as integers so that calculation
    is less expensive.
    """
    def __init__(self, min_interval, max_interval, ratio=.25, short_length=10, long_length=250):
        assert isinstance(min_interval, (int, float, Decimal))
        assert isinstance(max_interval, (int, float, Decimal))

        self.min_interval = int(float(min_interval) * 1000000)
        self.max_interval = int(float(max_interval) * 1000000)

        self.max_short_timer = int((self.max_interval - self.min_interval) * float(ratio))
        self.max_long_timer = int((self.max_interval - self.min_interval) * (1 - float(ratio)))
        self.short_unit = int(self.max_short_timer / float(short_length))
        self.long_unit = int(self.max_long_timer / float(long_length))

        self.short_interval = 0
        self.long_interval = 0

    def success(self):
        """Update the timer to reflect a successfull call"""
        self.short_interval = max(self.short_interval - self.short_unit, 0)
        self.long_interval = max(self.long_interval - self.long_unit, 0)

    def failure(self):
        """Update the timer to reflect a failed call"""
        self.short_interval = min(self.short_interval + self.short_unit, self.max_short_timer)
        self.long_interval = min(self.long_interval + self.long_unit, self.max_long_timer)

    def get_interval(self):
        return (self.min_interval + self.short_interval + self.long_interval) / 1000000.0
