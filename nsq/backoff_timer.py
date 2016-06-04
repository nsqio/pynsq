from __future__ import absolute_import

from decimal import Decimal


def _Decimal(v):
    if not isinstance(v, Decimal):
        return Decimal(str(v))
    return v


class BackoffTimer(object):
    """
    This is a timer that is smart about backing off exponentially when there are problems
    """
    def __init__(self, min_interval, max_interval, ratio=.25, short_length=10, long_length=250):
        assert isinstance(min_interval, (int, float, Decimal))
        assert isinstance(max_interval, (int, float, Decimal))

        self.min_interval = _Decimal(min_interval)
        self.max_interval = _Decimal(max_interval)

        self.max_short_timer = (self.max_interval - self.min_interval) * _Decimal(ratio)
        self.max_long_timer = (self.max_interval - self.min_interval) * (1 - _Decimal(ratio))
        self.short_unit = self.max_short_timer / _Decimal(short_length)
        self.long_unit = self.max_long_timer / _Decimal(long_length)

        self.short_interval = Decimal(0)
        self.long_interval = Decimal(0)
        self.update_interval()

    def success(self):
        """Update the timer to reflect a successfull call"""
        if self.interval == 0.0:
            return
        self.short_interval -= self.short_unit
        self.long_interval -= self.long_unit
        self.short_interval = max(self.short_interval, Decimal(0))
        self.long_interval = max(self.long_interval, Decimal(0))
        self.update_interval()

    def failure(self):
        """Update the timer to reflect a failed call"""
        self.short_interval += self.short_unit
        self.long_interval += self.long_unit
        self.short_interval = min(self.short_interval, self.max_short_timer)
        self.long_interval = min(self.long_interval, self.max_long_timer)
        self.update_interval()

    def update_interval(self):
        self.interval = float(self.min_interval + self.short_interval + self.long_interval)

    def get_interval(self):
        return self.interval
