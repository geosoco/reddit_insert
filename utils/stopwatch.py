"""
===============================================================================
# StopWatch
#
# A simple stopwatch class
#
#===============================================================================
"""

from timeit import default_timer as timer
from collections import OrderedDict


class StopWatch():
    """
    """

    def __init__(self, name = None):
        self.name = name
        self.start()

    def reset(self):
        self.end_time = None
        self.start_time = None


    def start(self):
        self.end_time = None
        self.start_time = timer()

    def stop(self):
        self.end_time = timer()

    def duration(self):
        if self.start_time is not None:
            end = self.end_time if self.end_time is not None else timer()
            return end - self.start_time
        else:
            return 0.0

    def __repr__(self):
        return "%s: %.4f" % (self.name if self.name is not None else "", self.duration())

    def __str__(self):
        return "%s: %.4f" % (self.name if self.name is not None else "", self.duration())




class StopWatchCollection():
    """
    """

    def __init__(self):
        self.dict = OrderedDict()


    def start(self, name):
        sw = StopWatch(name) if name not in self.dict else self.dict[name]
        sw.start()
        self.dict[name] = sw

    def stop(self, name):
        self.dict[name].stop()


    def get(self, name):
        return self.dict.get(name, None)


    def print_all(self):
        print("\n".join([str(v) for v in self.dict.values()]))

    def __repr__(self):
        return "\n".join([str(v) for v in self.dict.values()])
        



