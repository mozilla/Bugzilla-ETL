from dzAlerts.util.logs import Log
from dzAlerts.util.multiset import multiset
from dzAlerts.util.stats import Z_moment, stats2z_moment, z_moment2stats


class AggregationFunction(object):
    def __init__(self):
        """
        RETURN A ZERO-STATE AGGREGATE
        """
        Log.error("not implemented yet")

    def add(self, value):
        """
        ADD value TO AGGREGATE
        """
        Log.error("not implemented yet")


    def merge(self, agg):
        """
        ADD TWO AGGREGATES TOGETHER
        """
        Log.error("not implemented yet")

    def end(self):
        """
        RETURN AGGREGATE
        """




class WindowFunction(AggregationFunction):

    def __init__(self):
        """
        RETURN A ZERO-STATE AGGREGATE
        """
        Log.error("not implemented yet")


    def sub(self, value):
        """
        REMOVE value FROM AGGREGATE
        """
        Log.error("not implemented yet")




class Stats(WindowFunction):

    def __init__(self):
        self.total=Z_moment(0,0,0)


    def add(self, value):
        if value is None:
            return
        self.total+=stats2z_moment(value)

    def sub(self, value):
        if value is None:
            return
        self.total-=stats2z_moment(value)

    def merge(self, agg):
        self.total+=agg.total

    def end(self):
        return z_moment2stats(self.total)



class Min(WindowFunction):

    def __init__(self):
        self.total=multiset()


    def add(self, value):
        if value is None:
            return
        self.total.add(value)

    def sub(self, value):
        if value is None:
            return
        self.total.remove(value)


    def end(self):
        return min(self.total)
