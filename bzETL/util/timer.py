import time
from util.debug import D

class Timer:

    def __init__(self, description):
        self.description=description

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.clock()
        self.interval = self.end - self.start
        D.println(self.description + " took %.03f sec"%self.interval)


        
    @property
    def duration(self):
        return self.interval