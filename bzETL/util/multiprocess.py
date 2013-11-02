# encoding: utf-8
#
from multiprocessing.queues import Queue
from .logs import Log


class worker(object):
    def __init__(func, inbound, outbound, logging):
        logger = Log_usingInterProcessQueue(logging)



class Log_usingInterProcessQueue(Log):
    def __init__(self, outbound):
        self.outbound = outbound

    def write(self, template, params):
        self.outbound.put({"template": template, "param": params})


class Multiprocess(object):
    def __init__(self, functions):
        self.outbound = Queue()
        self.inbound = Queue()
        self.inbound = Queue()

        #MAKE

        #MAKE THREADS
        self.threads = []
        for t, f in enumerate(functions):
            thread = worker(
                "worker " + unicode(t),
                f,
                self.inbound,
                self.outbound,
            )
            self.threads.append(thread)


    def __enter__(self):
        return self

    #WAIT FOR ALL QUEUED WORK TO BE DONE BEFORE RETURNING
    def __exit__(self, a, b, c):
        try:
            self.inbound.close() # SEND STOPS TO WAKE UP THE WORKERS WAITING ON inbound.pop()
        except Exception, e:
            Log.warning("Problem adding to inbound", e)

        self.join()


    #IF YOU SENT A stop(), OR STOP, YOU MAY WAIT FOR SHUTDOWN
    def join(self):
        try:
            #WAIT FOR FINISH
            for t in self.threads:
                t.join()
        except (KeyboardInterrupt, SystemExit):
            Log.note("Shutdow Started, please be patient")
        except Exception, e:
            Log.error("Unusual shutdown!", e)
        finally:
            for t in self.threads:
                t.keep_running = False
            for t in self.threads:
                t.join()
            self.inbound.close()
            self.outbound.close()


    #RETURN A GENERATOR THAT HAS len(parameters) RESULTS (ANY ORDER)
    def execute(self, parameters):
        #FILL QUEUE WITH WORK
        self.inbound.extend(parameters)

        num = len(parameters)

        def output():
            for i in xrange(num):
                result = self.outbound.pop()
                yield result

        return output()

    #EXTERNAL COMMAND THAT RETURNS IMMEDIATELY
    def stop(self):
        self.inbound.close() #SEND STOPS TO WAKE UP THE WORKERS WAITING ON inbound.pop()
        for t in self.threads:
            t.keep_running = False




