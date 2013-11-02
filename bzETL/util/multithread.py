# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import threading
from .struct import nvl
from .logs import Log
from .threads import Queue, Thread


DEBUG = True

class worker_thread(threading.Thread):

    #in_queue MUST CONTAIN HASH OF PARAMETERS FOR load()
    def __init__(self, name, in_queue, out_queue, function):
        threading.Thread.__init__(self)
        self.name=name
        self.in_queue=in_queue
        self.out_queue=out_queue
        self.function=function
        self.keep_running=True
        self.num_runs=0
        self.start()

    #REQUIRED TO DETECT KEYBOARD, AND OTHER, INTERRUPTS
    def join(self, timeout=None):
        while self.isAlive():
            Log.note("Waiting on thread {{thread}}", {"thread":self.name})
            threading.Thread.join(self, nvl(timeout, 0.5))

    def run(self):
        got_stop=False
        while self.keep_running:
            request = self.in_queue.pop()
            if request == Thread.STOP:
                got_stop=True
                if self.in_queue.queue:
                    Log.warning("programmer error")
                break
            if not self.keep_running:
                break

            try:
                if DEBUG and hasattr(self.function, "func_name"):
                    Log.note("run {{function}}", {"function": self.function.func_name})
                result = self.function(**request)
                if self.out_queue:
                    self.out_queue.add({"response": result})
            except Exception, e:
                Log.warning("Can not execute with params={{params}}", {"params": request}, e)
                if self.out_queue:
                    self.out_queue.add({"exception": e})
            finally:
                self.num_runs += 1

        self.keep_running = False
        if self.num_runs==0:
            Log.warning("{{name}} thread did no work", {"name":self.name})
        if DEBUG and self.num_runs!=1:
            Log.note("{{name}} thread did {{num}} units of work", {
                "name":self.name,
                "num":self.num_runs
            })
        if got_stop and self.in_queue.queue:
            Log.warning("multithread programmer error")
        if DEBUG:
            Log.note("{{thread}} DONE", {"thread":self.name})


    def stop(self):
        self.keep_running=False








#PASS A SET OF FUNCTIONS TO BE EXECUTED (ONE PER THREAD)
#PASS AN (ITERATOR/LIST) OF PARAMETERS TO BE ISSUED TO NEXT AVAILABLE THREAD
class Multithread(object):


    def __init__(self, functions):
        self.outbound=Queue()
        self.inbound=Queue()

        #MAKE THREADS
        self.threads=[]
        for t, f in enumerate(functions):
            thread=worker_thread("worker "+unicode(t), self.inbound, self.outbound, f)
            self.threads.append(thread)



    def __enter__(self):
        return self

    #WAIT FOR ALL QUEUED WORK TO BE DONE BEFORE RETURNING
    def __exit__(self, type, value, traceback):
        try:
            if isinstance(value, Exception):
                self.inbound.close()
            self.inbound.add(Thread.STOP)
            self.join()
        except Exception, e:
            Log.warning("Problem sending stops", e)



    #IF YOU SENT A stop(), OR Thread.STOP, YOU MAY WAIT FOR SHUTDOWN
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
                t.keep_running=False
            self.inbound.close()
            self.outbound.close()
            for t in self.threads:
                t.join()


    #RETURN A GENERATOR THAT HAS len(parameters) RESULTS (ANY ORDER)
    def execute(self, request):
        #FILL QUEUE WITH WORK
        self.inbound.extend(request)

        num=len(request)
        def output():
            for i in xrange(num):
                result=self.outbound.pop()
                if "exception" in result:
                    raise result["exception"]
                else:
                    yield result["response"]
        return output()

    #EXTERNAL COMMAND THAT RETURNS IMMEDIATELY
    def stop(self):
        self.inbound.close() #SEND STOPS TO WAKE UP THE WORKERS WAITING ON inbound.pop()
        for t in self.threads:
            t.keep_running=False



