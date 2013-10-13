################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



import threading
import thread
from bzETL.util.struct import Null


class Lock():
    """
    SIMPLE LOCK (ACTUALLY, A PYTHON threadind.Condition() WITH notify() BEFORE EVERY RELEASE)
    """
    def __init__(self, name=""):
        self.monitor=threading.Condition()
        self.name=name

    def __enter__(self):
        self.monitor.acquire()
        return self

    def __exit__(self, a, b, c):
        self.monitor.notify()
        self.monitor.release()

    def wait(self, timeout=None):
        self.monitor.wait(timeout=timeout)


# SIMPLE MESSAGE QUEUE, multiprocessing.Queue REQUIRES SERIALIZATION, WHICH IS HARD TO USE JUST BETWEEN THREADS
class Queue():
    def __init__(self):
        self.keep_running=True
        self.lock=Lock("lock for queue")
        self.queue=[]

    def __iter__(self):
        while self.keep_running:
            try:
                value=self.queue.pop(0)
                if value==Thread.STOP:  #SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                    self.keep_running=False
                    raise StopIteration()
                yield value
            except StopIteration:
                pass
            except Exception, e:
                from .logs import Log
                Log.warning("Tell me about what happends here", e)

    def add(self, value):
        with self.lock:
            if self.keep_running:
                self.queue.append(value)
        return self

    def extend(self, values):
        with self.lock:
            if self.keep_running:
                self.queue.extend(values)

    def pop(self):
        with self.lock:
            while self.keep_running:
                if len(self.queue)>0:
                    value=self.queue.pop(0)
                    if value==Thread.STOP:  #SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                        self.keep_running=False
                    return value
                self.lock.wait()
            return Thread.STOP

    def close(self):
        with self.lock:
            self.keep_running=False



class AllThread():
    """
    RUN ALL ADDED FUNCTIONS IN PARALLEL, BE SURE TO HAVE JOINED BEFORE EXIT
    """

    def __init__(self):
        self.threads=[]

    def __enter__(self):
        return self

    #WAIT FOR ALL QUEUED WORK TO BE DONE BEFORE RETURNING
    def __exit__(self, type, value, traceback):
        self.join()

    def join(self):
        exceptions=[]
        try:
            for t in self.threads:
                response=t.join()
                if "exception" in response:
                    exceptions.append(response["exception"])
        except Exception, e:
            from .logs import Log
            Log.warning("Problem joining", e)

        if len(exceptions)>0:
            from .logs import Log
            Log.error("Problem in child threads", exceptions)



    def add(self, target, *args, **kwargs):
        """
        target IS THE FUNCTION TO EXECUTE IN THE THREAD
        """
        t=Thread.run(target, *args, **kwargs)
        self.threads.append(t)





class Thread():
    """
    join() ENHANCED TO ALLOW CAPTURE OF CTRL-C, AND RETURN POSSIBLE THREAD EXCEPTIONS
    run() ENHANCED TO CAPTURE EXCEPTIONS

    """
    def __init__(self, name, target, *args, **kwargs):
        self.name = name
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.response = Null
        self.synch_lock=Lock()

    def start(self):
        try:
            self.thread=thread.start_new_thread(Thread._run, (self, ))
        except Exception, e:
            from .logs import Log
            Log.error("Can not start thread", e)

    def _run(self):
        try:
            if self.target is not None:
                response=self.target(*self.args, **self.kwargs)
                with self.synch_lock:
                    self.response={"response":response}
        except Exception, e:
            with self.synch_lock:
                self.response={"exception":e}
        finally:
            del self.target, self.args, self.kwargs

    def is_alive(self):
        return self.response == Null


    def join(self, timeout=None):
        """
        RETURN THE RESULT OF THE THREAD EXECUTION (INCLUDING EXCEPTION)
        """
        if timeout is None:
            while True:
                with self.synch_lock:
                    if not self.is_alive():
                        break
                    self.synch_lock.wait(0.5)
                from .logs import Log
                Log.note("Waiting on thread {{thread}}", {"thread":self.name})
            return self.response
        else:
            with self.synch_lock:
                if self.is_alive():
                    self.synch_lock.wait(timeout)
            return self.response

    @staticmethod
    def run(target, *args, **kwargs):
        if hasattr(target, "func_name") and target.func_name != "<lambda>":
            name = "thread-" + target.func_name
        else:
            name = "thread-" + str(Thread.num_threads)

        Thread.num_threads += 1

        output=Thread(name, target, *args, **kwargs)
        output.start()
        return output



Thread.num_threads=0
Thread.STOP="stop"
Thread.TIMEOUT="TIMEOUT"









        
