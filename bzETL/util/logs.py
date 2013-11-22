# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from datetime import datetime, timedelta
import traceback
import logging
import sys
from .struct import listwrap, nvl

import struct, threads
from .strings import indent, expand_template
from .threads import Thread

DEBUG_LOGGING = False
ERROR="ERROR"
WARNING="WARNING"
NOTE="NOTE"

main_log = None
logging_multi = None



class Log(object):
    """
    FOR STRUCTURED LOGGING AND EXCEPTION CHAINING
    """

    @classmethod
    def new_instance(cls, settings):
        settings=struct.wrap(settings)
        if settings["class"]:
            if not settings["class"].startswith("logging.handlers."):
                return make_log_from_settings(settings)
            # elif settings["class"]=="sys.stdout":
                #CAN BE SUPER SLOW
            else:
                return Log_usingLogger(settings)
        if settings.file: return Log_usingFile(file)
        if settings.filename: return Log_usingFile(settings.filename)
        if settings.stream: return Log_usingStream(settings.stream)

    @classmethod
    def add_log(cls, log):
        logging_multi.add_log(log)


    @staticmethod
    def debug(template=None, params=None):
        """
        USE THIS FOR DEBUGGING (AND EVENTUAL REMOVAL)
        """
        Log.note(nvl(template, ""), params)


    @staticmethod
    def println(template, params=None):
        Log.note(template, params)

    @staticmethod
    def note(template, params=None):
        template="{{log_timestamp}} - "+template
        params = nvl(params, {}).copy()

        #NICE TO GATHER MANY MORE ITEMS FOR LOGGING (LIKE STACK TRACES AND LINE NUMBERS)
        params["log_timestamp"]=datetime.utcnow().strftime("%H:%M:%S")

        main_log.write(template, params)


    @staticmethod
    def warning(template, params=None, cause=None):
        if isinstance(params, BaseException):
            cause=params
            params = None

        if cause and not isinstance(cause, Except):
            cause=Except(WARNING, unicode(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), 0))

        e = Except(WARNING, template, params, cause, format_trace(traceback.extract_stack(), 1))
        Log.note(unicode(e))


    #raise an exception with a trace for the cause too
    @staticmethod
    def error(
        template,       #human readable template
        params=None,    #parameters for template
        cause=None,     #pausible cause
        offset=0        #stack trace offset (==1 if you do not want to report self)
    ):
        if params and isinstance(struct.listwrap(params)[0], BaseException):
            cause=params
            params = None

        if cause == None:
            cause = []
        elif isinstance(cause, list):
            pass
        elif isinstance(cause, Except):
            cause = [cause]
        else:
            cause = [Except(ERROR, unicode(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), offset))]

        trace=format_trace(traceback.extract_stack(), 1+offset)
        e=Except(ERROR, template, params, cause, trace)
        raise e


    #RUN ME FIRST TO SETUP THE THREADED LOGGING
    @staticmethod
    def start(settings=None):
        ##http://victorlin.me/2012/08/good-logging-practice-in-python/
        if not settings: return
        if not settings.log: return

        globals()["logging_multi"]=Log_usingMulti()
        globals()["main_log"] = Log_usingThread(logging_multi)

        for log in listwrap(settings.log):
            Log.add_log(Log.new_instance(log))


    @staticmethod
    def stop():
        main_log.stop()



    def write(self):
        Log.error("not implemented")


def format_trace(tbs, trim=0):
    tbs.reverse()
    list = []
    for filename, lineno, name, line in tbs[trim:]:
        item = 'at %s:%d (%s)\n' % (filename,lineno,name)
        list.append(item)
    return "".join(list)


#def format_trace(tb, trim=0):
#    list = []
#    for filename, lineno, name, line in traceback.extract_tb(tb)[0:-trim]:
#        item = 'File "%s", line %d, in %s\n' % (filename,lineno,name)
#        if line:
#            item = item + '\t%s\n' % line.strip()
#        list.append(item)
#    return "".join(list)





class Except(Exception):
    def __init__(self, type=ERROR, template=None, params=None, cause=None, trace=None):
        super(Exception, self).__init__(self)
        self.type=type
        self.template=template
        self.params=params
        self.cause=cause
        self.trace=trace

    @property
    def message(self):
        return unicode(self)

    def contains(self, value):
        if self.type==value:
            return True
        for c in self.cause:
            if c.contains(value):
                return True
        return False

    def __str__(self):
        output=self.type+": "+self.template
        if self.params: output=expand_template(output, self.params)

        if self.trace:
            output+="\n"+indent(self.trace)


        if self.cause:
            output+="\ncaused by\n\t"+"\nand caused by\n\t".join([c.__str__() for c in self.cause])

        return output+"\n"








class BaseLog(object):
    def write(self, template, params):
        pass

    def stop(self):
        pass



class Log_usingFile(BaseLog):

    def __init__(self, file):
        assert file

        from files import File
        self.file=File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock=threads.Lock()


    def write(self, template, params):
        from files import File
        with self.file_lock:
            File(self.filename).append(expand_template(template, params))



#WRAP PYTHON CLASSIC logger OBJECTS
class Log_usingLogger(BaseLog):
    def __init__(self, settings):
        self.logger=logging.Logger("unique name", level=logging.INFO)
        self.logger.addHandler(make_log_from_settings(settings))

        # TURNS OUT LOGGERS ARE REALLY SLOW TOO
        self.queue = threads.Queue()
        self.thread = Thread("log to logger", time_delta_pusher, appender=self.logger.info, queue=self.queue, interval=timedelta(seconds=0.3))
        self.thread.start()

    def write(self, template, params):
        # http://docs.python.org/2/library/logging.html#logging.LogRecord
        self.queue.add({"template": template, "params": params})

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingLogger sees stop, adding stop to queue\n")
            self.queue.add(Thread.STOP)  #BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingLogger done\n")
        except Exception, e:
            pass

        try:
            self.queue.close()
        except Exception, f:
            pass




def make_log_from_settings(settings):
    assert settings["class"]

    # IMPORT MODULE FOR HANDLER
    path=settings["class"].split(".")
    class_name=path[-1]
    path=".".join(path[:-1])
    temp=__import__(path, globals(), locals(), [class_name], -1)
    constructor=object.__getattribute__(temp, class_name)

    #IF WE NEED A FILE, MAKE SURE DIRECTORY EXISTS
    if settings.filename:
        from files import File
        f = File(settings.filename)
        if not f.parent.exists:
            f.parent.create()

    params = settings.dict
    del params['class']
    return constructor(**params)


def time_delta_pusher(please_stop, appender, queue, interval):
    """
    appender - THE FUNCTION THAT ACCEPTS A STRING
    queue - FILLED WITH LINES TO WRITE
    interval - timedelta
    USE IN A THREAD TO BATCH LOGS BY TIME INTERVAL
    """

    if not isinstance(interval, timedelta):
        Log.error("Expecting interval to be a timedelta")

    next_run = datetime.utcnow() + interval

    while not please_stop:
        Thread.sleep(till=next_run)
        next_run = datetime.utcnow() + interval
        logs = queue.pop_all()
        if logs:
            lines = []
            for log in logs:
                try:
                    if log == Thread.STOP:
                        please_stop.go()
                        next_run = datetime.utcnow()
                    else:
                        lines.append(expand_template(log.get("template", None), log.get("params", None)))
                except Exception, e:
                    if DEBUG_LOGGING:
                        sys.stdout.write("Trouble formatting logs: "+e.message)
                        raise e
            try:
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Last call to appender with "+str(len(lines))+" lines\n")
                appender(u"\n".join(lines)+u"\n")
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Done call to appender with "+str(len(lines))+" lines\n")
            except Exception, e:
                if DEBUG_LOGGING:
                    sys.stdout.write("Trouble with appender: "+e.message)
                    raise e


class Log_usingStream(BaseLog):
    #stream CAN BE AN OBJCET WITH write() METHOD, OR A STRING
    #WHICH WILL eval() TO ONE
    def __init__(self, stream):
        assert stream

        use_UTF8 = False

        if isinstance(stream, basestring):
            if stream.startswith("sys."):
                use_UTF8 = True  #sys.* ARE OLD AND CAN NOT HANDLE unicode
            self.stream = eval(stream)
            name = stream
        else:
            self.stream = stream
            name = "stream"

        #WRITE TO STREAMS CAN BE *REALLY* SLOW, WE WILL USE A THREAD
        from threads import Queue

        if use_UTF8:
            def utf8_appender(value):
                if isinstance(value, unicode):
                    value = value.encode('utf-8')
                self.stream.write(value)

            appender = utf8_appender
        else:
            appender = self.stream.write

        self.queue = Queue()
        self.thread = Thread("log to " + name, time_delta_pusher, appender=appender, queue=self.queue, interval=timedelta(seconds=0.3))
        self.thread.start()


    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
            raise e  #OH NO!

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingStream sees stop, adding stop to queue\n")
            self.queue.add(Thread.STOP)  #BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingStream done\n")
        except Exception, e:
            if DEBUG_LOGGING:
                raise e

        try:
            self.queue.close()
        except Exception, f:
            if DEBUG_LOGGING:
                raise f



class Log_usingThread(BaseLog):
    def __init__(self, logger):
        #DELAYED LOAD FOR THREADS MODULE
        from threads import Queue

        self.queue=Queue()
        self.logger=logger

        def worker(please_stop):
            while not please_stop:
                Thread.sleep(1)
                logs = self.queue.pop_all()
                for log in logs:
                    if log==Thread.STOP:
                        if DEBUG_LOGGING:
                            sys.stdout.write("Log_usingThread.worker() sees stop, filling rest of queue\n")
                        please_stop.go()
                    else:
                        self.logger.write(**log)
        self.thread=Thread("log thread", worker)
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template":template, "params":params})
            return self
        except Exception, e:
            sys.stdout.write("IF YOU SEE THIS, IT IS LIKELY YOU FORGOT TO RUN Log.start() FIRST\n")
            raise e  #OH NO!

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("injecting stop into queue\n")
            self.queue.add(Thread.STOP)  #BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingThread telling logger to stop\n")
            self.logger.stop()
        except Exception, e:
            if DEBUG_LOGGING:
                raise e


        try:
            self.queue.close()
        except Exception, f:
            if DEBUG_LOGGING:
                raise f



class Log_usingMulti(BaseLog):
    def __init__(self):
        self.many=[]

    def write(self, template, params):
        for m in self.many:
            try:
                m.write(template, params)
            except Exception, e:
                pass
        return self

    def add_log(self, logger):
        self.many.append(logger)
        return self

    def remove_log(self, logger):
        self.many.remove(logger)
        return self

    def clear_log(self):
        self.many=[]

    def stop(self):
        for m in self.many:
            try:
                m.stop()
            except Exception, e:
                pass



if not main_log:
    main_log = Log_usingStream("sys.stdout")
