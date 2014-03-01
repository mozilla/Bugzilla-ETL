# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import unicode_literals
import cProfile
from datetime import datetime, timedelta
import pstats
import logging
import sys

from .. import struct
from ..thread import threads
from ..struct import listwrap, nvl, Struct, wrap
from ..strings import indent, expand_template
from ..thread.threads import Thread


DEBUG_LOGGING = False
ERROR = "ERROR"
WARNING = "WARNING"
NOTE = "NOTE"


class Log(object):
    """
    FOR STRUCTURED LOGGING AND EXCEPTION CHAINING
    """
    trace = False
    main_log = None
    logging_multi = None
    profiler = None


    @classmethod
    def new_instance(cls, settings):
        settings = wrap(settings)

        if settings["class"]:
            if not settings["class"].startswith("logging.handlers."):
                return make_log_from_settings(settings)
                # elif settings["class"]=="sys.stdout":
                #CAN BE SUPER SLOW
            else:
                return Log_usingLogger(settings)
        if settings.file:
            return Log_usingFile(file)
        if settings.filename:
            return Log_usingFile(settings.filename)
        if settings.stream:
            return Log_usingStream(settings.stream)

    @classmethod
    def add_log(cls, log):
        cls.logging_multi.add_log(log)

    @classmethod
    def debug(cls, template=None, params=None):
        """
        USE THIS FOR DEBUGGING (AND EVENTUAL REMOVAL)
        """
        Log.note(nvl(template, ""), params, stack_depth=1)

    @classmethod
    def println(cls, template, params=None):
        Log.note(template, params, stack_depth=1)

    @classmethod
    def note(cls, template, params=None, stack_depth=0):
        # USE replace() AS POOR MAN'S CHILD TEMPLATE

        log_params = Struct(
            template=template,
            params=nvl(params, {}).copy(),
            timestamp=datetime.utcnow(),
        )
        if cls.trace:
            log_template = "{{timestamp|datetime}} - {{thread.name}} - {{location.file}}:{{location.line}} ({{location.method}}) - " + template.replace("{{", "{{params.")
            f = sys._getframe(stack_depth + 1)
            log_params.location = {
                "line": f.f_lineno,
                "file": f.f_code.co_filename,
                "method": f.f_code.co_name
            }
            thread = Thread.current()
            log_params.thread = {"name": thread.name, "id": thread.id}
        else:
            log_template = "{{timestamp|datetime}} - " + template.replace("{{", "{{params.")

        cls.main_log.write(log_template, log_params)

    @classmethod
    def warning(cls, template, params=None, cause=None):
        if isinstance(params, BaseException):
            cause = params
            params = None

        if cause and not isinstance(cause, Except):
            cause = Except(WARNING, unicode(cause), trace=extract_tb(0))

        e = Except(WARNING, template, params, cause, extract_stack(1))
        Log.note(unicode(e))

    @classmethod
    def error(
            cls,
            template, #human readable template
            params=None, #parameters for template
            cause=None, #pausible cause
            offset=0        #stack trace offset (==1 if you do not want to report self)
    ):
        """
        raise an exception with a trace for the cause too
        """
        if params and isinstance(struct.listwrap(params)[0], BaseException):
            cause = params
            params = None

        if cause == None:
            cause = []
        elif isinstance(cause, list):
            pass
        elif isinstance(cause, Except):
            cause = [cause]
        else:
            cause = [Except(ERROR, unicode(cause), trace=extract_tb(offset))]

        trace = extract_stack(1 + offset)
        e = Except(ERROR, template, params, cause, trace)
        raise e

    @classmethod
    def fatal(
            cls,
            template, #human readable template
            params=None, #parameters for template
            cause=None, #pausible cause
            offset=0    #stack trace offset (==1 if you do not want to report self)
    ):
        """
        SEND TO STDERR
        """
        if params and isinstance(struct.listwrap(params)[0], BaseException):
            cause = params
            params = None

        if cause == None:
            cause = []
        elif isinstance(cause, list):
            pass
        elif isinstance(cause, Except):
            cause = [cause]
        else:
            cause = [Except(ERROR, unicode(cause), trace=extract_tb(offset))]

        trace = extract_stack(1 + offset)
        e = Except(ERROR, template, params, cause, trace)
        sys.stderr.write(str(e))


    #RUN ME FIRST TO SETUP THE THREADED LOGGING
    @classmethod
    def start(cls, settings=None):
        ##http://victorlin.me/2012/08/good-logging-practice-in-python/
        if not settings:
            return

        cls.trace = cls.trace | nvl(settings.trace, False)
        if cls.trace:
            from ..thread.threads import Thread

        if not settings.log:
            return

        cls.logging_multi = Log_usingMulti()
        cls.main_log = Log_usingThread(cls.logging_multi)

        for log in listwrap(settings.log):
            Log.add_log(Log.new_instance(log))

        if settings.profile:
            cls.profiler = cProfile.Profile()
            cls.profiler.enable()


    @classmethod
    def stop(cls):
        if cls.profiler:
            from bzETL.util.cnv import CNV
            from bzETL.util.env.files import File

            p = pstats.Stats(cls.profiler)
            stats = [{
                "num_calls":d[1],
                "self_time":d[2],
                "total_time":d[3],
                "file":(f[0] if f[0] != "~" else "").replace("\\", "/"),
                "line":f[1],
                "method":f[2].lstrip("<").rstrip(">")
            }
                for f, d, in p.stats.iteritems()
            ]
            CNV.list2tab(stats)
            File("profile.tab").write(CNV.list2tab(stats))

        cls.main_log.stop()


    def write(self):
        Log.error("not implemented")

def extract_stack(start=0):
    """
    SNAGGED FROM traceback.py
    Extract the raw traceback from the current stack frame.

    Each item in the returned list is a quadruple (filename,
    line number, function name, text), and the entries are in order
    from newest to oldest
    """
    try:
        raise ZeroDivisionError
    except ZeroDivisionError:
        trace = sys.exc_info()[2]
        f = trace.tb_frame.f_back

    for i in range(start):
        f = f.f_back

    stack = []
    n = 0
    while f is not None:
        stack.append({
            "depth": n,
            "line": f.f_lineno,
            "file": f.f_code.co_filename,
            "method": f.f_code.co_name
        })
        f = f.f_back
        n += 1
    return stack


def extract_tb(start):
    """
    SNAGGED FROM traceback.py

    Return list of up to limit pre-processed entries from traceback.

    This is useful for alternate formatting of stack traces.  If
    'limit' is omitted or None, all entries are extracted.  A
    pre-processed stack trace entry is a quadruple (filename, line
    number, function name, text) representing the information that is
    usually printed for a stack trace.
    """
    tb = sys.exc_info()[2]
    for i in range(start):
        tb = tb.tb_next

    trace = []
    n = 0
    while tb is not None:
        f = tb.tb_frame
        trace.append({
            "depth": n,
            "file": f.f_code.co_filename,
            "line": tb.tb_lineno,
            "method": f.f_code.co_name
        })
        tb = tb.tb_next
        n += 1
    trace.reverse()
    return trace


def format_trace(tbs, start=0):
    trace = []
    for d in tbs[start:]:
        item = expand_template('at File {{file}}, line {{line}}, in {{method}}\n', d)
        trace.append(item)
    return "".join(trace)


class Except(Exception):
    def __init__(self, type=ERROR, template=None, params=None, cause=None, trace=None):
        super(Exception, self).__init__(self)
        self.type = type
        self.template = template
        self.params = params
        self.cause = cause
        self.trace = trace

    @property
    def message(self):
        return unicode(self)

    def contains(self, value):
        if self.type == value:
            return True
        for c in self.cause:
            if c.contains(value):
                return True
        return False

    def __str__(self):
        output = self.type + ": " + self.template
        if self.params:
            output = expand_template(output, self.params)

        if self.trace:
            output += "\n" + indent(format_trace(self.trace))

        if self.cause:
            output += "\ncaused by\n\t" + "\nand caused by\n\t".join([c.__str__() for c in self.cause])

        return output + "\n"


class BaseLog(object):
    def write(self, template, params):
        pass

    def stop(self):
        pass


class Log_usingFile(BaseLog):
    def __init__(self, file):
        assert file

        from ..env.files import File

        self.file = File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock = threads.Lock()

    def write(self, template, params):
        from ..env.files import File

        with self.file_lock:
            File(self.filename).append(expand_template(template, params))


#WRAP PYTHON CLASSIC logger OBJECTS
class Log_usingLogger(BaseLog):
    def __init__(self, settings):
        self.logger = logging.Logger("unique name", level=logging.INFO)
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
    path = settings["class"].split(".")
    class_name = path[-1]
    path = ".".join(path[:-1])
    constructor = None
    try:
        temp = __import__(path, globals(), locals(), [class_name], -1)
        constructor = object.__getattribute__(temp, class_name)
    except Exception, e:
        if settings.stream and not constructor:
            #PROVIDE A DEFAULT STREAM HANLDER
            constructor = Log_usingStream
        else:
            Log.error("Can not find class {{class}}", {"class": path}, e)

    #IF WE NEED A FILE, MAKE SURE DIRECTORY EXISTS
    if settings.filename:
        from ..env.files import File

        f = File(settings.filename)
        if not f.parent.exists:
            f.parent.create()

    settings['class'] = None
    params = struct.unwrap(settings)
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
                    if log is Thread.STOP:
                        please_stop.go()
                        next_run = datetime.utcnow()
                    else:
                        lines.append(expand_template(log.get("template", None), log.get("params", None)))
                except Exception, e:
                    sys.stderr.write("Trouble formatting logs: " + e.message)
                    # SWALLOW ERROR, GOT TO KEEP RUNNING
            try:
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Call to appender with " + str(len(lines)) + " lines\n")
                appender(u"\n".join(lines) + u"\n")
                if DEBUG_LOGGING and please_stop:
                    sys.stdout.write("Done call to appender with " + str(len(lines)) + " lines\n")
            except Exception, e:
                sys.stderr.write("Trouble with appender: " + e.message)
                # SWALLOW ERROR, GOT TO KEEP RUNNNIG


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
        from ..thread.threads import Queue

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
        from ..thread.threads import Queue

        self.queue = Queue()
        self.logger = logger

        def worker(please_stop):
            while not please_stop:
                Thread.sleep(1)
                logs = self.queue.pop_all()
                for log in logs:
                    if log is Thread.STOP:
                        if DEBUG_LOGGING:
                            sys.stdout.write("Log_usingThread.worker() sees stop, filling rest of queue\n")
                        please_stop.go()
                    else:
                        self.logger.write(**log)

        self.thread = Thread("log thread", worker)
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
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
        self.many = []

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
        self.many = []

    def stop(self):
        for m in self.many:
            try:
                m.stop()
            except Exception, e:
                pass


if not Log.main_log:
    Log.main_log = Log_usingStream("sys.stdout")
