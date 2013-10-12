################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

from _functools import partial
from datetime import datetime
import traceback
import logging
import sys
from .basic import listwrap

import struct, threads
from .files import File
from .strings import indent, expand_template
from .threads import Thread
from .struct import Null




ERROR="ERROR"
WARNING="WARNING"
NOTE="NOTE"


main_log=Null
logging_multi=Null



class Log(object):
    """
    FOR STRUCTURED LOGGING AND EXCEPTION CHAINING
    """

    @classmethod
    def new_instance(cls, settings):
        settings=struct.wrap(settings)
        if settings["class"] != Null:
            if not settings["class"].startswith("logging.handlers."):
                return make_log_from_settings(settings)
            else:
                return Log_usingLogger(settings)
        if settings.file != Null: return Log_usingFile(file)
        if settings.filename != Null: return Log_usingFile(settings.filename)
        if settings.stream != Null: return Log_usingStream(settings.stream)

    @classmethod
    def add_log(cls, log):
        logging_multi.add_log(log)





    @staticmethod
    def println(template, params=Null):
        Log.note(template, params)

    @staticmethod
    def note(template, params=Null):
        template="{{log_timestamp}} - "+template
        if params == Null:
            params = {}
        else:
            params = params.copy()

        #NICE TO GATHER MANY MORE ITEMS FOR LOGGING (LIKE STACK TRACES AND LINE NUMBERS)
        params["log_timestamp"]=datetime.utcnow().strftime("%H:%M:%S")

        main_log.write(template, params)


    @staticmethod
    def warning(template, params=Null, cause=Null):
        if isinstance(params, BaseException):
            cause=params
            params=Null

        if cause != Null and not isinstance(cause, Except):
            cause=Except(WARNING, unicode(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), 0))

        e = Except(WARNING, template, params, cause, format_trace(traceback.extract_stack(), 1))
        Log.note(unicode(e))

        
    #raise an exception with a trace for the cause too
    @staticmethod
    def error(
        template,       #human readable template
        params=Null,    #parameters for template
        cause=Null,     #pausible cause
        offset=0        #stack trace offset (==1 if you do not want to report self)
    ):
        if isinstance(params, BaseException):
            cause=params
            params=Null

        if cause != Null and not isinstance(cause, Except):
            cause=Except(ERROR, unicode(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), offset))

        trace=format_trace(traceback.extract_stack(), 1+offset)
        e=Except(ERROR, template, params, cause, trace)
        raise e


    #RUN ME FIRST TO SETUP THE THREADED LOGGING
    @classmethod
    def start(cls, settings=Null):
        ##http://victorlin.me/2012/08/good-logging-practice-in-python/
        if settings == Null: return
        if settings.log == Null: return

        globals()["logging_multi"]=Log_usingMulti()
        globals()["main_log"]=Log_usingThread(logging_multi)

        for log in listwrap(settings.log):
            Log.add_log(Log.new_instance(log))


    @classmethod
    def stop(cls):
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
    def __init__(self, type=ERROR, template=Null, params=Null, cause=Null, trace=Null):
        super(Exception, self).__init__(self)
        self.type=type
        self.template=template
        self.params=params
        self.cause=cause
        self.trace=trace

    @property
    def message(self):
        return unicode(self)

    def __str__(self):
        output=self.template
        if self.params != Null: output=expand_template(output, self.params)

        if self.trace != Null:
            output+="\n"+indent(self.trace)

        if self.cause != Null:
            output+="\ncaused by\n\t"+self.cause.__str__()

        return output+"\n"





class Log_usingFile(Log):

    def __init__(self, file):
        assert file != Null
        self.file=File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock=threads.Lock()


    def write(self, template, params):
        with self.file_lock:
            File(self.filename).append(expand_template(template, params))



#WRAP PYTHON CLASSIC logger OBJECTS
class Log_usingLogger(Log):
    def __init__(self, settings):
        self.logger=logging.Logger("unique name", level=logging.INFO)
        self.logger.addHandler(make_log_from_settings(settings))

    def write(self, template, params):
        # http://docs.python.org/2/library/logging.html#logging.LogRecord
        self.logger.info(expand_template(template, params))


def make_log_from_settings(settings):
    assert settings["class"] != Null

    # IMPORT MODULE FOR HANDLER
    path=settings["class"].split(".")
    class_name=path[-1]
    path=".".join(path[:-1])
    temp=__import__(path, globals(), locals(), [class_name], -1)
    constructor=object.__getattribute__(temp, class_name)

    if settings.filename != Null:
        f = File(settings.filename)
        if not f.parent.exists:
            f.parent.create()

    params = settings.dict
    del params['class']
    return constructor(**params)




class Log_usingStream(Log):

    #stream CAN BE AN OBJCET WITH write() METHOD, OR A STRING
    #WHICH WILL eval() TO ONE
    def __init__(self, stream):
        assert stream != Null
        if isinstance(stream, basestring):
            stream=eval(stream)
        self.stream=stream


    def write(self, template, params):
        try:
            self.stream.write(expand_template(template, params)+"\n")
        except Exception, e:
            pass

    def stop(self):
        pass


class Log_usingThread(Log):
    def __init__(self, logger):
        #DELAYED LOAD FOR THREADS MODULE
        from multithread import worker_thread
        from threads import Queue

        self.queue=Queue()
        self.thread=worker_thread("log thread", self.queue, Null, partial(Log_usingMulti.write, logger))

    def write(self, template, params):
        try:
            self.queue.add({"template":template, "params":params})
            return self
        except Exception, e:
            sys.stdout.write("IF YOU SEE THIS, IT IS LIKELY YOU FORGOT TO RUN Log.start() FIRST")
            raise e  #OH NO!

    def stop(self):
        try:
            self.queue.add(Thread.STOP)  #BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
        except Exception, e:
            pass

        try:
            self.queue.close()
        except Exception, f:
            pass



class Log_usingMulti(Log):
    def __init__(self):
        self.many=[]

    def write(self, template, params):
        for m in self.many:
            try:
                m.write(template, params)
            except Exception:
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



if main_log == Null:
    main_log=Log_usingStream(sys.stdout)
