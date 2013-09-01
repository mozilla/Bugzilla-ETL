################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
from _functools import partial

from datetime import datetime
import sys

import traceback
import logging
from util.files import File
from util.strings import indent, expand_template
from util import struct, threads
from util.threads import Thread

from util.struct import StructList, Struct


#for debugging
logging_thread=None
Logging_multi=None

class D(object):


    @classmethod
    def add_log(cls, log):
        logging_multi.add_log(log)


    @staticmethod
    def println(template, params=None):
        template="{{log_timestamp}} - "+template
        if params is None: params={}

        #NICE TO GATHER MANY MORE ITEMS FOR LOGGING (LIKE STACK TRACES AND LINE NUMBERS)
        params["log_timestamp"]=datetime.utcnow().strftime("%H:%M:%S")

        logging_thread.println(template, params)


    @staticmethod
    def warning(template, params=None, cause=None):
        if isinstance(params, BaseException):
            cause=params
            params=None

        if not isinstance(cause, Except):
            cause=Except(str(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), 0))

        e = Except(template, params, cause, format_trace(traceback.extract_stack(), 1))
        D.println(str(e))

    #raise an exception with a trace for the cause too
    @staticmethod
    def error(
        template,       #human readable template
        params=None,    #parameters for template
        cause=None,     #pausible cause
        offset=0        #stack trace offset (==1 if you do not want to report self)
    ):
        if isinstance(params, BaseException):
            cause=params
            params=None

        if not isinstance(cause, Except):
            cause=Except(str(cause), trace=format_trace(traceback.extract_tb(sys.exc_info()[2]), offset))

        trace=format_trace(traceback.extract_stack(), 1+offset)
        e=Except(template, params, cause, trace)
        raise e


    #RUN ME FIRST TO WARM UP THE LOGGING
    @classmethod
    def start(cls, settings=None):
        if settings is None:
            settings=Struct(**{"log":{"stream":sys.stdout}})
        
        #PART 2 OF 2 SETUP OF THREADED LOGGING
        #WE NOW CAN LOAD THE threads MODULE
        from util.multithread import worker_thread
        from threads import Queue
        logging_thread.queue=Queue()
        logging_thread.thread=worker_thread("log thread", logging_thread.queue, None, partial(Log_usingMulti.println, logging_multi))


        ##http://victorlin.me/2012/08/good-logging-practice-in-python/
        if settings is None: return
        if settings.log is None: return

        if not isinstance(settings.log, StructList): settings.log=[settings.log]
        for log in settings.log:
            D.add_log(Log.new_instance(log))

    @classmethod
    def stop(cls):
        logging_thread.stop()

D.info=D.println


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
    def __init__(self, template=None, params=None, cause=None, trace=None):
        super(Exception, self).__init__(self)
        self.template=template
        self.params=params
        self.cause=cause
        self.trace=trace

    @property
    def message(self):
        return str(self)

    def __str__(self):
        output=self.template
        if self.params is not None: output=expand_template(output, self.params)

        if self.trace is not None:
            output+="\n"+indent(self.trace)

        if self.cause is not None:
            output+="\ncaused by\n\t"+self.cause.__str__()

        return output+"\n"





class Log():
    @classmethod
    def new_instance(cls, settings):
        settings=struct.wrap(settings)
        if settings["class"] is not None:
            if settings["class"].startswith("util."):
                return make_log_from_settings(settings)
            return Log_usingLogger(settings)
        if settings.file is not None: return Log_usingFile(file)
        if settings.filename is not None: return Log_usingFile(settings.filename)
        if settings.stream is not None: return Log_usingStream(settings.stream)





class Log_usingFile():

    def __init__(self, file):
        assert file is not None
        self.file_name=file
        self.file_lock=threads.Lock()


    def println(self, template, params):
        with self.file_lock:
            File(self.filename).append(expand_template(template, params))



#WRAP PYTHON CLASSIC logger OBJECTS
class Log_usingLogger():
    def __init__(self, settings):
        self.logger=logging.Logger("unique name", level=logging.INFO)
        self.logger.addHandler(make_log_from_settings(settings))

    def println(self, template, params):
        # http://docs.python.org/2/library/logging.html#logging.LogRecord
        self.logger.info(expand_template(template, params))


def make_log_from_settings(settings):
    assert settings["class"] is not None

    # IMPORT MODULE FOR HANDLER
    path=settings["class"].split(".")
    class_name=path[-1]
    path=".".join(path[:-1])
    temp=__import__(path, globals(), locals(), [class_name], -1)
    constructor=object.__getattribute__(temp, class_name)

    params = settings.dict
    del params['class']
    return constructor(**params)




class Log_usingStream():

    #stream CAN BE AN OBJCET WITH write() METHOD, OR A STRING
    #WHICH WILL eval() TO ONE
    def __init__(self, stream):
        assert stream is not None
        if isinstance(stream, basestring):
            stream=eval(stream)
        self.stream=stream


    def println(self, template, params):
        try:
            self.stream.write(expand_template(template, params)+"\n")
        except Exception, e:
            pass


class Log_usingThread():
    def __init__(self, logger):
        self.logger=logger  #for later
        self.thread=None
        self.queue=None

    def println(self, template, params):
        try:
            self.queue.add({"template":template, "params":params})
            return self
        except Exception, e:
            sys.stdout.write("IF YOU SEE THIS, IT IS LIKELY YOU FORGOT TO RUN D.start() FIRST")
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



class Log_usingMulti():
    def __init__(self):
        self.many=[]

    def println(self, template, params):
        for m in self.many:
            try:
                m.println(template, params)
            except Exception, e:
                pass
        return self

    def add_log(self, logger):
        self.many.append(logger)
        return self

    def remove_log(self, logger):
        self.many.remove(logger)
        return self



#PART 1 OF 2 FOR SETTING UP THREADED LOGGING
#DEPENDS ON THE threads MODULE, SO WE WILL SETUP THE REST AFTER IT IS LOADED
logging_multi=Log_usingMulti()
#logging_multi.add_log(Log_usingStream(sys.stdout))
logging_thread=Log_usingThread(logging_multi)
