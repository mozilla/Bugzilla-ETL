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
from datetime import timedelta
import logging
import sys

from .. import struct
from .log_usingStream import Log_usingStream, time_delta_pusher
from .logs import BaseLog, DEBUG_LOGGING, Log
from ..thread import threads
from ..thread.threads import Thread



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
    log_instance = constructor(**params)
    return log_instance

