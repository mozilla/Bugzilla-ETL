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
from datetime import timedelta, datetime
import sys
from .elasticsearch import ElasticSearch
from ..struct import wrap
from ..thread.threads import Thread, Queue
from .logs import BaseLog, Log


class Log_usingElasticSearch(BaseLog):
    def __init__(self, settings):
        settings = wrap(settings)
        self.es = ElasticSearch(settings)
        self.queue = Queue()
        self.thread = Thread("log to " + settings.index, time_delta_pusher, es=self.es, queue=self.queue, interval=timedelta(seconds=1))
        self.thread.start()

    def write(self, template, params):
        try:
            if "template" in params:
                #DETECTED INNER TEMPLATE, ASSUME TRACE IS ON, SO DO NOT NEED THE OUTER TEMPLATE
                self.queue.add(params)
            else:
                self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
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


def time_delta_pusher(please_stop, es, queue, interval):
    """
    appender - THE FUNCTION THAT ACCEPTS A STRING
    queue - FILLED WITH LOG ENTRIES {"template":template, "params":params} TO WRITE
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
            last = 0
            for i, log in enumerate(logs):
                try:
                    if log is Thread.STOP:
                        please_stop.go()
                        last = i
                        next_run = datetime.utcnow()
                except Exception, e:
                    sys.stderr.write("Trouble formatting logs: " + e.message)
            try:
                es.extend([{"value":v} for v in logs[0:last]])
            except Exception, e:
                sys.stderr.write("Trouble with appender: " + e.message)

