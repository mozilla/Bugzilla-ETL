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
from ..cnv import CNV
from .elasticsearch import ElasticSearch
from ..struct import wrap
from ..thread.threads import Thread, Queue
from .logs import BaseLog, Log


class Log_usingElasticSearch(BaseLog):
    def __init__(self, settings):
        settings = wrap(settings)
        self.es = ElasticSearch(settings)

        aliases = self.es.get_aliases()
        if settings.index not in [a.index for a in aliases]:
            schema = CNV.JSON2object(CNV.object2JSON(SCHEMA), paths=True)
            self.es = ElasticSearch.create_index(settings, schema, limit_replicas=True)

        self.queue = Queue()
        self.thread = Thread("log to " + settings.index, time_delta_pusher, es=self.es, queue=self.queue, interval=timedelta(seconds=1))
        self.thread.start()

    def write(self, template, params):
        try:
            if params.get("template", None):
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
            try:
                last = len(logs)
                for i, log in enumerate(logs):
                    if log is Thread.STOP:
                        please_stop.go()
                        last = i
                        next_run = datetime.utcnow()
                if last > 0:
                    es.extend([{"value":v} for v in logs[0:last]])
            except Exception, e:
                # THREAD WILL END IF
                Log.error("problem logging to es", e)



SCHEMA = {
    "settings": {
        "index.number_of_shards": 3,
        "index.number_of_replicas": 2,
        "index.store.throttle.type": "merge",
        "index.store.throttle.max_bytes_per_sec": "2mb",
        "index.cache.filter.expire": "1m",
        "index.cache.field.type": "soft",
    },
    "mappings": {
        "_default_": {
            "dynamic_templates": [
                {
                    "values_strings": {
                        "match": "*",
                        "match_mapping_type" : "string",
                        "mapping": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                }
            ],
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": True,
                "enabled": True
            },
            "properties": {
                "timestamp": {
                    "type": "long",
                    "index": "not_analyzed",
                    "store": "yes"
                },
                "params": {
                    "type": "object",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                }
            }
        }
    }
}
