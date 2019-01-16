# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from jx_python import jx
from mo_dots import inverse
from mo_files import File
from mo_json import json2value, value2json
from mo_logs import Log, startup
from pyLibrary import convert


def main(settings):
    file = File(settings.param.alias_file)
    aliases = json2value(file.read())

    for v in aliases.values():
        v.candidates = convert.dict2Multiset(v.candidates)

    data = [
        {
            "lost": n,
            "found": d.canonical
        }
        for n, d in aliases.items()
        if d.canonical != None and n != d.canonical
    ]

    sorted = jx.sort(data, "found")
    for s in sorted:
        Log.note("{{found}} == {{lost}}", s)

    clean = {
        n: d.canonical
        for n, d in aliases.items()
        if d.canonical != None and n != d.canonical and n != ""
    }

    rev_clean = inverse(clean)
    Log.note(value2json(rev_clean, pretty=True))

    for k, v in rev_clean.items():
        if len(v) > 3:
            Log.note(value2json({k: v}, pretty=True))


def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception as e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
