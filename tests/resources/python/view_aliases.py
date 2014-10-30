# encoding: utf-8
#
from pyLibrary import struct
from pyLibrary.cnv import CNV
from pyLibrary.env.files import File
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q
from pyLibrary.env import startup


def main(settings):
    file = File(settings.param.alias_file)
    aliases = CNV.JSON2object(file.read())

    for v in aliases.values():
        v.candidates = CNV.dict2Multiset(v.candidates)

    data = [
        {
            "lost": n,
            "found": d.canonical
        }
        for n, d in aliases.items()
        if d.canonical != None and n != d.canonical
    ]

    sorted = Q.sort(data, "found")
    for s in sorted:
        Log.note("{{found}} == {{lost}}", s)

    clean = {
        n: d.canonical
        for n, d in aliases.items()
        if d.canonical != None and n != d.canonical and n != ""
    }

    rev_clean = struct.inverse(clean)
    Log.note(CNV.object2JSON(rev_clean, pretty=True))

    for k, v in rev_clean.items():
        if len(v) > 3:
            Log.note(CNV.object2JSON({k: v}, pretty=True))


def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.fatal("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
