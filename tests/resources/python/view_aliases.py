# encoding: utf-8
#
from pyLibrary import struct
from pyLibrary import convert
from pyLibrary.env.files import File
from pyLibrary.env.logs import Log
from pyLibrary.queries import jx
from pyLibrary.env import startup


def main(settings):
    file = File(settings.param.alias_file)
    aliases = convert.json2value(file.read())

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

    rev_clean = struct.inverse(clean)
    Log.note(convert.value2json(rev_clean, pretty=True))

    for k, v in rev_clean.items():
        if len(v) > 3:
            Log.note(convert.value2json({k: v}, pretty=True))


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
