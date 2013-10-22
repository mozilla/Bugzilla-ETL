from bzETL.util import struct
from bzETL.util.cnv import CNV
from bzETL.util.files import File
from bzETL.util.logs import Log
from bzETL.util.query import Q
from bzETL.util.startup import startup


def main(settings):

    file=File(settings.param.alias_file)
    aliases=CNV.JSON2object(file.read())
    for v in aliases.values():
        v.candidates=CNV.dict2Multiset(v.candidates)

    data=[
        {
            "lost":n,
            "found":d.canonical
        }
        for n, d in aliases.items()
        if d.canonical != None and n!=d.canonical
    ]




    sorted=Q.sort(data, "found")
    for s in sorted:
        Log.note("{{found}} == {{lost}}", s)


    clean={
        n: d.canonical
        for n, d in aliases.items()
        if d.canonical != None and n!=d.canonical and n!=""
    }

    rev_clean=struct.inverse(clean)
    Log.note(CNV.object2JSON(rev_clean, pretty=True))



def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__=="__main__":
    start()