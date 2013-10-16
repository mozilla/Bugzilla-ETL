from bzETL.util.cnv import CNV
from bzETL.util.files import File
from bzETL.util.logs import Log
from bzETL.util.query import Q
from bzETL.util.startup import startup
from bzETL.util.struct import Null


def main(settings):

    file=File(settings.param.alias_file)
    aliases=CNV.JSON2object(file.read())

    data=[
        {
            "lost":n,
            "found":d.canonical
        }
        for n, d in aliases.items()
        if d.canonical != Null and n!=d.canonical
    ]

    sorted=Q.sort(data, "found")
    for s in sorted:
        Log.note("{{found}} == {{lost}}", s)

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