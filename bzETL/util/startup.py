################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



import argparse
from .cnv import CNV
from . import struct
from .struct import Null
from .logs import Log
from .files import File

class startup():

    #PARAMETERS MATCH argparse.ArgumentParser.add_argument()
    #http://docs.python.org/dev/library/argparse.html#the-add-argument-method
    #name or flags - Either a name or a list of option strings, e.g. foo or -f, --foo.
    #action - The basic type of action to be taken when this argument is encountered at the command line.
    #nargs - The number of command-line arguments that should be consumed.
    #const - A constant value required by some action and nargs selections.
    #default - The value produced if the argument is absent from the command line.
    #type - The type to which the command-line argument should be converted.
    #choices - A container of the allowable values for the argument.
    #required - Whether or not the command-line option may be omitted (optionals only).
    #help - A brief description of what the argument does.
    #metavar - A name for the argument in usage messages.
    #dest - The name of the attribute to be added to the object returned by parse_args().
    @staticmethod
    def argparse(defs):
        parser = argparse.ArgumentParser()
        for d in defs:
            args=dict([(k,v) for k,v in d.items()])
            name=args.pop("name")
            if not isinstance(name, list): name=[name]
            parser.add_argument(*name, **args)
        return parser.parse_args()





    @staticmethod
    def read_settings(filename=Null):
        # READ SETTINGS
        if filename == Null:
            args=startup.argparse([
                {
                    "name":["--settings", "--settings-file", "--settings_file"],
                    "help":"path to JSON file with settings",
                    #"nargs":1,
                    "type":str,
                    "dest":"filename",
                    "default":"./settings.json"
                }
            ])
            filename=args.filename

        settings_file=File(filename)
        if not settings_file.exists:
            Log.error("Can not file settings file {{filename}}", {"filename":filename})
        json=settings_file.read()
        return CNV.JSON2object(json, flexible=True)