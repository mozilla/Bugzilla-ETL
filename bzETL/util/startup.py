################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



import argparse
from bzETL.util.basic import listwrap
from .cnv import CNV
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
        for d in listwrap(defs):
            args = d.copy()
            name = args.name
            args.name = Null
            parser.add_argument(*(listwrap(name).list), **(args.dict))
        return parser.parse_args()





    @staticmethod
    def read_settings(filename=Null, defs=Null):
        # READ SETTINGS
        if filename != Null:
            settings_file = File(filename)
            if not settings_file.exists:
                Log.error("Can not file settings file {{filename}}", {
                    "filename": settings_file.abspath
                })
            json = settings_file.read()
            settings = CNV.JSON2object(json, flexible=True)
            if defs != Null:
                settings.args = startup.argparse(defs)
            return settings

        if filename == Null:
            defs=listwrap(defs)
            defs.append({
                "name": ["--settings", "--settings-file", "--settings_file"],
                "help": "path to JSON file with settings",
                "type": str,
                "dest": "filename",
                "default": "./settings.json",
                "required": False
            })
            args = startup.argparse(defs)
            settings_file = File(args.filename)
            if not settings_file.exists:
                Log.error("Can not file settings file {{filename}}", {
                    "filename": settings_file.abspath
                })
            json = settings_file.read()
            settings = CNV.JSON2object(json, flexible=True)
            return settings
