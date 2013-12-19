# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import argparse
import os
import tempfile
import sys
import struct
from .struct import listwrap
from .cnv import CNV
from .logs import Log
from .files import File


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

def _argparse(defs):
    parser = argparse.ArgumentParser()
    for d in listwrap(defs):
        args = d.copy()
        name = args.name
        args.name = None
        parser.add_argument(*listwrap(name).list, **args.dict)
    namespace = parser.parse_args()
    output = {k: getattr(namespace, k) for k in vars(namespace)}
    return struct.wrap(output)


def read_settings(filename=None, defs=None):
    # READ SETTINGS
    if filename:
        settings_file = File(filename)
        if not settings_file.exists:
            Log.error("Can not file settings file {{filename}}", {
                "filename": settings_file.abspath
            })
        json = settings_file.read()
        settings = CNV.JSON2object(json, flexible=True)
        if defs:
            settings.args = _argparse(defs)
        return settings
    else:
        defs = listwrap(defs)
        defs.append({
            "name": ["--settings", "--settings-file", "--settings_file"],
            "help": "path to JSON file with settings",
            "type": str,
            "dest": "filename",
            "default": "./settings.json",
            "required": False
        })
        args = _argparse(defs)
        settings_file = File(args.filename)
        if not settings_file.exists:
            Log.error("Can not file settings file {{filename}}", {
                "filename": settings_file.abspath
            })
        json = settings_file.read()
        settings = CNV.JSON2object(json, flexible=True)
        settings.args = args
        return settings


# snagged from https://github.com/pycontribs/tendo/blob/master/tendo/singleton.py
# TODO: get licence
class SingleInstance:
    """
    ONLY ONE INSTANCE OF PROGRAM ALLOWED
    If you want to prevent your script from running in parallel just instantiate SingleInstance() class.
    If is there another instance already running it will exist the application with the message
    "Another instance is already running, quitting.", returning -1 error code.

    me = SingleInstance()

    This option is very useful if you have scripts executed by crontab at small amounts of time.

    Remember that this works by creating a lock file with a filename based on the full path to the script file.
    """
    def __init__(self, flavor_id=""):
        import sys
        self.initialized = False
        basename = os.path.splitext(os.path.abspath(sys.argv[0]))[0].replace("/", "-").replace(":", "").replace("\\", "-") + '-%s' % flavor_id + '.lock'
        self.lockfile = os.path.normpath(tempfile.gettempdir() + '/' + basename)


    def __enter__(self):
        Log.debug("SingleInstance lockfile: " + self.lockfile)
        if sys.platform == 'win32':
            try:
                # file already exists, we try to remove (in case previous execution was interrupted)
                if os.path.exists(self.lockfile):
                    os.unlink(self.lockfile)
                self.fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except Exception, e:
                Log.warning("Another instance is already running, quitting.", e)
                sys.exit(-1)
        else: # non Windows
            import fcntl
            self.fp = open(self.lockfile, 'w')
            try:
                fcntl.lockf(self.fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                Log.warning("Another instance is already running, quitting.")
                sys.exit(-1)
        self.initialized = True


    def __exit__(self, type, value, traceback):
        self.__del__()


    def __del__(self):
        import sys
        import os

        temp, self.initialized = self.initialized, False
        if not temp:
            return
        try:
            if sys.platform == 'win32':
                if hasattr(self, 'fd'):
                    os.close(self.fd)
                    os.unlink(self.lockfile)
            else:
                import fcntl
                fcntl.lockf(self.fp, fcntl.LOCK_UN)
                if os.path.isfile(self.lockfile):
                    os.unlink(self.lockfile)
        except Exception as e:
            Log.warning("Problem with SingleInstance __del__()", e)
            sys.exit(-1)

