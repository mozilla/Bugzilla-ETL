# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#



import codecs
from datetime import datetime
import os
import shutil
from .struct import listwrap, nvl
from .cnv import CNV


class File(object):

    def __init__(self, filename):
        if filename == None:
            from .logs import Log
            Log.error("File must be given a filename")
        #USE UNIX STANDARD
        self._filename = "/".join(filename.split(os.sep))


    @property
    def filename(self):
        return self._filename.replace("/", os.sep)

    @property
    def abspath(self):
        return os.path.abspath(self._filename)

    def backup_name(self, timestamp=None):
        """
        RETURN A FILENAME THAT CAN SERVE AS A BACKUP FOR THIS FILE
        """
        suffix = CNV.datetime2string(nvl(timestamp, datetime.now()), "%Y%m%d_%H%M%S")
        parts = self._filename.split(".")
        if len(parts) == 1:
            output = self._filename + "." + suffix
        elif len(parts) > 1 and parts[-2][-1] == "/":
            output = self._filename + "." + suffix
        else:
            parts.insert(-1, suffix)
            output = ".".join(parts)
        return output


    def read(self, encoding="utf-8"):
        with codecs.open(self._filename, "r", encoding=encoding) as file:
            return file.read()

    def read_ascii(self):
        if not self.parent.exists: self.parent.create()
        with open(self._filename, "r") as file:
            return file.read()

    def write_ascii(self, content):
        if not self.parent.exists: self.parent.create()
        with open(self._filename, "w") as file:
            file.write(content)

    def write(self, data):
        if not self.parent.exists: self.parent.create()
        with open(self._filename, "w") as file:
            for d in listwrap(data):
                file.write(d)

    def __iter__(self):
        #NOT SURE HOW TO MAXIMIZE FILE READ SPEED
        #http://stackoverflow.com/questions/8009882/how-to-read-large-file-line-by-line-in-python
        def output():
            with codecs.open(self._filename, "r", encoding="utf-8") as f:
                for line in f:
                    yield line
        return output()

    def append(self, content):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "a") as output_file:
            output_file.write(content)

    def add(self, content):
        return self.append(content)

    def extend(self, content):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "a") as output_file:
            for c in content:
                output_file.write(c)



    def delete(self):
        try:
            if os.path.isdir(self._filename):
                shutil.rmtree(self._filename)
            elif os.path.isfile(self._filename):
                os.remove(self._filename)
            return self
        except Exception, e:
            if e.strerror=="The system cannot find the path specified":
                return
            from .logs import Log
            Log.error("Could not remove file", e)

    def backup(self):
        names=self._filename.split("/")[-1].split(".")
        if len(names)==1:
            backup=File(self._filename+".backup "+datetime.utcnow().strftime("%Y%m%d %H%i%s"))


    def create(self):
        try:
            os.makedirs(self._filename)
        except Exception, e:
            from .logs import Log
            Log.error("Could not make directory {{dir_name}}", {"dir_name":self._filename}, e)


    @property
    def parent(self):
        return File("/".join(self._filename.split("/")[:-1]))

    @property
    def exists(self):
        if self._filename in ["", "."]: return True
        try:
            return os.path.exists(self._filename)
        except Exception, e:
            return False
