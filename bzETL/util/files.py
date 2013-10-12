################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



import codecs
from datetime import datetime
import os
import shutil
from .basic import listwrap
from .struct import Null


class File():

    def __init__(self, filename):
        assert filename != Null
        #USE UNIX STANDARD
        self._filename = "/".join(filename.split(os.sep))


    @property
    def filename(self):
        return self._filename.replace("/", os.sep)

    @property
    def abspath(self):
        return os.path.abspath(self._filename)

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

    def iter(self):
        return codecs.open(self._filename, "r")

    def append(self, content):
        if not self.parent.exists: self.parent.create()
        with open(self._filename, "a") as output_file:
            output_file.write(content)

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
            Log.warning("Could not remove file", e)

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