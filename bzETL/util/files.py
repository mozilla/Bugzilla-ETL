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


class File():

    def __init__(self, filename):
        #USE UNIX STANDARD
        self.filename = "/".join(filename.split(os.sep))


    def read(self, encoding="utf-8"):
        with codecs.open(self.filename, "r", encoding=encoding) as file:
            return file.read()

    def read_ascii(self):
        if not self.parent.exists: self.parent.create()
        with open(self.filename, "r") as file:
            return file.read()

    def write_ascii(self, content):
        if not self.parent.exists: self.parent.create()
        with open(self.filename, "w") as file:
            file.write(content)

    def write(self, data):
        if not self.parent.exists: self.parent.create()
        with open(self.filename, "w") as file:
            if not isinstance(data, list): data=[data]
            for d in data:
                file.write(d)

    def iter(self):
        return codecs.open(self.filename, "r")

    def append(self, content):
        if not self.parent.exists: self.parent.create()
        with open(self.filename, "a") as output_file:
            output_file.write(content)

    def delete(self):
        try:
            if os.path.isdir(self.filename):
                shutil.rmtree(self.filename)
            elif os.path.isfile(self.filename):
                os.remove(self.filename)
            return self
        except Exception, e:
            if e.strerror=="The system cannot find the path specified":
                return
            from .logs import Log
            Log.warning("Could not remove file", e)

    def backup(self):
        names=self.filename.split("/")[-1].split(".")
        if len(names)==1:
            backup=File(self.filename+".backup "+datetime.utcnow().strftime("%Y%m%d %H%i%s"))


    def create(self):
        try:
            os.makedirs(self.filename)
        except Exception, e:
            from .logs import Log
            Log.error("Could not make directory {{dir_name}}", {"dir_name":self.filename}, e)


    @property
    def parent(self):
        return File("/".join(self.filename.split("/")[:-1]))

    @property
    def exists(self):
        if self.filename in ["", "."]: return True
        return os.path.exists(self.filename)