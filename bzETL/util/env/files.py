# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from datetime import datetime
import io
import os
import shutil
from ..maths import crypto
from ..struct import listwrap, nvl
from ..cnv import CNV


class File(object):
    """
    ASSUMES ALL FILE CONTENT IS UTF8 ENCODED STRINGS
    """

    def __init__(self, filename, buffering=2 ** 14):
        """
        YOU MAY SET filename TO {"path":p, "key":k} FOR CRYPTO FILES
        """
        if filename == None:
            from ..env.logs import Log

            Log.error("File must be given a filename")
        elif isinstance(filename, basestring):
            self.key = None
            self._filename = "/".join(filename.split(os.sep))  # USE UNIX STANDARD
            self.buffering = buffering
        else:
            self.key = CNV.base642bytearray(filename.key)
            self._filename = "/".join(filename.path.split(os.sep))  # USE UNIX STANDARD
            self.buffering = buffering

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

    def read(self, encoding="utf8"):
        with open(self._filename, "rb") as f:
            content = f.read().decode(encoding)
            if self.key:
                return crypto.decrypt(content, self.key)
            else:
                return content

    def read_ascii(self):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "r") as f:
            return f.read()

    def write_ascii(self, content):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "w") as f:
            f.write(content)

    def write(self, data):
        if not self.parent.exists:
            self.parent.create()
        with open(self._filename, "wb") as f:
            if isinstance(data, list) and self.key:
                from ..env.logs import Log

                Log.error("list of data and keys are not supported, encrypt before sending to file")

            for d in listwrap(data):
                if not isinstance(d, unicode):
                    from ..env.logs import Log

                    Log.error("Expecting unicode data only")
                if self.key:
                    f.write(crypto.encrypt(d, self.key).encode("utf8"))
                else:
                    f.write(d.encode("utf8"))

    def __iter__(self):
        #NOT SURE HOW TO MAXIMIZE FILE READ SPEED
        #http://stackoverflow.com/questions/8009882/how-to-read-large-file-line-by-line-in-python
        #http://effbot.org/zone/wide-finder.htm
        def output():
            with io.open(self._filename, "rb") as f:
                for line in f:
                    yield line.decode("utf8")

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
            if e.strerror == "The system cannot find the path specified":
                return
            from ..env.logs import Log

            Log.error("Could not remove file", e)

    def backup(self):
        names = self._filename.split("/")[-1].split(".")
        if len(names) == 1:
            backup = File(self._filename + ".backup " + datetime.utcnow().strftime("%Y%m%d %H%i%s"))


    def create(self):
        try:
            os.makedirs(self._filename)
        except Exception, e:
            from ..env.logs import Log

            Log.error("Could not make directory {{dir_name}}", {"dir_name": self._filename}, e)


    @property
    def parent(self):
        return File("/".join(self._filename.split("/")[:-1]))

    @property
    def exists(self):
        if self._filename in ["", "."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception, e:
            return False

    def __bool__(self):
        return self.__nonzero__()


    def __nonzero__(self):
        """
        USED FOR FILE EXISTENCE TESTING
        """
        if self._filename in ["", "."]:
            return True
        try:
            return os.path.exists(self._filename)
        except Exception, e:
            return False
