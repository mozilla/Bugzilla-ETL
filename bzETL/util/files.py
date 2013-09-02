import codecs
import os
import shutil


class File():

    def __init__(self, filename):
        self.filename=filename


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
            shutil.rmtree(self.filename)
            return self
        except Exception, e:
            if e.strerror=="The system cannot find the path specified":
                return
            from util.debug import D
            D.warning("Could not remove file", e)

    def create(self):
        try:
            os.makedirs(self.filename)
        except Exception, e:
            from util.debug import D
            D.error("Could not make directory {{dir_name}}", {"dir_name":self.filename}, e)


    @property
    def parent(self):
        return File("/".join(self.filename.split("/")[:-1]))

    @property
    def exists(self):
        if self.filename in ["", "."]: return True
        return os.path.exists(self.filename)