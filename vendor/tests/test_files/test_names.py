# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

from mo_files import File
from mo_testing.fuzzytestcase import FuzzyTestCase


par = os.sep + ".."


class TestNames(FuzzyTestCase):

    def test_relative_self(self):
        f = File(".")
        self.assertEqual(f.parent.filename, "..")
        self.assertEqual(f.parent.parent.filename, ".."+par)
        self.assertEqual(f.parent.parent.parent.filename, ".."+par+par)

    def test_relative_self2(self):
        f = File("")
        self.assertEqual(f.parent.filename, "..")
        self.assertEqual(f.parent.parent.filename, ".."+par)
        self.assertEqual(f.parent.parent.parent.filename, ".."+par+par)

    def test_relative_name(self):
        f = File("test.txt")
        self.assertEqual(f.parent.filename, "")
        self.assertEqual(f.parent.parent.filename, "..")
        self.assertEqual(f.parent.parent.parent.filename, ".."+par)

    def test_relative_path(self):
        f = File("a/test.txt")
        self.assertEqual(f.parent.filename, "a")
        self.assertEqual(f.parent.parent.filename, "")
        self.assertEqual(f.parent.parent.parent.filename, "..")

    def test_grandparent(self):
        f = File.new_instance("tests/temp", "../..")
        self.assertEqual(f.filename, ".")
