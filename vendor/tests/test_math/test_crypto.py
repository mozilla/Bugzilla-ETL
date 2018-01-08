# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

import unittest
from mo_math import crypto
from pyLibrary import convert

from mo_json import json2value
from mo_math.randoms import Random


class TestCrypto(unittest.TestCase):
    def test_aes(self):
        crypto.DEBUG = True

        crypto.encrypt("this is a test", Random.bytes(32))
        crypto.encrypt("this is a longer test with more than 16bytes", Random.bytes(32))
        crypto.encrypt("", Random.bytes(32))
        crypto.encrypt("testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ", Random.bytes(32))
        crypto.encrypt("testing accented char àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ", Random.bytes(32))

    def test_aes_nothing(self):
        key = convert.base642bytearray(u'nm5/wK20R45AUtetHJwHTdOigvGTxP7NcH/41YE8AZo=')
        encrypted = crypto.encrypt("", key, salt=convert.base642bytearray("AehqWt1OdEgPJhCx6uylyg=="))
        self.assertEqual(
            json2value(encrypted),
            json2value(u'{"type": "AES256", "length": 0, "salt": "AehqWt1OdEgPJhCx6uylyg=="}')
        )

    def test_aes_on_char(self):
        key = convert.base642bytearray(u'nm5/wK20R45AUtetHJwHTdOigvGTxP7NcH/41YE8AZo=')
        encrypted = crypto.encrypt("kyle", key, salt=convert.base642bytearray("AehqWt1OdEgPJhCx6uylyg=="))
        self.assertEqual(
            json2value(encrypted),
            json2value(u'{"type": "AES256", "length": 4, "salt": "AehqWt1OdEgPJhCx6uylyg==", "data": "FXUGxdb9E+4UCKwsIT9ugQ=="}')
        )

