# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_logs.strings import is_hex
from mo_testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.env.git import get_git_revision
from pyLibrary.env.git import get_remote_revision


class TestGit(FuzzyTestCase):
    def test_get_revision(self):
        rev = get_git_revision()
        self.assertTrue(is_hex(rev))
        self.assertEqual(len(rev), 40)

    def test_get_remote_revision(self):
        rev = get_remote_revision('https://github.com/klahnakoski/pyLibrary.git', 'master')
        self.assertTrue(is_hex(rev))
        self.assertEqual(len(rev), 40)
