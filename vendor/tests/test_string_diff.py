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
from mo_collections import reverse
from mo_logs import Log
from mo_logs.strings import apply_diff


class TestStringDiff(unittest.TestCase):

    def setUp(self):
        Log.start()

    def tearDown(self):
        Log.stop()

    def test_1(self):
        data = end_1.split("\n")
        for d in reverse(diffs_1):
            data = apply_diff(data, d.split("\n"), reverse=True)

        assert "\n".join(data) == start_1

        for d in diffs_1:
            data = apply_diff(data, d.split("\n"))

        assert "\n".join(data) == end_1

    def test_2(self):
        data = end_2.split("\n")
        for d in reverse(diffs_2):
            data = apply_diff(data, d.split("\n"), reverse=True)

        assert "\n".join(data) == start_2

        for d in diffs_2:
            data = apply_diff(data, d.split("\n"))

        assert "\n".join(data) == end_2





start_1 = ""

end_1 = """We need to know:

- Can we do privileged operations easily? (bug 976614)
- Can we separate out privileged from non-privileged? (bug 976109)
- Long term viability of Social API (this bug)

We want/need chrome privs for:

- Avoiding Push notification permissions
- Avoiding gUM permission notifications
- Accessing system address books (future, after MLP)"""

diffs_1 = ["""@@ -0,0 +1,4 @@
+We need to know:
+
+- Can we do privileged operations easily?
+- Can we separate out privileged from non-privileged?
""", """@@ -4 +4,6 @@
-- Can we separate out privileged from non-privileged?+- Can we separate out privileged from non-privileged?
+
+We want/need chrome privs for:
+
+- Avoiding Push notification permissions
+- Avoiding gUM permission notifications
""", """@@ -9 +9,2 @@
-- Avoiding gUM permission notifications+- Avoiding gUM permission notifications
+- Accessing system address books (future, after MLP)
""", """@@ -3,2 +3 ,3 @@
-- Can we do privileged operations easily?
-- Can we separate out privileged from non-privileged?
+- Can we do privileged operations easily? (bug 976614)
+- Can we separate out privileged from non-privileged? (bug 976109)
+- Long term viability of Social API (this bug)
"""]

start_2 = ""

end_2 = """before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.

kward has the details.

Target Release Dates :
https://mana.mozilla.org/wiki/display/PM/Firefox+OS+Wave+Launch+Cross+Functional+View

Content Team Engagement & Tasks : https://appreview.etherpad.mozilla.org/40"""

diffs_2=["""@@ -0,0 +1,3 @@
+before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
+
+kward has the details.""",
"""@@ -1 +1 @@
-before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
+before china goes live (end January developer release, June general audience release) , the content team will have to manually update the settings for the china-ready apps currently in marketplace.""",
""" 	@@ -1 +1 @@
-before china goes live (end January developer release, June general audience release), the content team will have to manually update the settings for the china-ready apps currently in marketplace.
+before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
@@ -3 +3 ,6 @@
-kward has the details.+kward has the details.
+
+Target Release Dates :
+https://mana.mozilla.org/wiki/display/PM/Firefox+OS+Wave+Launch+Cross+Functional+View
+
+Content Team Engagement & Tasks : https://appreview.etherpad.mozilla.org/40"""
]
