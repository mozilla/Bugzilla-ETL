# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

class FlatList(list):
    """
    FlatList IS A RESULT OF FILTERING SETS OF TREES
    WE SAVED OURSELVES FROM COPYING ALL OBJECTS IN ALL PATHS OF ALL TREES,
    BUT WE ARE LEFT WITH THIS LIST OF TUPLES THAT POINT TO THE SAME
    """

    def __init__(self, path, data):
        """
        data IS A LIST OF TUPLES
        EACH TUPLE IS THE SEQUENCE OF OBJECTS FOUND ALONG A PATH IN A TREE
        IT IS EXPECTED len(data[i]) == len(path)+1 (data[i][0] IS THE ORIGINAL ROW OBJECT)
        """
        list.__init__(self)
        self.data = data
        self.path = path

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        """
        WE ARE NOW DOOMED TO COPY THE RECORDS (BECAUSE LISTS DOWN THE PATH ARE SPECIFIC ELEMENTS)
        """
        for d in self.data:
            r = d[-1]
            for i in range(len(self.path)):
                temp = dict(d[-i - 2])
                temp[self.path[-i - 1]] = r
                r = temp
            yield r
