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
from ..collections import MIN
from ..env.logs import Log
from ..struct import nvl, split_field, wrap


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

    def select(self, field_name):
        if isinstance(field_name, dict):
            field_name=field_name.value

        if isinstance(field_name, basestring):
            # RETURN LIST OF VALUES
            if len(split_field(field_name)) == 1:
                if self.path[0] == field_name:
                    return [d[1] for d in self.data]
                else:
                    return [d[0][field_name] for d in self.data]
            else:
                keys = split_field(field_name)
                depth = nvl(MIN([i for i, (k, p) in enumerate(zip(keys, self.path)) if k != p]), len(self.path))  # LENGTH OF COMMON PREFIX
                short_keys = keys[depth:]

                output = []
                _select1((wrap(d[depth]) for d in self.data), short_keys, 0, output)
                return output

        Log.error("multiselect over FlatList not supported")


def _select1(data, field, depth, output):
    """
    SELECT A SINGLE FIELD
    """
    for d in data:
        for i, f in enumerate(field[depth:]):
            d = d[f]
            if d == None:
                output.append(None)
                break
            elif isinstance(d, list):
                _select1(d, field, i + 1, output)
                break
        else:
            output.append(d)
