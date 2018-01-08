# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import unittest
from mo_dots.lists import FlatList
from mo_future import PY2, PY3


class NaiveList(list):
    def __init__(self, value):
        self.list = value

    def __getitem__(self, slice):
        return self.__getslice__(slice.start, slice.stop)

    def __getslice__(self, i, j):
        if i < 0:  # CLAMP i TO A REASONABLE RANGE
            i = 0
        elif i > len(self.list):
            i = len(self.list)

        if j < 0:  # CLAMP j TO A REASONABLE RANGE
            j = 0
        elif j > len(self.list):
            j = len(self.list)

        if i > j:  # DO NOT ALLOW THE IMPOSSIBLE
            i = j

        return [self.list[index] for index in range(i, j)]

    def __len__(self):
        return len(self.list)



class TestPython(unittest.TestCase):
    def test_space(self):
        if PY3:
            assert u" " != b" "
        else:
            assert u" " == b" "

    def test_slice(self):
        my_list = NaiveList(['a', 'b', 'c', 'd', 'e'])

        assert 0 == len(my_list[-2:0])
        assert (0 if PY2 else 1) == len(my_list[-1:1])  # EXPECT 1
        assert 2 == len(my_list[0:2])
        assert 2 == len(my_list[1:3])
        assert 2 == len(my_list[2:4])
        assert 2 == len(my_list[3:5])
        assert 1 == len(my_list[4:6])
        assert 0 == len(my_list[5:7])

    def test_over_slice_left(self):
        my_list = NaiveList(['a', 'b', 'c', 'd', 'e'])

        assert 2 == len(my_list[1:3])
        assert 3 == len(my_list[0:3])
        assert (0 if PY2 else 3) == len(my_list[-1:3])  # EXPECT 3
        assert (0 if PY2 else 3) == len(my_list[-2:3])  # EXPECT 3
        assert (1 if PY2 else 3) == len(my_list[-3:3])  # EXPECT 3
        assert (2 if PY2 else 3) == len(my_list[-4:3])  # EXPECT 3


    def test_over_slice_right(self):
        my_list = NaiveList(['a', 'b', 'c', 'd', 'e'])

        assert 3 == len(my_list[1:4])
        assert 4 == len(my_list[1:5])
        assert 4 == len(my_list[1:6])
        assert 4 == len(my_list[1:7])
        assert 4 == len(my_list[1:8])
        assert 4 == len(my_list[1:9])

    def test_better_slice(self):
        my_list = FlatList(['a', 'b', 'c', 'd', 'e'])

        assert 0 == len(my_list[-2:0:])
        assert 1 == len(my_list[-1:1:])
        assert 2 == len(my_list[0:2:])
        assert 2 == len(my_list[1:3:])
        assert 2 == len(my_list[2:4:])
        assert 2 == len(my_list[3:5:])
        assert 1 == len(my_list[4:6:])
        assert 0 == len(my_list[5:7:])

    def test_better_over_slice_left(self):
        my_list = FlatList(['a', 'b', 'c', 'd', 'e'])

        assert 2 == len(my_list[1:3:])
        assert 3 == len(my_list[0:3:])
        assert 3 == len(my_list[-1:3:])
        assert 3 == len(my_list[-2:3:])
        assert 3 == len(my_list[-3:3:])
        assert 3 == len(my_list[-4:3:])


    def test_better_over_slice_right(self):
        my_list = FlatList(['a', 'b', 'c', 'd', 'e'])

        assert 3 == len(my_list[1:4:])
        assert 4 == len(my_list[1:5:])
        assert 4 == len(my_list[1:6:])
        assert 4 == len(my_list[1:7:])
        assert 4 == len(my_list[1:8:])
        assert 4 == len(my_list[1:9:])

