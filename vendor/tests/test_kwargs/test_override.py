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

from mo_testing.fuzzytestcase import FuzzyTestCase

from mo_kwargs import override

kw = {"require": 1, "optional": 2}


class TestOverride(FuzzyTestCase):

    def test_nothing_w_nothing(self):
        result = nothing()
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result["kwargs"]), 1)

    def test_nothing_w_require(self):
        result = nothing(require=3)
        self.assertEqual(result, {"require": 3})

    def test_nothing_w_optional(self):
        result = nothing(optional=3)
        self.assertEqual(result, {"optional": 3})

    def test_nothing_w_both(self):
        result = nothing(require=3, optional=3)
        self.assertEqual(result, {"require": 3, "optional": 3})

    def test_nothing_w_nothing_and_kwargs(self):
        result = nothing(kwargs=kw)
        self.assertEqual(result, kw)

    def test_nothing_w_require_and_kwargs(self):
        result = nothing(require=3, kwargs=kw)
        self.assertEqual(result, {"require": 3})

    def test_nothing_w_optional_and_kwargs(self):
        result = nothing(optional=3, kwargs=kw)
        self.assertEqual(result, {"optional": 3})

    def test_nothing_w_both_and_kwargs(self):
        result = nothing(require=3, optional=3, kwargs=kw)
        self.assertEqual(result, {"require": 3, "optional": 3})


    def test_required_w_nothing(self):
        self.assertRaises(Exception, required)

    def test_required_w_require(self):
        result = required(require=3)
        self.assertEqual(result, {"require": 3})

    def test_required_w_optional(self):
        self.assertRaises(Exception, required, optional=3)

    def test_required_w_both(self):
        result = required(require=3, optional=3)
        self.assertEqual(result, {"require": 3, "optional": 3})

    def test_required_w_required_and_kwargs(self):
        result = required(kwargs=kw)
        self.assertEqual(result, kw)

    def test_required_w_require_and_kwargs(self):
        result = required(require=3, kwargs=kw)
        self.assertEqual(result, {"require": 3, "optional":2, "kwargs":{}})

    def test_required_w_optional_and_kwargs(self):
        result = required(optional=3, kwargs=kw)
        self.assertEqual(result, {"optional": 3})

    def test_required_w_both_and_kwargs(self):
        result = required(require=3, optional=3, kwargs=kw)
        self.assertEqual(result, {"require": 3, "optional":3, "kwargs":{}})


    def test_kwargs_w_nothing(self):
        result = kwargs()
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result["kwargs_"]), 0)
        self.assertEqual(len(result["kwargs"]), 1)
        self.assertEqual(len(result["kwargs"]["kwargs"]), 1)

    def test_kwargs_w_require(self):
        result = kwargs(require=3)
        self.assertEqual(result, {"kwargs": {"require": 3}})

    def test_kwargs_w_optional(self):
        result = kwargs(optional=2)
        self.assertEqual(len(result["kwargs_"]), 0)
        self.assertEqual(len(result["kwargs"]), 2)
        self.assertEqual(result["kwargs"]["optional"], 2)

    def test_kwargs_w_both(self):
        result = kwargs(require=1, optional=2)
        self.assertEqual(result["kwargs"], kw)

    def test_kwargs_w_required_and_kwargs(self):
        result = kwargs(kwargs=kw)
        self.assertEqual(result, {"kwargs": {"require": 1, "optional": 2}})

    def test_kwargs_w_require_and_kwargs(self):
        result = kwargs(require=3, kwargs=kw)
        self.assertEqual(result, {"kwargs":{"require": 3}})

    def test_kwargs_w_optional_and_kwargs(self):
        result = kwargs(optional=2, kwargs=kw)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result["kwargs"]), 3)
        self.assertEqual(result["kwargs"]["optional"], 2)
        self.assertEqual(result["kwargs"]["require"], 1)

    def test_kwargs_w_both_and_kwargs(self):
        result = kwargs(require=1, optional=2, kwargs=kw)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result["kwargs_"]), 0)
        self.assertEqual(len(result["kwargs"]), 3)
        self.assertEqual(len(result["kwargs"]["kwargs"]), 3)


@override
def nothing(kwargs=None):
    return kwargs


@override
def required(require, optional=2, kwargs=None):
    return {"require": require, "optional": optional, "kwargs": kwargs}


@override
def kwargs(kwargs=None, **kwargs_):
    return {"kwargs": kwargs, "kwargs_": kwargs_}


class TestObject(object):

    @override
    def __init__(self, require, optional=3, kwargs=None):
        self.require=require
        self.optional=optional
        self.kwargs=kwargs

    @override
    def nothing(self, kwargs=None):
        return kwargs

    @override
    def required(self, require, optional=3, kwargs=None):
        return {"require": require, "optional": optional, "kwargs": kwargs}

    @override
    def kwargs(self, kwargs=None, **kwargs_):
        return {"kwargs_": kwargs_, "kwargs": kwargs}

