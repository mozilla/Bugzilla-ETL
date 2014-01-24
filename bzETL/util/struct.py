# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

_get = object.__getattribute__


class Struct(dict):
    """
    Struct is an anonymous class with some properties good for manipulating JSON

    0) a.b==a["b"]
    1) the IDE does tab completion, so my spelling mistakes get found at "compile time"
    2) it deals with missing keys gracefully, so I can put it into set operations (database
       operations) without choking
    2b) missing keys is important when dealing with JSON, which is often almost anything
    3) you can access JSON paths as a variable:   a["b.c"]==a.b.c
    4) attribute names (keys) are corrected to unicode - it appears Python object.getattribute()
       is called with str() even when using from __future__ import unicode_literals

    MORE ON MISSING VALUES: http://www.numpy.org/NA-overview.html
    IT ONLY CONSIDERS THE LEGITIMATE-FIELD-WITH-MISSING-VALUE (Statistical Null)
    AND DOES NOT LOOK AT FIELD-DOES-NOT-EXIST-IN-THIS-CONTEXT (Database Null)

    The Struct is a common pattern in many frameworks (I am still working on this list)

    jinja2.environment.Environment.getattr()
    argparse.Environment() - code performs setattr(e, name, value) on instances of Environment
    collections.namedtuple() - gives attribute names to tuple indicies

    """

    def __init__(self, **map):
        """
        THIS WILL MAKE A COPY, WHICH IS UNLIKELY TO BE USEFUL
        USE struct.wrap() INSTEAD
        """
        dict.__init__(self)
        object.__setattr__(self, "__dict__", map)  #map IS A COPY OF THE PARAMETERS

    def __bool__(self):
        return True

    def __nonzero__(self):
        d = _get(self, "__dict__")
        return True if d else False

    def __str__(self):
        return dict.__str__(_get(self, "__dict__"))

    def __getitem__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        d = _get(self, "__dict__")

        if key.find(".") >= 0:
            key = key.replace("\.", "\a")
            seq = [k.replace("\a", ".") for k in key.split(".")]
            for n in seq:
                d = getdefault(d, n)
            return wrap(d)

        return getdefaultwrapped(d, key)

    def __setitem__(self, key, value):
        if key == "":
            from .logs import Log

            Log.error("key is empty string.  Probably a bad idea")
        if isinstance(key, str):
            key = key.decode("utf8")

        try:
            d = _get(self, "__dict__")
            value = unwrap(value)
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
                return self

            key = key.replace("\.", "\a")
            seq = [k.replace("\a", ".") for k in key.split(".")]
            for k in seq[:-1]:
                d = getdefault(d, k)
            if value == None:
                d.pop(seq[-1], None)
            else:
                d[seq[-1]] = value
            return self
        except Exception, e:
            raise e

    def __getattribute__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        try:
            output = _get(self, key)
            if key=="__dict__":
                return output
            return wrap(output)
        except Exception:
            d = _get(self, "__dict__")
            return _Null(d, key)

    def __setattr__(self, key, value):
        if isinstance(key, str):
            key = key.decode("utf8")

        value = unwrap(value)
        if value is None:
            d = _get(self, "__dict__")
            d.pop(key, None)
        else:
            object.__setattr__(self, key, value)
        return self

    def items(self):
        d = _get(self, "__dict__")
        return ((k, wrap(v)) for k, v in d.items())

    def iteritems(self):
        #LOW LEVEL ITERATION, NO WRAPPING
        d = _get(self, "__dict__")
        return d.iteritems()

    def keys(self):
        d = _get(self, "__dict__")
        return set(d.keys())

    def values(self):
        d = _get(self, "__dict__")
        return (wrap(v) for v in d.values())

    @property
    def dict(self):
        return _get(self, "__dict__")

    @property
    def __class__(self):
        return dict

    def copy(self):
        d = _get(self, "__dict__")
        return Struct(**d)

    def __delitem__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        if key.find(".") == -1:
            d = _get(self, "__dict__")
            d.pop(key, None)
            return

        d = _get(self, "__dict__")
        key = key.replace("\.", "\a")
        seq = [k.replace("\a", ".") for k in key.split(".")]
        for k in seq[:-1]:
            d = d[k]
        d.pop(seq[-1], None)

    def __delattr__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        d = _get(self, "__dict__")
        d.pop(key, None)

    def keys(self):
        d = _get(self, "__dict__")
        return d.keys()

# KEEP TRACK OF WHAT ATTRIBUTES ARE REQUESTED, MAYBE SOME (BUILTIN) ARE STILL USEFUL
requested = set()


def setdefault(obj, key, value):
    """
    DO NOT USE __dict__.setdefault(obj, key, value), IT DOES NOT CHECK FOR obj[key] == None
    """
    v = obj.get(key, None)
    if v == None:
        obj[key] = value
        return value
    return v


def getdefault(obj, key):
    try:
        return obj[key]
    except Exception, e:
        return _Null(obj, key)

def getdefaultwrapped(obj, key):
    o = obj.get(key, None)
    if o == None:
        return _Null(obj, key)
    return wrap(o)


def _assign(null, key, value, force=True):
    """
    value IS ONLY ASSIGNED IF self.obj[self.path][key] DOES NOT EXIST
    """
    d = _get(null, "__dict__")
    o = d["obj"]
    if isinstance(o, _Null):
        o = _assign(o, d["path"], {}, False)
    else:
        o = setdefault(o, d["path"], {})

    if force:
        o[key] = value
    else:
        value = setdefault(o, key, value)
    return value


class _Null(object):
    """
    Structural Null provides closure under the dot (.) operator
        Null[x] == Null
        Null.x == Null
    """

    def __init__(self, obj=None, path=None):
        d = _get(self, "__dict__")
        d["obj"] = obj
        d["path"] = path

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return False

    def __le__(self, other):
        return False

    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return other is None or isinstance(other, _Null)

    def __ne__(self, other):
        return other is not None and not isinstance(other, _Null)

    def __getitem__(self, key):
        return _Null(self, key)

    def __len__(self):
        return 0

    def __iter__(self):
        return ZeroList.__iter__()

    def last(self):
        """
        IN CASE self IS INTERPRETED AS A list
        """
        return Null

    def right(self, num=None):
        return EmptyList

    def __getattribute__(self, key):
        try:
            output = _get(self, key)
            return output
        except Exception, e:
            return _Null(self, key)

    def __setattr__(self, key, value):
        _Null.__setitem__(self, key, value)

    def __setitem__(self, key, value):
        try:
            value = unwrap(value)
            if key.find(".") == -1:
                _assign(self, key, value)
                return self

            key = key.replace("\.", "\a")
            seq = [k.replace("\a", ".") for k in key.split(".")]
            d = _assign(self, seq[0], {}, False)
            for k in seq[1:-1]:
                o = {}
                d[k] = o
                d = o
            d[seq[-1]] = value
            return self
        except Exception, e:
            raise e

    def keys(self):
        return set()

    def pop(self, key, default=None):
        return None

    def __str__(self):
        return "None"


Null = _Null()
EmptyList = Null

ZeroList = []


class StructList(list):
    """
    ENCAPSULATES HANDING OF Nulls BY wrapING ALL MEMBERS AS NEEDED
    ENCAPSULATES FLAT SLICES ([::]) FOR USE IN WINDOW FUNCTIONS
    """

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        # list.__init__(self)
        if vals == None:
            self.list = []
        elif isinstance(vals, StructList):
            self.list = vals.list
        else:
            self.list = vals

    def __getitem__(self, index):
        if isinstance(index, slice):
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                from .logs import Log
                Log.error("slice step must be None, do not know how to deal with values")
            length = len(_get(self, "list"))

            i = index.start
            i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return StructList(_get(self, "list")[i:j])

        if index < 0 or len(_get(self, "list")) <= index:
            return Null
        return wrap(_get(self, "list")[index])

    def __setitem__(self, i, y):
        _get(self, "list")[i] = unwrap(y)

    def __iter__(self):
        return (wrap(v) for v in _get(self, "list"))

    def __contains__(self, item):
        return list.__contains__(_get(self, "list"), item)

    def append(self, val):
        _get(self, "list").append(unwrap(val))
        return self

    def __str__(self):
        return _get(self, "list").__str__()

    def __len__(self):
        return _get(self, "list").__len__()

    @property
    def __class__(self):
        return list

    def __getslice__(self, i, j):
        from .logs import Log

        Log.error("slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes.  Use [start:stop:step]")

    def remove(self, x):
        _get(self, "list").remove(x)
        return self

    def extend(self, values):
        for v in values:
            _get(self, "list").append(unwrap(v))
        return self

    def pop(self):
        return _get(self, "list").pop()

    def __add__(self, value):
        output = list(_get(self, "list"))
        output.extend(value)
        return StructList(vals=output)

    def __or__(self, value):
        output = list(_get(self, "list"))
        output.append(value)
        return StructList(vals=output)

    def right(self, num=None):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT
        """
        if num == None:
            return StructList([_get(self, "list")[-1]])
        if num <= 0:
            return EmptyList
        return StructList(_get(self, "list")[-num])

    def last(self):
        """
        RETURN LAST ELEMENT IN StructList
        """
        if _get(self, "list"):
            return wrap(_get(self, "list")[-1])
        return Null

    def __getattribute__(self, key):
        try:
            output = _get(self, key)
            return output
        except Exception, e:
            return StructList([v[key] for v in _get(self, "list")])

def wrap(v):
    if v is None:
        return Null
    if isinstance(v, (Struct, _Null, StructList)):
        return v
    if isinstance(v, dict):
        m = Struct()
        object.__setattr__(m, "__dict__", v)  # INJECT m.__dict__=v SO THERE IS NO COPY
        return m
    if isinstance(v, list):
        return StructList(v)
    return v


def unwrap(v):
    if isinstance(v, Struct):
        return _get(v, "__dict__")
    if isinstance(v, StructList):
        return v.list
    if v == None:
        return None
    return v


def inverse(d):
    """
    reverse the k:v pairs
    """
    output = {}
    for k, v in unwrap(d).iteritems():
        output[v] = output.get(v, [])
        output[v].append(k)
    return output


def nvl(*args):
    #pick the first none-null value
    for a in args:
        if a != None:
            return a
    return Null


def listwrap(value):
    """
    OFTEN IT IS NICE TO ALLOW FUNCTION PARAMETERS TO BE ASSIGNED A VALUE,
    OR A list-OF-VALUES, OR NULL.  CHECKING FOR THIS IS TEDIOUS AND WE WANT TO CAST
    FROM THOSE THREE CASES TO THE SINGLE CASE OF A LIST

    Null -> []
    value -> [value]
    [...] -> [...]  (unchanged list)

    #BEFORE
    if a is not None:
        if not isinstance(a, list):
            a=[a]
        for x in a:
            #do something


    #AFTER
    for x in listwrap(a):
        #do something

    """
    if value == None:
        return []
    elif isinstance(value, list):
        return wrap(value)
    else:
        return wrap([value])


def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field.find(".") >= 0:
        field = field.replace("\.", "\a")
        return [k.replace("\a", "\.") for k in field.split(".")]
    else:
        return [field]




