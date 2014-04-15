# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from types import NoneType, GeneratorType

_get = object.__getattribute__

DEBUG = False


class Struct(dict):
    """
    Struct is an anonymous class with some properties good for manipulating JSON

    0) a.b==a["b"]
    1) the IDE does tab completion, and my spelling mistakes get found at "compile time"
    2) it deals with missing keys gracefully, so I can put it into set operations (database
       operations) without choking
       a = wrap({})
       > a == {}
       a.b is Null
       > True
       a.b.c == None
       > True
    2b) missing keys is important when dealing with JSON, which is often almost anything
    3) you can access paths as a variable:   a["b.c"]==a.b.c
    4) you can set paths to values, missing objects along the path are created:
       a = wrap({})
       > a == {}
       a["b.c"] = 42
       > a == {"b": {"c": 42}}
    5) attribute names (keys) are corrected to unicode - it appears Python object.getattribute()
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
        USE wrap() INSTEAD
        """
        dict.__init__(self)
        if DEBUG:
            d = _get(self, "__dict__")
            for k, v in map.items():
                d[literal_field(k)] = unwrap(v)
        else:
            object.__setattr__(self, "__dict__", map)

    def __bool__(self):
        return True

    def __nonzero__(self):
        d = _get(self, "__dict__")
        return True if d else False

    def __str__(self):
        try:
            return "Struct("+dict.__str__(_get(self, "__dict__"))+")"
        except Exception, e:
            return "{}"

    def __repr__(self):
        try:
            return "Struct("+dict.__repr__(_get(self, "__dict__"))+")"
        except Exception, e:
            return "Struct{}"

    def __contains__(self, item):
        if Struct.__getitem__(self, item):
            return True
        return False

    def __getitem__(self, key):
        if isinstance(key, str):
            key = key.decode("utf8")

        d = _get(self, "__dict__")

        if key.find(".") >= 0:
            seq = split_field(key)
            for n in seq:
                d = _getdefault(d, n)
            return wrap(d)

        o = d.get(key, None)
        if o == None:
            return NullType(d, key)
        return wrap(o)

    def __setitem__(self, key, value):
        if key == "":
            from .env.logs import Log

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

            seq = split_field(key)
            for k in seq[:-1]:
                d = _getdefault(d, k)
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
            return wrap(output)
        except Exception:
            d = _get(self, "__dict__")
            return NullType(d, key)

    def __setattr__(self, key, value):
        if isinstance(key, str):
            ukey = key.decode("utf8")
        else:
            ukey = key

        value = unwrap(value)
        if value is None:
            d = _get(self, "__dict__")
            d.pop(key, None)
        else:
            object.__setattr__(self, ukey, value)
        return self

    def __hash__(self):
        d = _get(self, "__dict__")
        return hash_value(d)

    def __eq__(self, other):
        if not isinstance(other, dict):
            return False
        e = unwrap(other)
        d = _get(self, "__dict__")
        for k, v in d.items():
            if e.get(k, None) != v:
                return False
        for k, v in e.items():
            if d.get(k, None) != v:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def get(self, key, default):
        d = _get(self, "__dict__")
        return d.get(key, default)

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
        seq = split_field(key)
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

    def setdefault(self, k, d=None):
        if self[k] == None:
            self[k]=d

# KEEP TRACK OF WHAT ATTRIBUTES ARE REQUESTED, MAYBE SOME (BUILTIN) ARE STILL USEFUL
requested = set()


def _str(value, depth):
    """
    FOR DEBUGGING POSSIBLY RECURSIVE STRUCTURES
    """
    output = []
    if depth >0 and isinstance(value, dict):
        for k, v in value.items():
            output.append(str(k) + "=" + _str(v, depth - 1))
        return "{" + ",\n".join(output) + "}"
    elif depth >0 and isinstance(value, list):
        for v in value:
            output.append(_str(v, depth-1))
        return "[" + ",\n".join(output) + "]"
    else:
        return str(type(value))


def _setdefault(obj, key, value):
    """
    DO NOT USE __dict__.setdefault(obj, key, value), IT DOES NOT CHECK FOR obj[key] == None
    """
    v = obj.get(key, None)
    if v == None:
        obj[key] = value
        return value
    return v


def set_default(original, default):
    return wrap(_all_default(unwrap(original), unwrap(default)))


def _all_default(d, default):
    """
    ANY VALUE NOT SET WILL BE SET BY THE default
    THIS IS RECURSIVE
    """
    if default is None:
        return
    for k, default_value in default.items():
        existing_value = d.get(k, None)
        if existing_value is None:
            d[k] = default_value
        elif isinstance(existing_value, dict) and isinstance(default_value, dict):
            _all_default(existing_value, default_value)


def _getdefault(obj, key):
    try:
        return obj[key]
    except Exception, e:
        return NullType(obj, key)


def _assign(obj, path, value, force=True):
    """
    value IS ASSIGNED TO obj[self.path][key]
    force=False IF YOU PREFER TO use setDefault()
    """
    if isinstance(obj, NullType):
        d = _get(obj, "__dict__")
        o = d["obj"]
        p = d["path"]
        s = split_field(p)+path
        return _assign(o, s, value)

    path0 = path[0]

    if len(path) == 1:
        if force:
            obj[path0] = value
        else:
            _setdefault(obj, path0, value)
        return

    old_value = obj.get(path0, None)
    if old_value == None:
        if value == None:
            return
        else:
            old_value = {}
            obj[path0] = old_value
    _assign(old_value, path[1:], value)


class NullType(object):
    """
    Structural Null provides closure under the dot (.) operator
        Null[x] == Null
        Null.x == Null

    Null INSTANCES WILL TRACK THE
    """

    def __init__(self, obj=None, path=None):
        d = _get(self, "__dict__")
        d["obj"] = obj
        d["path"] = path

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def __add__(self, other):
        return Null

    def __radd__(self, other):
        return Null

    def __sub__(self, other):
        return Null

    def __rsub__(self, other):
        return Null

    def __mul__(self, other):
        return Null

    def __rmul__(self, other):
        return Null

    def __div__(self, other):
        return Null

    def __rdiv__(self, other):
        return Null

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return False

    def __le__(self, other):
        return False

    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return other is None or isinstance(other, NullType)

    def __ne__(self, other):
        return other is not None and not isinstance(other, NullType)

    def __getitem__(self, key):
        return NullType(self, key)

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
            return NullType(self, key)

    def __setattr__(self, key, value):
        NullType.__setitem__(self, key, value)

    def __setitem__(self, key, value):
        try:
            d = _get(self, "__dict__")
            o = d["obj"]
            path = d["path"]
            seq = split_field(path)+split_field(key)

            _assign(o, seq, value)
        except Exception, e:
            raise e

    def keys(self):
        return set()

    def items(self):
        return []

    def pop(self, key, default=None):
        return Null

    def __str__(self):
        return "None"

    def __repr__(self):
        return "Null"


Null = NullType()
EmptyList = Null

ZeroList = []
def return_zero_list():
    return []

def return_zero_set():
    return set()




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
                from .env.logs import Log
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

    def __getattribute__(self, key):
        try:
            if key != "index":  # WE DO NOT WANT TO IMPLEMENT THE index METHOD
                output = _get(self, key)
                return output
        except Exception, e:
            pass
        return StructList([v.get(key, None) for v in _get(self, "list")])

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

    def __getslice__(self, i, j):
        from .env.logs import Log

        Log.error("slicing is broken in Python 2.7: a[i:j] == a[i+len(a), j] sometimes.  Use [start:stop:step]")

    def copy(self):
        return StructList(list(_get(self, "list")))

    def remove(self, x):
        _get(self, "list").remove(x)
        return self

    def extend(self, values):
        for v in values:
            _get(self, "list").append(unwrap(v))
        return self

    def pop(self):
        return wrap(_get(self, "list").pop())

    def __add__(self, value):
        output = list(_get(self, "list"))
        output.extend(value)
        return StructList(vals=output)

    def __or__(self, value):
        output = list(_get(self, "list"))
        output.append(value)
        return StructList(vals=output)

    def __radd__(self, other):
        output = list(other)
        output.extend(_get(self, "list"))
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

    def leftBut(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        if num == None:
            return StructList([_get(self, "list")[:-1:]])
        if num <= 0:
            return EmptyList
        return StructList(_get(self, "list")[:-num:])

    def last(self):
        """
        RETURN LAST ELEMENT IN StructList
        """
        if _get(self, "list"):
            return wrap(_get(self, "list")[-1])
        return Null

    def map(self, oper, includeNone=True):
        if includeNone:
            return StructList([oper(v) for v in _get(self, "list")])
        else:
            return StructList([oper(v) for v in _get(self, "list") if v != None])


def wrap(v):
    type = v.__class__

    if type is Struct:
        return v
    elif type is dict:
        m = Struct()
        object.__setattr__(m, "__dict__", v)  # INJECT m.__dict__=v SO THERE IS NO COPY
        return m
    elif type is StructList:
        return v
    elif type is list:
        if DEBUG:
            for sv in v:
                # IN PRACTICE WE DO NOT EXPECT TO GO THROUGH THIS LIST, IF ANY ARE WRAPPED, THE FIRST IS PROBABLY WRAPPED
                # WARNING!  THIS IS VERY SLOW
                if isinstance(sv, (Struct, StructList)):
                    from .env.logs import Log
                    Log.warning("Please unwrap members of list before wrapping list.  Fixed for now.")

                    #MUST KEEP THE LIST
                    temp = [unwrap(ssv) for ssv in v]
                    del v[:]
                    v.extend(temp)
                    return StructList(v)
        return StructList(v)
    elif type is GeneratorType:
        return (wrap(vv) for vv in v)
    elif v is None:
        return Null
    else:
        return v


def unwrap(v):
    type = v.__class__
    if type is Struct:
        d = _get(v, "__dict__")
        return d
    elif type is StructList:
        return v.list
    elif type is NullType:
        return None
    elif type is GeneratorType:
        return (unwrap(vv) for vv in v)
    else:
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

def zip(keys, values):
    output = Struct()
    for i, k in enumerate(keys):
        output[k] = values[i]
    return output

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
        return wrap([unwrap(value)])


def tuplewrap(value):
    """
    INTENDED TO TURN lists INTO tuples FOR USE AS KEYS
    """
    if isinstance(value, (list, set, tuple, GeneratorType)):
        return tuple(tuplewrap(v) if isinstance(v, (list, tuple, GeneratorType)) else v for v in value)
    return unwrap(value),



def literal_field(field):
    """
    RETURN SAME WITH . ESCAPED
    """
    return field.replace(".", "\.")

def cpython_split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field.find(".") >= 0:
        field = field.replace("\.", "\a")
        return [k.replace("\a", ".") for k in field.split(".")]
    else:
        return [field]

def pypy_split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    from .jsons import UnicodeBuilder

    if not field:
        return []

    output = []
    curr = UnicodeBuilder()
    i = 0
    while i < len(field):
        c = field[i]
        i += 1
        if c == "\\":
            c = field[i]
            i += 1
            if c == ".":
                curr.append(".")
            else:
                curr.append("\\")
                curr.append(c)
        elif c == ".":
            output.append(curr.build())
            curr = UnicodeBuilder()
    output.append(curr.build())
    return output

# try:
#     import __pypy__
#     split_field = pypy_split_field
# except ImportError:
split_field = cpython_split_field


def join_field(field):
    """
    RETURN field SEQUENCE AS STRING
    """
    return ".".join([f.replace(".", "\.") for f in field])


def hash_value(v):
    if isinstance(v, (set, tuple, list)):
        return hash(tuple(hash_value(vv) for vv in v))
    elif not isinstance(v, dict):
        return hash(v)
    else:
        return hash(tuple(sorted(hash_value(vv) for vv in v.values())))


