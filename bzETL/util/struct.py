#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

SPECIAL=["keys", "values", "items", "iteritems", "dict",  "copy"]



class Struct(dict):
    """
    Struct is an anonymous class with some properties good for manipulating JSON

    0) a.b==a["b"]
    1) the IDE does tab completion, so my spelling mistakes get found at "compile time"
    2) it deals with missing keys gracefully, so I can put it into set operations (database operations) without choking
    2b) missing keys is important when dealing with JSON, which is often almost anything
    3) also, which I hardly use, is storing JSON paths in a variable, so :   a["b.c"]==a.b.c

    MORE ON MISSING VALUES: http://www.numpy.org/NA-overview.html
    IT ONLY CONSIDERS THE LEGITIMATE-FIELD-WITH-MISSING-VALUE (Statistical Null)
    AND DOES NOT LOOK AT FIELD-DOES-NOT-EXIST-IN-THIS-CONTEXT (Database Null)
    """

    
    def __init__(self, **map):
        dict.__init__(self)
        object.__setattr__(self, "__dict__", map)  #map IS A COPY OF THE PARAMETERS

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __str__(self):
        return dict.__str__(object.__getattribute__(self, "__dict__"))
    
    def __getitem__(self, key):
        d=object.__getattribute__(self, "__dict__")

        if key.find(".")>=0:
            key=key.replace("\.", "\a")
            seq=[k.replace("\a", ".") for k in key.split(".")]
            for n in seq:
                d=d.get(n, None)
                if d == None:
                    return Null
                d=unwrap(d)
            return wrap(d)

        return wrap(d.get(key, Null))

    def __setitem__(self, key, value):
        try:
            d=object.__getattribute__(self, "__dict__")
            value=unwrap(value)
            if key.find(".") == -1:
                if value is None:
                    d.pop(key, None)
                else:
                    d[key] = value
                return self

            key=key.replace("\.", "\a")
            seq=[k.replace("\a", ".") for k in key.split(".")]
            for k in seq[:-1]:
                d = d[k]
            if value == None:
                d.pop(seq[-1], None)
            else:
                d[seq[-1]] = value
            return self
        except Exception, e:
            raise e

    def __getattribute__(self, key):
        d=object.__getattribute__(self, "__dict__")
        if key not in SPECIAL:
            return wrap(d.get(key, Null))

        #SOME dict FUNCTIONS
        if key == "items":
            def temp():
                _is = dict.__getattribute__(d, "items")
                return [(k, wrap(v)) for k, v in _is()]
            return temp
        if key == "iteritems":
            #LOW LEVEL ITERATION
            return d.iteritems
        if key=="keys":
            def temp():
                k=dict.__getattribute__(d, "keys")
                return set(k())
            return temp
        if key=="values":
            def temp():
                vs=dict.__getattribute__(d, "values")
                return [wrap(v) for v in vs()]
            return temp
        if key=="dict":
            return d
        if key=="copy":
            o = wrap({k: v for k, v in d.items()})
            def output():
                return o
            return output

    def __setattr__(self, key, value):
        Struct.__setitem__(self, key, value)
        # dict.__setattr__(self, unicode(key), value)


    def __delitem__(self, key):
        d=object.__getattribute__(self, "__dict__")

        if key.find(".") == -1:
            d.pop(key, None)

        key=key.replace("\.", "\a")
        seq=[k.replace("\a", ".") for k in key.split(".")]
        for k in seq[:-1]:
            d = d[k]
        d.pop(seq[-1], None)

    def keys(self):
        d=object.__getattribute__(self, "__dict__")
        return d.keys()


# KEEP TRACK OF WHAT ATTRIBUTES ARE REQUESTED, MAYBE SOME (BUILTIN) ARE STILL USEFUL
requested = set()


class NullStruct(object):
    """
    Structural Null provides closure under the dot (.) operator
        Null[x] == None
        Null.x == None



    """

    def __init__(self):
        pass

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
        return other is Null or other is None

    def __ne__(self, other):
        return other is not Null and other is not None

    def __getitem__(self, key):
        return self

    def __len__(self):
        return 0

    def __iter__(self):
        return ZeroList.__iter__()

    def __getattribute__(self, key):
        if key not in SPECIAL:
            return Null

        #SOME dict FUNCTIONS
        if key == "items":
            def temp():
                return ZeroList
            return temp
        if key == "iteritems":
            #LOW LEVEL ITERATION
            return self.__iter__()
        if key=="keys":
            def temp():
                return ZeroList
            return temp
        if key=="values":
            def temp():
                return ZeroList
            return temp
        if key=="dict":
            return Null
        if key=="copy":
            #THE INTENT IS USUALLY PREPARE FOR UPDATES
            def output():
                return Struct()
            return output

    def keys(self):
        return set()

    def __str__(self):
        return "None"


Null = NullStruct()

ZeroList=[]


class StructList(list):

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        list.__init__(self)
        if vals == None:
            self.list=[]
        elif isinstance(vals, StructList):
            self.list=vals.list
        else:
            self.list=vals

    def __getitem__(self, index):
        if index < 0 or len(self.list) <= index:
            return Null
        return wrap(self.list[index])

    def __setitem__(self, i, y):
        self.list[i]=unwrap(y)

    def __iter__(self):
        i=self.list.__iter__()
        while True:
            yield wrap(i.next())

    def append(self, val):
        self.list.append(unwrap(val))
        return self

    def __str__(self):
        return self.list.__str__()

    def __len__(self):
        return self.list.__len__()

    def __getslice__(self, i, j):
        return wrap(self.list[i:j])

    def remove(self, x):
        self.list.remove(x)
        return self

    def extend(self, values):
        for v in values:
            self.list.append(unwrap(v))
        return self


def wrap(v):
    if v == None:
        return Null
    if isinstance(v, (Struct, StructList)):
        return v
    if isinstance(v, dict):
        m = Struct()
        object.__setattr__(m, "__dict__", v) #INJECT m.__dict__=v SO THERE IS NO COPY
        return m
    if isinstance(v, list):
        return StructList(v)
    return v


def unwrap(v):
    if isinstance(v, Struct):
        return object.__getattribute__(v, "__dict__")
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
    return wrap(output)




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
