################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################

SPECIAL=["keys", "values", "items", "dict",  "copy"]



class Struct(dict):
    """
    Struct is an anonymous class with some properties good for manipulating JSON

    0) a.b==a["b"]
    1) the IDE does tab completion, so my spelling mistakes do not get found at runtime
    2) it deals with missing keys gracefully, so I can put it into set operations (database operations) without choking
    2b) missing keys is important when dealing with JSON, which is often almost anything
    3) also, which I hardly use, is storing JSON paths in a variable, so :   a["b.c"]==a.b.c

    MORE ON MISSING VALUES: http://www.numpy.org/NA-overview.html
    IT ONLY CONSIDERS THE STATISTICAL NULL, WHICH LOOKS AT LEGITIMATE-FIELD-WITH-MISSING-VALUE (Statistical Null)
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
            key=key.replace("\\.", "\a")
            seq=[k.replace("\a", ".") for k in key.split(".")]
            for n in seq:
                d=d[n]
            return wrap(d)

        if key not in d: return Null
        return wrap(d[key])

    def __setitem__(self, key, value):
        try:
            d=object.__getattribute__(self, "__dict__")
            value=unwrap(value)
            if key.find(".") == -1:
                if value == Null:
                    del d[key]
                else:
                    d[key] = value
                return self

            key=key.replace("\.", "\a")
            seq=[k.replace("\a", ".") for k in key.split(".")]
            for k in seq[:-1]: d=d[k]
            if value == Null:
                del d[seq[-1]]
            else:
                d[seq[-1]] = value
            return self
        except Exception, e:
            raise e

    def __getattribute__(self, key):
        d=object.__getattribute__(self, "__dict__")
        if key not in SPECIAL:
            if key not in d: return Null
            return wrap(d[key])

        #SOME dict FUNCTIONS
        if key in ["keys", "values", "items"]:
            return dict.__getattribute__(d, key)
        if key=="dict":
            return d
        if key=="copy":
            o = wrap({k: v for k, v in d.items()})
            def output():
                return o
            return output



    def __setattr__(self, key, value):
        dict.__setattr__(self, unicode(key), value)


    def __delitem__(self, key):
        d=object.__getattribute__(self, "__dict__")

        if key.find(".")>=0:
            seq=key.split(".")
            for k in seq[0,-1]: d=d[k]
            del d[seq[-1]]
            return
        del d[key]

    def keys(self):
        d=object.__getattribute__(self, "__dict__")
        return d.keys()


# KEEP TRACK OF WHAT ATTRIBUTES ARE REQUESTED, MAYBE SOME (BUILTIN) ARE STILL USEFUL
requested = set()


class NullStruct(object):
    """
    STRUCTURAL NULL:
        Null[x] == Null
        Null.x == Null

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
        return id(Null) == id(other)

    def __ne__(self, other):
        return id(Null) != id(other)


    def __getitem__(self, key):
        return self




    def __getattribute__(self, key):
        requested.add(key)
        return self

    def keys(self):
        return set()

    def __str__(self):
        return "None"


Null = NullStruct()


class StructList(list):

    def __init__(self, vals=Null):
        """ USE THE vals, NOT A COPY """
        if vals == Null:
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
        self.list[i]=y

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
        self.list.extend(values)
        return self


def wrap(v):
    if v is None or v == Null:
        return Null
    if isinstance(v, Struct):
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
        return v.dict
    if v == Null:
        return None
    return v
