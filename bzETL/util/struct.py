################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################



import copy
import functools

SPECIAL=["keys", "values", "items", "dict",  "copy"]



class Struct(dict):
    """
    Struct is an anonymous class with some properties good for manipulating JSON

    0) a.b==a["b"]
    1) the IDE does tab completion, so my spelling mistakes do not get found at runtime
    2) it deals with missing keys gracefully, so I can put it into set operations (database operations) without choking
    2b) missing keys is important when dealing with JSON, which is often almost anything
    3) also, which I hardly use, is storing JSON paths in a variable, so :   a["b.c"]==a.b.c

    """

    
    def __init__(self, **map):
        dict.__init__(self)
        object.__setattr__(self, "__dict__", map)  #map IS A COPY OF THE PARAMETERS

    def __str__(self):
        return dict.__str__(object.__getattribute__(self, "__dict__"))
    
    def __getitem__(self, key):
        d=object.__getattribute__(self, "__dict__")

        if key.find(".")>=0:
            for n in key.split("."):
                d=d[n]
            return wrap(d)

        if key not in d: return None
        return wrap(d[key])

    def __setitem__(self, key, value):
        return Struct.__setattr__(self, key, value)

    def __getattribute__(self, key):
        d=object.__getattribute__(self, "__dict__")
        if key not in SPECIAL:
            if key not in d: return None
            return wrap(d[key])

        #SOME dict FUNCTIONS
        if key in ["keys", "values", "items"]:
            return dict.__getattribute__(d, key)
        if key=="dict":
            return d
        if key=="copy":
            return functools.partial(object.__getattribute__(Struct, "copy"), self)



    def __setattr__(self, key, value):
        try:
            d=object.__getattribute__(self, "__dict__")
            value=unwrap(value)
            
            if key.find(".")>=0:
                seq=key.split(".")
                for k in seq[0,-1]: d=d[k]
                d[seq[-1]]=value
                return self
            d[key]=value
        except Exception, e:
            if key.find(".")>=0:
                seq=key.split(".")
                for k in seq[0,-1]: d=d[k]
                d[seq[-1]]=value
                return self
            d[key]=value
            raise e


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

    def copy(self):
        d=object.__getattribute__(self, "__dict__")
        return wrap(copy.deepcopy(d))




class StructList(list):

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        if vals is None:
            self.list=[]
        elif isinstance(vals, StructList):
            self.list=vals.list
        else:
            self.list=vals

    def __getitem__(self, index):
        if index >= len(self.list):
            return None
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
    if v is None:
        return None
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
    return v
