################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
from .struct import Null

from .logs import Log

class multiset():

    def __init__(self, list=Null, key_field=Null, count_field=Null):
        if not key_field and not count_field:
            self.dic = dict()
            for i in list:
                self.add(i)
            return
        else:
            self.dic={i[key_field]:i[count_field] for i in list}
        

    def __iter__(self):
        for k, m in self.dic.items():
            for i in range(m):
                yield k


    def items(self):
        return self.dic.items()

    def keys(self):
        return self.dic.keys()

    def add(self, value):
        if value in self.dic:
            self.dic[value]+=1
        else:
            self.dic[value]=1

    def extend(self, values):
        for v in values:
            self.add(v)

    def remove(self, value):
        if value not in self.dic:
            Log.error("{{value}} is not in multiset", {"value":value})

        count=self.dic[value]
        count-=1
        if count==0:
            del(self.dic[value])
        else:
            self.dic[value]=count

    def __len__(self):
        return sum(self.dic.values())


    def count(self, value):
        if value in self.dic:
            return self.dic[value]
        else:
            return 0
