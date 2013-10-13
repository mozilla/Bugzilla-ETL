################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
from .struct import Null, wrap


def nvl(*args):
    #pick the first none-null value
    for a in args:
        if a is not None and a != Null: return a
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
    if value == Null:
        return []
    elif isinstance(value, list):
        return wrap(value)
    else:
        return wrap([value])
