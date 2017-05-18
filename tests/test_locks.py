# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import sys
import threading
from pyLibrary.thread import threads
from pyLibrary.maths.randoms import Random


num_thread = 800
num_loop = 1000
lock = threads.Lock()
state = {"count": 0}


def locker(i):
    if Random.int(2) == 0:
        with lock:
            return locker(i)
    else:
        with lock:
            sys.stdout.write("hi " + str(i) + "\n")
            state["count"] += 1


def lock_loop():
    for i in range(num_loop):
        locker(i)


def test_locks():
    t = []
    for i in range(num_thread):
        thread = threading.Thread(target=lock_loop)
        thread.start()
        t.append(thread)

    for i in range(num_thread):
        t[i].join()

    assert state["count"] == num_thread * num_loop

    sys.stdout.write("ok\n")

if __name__=="__main__":
    test_locks()
