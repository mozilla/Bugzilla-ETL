import thread
from threading import Lock
import sys
import threading
from bzETL.util import threads
from bzETL.util.randoms import Random


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
            sys.stdout.write("hi " + unicode(i) + "\n")
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
