#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

from time import time

import pub
from sync import SyncConn

print("Opening connection.")
sync = SyncConn()

print("Publishing message.")
data = ('test data: %d' % (time()))
sync.send(pub('test_topic', data))

print("Waiting for response.")
data = sync.run_loop(one_response=True)
if data is None:
    raise ValueError("We expected a response and didn't get one.")

print("Response: %s" % (data))

