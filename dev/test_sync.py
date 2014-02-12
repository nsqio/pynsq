#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

from time import time

from nsq import pub
from nsq.sync import SyncConn

print("Opening connection.")
sync = SyncConn()

print("Publishing message.")
data = ('test data: %d' % (time()))
sync.send(pub('test_topic', data))

print("Waiting for response.")
sync.run_loop(one_response=True)

