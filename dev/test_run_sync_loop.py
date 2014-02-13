#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '.')

from nsq.nsq import subscribe, ready
from nsq.sync import SyncConn

print("Opening connection.")
sync = SyncConn()

print("Subscribing.")
sync.send(subscribe('test_topic', 'test_channel'))

print("Sending ready.")
sync.send(ready(10))

print("Waiting for response.")
sync.run_loop(one_response=True)

print("Running receive loop.")
sync.run_loop()

