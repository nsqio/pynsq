#!/usr/bin/env python2.7

from nsq import pub
from nsq.sync import SyncConn

print("Opening connection.")
sync = SyncConn()

print("Publishing message.")
sync.send(pub('test_topic', 'test_data_0857'))

print("Waiting for response.")
sync.run_loop(one_response=True)

