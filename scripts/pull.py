#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

from nsq.sync import SyncConn

def processor(message):
    print("Processor received: %s %s" % (message, message.body))
    message.finish()

features = { }
s = SyncConn(features=features, message_processor=processor)

s.send_sub('test', 'test_channel')
s.send_rdy(10)

try:
    s.run_loop(reason='long running')
except KeyboardInterrupt:
    print("Closing connection.")
    s.send_cls()

    print("Ended via break.")
