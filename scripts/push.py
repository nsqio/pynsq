#!/usr/bin/env python2.7

import sys
sys.path.insert(0, '..')

from nsq.sync import SyncConn

features = { }
s = SyncConn(features=features)

#s.send_pub('test', 'Test @ 1153')
s.send_mpub('test', ['Message 1', 'Message 2'])
