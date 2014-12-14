from __future__ import absolute_import

import nsq
import unittest


class WriterUnitTest(unittest.TestCase):

    def setUp(self):
        super(WriterUnitTest, self).setUp()

    def test_constructor(self):
        name = 'test'
        reconnect_interval = 10.0
        writer = nsq.Writer(nsqd_tcp_addresses=['127.0.0.1:4150'],
                            reconnect_interval=reconnect_interval,
                            name=name)
        self.assertEqual(writer.name, name)
        self.assertEqual(0, len(writer.conn_kwargs))
        self.assertEqual(writer.reconnect_interval, reconnect_interval)

    def test_bad_writer_arguments(self):
        bad_options = dict(foo=10)

        self.assertRaises(
            AssertionError,
            nsq.Writer,
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            reconnect_interval=15.0,
            name='test', **bad_options)
