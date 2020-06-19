from __future__ import absolute_import
import time
import unittest

import tornado.gen

import nsq

from .test_reader import IntegrationBase


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


class WriterIntegrationTest(IntegrationBase):
    identify_options = {
        'user_agent': 'sup',
        'heartbeat_interval': 10,
        'output_buffer_size': 4096,
        'output_buffer_timeout': 50
    }

    nsqd_command = ['nsqd', '--verbose']

    def test_writer_mpub_one(self):
        topic = 'test_writer_mpub_%s' % time.time()

        w = nsq.Writer(nsqd_tcp_addresses=['127.0.0.1:4150'], **self.identify_options)

        def trypub():
            w.mpub(topic, b'{"one": 1}', callback=pubcb)

        def pubcb(conn, data):
            if isinstance(data, nsq.protocol.Error):
                if 'no open connections' in str(data):
                    self.io_loop.call_later(0.1, trypub)
                    return
            self.stop(data)

        self.io_loop.call_later(0.1, trypub)
        result = self.wait()
        print(str(result))
        assert not isinstance(result, Exception)

    def test_writer_await_pub(self):
        topic = 'test_writer_mpub_%s' % time.time()

        w = nsq.Writer(nsqd_tcp_addresses=['127.0.0.1:4150'], **self.identify_options)

        @tornado.gen.coroutine
        def trypub():
            yield w.pub(topic, b'{"one": 1}')
            yield w.pub(topic, b'{"two": 2}')
            self.stop("OK")

        self.io_loop.call_later(0.1, trypub)
        result = self.wait()
        print(str(result))
        assert result == "OK"
