# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import functools
import random

import tornado.concurrent

from ._compat import string_types
from ._compat import bytes_types
from ._compat import func_args
from .client import Client
from .conn import AsyncConn
from . import protocol

logger = logging.getLogger(__name__)


def make_exc_future(exc):
    fut = tornado.concurrent.Future()
    fut.set_exception(exc)
    fut.exception()  # suppress not-yielded-future-exception warning
    return fut


class Writer(Client):
    r"""
    A high-level producer class built on top of the `Tornado IOLoop <http://tornadoweb.org>`_
    supporting async publishing (``PUB`` & ``MPUB`` & ``DPUB``) of messages
    to ``nsqd`` over the TCP protocol.

    Example publishing a message repeatedly using a Tornado IOLoop periodic callback::

        import nsq
        import tornado.ioloop
        import time

        def pub_message():
            writer.pub('test', time.strftime('%H:%M:%S'), finish_pub)

        def finish_pub(conn, data):
            print(data)

        writer = nsq.Writer(['127.0.0.1:4150'])
        tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
        nsq.run()

    Example publishing a message from a Tornado HTTP request handler::

        import functools
        import tornado.httpserver
        import tornado.ioloop
        import tornado.options
        import tornado.web
        from nsq import Writer, Error
        from tornado.options import define, options

        class MainHandler(tornado.web.RequestHandler):
            @property
            def nsq(self):
                return self.application.nsq

            def get(self):
                topic = 'log'
                msg = b'Hello world'
                msg_cn = u'Hello 世界'.encode('utf-8')

                self.nsq.pub(topic, msg)
                self.nsq.mpub(topic, [msg, msg_cn])
                self.nsq.dpub(topic, 60000, msg)

                # customize callback
                callback = functools.partial(self.finish_pub, topic=topic, msg=msg)
                self.nsq.pub(topic, msg, callback=callback)

                self.write(msg)

            def finish_pub(self, conn, data, topic, msg):
                if isinstance(data, Error):
                    # try to re-pub message again if pub failed
                    # (missing functionality: delay retry, limit attempts)
                    self.nsq.pub(topic, msg)

        class Application(tornado.web.Application):
            def __init__(self, handlers, **settings):
                self.nsq = Writer(['127.0.0.1:4150'])
                super(Application, self).__init__(handlers, **settings)

    :param nsqd_tcp_addresses: a sequence with elements of the form 'address:port' corresponding
        to the ``nsqd`` instances this writer should publish to

    :param name: a string that is used for logging messages (defaults to first nsqd address)

    :param \*\*kwargs: passed to :class:`nsq.AsyncConn` initialization
    """
    def __init__(self, nsqd_tcp_addresses, reconnect_interval=15.0, name=None, **kwargs):
        super(Writer, self).__init__(**kwargs)

        if not isinstance(nsqd_tcp_addresses, (list, set, tuple)):
            assert isinstance(nsqd_tcp_addresses, string_types)
            nsqd_tcp_addresses = [nsqd_tcp_addresses]
        assert nsqd_tcp_addresses

        self.name = name or nsqd_tcp_addresses[0]
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.conns = {}

        # Verify keyword arguments
        valid_args = func_args(AsyncConn.__init__)
        diff = set(kwargs) - set(valid_args)
        assert len(diff) == 0, 'Invalid keyword argument(s): %s' % list(diff)

        self.conn_kwargs = kwargs
        assert isinstance(reconnect_interval, (int, float))
        self.reconnect_interval = reconnect_interval

        self.io_loop.add_callback(self._run)

    def _run(self):
        logger.info('starting writer...')
        self.connect()

    def pub(self, topic, msg, callback=None):
        """
        publish a message to nsq

        :param topic: nsq topic
        :param msg: message body (bytes)
        :param callback: function which takes (conn, data) (data may be nsq.Error)
        """
        return self._pub('pub', topic, msg, callback=callback)

    def mpub(self, topic, msg, callback=None):
        """
        publish multiple messages in one command (efficiently)

        :param topic: nsq topic
        :param msg: list of messages bodies (which are bytes)
        :param callback: function which takes (conn, data) (data may be nsq.Error)
        """
        if isinstance(msg, bytes_types):
            msg = [msg]
        assert isinstance(msg, (list, set, tuple))

        return self._pub('mpub', topic, msg, callback=callback)

    def dpub(self, topic, delay_ms, msg, callback=None):
        """
        publish multiple messages in one command (efficiently)

        :param topic: nsq topic
        :param delay_ms: tell nsqd to delay delivery for this long (integer milliseconds)
        :param msg: message body (bytes)
        :param callback: function which takes (conn, data) (data may be nsq.Error)
        """
        return self._pub('dpub', topic, msg, delay_ms, callback=callback)

    def _pub(self, command, topic, msg, delay_ms=None, callback=None):
        if not callback:
            callback = functools.partial(self._finish_pub,
                                         command=command, topic=topic, msg=msg)
        open_connections = [
            conn for conn in self.conns.values()
            if conn.connected()
        ]
        if not open_connections:
            exc = protocol.SendError('no open connections')
            callback(None, exc)
            return make_exc_future(exc)

        conn = random.choice(open_connections)
        conn.callback_queue.append(callback)
        cmd = getattr(protocol, command)

        if command == 'dpub':
            args = (topic, delay_ms, msg)
        else:
            args = (topic, msg)

        try:
            return conn.send(cmd(*args))
        except Exception as e:
            logger.exception('[%s] failed to send %s' % (conn.id, command))
            exc = protocol.SendError('send error', e)
            callback(None, exc)
            conn.close()
            return make_exc_future(exc)

    def _on_connection_error(self, conn, error, **kwargs):
        super(Writer, self)._on_connection_error(conn, error, **kwargs)
        while conn.callback_queue:
            callback = conn.callback_queue.pop(0)
            callback(conn, error)

    def _on_connection_response(self, conn, data=None, **kwargs):
        if conn.callback_queue:
            callback = conn.callback_queue.pop(0)
            callback(conn, data)

    def connect(self):
        for addr in self.nsqd_tcp_addresses:
            host, port = addr.split(':')
            self.connect_to_nsqd(host, int(port))

    def connect_to_nsqd(self, host, port):
        assert isinstance(host, string_types)
        assert isinstance(port, int)

        conn = AsyncConn(host, port, **self.conn_kwargs)
        conn.on('identify', self._on_connection_identify)
        conn.on('identify_response', self._on_connection_identify_response)
        conn.on('auth', self._on_connection_auth)
        conn.on('auth_response', self._on_connection_auth_response)
        conn.on('error', self._on_connection_error)
        conn.on('response', self._on_connection_response)
        conn.on('close', self._on_connection_close)
        conn.on('ready', self._on_connection_ready)
        conn.on('heartbeat', self.heartbeat)

        if conn.id in self.conns:
            return

        logger.info('[%s] connecting to nsqd', conn.id)
        conn.connect()
        conn.callback_queue = []

    def _on_connection_ready(self, conn, **kwargs):
        # re-check to make sure another connection didn't beat this one
        if conn.id in self.conns:
            logger.warning(
                '[%s] connected but another matching connection already exists', conn.id)
            conn.close()
            return
        self.conns[conn.id] = conn

    def _on_connection_close(self, conn, **kwargs):
        if conn.id in self.conns:
            del self.conns[conn.id]

        for callback in conn.callback_queue:
            try:
                callback(conn, protocol.ConnectionClosedError())
            except Exception:
                logger.exception('[%s] uncaught exception in callback', conn.id)

        logger.warning('[%s] connection closed', conn.id)
        logger.info('[%s] attempting to reconnect in %0.2fs', conn.id, self.reconnect_interval)
        reconnect_callback = functools.partial(self.connect_to_nsqd,
                                               host=conn.host, port=conn.port)
        self.io_loop.call_later(self.reconnect_interval, reconnect_callback)

    def _finish_pub(self, conn, data, command, topic, msg):
        if isinstance(data, protocol.Error):
            logger.error('[%s] failed to %s (%s, %s), data is %s',
                         conn.id if conn else 'NA', command, topic, msg, data)
