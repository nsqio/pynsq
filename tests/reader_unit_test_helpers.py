from __future__ import absolute_import
from __future__ import with_statement

from mock import patch, create_autospec
from tornado.ioloop import IOLoop

import nsq
from nsq import event

_conn_port = 4150


def get_reader(io_loop=None, max_in_flight=5):
    return nsq.Reader("test", "test",
                      message_handler=_message_handler,
                      lookupd_http_addresses=["http://test.local:4161"],
                      max_in_flight=max_in_flight,
                      io_loop=io_loop)


def get_ioloop():
    ioloop = create_autospec(IOLoop)
    ioloop.time.return_value = 0
    return ioloop


def get_conn(reader):
    global _conn_port
    with patch('nsq.async.tornado.iostream.IOStream', autospec=True):
        conn = reader.connect_to_nsqd('localhost', _conn_port)
    _conn_port += 1
    conn.trigger(event.READY, conn=conn)
    return conn


def send_message(conn):
    msg = _get_message(conn)
    conn.in_flight += 1
    conn.trigger(event.MESSAGE, conn=conn, message=msg)
    return msg


def _get_message(conn):
    msg = nsq.Message("1234", "{}", 1234, 0)
    msg.on('finish', conn._on_message_finish)
    msg.on('requeue', conn._on_message_requeue)
    return msg


def _message_handler(msg):
    msg.enable_async()
