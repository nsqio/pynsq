from __future__ import absolute_import

import time

from mock import patch, create_autospec
from tornado.ioloop import IOLoop
from tornado.concurrent import Future

import nsq
from nsq import event

_conn_port = 4150


def get_reader(max_in_flight=5):
    return nsq.Reader("test", "test",
                      message_handler=_message_handler,
                      lookupd_http_addresses=["http://localhost:4161"],
                      max_in_flight=max_in_flight,
                      max_backoff_duration=2.0,
                      )


def get_ioloop():
    ioloop = create_autospec(IOLoop, instance=True)
    ioloop.time.side_effect = time.time
    ioloop.call_later.side_effect = lambda dt, cb: ioloop.add_timeout(time.time() + dt, cb)
    return ioloop


def get_conn(reader):
    global _conn_port
    with patch('nsq.conn.tornado.iostream.IOStream', autospec=True) as iostream:
        instance = iostream.return_value
        instance.connect.return_value = Future()
        instance.read_bytes.return_value = Future()
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
