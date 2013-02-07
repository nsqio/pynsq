from __future__ import with_statement
import os
import sys
try:
    import simplejson as json
except ImportError:
    import json # pyflakes.ignore

import struct
from mock import MagicMock, patch


# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq
import mock_socket

class MockStream(object):
    def __init__(self, *args, **kwargs):
        # replace all the methods that might get called with a mock object
        for attr in ['read_bytes', 'write', 'set_close_callback', 'connect']:
            setattr(self, attr, MagicMock())

def _get_test_conn():
    conn = nsq.async.AsyncConn('test', 4150, MagicMock(), MagicMock(), MagicMock())
    # now set the stream attribute, which is ordinarily set in conn.connect()
    conn.stream = MockStream()
    return conn

@patch('nsq.async.socket', new=mock_socket)
@patch('nsq.async.tornado.iostream.IOStream', new=MockStream)
def test_connect():
    conn = _get_test_conn()
    conn.connect()
    assert conn.stream.set_close_callback.called_once_with(conn._socket_close)
    assert conn.stream.connect.called_once_with((conn.host, conn.port), conn._connect_callback)
    assert conn.connecting
    assert not conn.connected

    # now ensure that we will bail if we were already in a connecting or connected state
    conn = _get_test_conn()
    conn.connecting = True
    conn.connect()
    assert not conn.stream.set_close_callback.called
    assert not conn.stream.connect.called

    conn = _get_test_conn()
    conn.connected = True
    conn.connect()
    assert not conn.stream.set_close_callback.called
    assert not conn.stream.connect.called

def test_connect_callback():
    conn = _get_test_conn()
    # simulate having called `conn.connect` by setting `connecting` to True
    conn.connecting = True
    with patch.object(conn, '_start_read') as mock_start_read:
        conn._connect_callback()
        assert not conn.connecting
        assert conn.connected
        assert conn.stream.write.called_once_with(nsq.nsq.MAGIC_V2)
        assert mock_start_read.called_once_with()
        assert conn.connect_callback.called_once_with(conn)

def test_start_read():
    conn = _get_test_conn()
    conn._start_read()
    assert conn.stream.read_bytes.called_once_with(4, conn._read_size)

def test_read_size():
    conn = _get_test_conn()
    body_size = 6
    body_size_packed = struct.pack('>l', body_size)
    conn._read_size(body_size_packed)
    assert conn.stream.read_bytes.called_once_with(body_size, conn._read_body)

    # now test that we get the right behavior when we get malformed data
    # for this, we'll want to stick on mock on conn.close
    conn.stream.read_bytes.reset_mock()
    with patch.object(conn, 'close') as mock_close:
        conn._read_size('asdfasdf')
        assert mock_close.called_once_with()
        assert not conn.stream.read_bytes.called

def test_read_body():
    conn = _get_test_conn()
    with patch('nsq.async.tornado.ioloop.IOLoop.instance') as mock_io_loop:
        mock_ioloop_addcb = MagicMock()
        mock_io_loop.return_value.add_callback = mock_ioloop_addcb
        data ='NSQ'
        conn._read_body(data)
        assert conn.data_callback.called_once_with(data)
        assert mock_ioloop_addcb.called_once_with(conn._start_read)

        # now test functionality when the data_callback fails
        conn.data_callback.reset_mock()
        mock_ioloop_addcb.reset_mock()
        conn.data_callback.return_value = Exception("Boom.")
        conn._read_body(data)
        # verify that we still added callback for the next start_read
        assert mock_ioloop_addcb.called_once_with(conn._start_read)
