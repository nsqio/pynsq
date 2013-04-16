from __future__ import with_statement
import os
import sys

import struct
from mock import patch, create_autospec
from tornado.iostream import IOStream


# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq

# The functions below are meant to be used as specs for mocks of callbacks. Yay dynamic typing

def one_arg_fxn(arg):
    pass

def two_arg_fxn(arg1, arg2):
    pass

def _get_test_conn():
    conn = nsq.async.AsyncConn('test', 4150, create_autospec(one_arg_fxn),
        create_autospec(two_arg_fxn), create_autospec(one_arg_fxn))
    # now set the stream attribute, which is ordinarily set in conn.connect()
    conn.stream = create_autospec(IOStream)
    return conn

@patch('nsq.async.socket', autospec=True)
@patch('nsq.async.tornado.iostream.IOStream', autospec=True)
def test_connect(mock_iostream, mock_socket):
    conn = _get_test_conn()
    conn.connect()
    conn.stream.set_close_callback.assert_called_once_with(conn._socket_close)
    conn.stream.connect.assert_called_once_with((conn.host, conn.port), conn._connect_callback)
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
    with patch.object(conn, '_start_read', autospec=True) as mock_start_read:
        conn._connect_callback()
        assert not conn.connecting
        assert conn.connected
        conn.stream.write.assert_called_once_with(nsq.nsq.MAGIC_V2)
        mock_start_read.assert_called_once_with()
        conn.connect_callback.assert_called_once_with(conn)

def test_start_read():
    conn = _get_test_conn()
    conn._start_read()
    conn.stream.read_bytes.assert_called_once_with(4, conn._read_size)

def test_read_size():
    conn = _get_test_conn()
    body_size = 6
    body_size_packed = struct.pack('>l', body_size)
    conn._read_size(body_size_packed)
    conn.stream.read_bytes.assert_called_once_with(body_size, conn._read_body)

    # now test that we get the right behavior when we get malformed data
    # for this, we'll want to stick on mock on conn.close
    conn.stream.read_bytes.reset_mock()
    with patch.object(conn, 'close', autospec=True) as mock_close:
        conn._read_size('asdfasdf')
        mock_close.assert_called_once_with()
        assert not conn.stream.read_bytes.called

def test_read_body():
    conn = _get_test_conn()
    # I won't autospec the mock below, it doesn't seem to want to behave. 
    # I only assert against one of its attrs anyway, which I will spec
    with patch('nsq.async.tornado.ioloop.IOLoop.instance') as mock_io_loop:
        mock_ioloop_addcb = create_autospec(one_arg_fxn)
        mock_io_loop.return_value.add_callback = mock_ioloop_addcb
        data ='NSQ'
        conn._read_body(data)
        conn.data_callback.assert_called_once_with(conn, data)
        mock_ioloop_addcb.assert_called_once_with(conn._start_read)

        # now test functionality when the data_callback fails
        conn.data_callback.reset_mock()
        mock_ioloop_addcb.reset_mock()
        conn.data_callback.return_value = Exception("Boom.")
        conn._read_body(data)
        # verify that we still added callback for the next start_read
        mock_ioloop_addcb.assert_called_once_with(conn._start_read)
