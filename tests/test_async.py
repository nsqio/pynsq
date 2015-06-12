from __future__ import absolute_import

from __future__ import with_statement
import os
import sys

import struct
try:  # py2
    from mock import patch, create_autospec, MagicMock
except ImportError:  # py3
    from unittest.mock import patch, create_autospec, MagicMock
from tornado.iostream import IOStream

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from nsq.async import AsyncConn
from nsq import protocol


# The functions below are meant to be used as specs for mocks of callbacks. Yay dynamic typing
def f(*args, **kwargs):
    pass


def _get_test_conn(io_loop=None):
    conn = AsyncConn('test', 4150, io_loop=io_loop)
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
    assert conn.state == 'CONNECTING'

    # now ensure that we will bail if we were already in a connecting or connected state
    conn = _get_test_conn()
    conn.state = 'CONNECTING'
    conn.connect()
    assert not conn.stream.set_close_callback.called
    assert not conn.stream.connect.called

    conn = _get_test_conn()
    conn.state = 'CONNECTED'
    conn.connect()
    assert not conn.stream.set_close_callback.called
    assert not conn.stream.connect.called


def test_connect_callback():
    conn = _get_test_conn()
    on_connect = create_autospec(f)
    conn.on('connect', on_connect)
    # simulate having called `conn.connect` by setting state to `connecting`
    conn.state = 'CONNECTING'
    with patch.object(conn, '_start_read', autospec=True) as mock_start_read:
        conn._connect_callback()
        assert conn.state == 'CONNECTED'
        conn.stream.write.assert_called_once_with(protocol.MAGIC_V2)
        mock_start_read.assert_called_once_with()
        on_connect.assert_called_once_with(conn=conn)


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
    mock_io_loop = MagicMock()

    conn = _get_test_conn(io_loop=mock_io_loop)
    on_data = create_autospec(f)
    conn.on('data', on_data)

    mock_ioloop_addcb = create_autospec(f)
    mock_io_loop.add_callback = mock_ioloop_addcb
    data = 'NSQ'
    conn._read_body(data)
    on_data.assert_called_once_with(conn=conn, data=data)
    mock_ioloop_addcb.assert_called_once_with(conn._start_read)

    # now test functionality when the data_callback fails
    on_data.reset_mock()
    mock_ioloop_addcb.reset_mock()
    on_data.return_value = Exception("Boom.")
    conn._read_body(data)
    # verify that we still added callback for the next start_read
    mock_ioloop_addcb.assert_called_once_with(conn._start_read)
