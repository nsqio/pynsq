from __future__ import with_statement
import os
import sys
import random

from mock import Mock, patch

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq

def two_arg_fxn(arg1, arg2):
    pass

def _get_test_conn():
    conn = Mock()
    conn.task = "test"
    conn.in_flight = 50
    conn.ready = 25
    conn.max_rdy_count = 50
    return conn

def _get_test_message():
    msg = nsq.Message("1234", "{}", 1234, 0)
    return msg

@patch('nsq.async.tornado.iostream.IOStream', autospec=True)
@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_easy(mock_ioloop, mock_iostream):
    conn = _get_test_conn()
    msg = _get_test_message()

    instance = Mock()
    mock_ioloop.instance.return_value = instance

    r = nsq.Reader({"test" : two_arg_fxn}, "test", "test", nsqd_tcp_addresses = ["test:9999",], max_in_flight = 5)
    r.conns["test:9999:test"] = conn
    r.total_ready = 0

    r._client_callback(nsq.REQ, task="test", conn=conn, message=msg)

    assert r.backoff_block["test"] == True
    assert r.backoff_timer["test"].get_interval() > 0
    assert instance.add_timeout.called

    send_args, send_kwargs = conn.send.call_args
    assert send_args[0] == 'RDY 0\n'

    timeout_args, timeout_kwargs = instance.add_timeout.call_args
    timeout_args[1]()
    assert r.backoff_block["test"] == False
    send_args, send_kwargs = conn.send.call_args
    assert send_args[0] == 'RDY 1\n'

    r._client_callback(nsq.FIN, task="test", conn=conn, message=msg)
    assert r.backoff_block["test"] == False
    assert r.backoff_timer["test"].get_interval() == 0

    send_args, send_kwargs = conn.send.call_args
    assert send_args[0] == 'RDY 5\n'

@patch('nsq.async.tornado.iostream.IOStream', autospec=True)
@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_hard(mock_ioloop, mock_iostream):
    conn = _get_test_conn()
    msg = _get_test_message()

    instance = Mock()
    mock_ioloop.instance.return_value = instance

    r = nsq.Reader({"test" : two_arg_fxn}, "test", "test", nsqd_tcp_addresses = ["test:9999",], max_in_flight = 5)
    r.conns["test:9999:test"] = conn
    r.total_ready = 0

    num_fails = 0
    fail = True
    last_timeout_time = 0
    for i in range(50):
        if fail:
            r._client_callback(nsq.REQ, task="test", conn=conn, message=msg)
            num_fails += 1

            send_args, send_kwargs = conn.send.call_args
            assert send_args[0] == 'RDY 0\n'
        else:
            r._client_callback(nsq.FIN, task="test", conn=conn, message=msg)
            num_fails -= 1

        assert r.backoff_block["test"] == True
        assert r.backoff_timer["test"].get_interval() > 0
        assert instance.add_timeout.called

        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block["test"] == False
        send_args, send_kwargs = conn.send.call_args
        assert send_args[0] == 'RDY 1\n'

        fail = True
        if random.random() < 0.1 and num_fails > 1:
            fail = False

    for i in range(num_fails+1):
        r._client_callback(nsq.FIN, task="test", conn=conn, message=msg)
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]

    r._client_callback(nsq.FIN, task="test", conn=conn, message=msg)
    assert r.backoff_block["test"] == False
    assert r.backoff_timer["test"].get_interval() == 0

    send_args, send_kwargs = conn.send.call_args_list[-3]
    assert send_args[0] == 'RDY 5\n'
