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
    conn.rdy = 0
    conn.last_rdy = 0
    conn.in_flight = 0
    conn.max_rdy_count = 2500
    return conn

def _get_test_message():
    return nsq.Message("1234", "{}", 1234, 0)

@patch('nsq.async.tornado.iostream.IOStream', autospec=True)
@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_easy(mock_ioloop, mock_iostream):
    conn = _get_test_conn()
    msg = _get_test_message()
    
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = nsq.Reader("test", "test", message_handler=two_arg_fxn, nsqd_tcp_addresses=["test:9999"], max_in_flight=5)
    r.conns["test:9999"] = conn
    r._send_rdy(conn, 5)
    
    conn.in_flight += 1
    conn.rdy -= 1
    r.total_rdy -= 1
    
    r._message_responder(nsq.REQ, conn=conn, message=msg)
    assert r.backoff_block == True
    assert r.backoff_timer.get_interval() > 0
    assert instance.add_timeout.called
    
    timeout_args, timeout_kwargs = instance.add_timeout.call_args
    timeout_args[1]()
    assert r.backoff_block == False
    send_args, send_kwargs = conn.send.call_args
    assert send_args[0] == 'RDY 1\n'
    
    conn.in_flight += 1
    conn.rdy -= 1
    r.total_rdy -= 1
    
    r._message_responder(nsq.FIN, conn=conn, message=msg)
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    expected_args = ['RDY 5\n', 'REQ 1234 0\n',
        'RDY 0\n', 'RDY 1\n',
        'FIN 1234\n', 'RDY 5\n']
    assert conn.send.call_args_list == [((arg,),) for arg in expected_args]

@patch('nsq.async.tornado.iostream.IOStream', autospec=True)
@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_hard(mock_ioloop, mock_iostream):
    conn = _get_test_conn()
    msg = _get_test_message()
    
    expected_args = []
    
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = nsq.Reader("test", "test", message_handler=two_arg_fxn, nsqd_tcp_addresses=["test:9999"], max_in_flight=5)
    r.conns["test:9999"] = conn
    r._send_rdy(conn, 5)
    expected_args.append('RDY 5\n')
    
    num_fails = 0
    fail = True
    last_timeout_time = 0
    for i in range(50):
        conn.in_flight += 1
        conn.rdy -= 1
        r.total_rdy -= 1
        
        if fail:
            print 'REQ'
            r._message_responder(nsq.REQ, conn=conn, message=msg)
            num_fails += 1
            
            expected_args.append('REQ 1234 0\n')
            if num_fails == 1:
                expected_args.append('RDY 0\n')
        else:
            print 'FIN'
            r._message_responder(nsq.FIN, conn=conn, message=msg)
            num_fails -= 1
            
            expected_args.append('FIN 1234\n')
        
        assert r.backoff_block == True
        assert r.backoff_timer.get_interval() > 0
        assert instance.add_timeout.called
        
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block == False
        expected_args.append('RDY 1\n')
        
        fail = True
        if random.random() < 0.3 and num_fails > 1:
            fail = False
    
    for i in range(num_fails - 1):
        conn.in_flight += 1
        conn.rdy -= 1
        r.total_rdy -= 1
        
        r._message_responder(nsq.FIN, conn=conn, message=msg)
        expected_args.append('FIN 1234\n')
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        expected_args.append('RDY 1\n')
    
    conn.in_flight += 1
    conn.rdy -= 1
    r.total_rdy -= 1
    
    r._message_responder(nsq.FIN, conn=conn, message=msg)
    expected_args.append('FIN 1234\n')
    expected_args.append('RDY 5\n')
    
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    assert conn.send.call_args_list == [((arg,),) for arg in expected_args]
