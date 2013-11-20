from __future__ import with_statement
import os
import sys
import random
import time

from mock import Mock, patch

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq


def _message_handler(msg):
    msg.enable_async()

def _get_reader():
    return nsq.Reader("test", "test", 
        message_handler=_message_handler, lookupd_http_addresses=["http://test.local:4161"], 
        max_in_flight=5)

_conn_port = 4150
def _get_conn(reader):
    global _conn_port
    with patch('nsq.async.tornado.iostream.IOStream', autospec=True):
        conn = reader.connect_to_nsqd('localhost', _conn_port)
    _conn_port += 1
    conn.trigger('ready', conn=conn)
    return conn

def _send_message(conn):
    msg = _get_message(conn)
    conn.trigger('message', conn=conn, message=msg)
    return msg

def _get_message(conn):
    msg = nsq.Message("1234", "{}", 1234, 0)
    msg.on('finish', conn._on_message_finish)
    msg.on('requeue', conn._on_message_requeue)
    return msg

@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_easy(mock_ioloop):
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = _get_reader()
    conn = _get_conn(r)
    
    msg = _send_message(conn)
    
    msg.trigger('finish', message=msg)
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    msg = _send_message(conn)
    
    msg.trigger('requeue', message=msg)
    assert r.backoff_block == True
    assert r.backoff_timer.get_interval() > 0
    assert instance.add_timeout.called
    
    timeout_args, timeout_kwargs = instance.add_timeout.call_args
    timeout_args[1]()
    assert r.backoff_block == False
    send_args, send_kwargs = conn.stream.write.call_args
    assert send_args[0] == 'RDY 1\n'
    
    msg = _send_message(conn)
    
    msg.trigger('finish', message=msg)
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    expected_args = ['SUB test test\n', 
        'RDY 1\n', 'RDY 5\n',
        'FIN 1234\n', 'REQ 1234 0\n',
        'RDY 0\n', 'RDY 1\n',
        'FIN 1234\n', 'RDY 5\n']
    assert conn.stream.write.call_args_list == [((arg,),) for arg in expected_args]

@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_hard(mock_ioloop):
    expected_args = ['SUB test test\n', 'RDY 1\n', 'RDY 5\n']
    
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = _get_reader()
    conn = _get_conn(r)
    
    num_fails = 0
    fail = True
    last_timeout_time = 0
    for i in range(50):
        msg = _send_message(conn)
        
        if fail:
            msg.trigger('requeue', message=msg)
            num_fails += 1
            
            expected_args.append('REQ 1234 0\n')
            expected_args.append('RDY 0\n')
        else:
            msg.trigger('finish', message=msg)
            num_fails -= 1
            
            expected_args.append('FIN 1234\n')
            expected_args.append('RDY 0\n')
        
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
        msg = _send_message(conn)
        
        msg.trigger('finish', message=msg)
        expected_args.append('FIN 1234\n')
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        expected_args.append('RDY 0\n')
        expected_args.append('RDY 1\n')
    
    msg = _send_message(conn)
    
    msg.trigger('finish', message=msg)
    expected_args.append('FIN 1234\n')
    expected_args.append('RDY 5\n')
    
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    for i, call in enumerate(conn.stream.write.call_args_list):
        print "%d: %s" % (i, call)
    assert conn.stream.write.call_args_list == [((arg,),) for arg in expected_args]

@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_many_conns(mock_ioloop):
    num_conns = 5
    
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = _get_reader()
    
    conns = []
    for i in range(num_conns):
        conn = _get_conn(r)
        conn.expected_args = ['SUB test test\n', 'RDY 1\n']
        conn.fails = 0
        conns.append(conn)
    
    fail = True
    total_fails = 0
    last_timeout_time = 0
    conn = random.choice(conns)
    for i in range(50):
        msg = _send_message(conn)
        
        if r.backoff_timer.get_interval() == 0:
            conn.expected_args.append('RDY 1\n')
        
        if fail or not conn.fails:
            msg.trigger('requeue', message=msg)
            total_fails += 1
            conn.fails += 1
            
            conn.expected_args.append('REQ 1234 0\n')
            for c in conns:
                c.expected_args.append('RDY 0\n')
        else:
            msg.trigger('finish', message=msg)
            total_fails -= 1
            conn.fails -= 1
            
            conn.expected_args.append('FIN 1234\n')
            for c in conns:
                c.expected_args.append('RDY 0\n')
        
        assert r.backoff_block == True
        assert r.backoff_timer.get_interval() > 0
        assert instance.add_timeout.called
        
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block == False
        conn.expected_args.append('RDY 1\n')
        
        fail = True
        if random.random() < 0.3 and total_fails > 1:
            fail = False
    
    while total_fails:
        print "%r: %d fails (%d total_fails)" % (conn, c.fails, total_fails)
        
        if not conn.fails:
            # force an idle connection
            for c in conns:
                if c.rdy > 0:
                    c.last_msg_timestamp = time.time() - 60
                    c.expected_args.append('RDY 0\n')
            conn = r._redistribute_rdy_state()
            conn.expected_args.append('RDY 1\n')
            continue
        
        msg = _send_message(conn)
        
        msg.trigger('finish', message=msg)
        total_fails -= 1
        conn.fails -= 1
        
        conn.expected_args.append('FIN 1234\n')
        
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        
        if total_fails > 0:
            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('RDY 1\n')
        else:
            for c in conns:
                c.expected_args.append('RDY 1\n')
    
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    for c in conns:
        for i, call in enumerate(c.stream.write.call_args_list):
            print "%d: %s" % (i, call)
        assert c.stream.write.call_args_list == [((arg,),) for arg in c.expected_args]

@patch('nsq.async.tornado.ioloop.IOLoop', autospec=True)
def test_backoff_conns_disconnect(mock_ioloop):
    num_conns = 5
    
    instance = Mock()
    mock_ioloop.instance.return_value = instance
    
    r = _get_reader()
    
    conns = []
    for i in range(num_conns):
        conn = _get_conn(r)
        conn.expected_args = ['SUB test test\n', 'RDY 1\n']
        conn.fails = 0
        conns.append(conn)
    
    fail = True
    total_fails = 0
    last_timeout_time = 0
    conn = random.choice(conns)
    for i in range(50):
        if i % 5 == 0:
            if len(r.conns) == num_conns:
                conn.trigger('close', conn=conn)
                conns.remove(conn)
                if conn.rdy and r.backoff_timer.get_interval():
                    assert r.need_rdy_redistributed
                conn = r._redistribute_rdy_state()
                if not conn:
                    conn = random.choice(conns)
                else:
                    conn.expected_args.append('RDY 1\n')
                continue
            else:
                c = _get_conn(r)
                c.expected_args = ['SUB test test\n']
                c.fails = 0
                conns.append(c)
        
        msg = _send_message(conn)
        
        if r.backoff_timer.get_interval() == 0:
            conn.expected_args.append('RDY 1\n')
        
        if fail or not conn.fails:
            msg.trigger('requeue', message=msg)
            total_fails += 1
            conn.fails += 1
            
            conn.expected_args.append('REQ 1234 0\n')
            for c in conns:
                c.expected_args.append('RDY 0\n')
        else:
            msg.trigger('finish', message=msg)
            total_fails -= 1
            conn.fails -= 1
            
            conn.expected_args.append('FIN 1234\n')
            for c in conns:
                c.expected_args.append('RDY 0\n')
        
        assert r.backoff_block == True
        assert r.backoff_timer.get_interval() > 0
        assert instance.add_timeout.called
        
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block == False
        conn.expected_args.append('RDY 1\n')
        
        fail = True
        if random.random() < 0.3 and total_fails > 1:
            fail = False
    
    while total_fails:
        print "%r: %d fails (%d total_fails)" % (conn, c.fails, total_fails)
        
        msg = _send_message(conn)
        
        msg.trigger('finish', message=msg)
        total_fails -= 1
        
        conn.expected_args.append('FIN 1234\n')
        
        timeout_args, timeout_kwargs = instance.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        
        if total_fails > 0:
            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('RDY 1\n')
        else:
            for c in conns:
                c.expected_args.append('RDY 1\n')
    
    assert r.backoff_block == False
    assert r.backoff_timer.get_interval() == 0
    
    for c in conns:
        for i, call in enumerate(c.stream.write.call_args_list):
            print "%d: %s" % (i, call)
        assert c.stream.write.call_args_list == [((arg,),) for arg in c.expected_args]
