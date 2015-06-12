from __future__ import absolute_import

from __future__ import with_statement
import os
import sys
import random
import time

try:
    from mock import create_autospec, MagicMock, patch
except ImportError:
    from unittest.mock import create_autospec, MagicMock, patch
from tornado.ioloop import IOLoop

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq
from nsq import compat, event


_conn_port = 4150


def _message_handler(msg):
    msg.enable_async()


def _get_reader(io_loop=None, max_in_flight=5):
    return nsq.Reader("test", "test",
                      message_handler=_message_handler,
                      lookupd_http_addresses=["http://test.local:4161"],
                      max_in_flight=max_in_flight,
                      io_loop=io_loop)


def _get_conn(reader):
    global _conn_port
    with patch('nsq.async.tornado.iostream.IOStream', autospec=True):
        conn = reader.connect_to_nsqd('localhost', _conn_port)
    _conn_port += 1
    conn.trigger(event.READY, conn=conn)
    return conn


def _send_message(conn):
    msg = _get_message(conn)
    conn.trigger(event.MESSAGE, conn=conn, message=msg)
    return msg


def _get_message(conn):
    msg = nsq.Message("1234", "{}", 1234, 0)
    msg.on('finish', conn._on_message_finish)
    msg.on('requeue', conn._on_message_requeue)
    return msg


def _make_ioloop():
    mock_ioloop = create_autospec(IOLoop)
    mock_ioloop.time = MagicMock()
    mock_ioloop.time.return_value = time.time()
    return mock_ioloop


def _assert_wrote_expected(conn, expected_args):
    assert conn.stream.write.call_args_list == [((compat.b(arg),),) for arg in expected_args]


def test_backoff_easy():
    mock_ioloop = _make_ioloop()
    r = _get_reader(mock_ioloop)
    conn = _get_conn(r)

    msg = _send_message(conn)

    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    msg = _send_message(conn)

    msg.trigger(event.REQUEUE, message=msg)
    assert r.backoff_block is True
    assert r.backoff_timer.get_interval() > 0
    assert mock_ioloop.add_timeout.called

    timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
    timeout_args[1]()
    assert r.backoff_block is False
    send_args, send_kwargs = conn.stream.write.call_args
    assert send_args[0] == compat.b('RDY 1\n')

    msg = _send_message(conn)

    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    expected_args = [
        'SUB test test\n',
        'RDY 1\n',
        'RDY 5\n',
        'FIN 1234\n',
        'RDY 0\n',
        'REQ 1234 0\n',
        'RDY 1\n',
        'RDY 5\n',
        'FIN 1234\n'
    ]
    _assert_wrote_expected(conn, expected_args)


def test_backoff_out_of_order():
    mock_ioloop = _make_ioloop()
    r = _get_reader(mock_ioloop, max_in_flight=4)
    conn1 = _get_conn(r)
    conn2 = _get_conn(r)

    msg = _send_message(conn1)

    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    msg = _send_message(conn1)

    msg.trigger(event.REQUEUE, message=msg)
    assert r.backoff_block is True
    assert r.backoff_timer.get_interval() > 0
    assert mock_ioloop.add_timeout.called
    timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args

    msg = _send_message(conn1)

    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is True
    assert r.backoff_timer.get_interval() == 0

    timeout_args[1]()
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    expected_args = [
        'SUB test test\n',
        'RDY 1\n',
        'RDY 2\n',
        'FIN 1234\n',
        'RDY 0\n',
        'REQ 1234 0\n',
        'FIN 1234\n',
        'RDY 2\n',
    ]
    _assert_wrote_expected(conn1, expected_args)

    expected_args = [
        'SUB test test\n',
        'RDY 1\n',
        'RDY 0\n',
        'RDY 2\n'
    ]
    _assert_wrote_expected(conn2, expected_args)


def test_backoff_requeue_recovery():
    mock_ioloop = _make_ioloop()
    r = _get_reader(mock_ioloop, max_in_flight=2)
    conn = _get_conn(r)
    msg = _send_message(conn)

    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0
    assert mock_ioloop.add_timeout.call_count == 1

    msg = _send_message(conn)

    # go into backoff
    msg.trigger(event.REQUEUE, message=msg)
    assert r.backoff_block is True
    assert r.backoff_timer.get_interval() > 0
    assert mock_ioloop.add_timeout.call_count == 2
    timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args

    # elapse time
    timeout_args[1]()
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() != 0

    msg = _send_message(conn)

    # This should not move out of backoff (since backoff=False)
    msg.trigger(event.REQUEUE, message=msg, backoff=False)
    assert r.backoff_block is True
    assert r.backoff_timer.get_interval() != 0
    assert mock_ioloop.add_timeout.call_count == 3
    timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args

    # elapse time
    timeout_args[1]()
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() != 0

    # this should move out of backoff state
    msg = _send_message(conn)
    msg.trigger(event.FINISH, message=msg)
    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    print(conn.stream.write.call_args_list)

    expected_args = [
        'SUB test test\n',
        'RDY 1\n',
        'RDY 2\n',
        'FIN 1234\n',
        'RDY 0\n',
        'REQ 1234 0\n',
        'RDY 1\n',
        'RDY 0\n',
        'REQ 1234 0\n',
        'RDY 1\n',
        'RDY 2\n',
        'FIN 1234\n'
    ]
    _assert_wrote_expected(conn, expected_args)


def test_backoff_hard():
    mock_ioloop = _make_ioloop()
    r = _get_reader(io_loop=mock_ioloop)
    conn = _get_conn(r)

    expected_args = ['SUB test test\n', 'RDY 1\n', 'RDY 5\n']

    num_fails = 0
    fail = True
    last_timeout_time = 0
    for i in range(50):
        msg = _send_message(conn)

        if fail:
            msg.trigger(event.REQUEUE, message=msg)
            num_fails += 1

            expected_args.append('RDY 0\n')
            expected_args.append('REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            num_fails -= 1

            expected_args.append('RDY 0\n')
            expected_args.append('FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        expected_args.append('RDY 1\n')

        fail = True
        if random.random() < 0.3 and num_fails > 1:
            fail = False

    for i in range(num_fails - 1):
        msg = _send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        expected_args.append('RDY 0\n')
        expected_args.append('FIN 1234\n')
        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        expected_args.append('RDY 1\n')

    msg = _send_message(conn)

    msg.trigger(event.FINISH, message=msg)
    expected_args.append('RDY 5\n')
    expected_args.append('FIN 1234\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for i, call in enumerate(conn.stream.write.call_args_list):
        print("%d: %s" % (i, call))
    _assert_wrote_expected(conn, expected_args)


def test_backoff_many_conns():
    mock_ioloop = _make_ioloop()
    r = _get_reader(io_loop=mock_ioloop)

    num_conns = 5
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
            msg.trigger(event.REQUEUE, message=msg)
            total_fails += 1
            conn.fails += 1

            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            total_fails -= 1
            conn.fails -= 1

            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        conn.expected_args.append('RDY 1\n')

        fail = True
        if random.random() < 0.3 and total_fails > 1:
            fail = False

    while total_fails:
        print("%r: %d fails (%d total_fails)" % (conn, conn.fails, total_fails))

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

        msg.trigger(event.FINISH, message=msg)
        total_fails -= 1
        conn.fails -= 1

        if total_fails > 0:
            for c in conns:
                c.expected_args.append('RDY 0\n')
        else:
            for c in conns:
                c.expected_args.append('RDY 1\n')

        conn.expected_args.append('FIN 1234\n')

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]

        if total_fails > 0:
            conn.expected_args.append('RDY 1\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for c in conns:
        for i, call in enumerate(c.stream.write.call_args_list):
            print("%d: %s" % (i, call))
        _assert_wrote_expected(c, c.expected_args)


def test_backoff_conns_disconnect():
    mock_ioloop = _make_ioloop()
    r = _get_reader(io_loop=mock_ioloop)

    num_conns = 5
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
                conn.trigger(event.CLOSE, conn=conn)
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
            msg.trigger(event.REQUEUE, message=msg)
            total_fails += 1
            conn.fails += 1

            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            total_fails -= 1
            conn.fails -= 1

            for c in conns:
                c.expected_args.append('RDY 0\n')
            conn.expected_args.append('FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        conn.expected_args.append('RDY 1\n')

        fail = True
        if random.random() < 0.3 and total_fails > 1:
            fail = False

    while total_fails:
        print("%r: %d fails (%d total_fails)" % (conn, conn.fails, total_fails))

        msg = _send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        total_fails -= 1
        conn.fails -= 1

        if total_fails > 0:
            for c in conns:
                c.expected_args.append('RDY 0\n')
        else:
            for c in conns:
                c.expected_args.append('RDY 1\n')

        conn.expected_args.append('FIN 1234\n')

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]

        if total_fails > 0:
            conn.expected_args.append('RDY 1\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for c in conns:
        for i, call in enumerate(c.stream.write.call_args_list):
            print("%d: %s" % (i, call))
        _assert_wrote_expected(c, c.expected_args)
