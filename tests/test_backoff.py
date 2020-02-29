from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import random
import time

from mock import call, patch
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from .reader_unit_test_helpers import (
    get_reader,
    get_ioloop,
    get_conn,
    send_message
)

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from nsq import event


class BackoffTests(AsyncTestCase):

    @gen_test
    def test_backoff_easy(self):
        r = get_reader()
        conn = get_conn(r)

        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        msg = send_message(conn)

        msg.trigger(event.REQUEUE, message=msg)
        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0

        yield gen.sleep(r.backoff_timer.get_interval() + 0.05)

        assert r.backoff_block is False
        send_args, send_kwargs = conn.stream.write.call_args
        assert send_args[0] == b'RDY 1\n'

        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        expected_args = [
            b'SUB test test\n',
            b'RDY 1\n',
            b'RDY 5\n',
            b'FIN 1234\n',
            b'RDY 0\n',
            b'REQ 1234 0\n',
            b'RDY 1\n',
            b'RDY 5\n',
            b'FIN 1234\n'
        ]
        assert conn.stream.write.call_args_list == [call(arg) for arg in expected_args]

    @gen_test
    def test_backoff_out_of_order(self):
        r = get_reader(max_in_flight=4)
        conn1 = get_conn(r)
        conn2 = get_conn(r)

        msg = send_message(conn1)

        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        msg = send_message(conn1)

        msg.trigger(event.REQUEUE, message=msg)
        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        backoff_interval = r.backoff_timer.get_interval()

        msg = send_message(conn1)

        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() == 0

        yield gen.sleep(backoff_interval + 0.05)

        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        expected_args = [
            b'SUB test test\n',
            b'RDY 1\n',
            b'RDY 2\n',
            b'FIN 1234\n',
            b'RDY 0\n',
            b'REQ 1234 0\n',
            b'FIN 1234\n',
            b'RDY 2\n',
        ]
        assert conn1.stream.write.call_args_list == [call(arg) for arg in expected_args]

        expected_args = [
            b'SUB test test\n',
            b'RDY 1\n',
            b'RDY 0\n',
            b'RDY 2\n'
        ]
        assert conn2.stream.write.call_args_list == [call(arg) for arg in expected_args]

    @gen_test
    def test_backoff_requeue_recovery(self):
        r = get_reader(max_in_flight=2)
        conn = get_conn(r)
        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        msg = send_message(conn)

        # go into backoff
        msg.trigger(event.REQUEUE, message=msg)
        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0

        yield gen.sleep(r.backoff_timer.get_interval() + 0.05)

        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() != 0

        msg = send_message(conn)

        # This should not move out of backoff (since backoff=False)
        msg.trigger(event.REQUEUE, message=msg, backoff=False)
        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() != 0

        # elapse time
        yield gen.sleep(r.backoff_timer.get_interval() + 0.05)

        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() != 0

        # this should move out of backoff state
        msg = send_message(conn)
        msg.trigger(event.FINISH, message=msg)
        assert r.backoff_block is False
        assert r.backoff_timer.get_interval() == 0

        print(conn.stream.write.call_args_list)

        expected_args = [
            b'SUB test test\n',
            b'RDY 1\n',
            b'RDY 2\n',
            b'FIN 1234\n',
            b'RDY 0\n',
            b'REQ 1234 0\n',
            b'RDY 1\n',
            b'RDY 0\n',
            b'REQ 1234 0\n',
            b'RDY 1\n',
            b'RDY 2\n',
            b'FIN 1234\n'
        ]
        assert conn.stream.write.call_args_list == [call(arg) for arg in expected_args]


@patch("tornado.ioloop.IOLoop.current")
def test_backoff_hard(mock_ioloop_current):
    mock_ioloop = get_ioloop()
    mock_ioloop_current.return_value = mock_ioloop
    r = get_reader()
    conn = get_conn(r)

    expected_args = [b'SUB test test\n', b'RDY 1\n', b'RDY 5\n']

    num_fails = 0
    fail = True
    last_timeout_time = 0
    for i in range(50):
        msg = send_message(conn)

        if fail:
            msg.trigger(event.REQUEUE, message=msg)
            num_fails += 1

            expected_args.append(b'RDY 0\n')
            expected_args.append(b'REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            num_fails -= 1

            expected_args.append(b'RDY 0\n')
            expected_args.append(b'FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        expected_args.append(b'RDY 1\n')

        fail = True
        if random.random() < 0.3 and num_fails > 1:
            fail = False

    for i in range(num_fails - 1):
        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        expected_args.append(b'RDY 0\n')
        expected_args.append(b'FIN 1234\n')
        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            timeout_args[1]()
            last_timeout_time = timeout_args[0]
        expected_args.append(b'RDY 1\n')

    msg = send_message(conn)

    msg.trigger(event.FINISH, message=msg)
    expected_args.append(b'RDY 5\n')
    expected_args.append(b'FIN 1234\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for i, f in enumerate(conn.stream.write.call_args_list):
        print("%d: %s" % (i, f))
    assert conn.stream.write.call_args_list == [call(arg) for arg in expected_args]


@patch("tornado.ioloop.IOLoop.current")
def test_backoff_many_conns(mock_ioloop_current):
    mock_ioloop = get_ioloop()
    mock_ioloop_current.return_value = mock_ioloop
    r = get_reader()

    num_conns = 5
    conns = []
    for i in range(num_conns):
        conn = get_conn(r)
        conn.expected_args = [b'SUB test test\n', b'RDY 1\n']
        conn.last_exp_rdy = b'RDY 1\n'
        conn.fails = 0
        conns.append(conn)

    fail = True
    total_fails = 0
    last_timeout_time = 0
    conn = random.choice(conns)
    for i in range(50):
        msg = send_message(conn)

        if r.backoff_timer.get_interval() == 0:
            add_exp_rdy(conn, b'RDY 1\n')

        if fail or not conn.fails:
            msg.trigger(event.REQUEUE, message=msg)
            total_fails += 1
            conn.fails += 1

            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
            conn.expected_args.append(b'REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            total_fails -= 1
            conn.fails -= 1

            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
            conn.expected_args.append(b'FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        add_exp_rdy(conn, b'RDY 1\n')

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
                    add_exp_rdy(c, b'RDY 0\n')
            conn = r._redistribute_rdy_state()
            add_exp_rdy(conn, b'RDY 1\n')
            continue

        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        total_fails -= 1
        conn.fails -= 1

        if total_fails > 0:
            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
        else:
            for c in conns:
                add_exp_rdy(c, b'RDY 1\n')

        conn.expected_args.append(b'FIN 1234\n')

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]

        if total_fails > 0:
            add_exp_rdy(conn, b'RDY 1\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for c in conns:
        for i, f in enumerate(c.stream.write.call_args_list):
            print("%d: %s" % (i, f))
        assert c.stream.write.call_args_list == [call(arg) for arg in c.expected_args]


@patch("tornado.ioloop.IOLoop.current")
def test_backoff_conns_disconnect(mock_ioloop_current):
    mock_ioloop = get_ioloop()
    mock_ioloop_current.return_value = mock_ioloop
    r = get_reader()

    num_conns = 5
    conns = []
    for i in range(num_conns):
        conn = get_conn(r)
        conn.expected_args = [b'SUB test test\n', b'RDY 1\n']
        conn.last_exp_rdy = b'RDY 1\n'
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
                    add_exp_rdy(conn, b'RDY 1\n')
                continue
            else:
                c = get_conn(r)
                c.expected_args = [b'SUB test test\n']
                c.last_exp_rdy = b'RDY 0\n'
                c.fails = 0
                conns.append(c)

        msg = send_message(conn)

        if r.backoff_timer.get_interval() == 0:
            add_exp_rdy(conn, b'RDY 1\n')

        if fail or not conn.fails:
            msg.trigger(event.REQUEUE, message=msg)
            total_fails += 1
            conn.fails += 1

            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
            conn.expected_args.append(b'REQ 1234 0\n')
        else:
            msg.trigger(event.FINISH, message=msg)
            total_fails -= 1
            conn.fails -= 1

            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
            conn.expected_args.append(b'FIN 1234\n')

        assert r.backoff_block is True
        assert r.backoff_timer.get_interval() > 0
        assert mock_ioloop.add_timeout.called

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]
        assert r.backoff_block is False
        add_exp_rdy(conn, b'RDY 1\n')

        fail = True
        if random.random() < 0.3 and total_fails > 1:
            fail = False

    while total_fails:
        print("%r: %d fails (%d total_fails)" % (conn, conn.fails, total_fails))

        msg = send_message(conn)

        msg.trigger(event.FINISH, message=msg)
        total_fails -= 1
        conn.fails -= 1

        if total_fails > 0:
            for c in conns:
                add_exp_rdy(c, b'RDY 0\n')
        else:
            for c in conns:
                add_exp_rdy(c, b'RDY 1\n')

        conn.expected_args.append(b'FIN 1234\n')

        timeout_args, timeout_kwargs = mock_ioloop.add_timeout.call_args
        if timeout_args[0] != last_timeout_time:
            conn = timeout_args[1]()
            last_timeout_time = timeout_args[0]

        if total_fails > 0:
            add_exp_rdy(conn, b'RDY 1\n')

    assert r.backoff_block is False
    assert r.backoff_timer.get_interval() == 0

    for c in conns:
        for i, f in enumerate(c.stream.write.call_args_list):
            print("%d: %s" % (i, f))
        assert c.stream.write.call_args_list == [call(arg) for arg in c.expected_args]


def add_exp_rdy(conn, rdy):
    if conn.last_exp_rdy != rdy:
        conn.expected_args.append(rdy)
        conn.last_exp_rdy = rdy
