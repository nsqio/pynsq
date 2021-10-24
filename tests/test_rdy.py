import pytest
from mock import patch

from nsq import event

from .reader_unit_test_helpers import (
    get_reader,
    get_conn,
    get_ioloop,
    send_message
)


def test_initial_probe_then_full_throttle():
    r = get_reader()
    conn = get_conn(r)
    assert conn.rdy == 1
    send_message(conn)
    assert conn.rdy == r._connection_max_in_flight()


def test_new_conn_throttles_down_existing_conns():
    r = get_reader()
    conn1 = get_conn(r)
    send_message(conn1)
    assert conn1.rdy == r._connection_max_in_flight()

    conn2 = get_conn(r)
    assert conn2.rdy == 1
    assert conn1.rdy == r._connection_max_in_flight()


def test_new_conn_respects_max_in_flight():
    max_in_flight = 1
    r = get_reader(max_in_flight)
    get_conn(r)
    get_conn(r)


@pytest.mark.parametrize(['conn_count', 'max_in_flight'], [
    [5, 10],
    [5, 4],
])
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_complete_backoff(ioloop_current_mock, conn_count, max_in_flight):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    r = get_reader(max_in_flight=max_in_flight)
    conns = [get_conn(r) for _ in range(conn_count)]

    msg = send_message(conns[0])
    msg.trigger(event.REQUEUE, message=msg)

    timeout_args, _ = ioloop_mock.add_timeout.call_args
    conn = timeout_args[1]()
    assert r.total_rdy == 1

    msg = send_message(conn)
    msg.trigger(event.FINISH, message=msg)
    assert r.total_rdy == max_in_flight


@pytest.mark.parametrize(['conn_count', 'old_max', 'new_max'], [
    [4, 8, 12],
    [4, 3, 12],
    [4, 2, 3],
    [4, 12, 8],
    [4, 12, 3],
    [4, 3, 2],
])
def test_rdy_set_max_in_flight(old_max, new_max, conn_count):
    r = get_reader(old_max)
    conns = [get_conn(r) for _ in range(conn_count)]
    for c in conns:
        msg = send_message(c)
        msg.trigger(event.FINISH, message=msg)
    r.set_max_in_flight(new_max)
    assert r.total_rdy <= new_max
    for c in conns:
        msg = send_message(c)
        msg.trigger(event.FINISH, message=msg)
    assert r.total_rdy == new_max


def test_rdy_disable_enable():
    max_in_flight = 10
    r = get_reader(max_in_flight)
    conns = [get_conn(r) for _ in range(max_in_flight // 2 - 1)]
    msg = send_message(conns[0])

    r.set_max_in_flight(0)
    assert r.total_rdy == 0

    conns.append(get_conn(r))
    assert r.total_rdy == 0

    msg.trigger(event.FINISH, message=msg)
    assert r.total_rdy == 0

    r.set_max_in_flight(max_in_flight)
    assert r.total_rdy == len(conns)


@patch("tornado.ioloop.IOLoop.current")
def test_rdy_disable_enable_in_backoff(ioloop_current_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    max_in_flight = 10
    r = get_reader(max_in_flight)
    conns = [get_conn(r) for _ in range(2)]
    msg = send_message(conns[0])

    r.set_max_in_flight(0)
    assert r.total_rdy == 0

    msg.trigger(event.REQUEUE, message=msg)
    timeout_args, _ = ioloop_mock.add_timeout.call_args
    timeout_args[1]()
    assert r.total_rdy == 0

    r.set_max_in_flight(max_in_flight)
    assert r.total_rdy == 1


@patch("random.randrange")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_retry(ioloop_current_mock, randrange_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    randrange_mock.return_value = 0
    r = get_reader(max_in_flight=1)
    get_conn(r)
    r.set_max_in_flight(0)
    c = get_conn(r)
    r.set_max_in_flight(1)
    if c.rdy_timeout is not None:
        timeout_args, _ = ioloop_mock.add_timeout.call_args
        timeout_args[1]()
    assert r.total_rdy <= r.max_in_flight


@patch("random.randrange")
@patch("time.time")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_redistribution_idle_conns_lose_rdy(ioloop_current_mock, time_mock, randrange_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    t = 1
    time_mock.side_effect = lambda: t
    randrange_mock.return_value = 1
    r = get_reader(max_in_flight=2)
    conn1 = get_conn(r)
    conn2 = get_conn(r)
    conn3 = get_conn(r)
    assert conn1.rdy == conn2.rdy == 1
    assert conn3.rdy == 0
    msg = send_message(conn1)
    msg.trigger(event.FINISH, message=msg)
    t += r.low_rdy_idle_timeout - 0.1
    r._redistribute_rdy_state()
    assert conn1.rdy == conn2.rdy == 1
    assert conn3.rdy == 0
    t += 0.2
    r._redistribute_rdy_state()
    assert conn1.rdy == 0
    assert conn2.rdy == conn3.rdy == 1


@patch("time.time")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_redistribution_doesnt_reallocate_rdy_in_use(ioloop_current_mock, time_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    t = 1
    time_mock.side_effect = lambda: t
    r = get_reader(max_in_flight=2)
    conn1 = get_conn(r)
    conn2 = get_conn(r)
    get_conn(r)
    assert r.total_rdy == 2
    msg1 = send_message(conn1)
    msg2 = send_message(conn2)
    msg1.trigger(event.FINISH, message=msg1)
    t += r.low_rdy_idle_timeout + 0.1
    r._redistribute_rdy_state()
    assert r.total_rdy == 1
    msg2.trigger(event.FINISH, message=msg2)
    r._redistribute_rdy_state()
    assert r.total_rdy == 2


@patch("random.randrange")
@patch("time.time")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_redistribution_isnt_pinned_to_conns(ioloop_current_mock, time_mock, randrange_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    t = 1
    time_mock.side_effect = lambda: t
    randrange_mock.return_value = 1
    r = get_reader(max_in_flight=2)
    conn1 = get_conn(r)
    conn2 = get_conn(r)
    conn3 = get_conn(r)
    assert conn1.rdy == conn2.rdy == 1
    assert conn3.rdy == 0
    msg1 = send_message(conn1)
    msg2 = send_message(conn2)
    msg1.trigger(event.FINISH, message=msg1)
    t += 1
    r._redistribute_rdy_state()
    assert conn1.rdy == 1
    assert conn2.rdy == conn3.rdy == 0
    t += 1
    msg2.trigger(event.FINISH, message=msg2)
    r._redistribute_rdy_state()
    assert conn1.rdy == conn3.rdy == 1
    assert conn2.rdy == 0


@patch("random.choice")
@patch("random.randrange")
@patch("time.time")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_redistribution_idle_conns_lose_rdy_in_backoff(
        ioloop_current_mock, time_mock, randrange_mock, choice_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    t = 1
    time_mock.side_effect = lambda: t
    randrange_mock.return_value = 2
    r = get_reader(max_in_flight=2)
    conn1 = get_conn(r)
    conn2 = get_conn(r)
    conn3 = get_conn(r)
    choice_mock.return_value = conn1
    assert conn1.rdy == conn2.rdy == 1
    assert conn3.rdy == 0
    msg = send_message(conn1)
    msg.trigger(event.REQUEUE, message=msg)
    timeout_args, _ = ioloop_mock.add_timeout.call_args
    timeout_args[1]()
    assert conn1.rdy == r.total_rdy == 1
    t += r.low_rdy_idle_timeout + 0.1
    r._redistribute_rdy_state()
    assert conn3.rdy == r.total_rdy == 1


@patch("random.choice")
@patch("time.time")
@patch("tornado.ioloop.IOLoop.current")
def test_rdy_redistribution_doesnt_reallocate_rdy_in_use_in_backoff(
        ioloop_current_mock, time_mock, choice_mock):
    ioloop_mock = get_ioloop()
    ioloop_current_mock.return_value = ioloop_mock
    t = 1
    time_mock.side_effect = lambda: t
    r = get_reader(max_in_flight=2)
    conn1 = get_conn(r)
    conn2 = get_conn(r)
    get_conn(r)
    assert r.total_rdy == 2
    msg = send_message(conn1)
    send_message(conn2)
    msg.trigger(event.REQUEUE, message=msg)
    timeout_args, _ = ioloop_mock.add_timeout.call_args
    choice_mock.return_value = conn1
    timeout_args[1]()
    assert r.total_rdy == 1
    t += r.low_rdy_idle_timeout + 0.1
    r._redistribute_rdy_state()
    assert r.total_rdy == 0
