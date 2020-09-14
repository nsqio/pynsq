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
