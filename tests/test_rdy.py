from .reader_unit_test_helpers import (
    get_reader,
    get_conn,
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
    assert r.total_rdy == max_in_flight
