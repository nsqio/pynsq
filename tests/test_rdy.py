from .reader_unit_test_helpers import (
    get_reader,
    get_ioloop,
    get_conn,
    send_message
)


def test_initial_probe_then_full_throttle():
    mock_ioloop = get_ioloop()
    r = get_reader(mock_ioloop)
    conn = get_conn(r)
    assert conn.rdy == 1
    send_message(conn)
    assert conn.rdy == r._connection_max_in_flight()


def test_new_conn_throttles_down_existing_conns():
    mock_ioloop = get_ioloop()
    r = get_reader(mock_ioloop)
    conn1 = get_conn(r)
    send_message(conn1)
    assert conn1.rdy == r._connection_max_in_flight()

    conn2 = get_conn(r)
    assert conn2.rdy == 1
    assert conn1.rdy == r._connection_max_in_flight()
