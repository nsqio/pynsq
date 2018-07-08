from __future__ import absolute_import
from __future__ import unicode_literals

import time

from . import mock_socket

from nsq._compat import struct_h, struct_l, struct_q
from nsq import protocol, sync
sync.socket = mock_socket


def mock_write(c, data):
    c.s.queue_recv(data)


def mock_response_write(c, frame_type, data):
    body_size = 4 + len(data)
    body_size_packed = struct_l.pack(body_size)
    frame_type_packed = struct_l.pack(frame_type)
    mock_write(c, body_size_packed + frame_type_packed + data)


def mock_response_write_message(c, timestamp, attempts, id, body):
    timestamp_packed = struct_q.pack(timestamp)
    attempts_packed = struct_h.pack(attempts)
    id = ("%016d" % id).encode()
    mock_response_write(
        c, protocol.FRAME_TYPE_MESSAGE, timestamp_packed + attempts_packed + id + body)


def test_sync_authenticate_subscribe():
    c = sync.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(protocol.identify({'short_id': 'test', 'long_id': 'test.example',
                              'client_id': 'test', 'hostname': 'test.example'}))
    c.send(protocol.subscribe('test', 'ch'))

    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, b'OK')
    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, b'OK')

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == b'OK'

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == b'OK'


def test_sync_receive_messages():
    c = sync.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(protocol.identify({'short_id': 'test', 'long_id': 'test.example',
                              'client_id': 'test', 'hostname': 'test.example'}))
    c.send(protocol.subscribe('test', 'ch'))

    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, b'OK')
    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, b'OK')

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == b'OK'

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == b'OK'

    for i in range(10):
        c.send(protocol.ready(1))
        body = ('{"data": {"test_key": %d}}' % i).encode()
        ts = int(time.time() * 1000 * 1000)
        mock_response_write_message(c, ts, 0, i, body)
        resp = c.read_response()
        unpacked = protocol.unpack_response(resp)
        assert unpacked[0] == protocol.FRAME_TYPE_MESSAGE
        msg = protocol.decode_message(unpacked[1])
        assert msg.timestamp == ts
        assert msg.id == ("%016d" % i).encode()
        assert msg.attempts == 0
        assert msg.body == body
