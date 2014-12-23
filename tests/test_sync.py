from __future__ import absolute_import

import struct
import time

from . import mock_socket
from nsq import protocol, sync
sync.socket = mock_socket


def mock_write(c, data):
    c.s.queue_recv(data)


def mock_response_write(c, frame_type, data):
    body_size = 4 + len(data)
    body_size_packed = struct.pack('>l', body_size)
    frame_type_packed = struct.pack('>l', frame_type)
    mock_write(c, body_size_packed + frame_type_packed + data)


def mock_response_write_message(c, timestamp, attempts, id, body):
    timestamp_packed = struct.pack('>q', timestamp)
    attempts_packed = struct.pack('>h', attempts)
    id = "%016d" % id
    mock_response_write(c, protocol.FRAME_TYPE_MESSAGE, timestamp_packed + attempts_packed + id + body)


def test_sync_authenticate_subscribe():
    c = sync.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(protocol.identify({'short_id': 'test', 'long_id': 'test.example'}))
    c.send(protocol.subscribe('test', 'ch'))

    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, 'OK')
    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, 'OK')

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == 'OK'

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == 'OK'


def test_sync_receive_messages():
    c = sync.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(protocol.identify({'short_id': 'test', 'long_id': 'test.example'}))
    c.send(protocol.subscribe('test', 'ch'))

    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, 'OK')
    mock_response_write(c, protocol.FRAME_TYPE_RESPONSE, 'OK')

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == 'OK'

    resp = c.read_response()
    unpacked = protocol.unpack_response(resp)
    assert unpacked[0] == protocol.FRAME_TYPE_RESPONSE
    assert unpacked[1] == 'OK'

    for i in range(10):
        c.send(protocol.ready(1))
        body = '{"data": {"test_key": %d}}' % i
        ts = int(time.time() * 1000 * 1000)
        mock_response_write_message(c, ts, 0, i, body)
        resp = c.read_response()
        unpacked = protocol.unpack_response(resp)
        assert unpacked[0] == protocol.FRAME_TYPE_MESSAGE
        msg = protocol.decode_message(unpacked[1])
        assert msg.timestamp == ts
        assert msg.id == "%016d" % i
        assert msg.attempts == 0
        assert msg.body == body
