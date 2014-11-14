import struct
import time
import os
import six
import sys

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from tests import mock_socket
import nsq
nsq.sync.socket = mock_socket


def mock_write(c, data):
    c.s.queue_recv(data)


def mock_response_write(c, frame_type, data):
    if not isinstance(data, six.binary_type):
        data = data.encode('utf-8')

    body_size = 4 + len(data)
    body_size_packed = struct.pack('>l', body_size)
    frame_type_packed = struct.pack('>l', frame_type)
    mock_write(c, body_size_packed + frame_type_packed + data)


def mock_response_write_message(c, timestamp, attempts, id, body):
    if not isinstance(body, six.binary_type):
        body = body.encode('utf-8')

    timestamp_packed = struct.pack('>q', timestamp)
    attempts_packed = struct.pack('>h', attempts)
    id = ("%016d" % id).encode('utf-8')
    mock_response_write(c, nsq.FRAME_TYPE_MESSAGE,
                        timestamp_packed + attempts_packed + id + body)


def test_sync_authenticate_subscribe():
    c = nsq.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(nsq.identify({'short_id': 'test', 'long_id': 'test.example'}))
    c.send(nsq.subscribe('test', 'ch'))

    mock_response_write(c, nsq.FRAME_TYPE_RESPONSE, 'OK')
    mock_response_write(c, nsq.FRAME_TYPE_RESPONSE, 'OK')

    resp = c.read_response()
    unpacked = nsq.unpack_response(resp)
    assert unpacked[0] == nsq.FRAME_TYPE_RESPONSE
    assert unpacked[1] == six.b('OK')

    resp = c.read_response()
    unpacked = nsq.unpack_response(resp)
    assert unpacked[0] == nsq.FRAME_TYPE_RESPONSE
    assert unpacked[1] == six.b('OK')


def test_sync_receive_messages():
    c = nsq.SyncConn()
    c.connect("127.0.0.1", 4150)

    c.send(nsq.identify({'short_id': 'test', 'long_id': 'test.example'}))
    c.send(nsq.subscribe('test', 'ch'))

    mock_response_write(c, nsq.FRAME_TYPE_RESPONSE, 'OK')
    mock_response_write(c, nsq.FRAME_TYPE_RESPONSE, 'OK')

    resp = c.read_response()
    unpacked = nsq.unpack_response(resp)
    assert unpacked[0] == nsq.FRAME_TYPE_RESPONSE
    assert unpacked[1] == six.b('OK')

    resp = c.read_response()
    unpacked = nsq.unpack_response(resp)
    assert unpacked[0] == nsq.FRAME_TYPE_RESPONSE
    assert unpacked[1] == six.b('OK')

    for i in range(10):
        c.send(nsq.ready(1))
        body = '{"data": {"test_key": %d}}' % i
        body = body.encode('utf-8')
        ts = int(time.time() * 1000 * 1000)
        mock_response_write_message(c, ts, 0, i, body)
        resp = c.read_response()
        unpacked = nsq.unpack_response(resp)
        assert unpacked[0] == nsq.FRAME_TYPE_MESSAGE
        msg = nsq.decode_message(unpacked[1])
        assert msg.timestamp == ts
        assert msg.id == ("%016d" % i).encode('utf-8')
        assert msg.attempts == 0
        assert msg.body == body
