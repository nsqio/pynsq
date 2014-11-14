import struct
import re

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from .message import Message
import six


MAGIC_V2 = six.b('  V2')
NL = six.b('\n')

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2


class Error(Exception):
    pass


class SendError(Error):
    def __init__(self, msg, error=None):
        self.msg = msg
        self.error = error

    def __str__(self):
        return 'SendError: %s (%s)' % (self.msg, self.error)

    def __repr__(self):
        return 'SendError: %s (%s)' % (self.msg, self.error)


class ConnectionClosedError(Error):
    pass


class IntegrityError(Error):
    pass


def unpack_response(data):
    frame = struct.unpack('>l', data[:4])[0]
    return frame, data[4:]


def decode_message(data):
    timestamp = struct.unpack('>q', data[:8])[0]
    attempts = struct.unpack('>h', data[8:10])[0]
    id = data[10:26]
    body = data[26:]
    return Message(id, body, timestamp, attempts)


def _command(cmd, body, *params):
    body_data = six.b('')
    params_data = bytearray()

    assert isinstance(cmd, six.binary_type)

    if body:
        assert isinstance(body, six.string_types + (six.binary_type,)), \
            'body must be a string or binary type'
        if not isinstance(body, six.binary_type):
            body = body.encode('utf-8')

        body_data = struct.pack('>l', len(body)) + body
    for p in params:
        assert isinstance(p, six.string_types + (six.binary_type,)), \
            'params must be a string or binary type'
        if not isinstance(p, six.binary_type):
            p = p.encode('utf-8')

        params_data += six.b(' ') + p

    assert isinstance(body_data, six.binary_type)
    return bytes(cmd + params_data + NL + body_data)


def subscribe(topic, channel):
    assert valid_topic_name(topic)
    assert valid_channel_name(channel)
    return _command(six.b('SUB'), None, topic, channel)


def identify(data):
    return _command(six.b('IDENTIFY'), json.dumps(data))



def auth(data):
    return _command(six.b('AUTH'), data)



def ready(count):
    assert isinstance(count, int), 'ready count must be an integer'
    assert count >= 0, 'ready count cannot be negative'
    return _command(six.b('RDY'), None, str(count))


def finish(id):
    return _command(six.b('FIN'), None, id)


def requeue(id, time_ms=0):
    assert isinstance(time_ms, int), 'requeue time_ms must be an integer'
    return _command(six.b('REQ'), None, id, str(time_ms))


def touch(id):
    return _command(six.b('TOUCH'), None, id)


def nop():
    return _command(six.b('NOP'), None)


def pub(topic, data):
    assert valid_topic_name(topic)
    return _command(six.b('PUB'), data, topic)


def mpub(topic, data):
    assert valid_topic_name(topic)

    assert isinstance(data, (set, list))
    body = struct.pack('>l', len(data))
    for m in data:
        body += struct.pack('>l', len(m)) + m
    return _command(six.b('MPUB'), body, topic)


def valid_topic_name(topic):
    if not 0 < len(topic) < 65:
        return False
    if re.match(r'^[\.a-zA-Z0-9_-]+$', topic):
        return True
    return False


def valid_channel_name(channel):
    if not 0 < len(channel) < 65:
        return False
    if re.match(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$', channel):
        return True
    return False
