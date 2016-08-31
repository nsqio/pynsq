from __future__ import absolute_import
from __future__ import unicode_literals
import re

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from ._compat import bytes_types
from ._compat import to_bytes
from ._compat import struct_h, struct_l, struct_q
from .message import Message

MAGIC_V2 = b'  V2'
NL = b'\n'

FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2

# commmands
AUTH = b'AUTH'
FIN = b'FIN'  # success
IDENTIFY = b'IDENTIFY'
MPUB = b'MPUB'
NOP = b'NOP'
PUB = b'PUB'  # publish
RDY = b'RDY'
REQ = b'REQ'  # requeue
SUB = b'SUB'
TOUCH = b'TOUCH'
DPUB = b'DPUB'  # deferred publish


class Error(Exception):
    pass


class SendError(Error):
    def __init__(self, msg, error=None):
        self.msg = msg
        self.error = error

    def __str__(self):
        return 'SendError: %s (%s)' % (self.msg, self.error)

    __repr__ = __str__


class ConnectionClosedError(Error):
    pass


class IntegrityError(Error):
    pass


def unpack_response(data):
    frame = struct_l.unpack(data[:4])[0]
    return frame, data[4:]


def decode_message(data):
    timestamp = struct_q.unpack(data[:8])[0]
    attempts = struct_h.unpack(data[8:10])[0]
    id = data[10:26]
    body = data[26:]
    return Message(id, body, timestamp, attempts)


def _command(cmd, body, *params):
    body_data = b''
    params_data = b''
    if body:
        assert isinstance(body, bytes_types), 'body must be a bytestring'
        body_bytes = to_bytes(body)  # raises if not convertible to bytes
        body_data = struct_l.pack(len(body)) + body_bytes
    if len(params):
        params = [to_bytes(p) for p in params]
        params_data = b' ' + b' '.join(params)
    return b''.join((cmd, params_data, NL, body_data))


def subscribe(topic, channel):
    assert valid_topic_name(topic)
    assert valid_channel_name(channel)
    return _command(SUB, None, topic, channel)


def identify(data):
    return _command(IDENTIFY, to_bytes(json.dumps(data)))


def auth(data):
    return _command(AUTH, data)


def ready(count):
    assert isinstance(count, int), 'ready count must be an integer'
    assert count >= 0, 'ready count cannot be negative'
    return _command(RDY, None, str(count))


def finish(id):
    return _command(FIN, None, id)


def requeue(id, time_ms=0):
    assert isinstance(time_ms, int), 'requeue time_ms must be an integer'
    return _command(REQ, None, id, str(time_ms))


def touch(id):
    return _command(TOUCH, None, id)


def nop():
    return _command(NOP, None)


def pub(topic, data):
    return _command(PUB, data, topic)


def mpub(topic, data):
    assert isinstance(data, (set, list))
    body = struct_l.pack(len(data))
    for m in data:
        assert isinstance(m, bytes_types), 'message bodies must be bytestrings'
        body += struct_l.pack(len(m)) + to_bytes(m)
    return _command(MPUB, body, topic)


def dpub(topic, delay_ms, data):
    assert isinstance(delay_ms, int), 'dpub delay_ms must be an integer'
    return _command(DPUB, data, topic, str(delay_ms))


VALID_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')


def _is_valid_name(name):
    if not 0 < len(name) < 65:
        return False
    if VALID_NAME_RE.match(name):
        return True
    return False


def valid_topic_name(topic):
    return _is_valid_name(topic)


def valid_channel_name(channel):
    return _is_valid_name(channel)
