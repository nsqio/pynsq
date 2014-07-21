import struct
import re

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from message import Message


MAGIC_V2 = '  V2'
NL = '\n'

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
    body_data = ''
    params_data = ''
    if body:
        assert isinstance(body, str), 'body must be a string'
        body_data = struct.pack('>l', len(body)) + body
    if len(params):
        params = [p.encode('utf-8') if isinstance(p, unicode) else p for p in params]
        params_data = ' ' + ' '.join(params)
    return '%s%s%s%s' % (cmd, params_data, NL, body_data)


def subscribe(topic, channel):
    assert valid_topic_name(topic)
    assert valid_channel_name(channel)
    return _command('SUB', None, topic, channel)


def identify(data):
    return _command('IDENTIFY', json.dumps(data))

def auth(data):
    return _command('AUTH', data)

def ready(count):
    assert isinstance(count, int), 'ready count must be an integer'
    assert count >= 0, 'ready count cannot be negative'
    return _command('RDY', None, str(count))


def finish(id):
    return _command('FIN', None, id)


def requeue(id, time_ms=0):
    assert isinstance(time_ms, int), 'requeue time_ms must be an integer'
    return _command('REQ', None, id, str(time_ms))


def touch(id):
    return _command('TOUCH', None, id)


def nop():
    return _command('NOP', None)


def pub(topic, data):
    return _command('PUB', data, topic)


def mpub(topic, data):
    assert isinstance(data, (set, list))
    body = struct.pack('>l', len(data))
    for m in data:
        body += struct.pack('>l', len(m)) + m
    return _command('MPUB', body, topic)


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
