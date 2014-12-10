# -*- coding: utf-8 -*-

import struct
import pytest
import os
import six
import sys

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq


def pytest_generate_tests(metafunc):
    identify_dict_ascii = {'a': 1, 'b': 2}
    identify_dict_unicode = {'c': six.u('w\xc3\xa5\xe2\x80\xa0')}
    identify_body_ascii = json.dumps(identify_dict_ascii)
    identify_body_unicode = json.dumps(identify_dict_unicode)

    msgs = [six.b('asdf'), six.b('ghjk'), six.b('abcd')]
    mpub_body = bytearray(struct.pack('>l', len(msgs)))
    for m in msgs:
        mpub_body += bytearray(struct.pack('>l', len(m))) + m
    mpub_body = bytes(mpub_body)

    if metafunc.function == test_command:
        for cmd_method, kwargs, result in [
                (nsq.identify,
                    {'data': identify_dict_ascii},
                    six.b('IDENTIFY\n') + struct.pack('>l', len(identify_body_ascii)) +
                    str(identify_body_ascii).encode('utf-8')),
                (nsq.identify,
                    {'data': identify_dict_unicode},
                    six.b('IDENTIFY\n') + struct.pack('>l', len(identify_body_unicode)) +
                    str(identify_body_unicode).encode('utf-8')),
                (nsq.subscribe,
                    {'topic': 'test_topic', 'channel': 'test_channel'},
                    six.b('SUB test_topic test_channel\n')),
                (nsq.finish,
                    {'id': 'test'},
                    six.b('FIN test\n')),
                (nsq.finish,
                    {'id': six.u('\u2020est \xfcn\xee\xe7\xf8\u2202\xe9')},
                    six.b('FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n')),
                (nsq.requeue,
                    {'id': 'test'},
                    six.b('REQ test 0\n')),
                (nsq.requeue,
                    {'id': 'test', 'time_ms': 60},
                    six.b('REQ test 60\n')),
                (nsq.touch,
                    {'id': 'test'},
                    six.b('TOUCH test\n')),
                (nsq.ready,
                    {'count': 100},
                    six.b('RDY 100\n')),
                (nsq.nop,
                    {},
                    six.b('NOP\n')),
                (nsq.pub,
                    {'topic': 'test', 'data': msgs[0]},
                    six.b('PUB test\n') + struct.pack('>l', len(msgs[0])) + msgs[0]),
                (nsq.mpub,
                    {'topic': 'test', 'data': msgs},
                    six.b('MPUB test\n') + struct.pack('>l', len(mpub_body)) + mpub_body)
                ]:
            metafunc.addcall(funcargs=dict(cmd_method=cmd_method, kwargs=kwargs, result=result))


def test_command(cmd_method, kwargs, result):
    assert cmd_method(**kwargs) == result


def test_unicode_topic():
    pytest.raises(AssertionError, nsq.pub, 'Ťopic', 'body')
