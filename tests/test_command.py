from __future__ import absolute_import

import struct
import pytest
import os
import sys

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from nsq import protocol


def pytest_generate_tests(metafunc):
    identify_dict_ascii = {'a': 1, 'b': 2}
    identify_dict_unicode = {'c': u'w\xc3\xa5\xe2\x80\xa0'}
    identify_body_ascii = json.dumps(identify_dict_ascii)
    identify_body_unicode = json.dumps(identify_dict_unicode)

    msgs = [b'asdf', b'ghjk', b'abcd']
    mpub_body = (
        struct.pack('>l', len(msgs)) +
        b''.join(struct.pack('>l', len(m)) + m for m in msgs)
    )
    if metafunc.function == test_command:
        for cmd_method, kwargs, result in [
                (protocol.identify,
                    {'data': identify_dict_ascii},
                    b'IDENTIFY\n' + struct.pack('>l', len(identify_body_ascii)) +
                    identify_body_ascii.encode("utf8")),
                (protocol.identify,
                    {'data': identify_dict_unicode},
                    b'IDENTIFY\n' + struct.pack('>l', len(identify_body_unicode)) +
                    identify_body_unicode.encode("utf8")),
                (protocol.subscribe,
                    {'topic': 'test_topic', 'channel': 'test_channel'},
                    b'SUB test_topic test_channel\n'),
                (protocol.finish,
                    {'id': 'test'},
                    b'FIN test\n'),
                (protocol.finish,
                    {'id': u'\u2020est \xfcn\xee\xe7\xf8\u2202\xe9'},
                    b'FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n'),
                (protocol.requeue,
                    {'id': 'test'},
                    b'REQ test 0\n'),
                (protocol.requeue,
                    {'id': 'test', 'time_ms': 60},
                    b'REQ test 60\n'),
                (protocol.touch,
                    {'id': 'test'},
                    b'TOUCH test\n'),
                (protocol.ready,
                    {'count': 100},
                    b'RDY 100\n'),
                (protocol.nop,
                    {},
                    b'NOP\n'),
                (protocol.pub,
                    {'topic': 'test', 'data': msgs[0]},
                    b'PUB test\n' + struct.pack('>l', len(msgs[0])) + msgs[0]),
                (protocol.mpub,
                    {'topic': 'test', 'data': msgs},
                    b'MPUB test\n' + struct.pack('>l', len(mpub_body)) + mpub_body)
                ]:
            metafunc.addcall(funcargs=dict(cmd_method=cmd_method, kwargs=kwargs, result=result))


def test_command(cmd_method, kwargs, result):
    assert cmd_method(**kwargs) == result
