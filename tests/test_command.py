from __future__ import absolute_import
from __future__ import unicode_literals

import os
import sys
import json

import pytest

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from nsq._compat import to_bytes
from nsq._compat import struct_l
from nsq import protocol


identify_dict_ascii = {'a': 1, 'b': 2}
identify_dict_unicode = {'c': u'w\xc3\xa5\xe2\x80\xa0'}
identify_body_ascii = to_bytes(json.dumps(identify_dict_ascii))
identify_body_unicode = to_bytes(json.dumps(identify_dict_unicode))
msgs = [b'asdf', b'ghjk', b'abcd']
mpub_body = struct_l.pack(len(msgs)) + b''.join(struct_l.pack(len(m)) + m for m in msgs)


@pytest.mark.parametrize(['cmd_method', 'kwargs', 'result'], [
    pytest.param(protocol.identify,
                 {'data': identify_dict_ascii},
                 b'IDENTIFY\n' + struct_l.pack(len(identify_body_ascii))
                 + to_bytes(identify_body_ascii),  # noqa: W503
                 id="identify-ascii"),
    pytest.param(protocol.identify,
                 {'data': identify_dict_unicode},
                 b'IDENTIFY\n' + struct_l.pack(len(identify_body_unicode))
                 + to_bytes(identify_body_unicode),  # noqa: W503
                 id="identify-unicode"),
    pytest.param(protocol.subscribe,
                 {'topic': 'test_topic', 'channel': 'test_channel'},
                 b'SUB test_topic test_channel\n',
                 id="subscribe-topic"),
    pytest.param(protocol.finish,
                 {'id': 'test'},
                 b'FIN test\n',
                 id="finish-ascii"),
    pytest.param(protocol.finish,
                 {'id': u'\u2020est \xfcn\xee\xe7\xf8\u2202\xe9'},
                 b'FIN \xe2\x80\xa0est \xc3\xbcn\xc3\xae\xc3\xa7\xc3\xb8\xe2\x88\x82\xc3\xa9\n',
                 id="finish-binary"),
    pytest.param(protocol.requeue,
                 {'id': 'test'},
                 b'REQ test 0\n',
                 id="requeue-0"),
    pytest.param(protocol.requeue,
                 {'id': 'test', 'time_ms': 60},
                 b'REQ test 60\n',
                 id="requeue-60"),
    pytest.param(protocol.touch,
                 {'id': 'test'},
                 b'TOUCH test\n',
                 id="touch"),
    pytest.param(protocol.ready,
                 {'count': 100},
                 b'RDY 100\n',
                 id="ready"),
    pytest.param(protocol.nop,
                 {},
                 b'NOP\n',
                 id="nop"),
    pytest.param(protocol.pub,
                 {'topic': 'test', 'data': msgs[0]},
                 b'PUB test\n' + struct_l.pack(len(msgs[0])) + to_bytes(msgs[0]),
                 id="pub"),
    pytest.param(protocol.mpub,
                 {'topic': 'test', 'data': msgs},
                 b'MPUB test\n' + struct_l.pack(len(mpub_body)) + to_bytes(mpub_body),
                 id="mpub"),
])
def test_command(cmd_method, kwargs, result):
    assert cmd_method(**kwargs) == result


def test_unicode_body():
    pytest.raises(AssertionError, protocol.pub, 'topic', u'unicode body')
