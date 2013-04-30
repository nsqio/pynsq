import struct
import os
import sys
try:
    import simplejson as json
except ImportError:
    import json # pyflakes.ignore

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

import nsq


def pytest_generate_tests(metafunc):
    identify_body = json.dumps({'a': 1, 'b': 2})
    msgs = ['asdf', 'ghjk', 'abcd']
    mpub_body = struct.pack('>l', len(msgs)) + ''.join(struct.pack('>l', len(m)) + m for m in msgs)
    if metafunc.function == test_command:
        for cmd_method, kwargs, result in [
                (nsq.identify, 
                    {'data': {'a': 1, 'b': 2}}, 
                    'IDENTIFY\n' + struct.pack('>l', len(identify_body)) + identify_body),
                (nsq.subscribe, 
                    {'topic': 'test_topic', 'channel': 'test_channel'}, 
                    'SUB test_topic test_channel\n'),
                (nsq.finish, 
                    {'id': 'test'},
                    'FIN test\n'),
                (nsq.requeue, 
                    {'id': 'test'},
                    'REQ test 0\n'),
                (nsq.requeue, 
                    {'id': 'test', 'time_ms': 60},
                    'REQ test 60\n'),
                (nsq.touch, 
                    {'id': 'test'}, 
                    'TOUCH test\n'),
                (nsq.ready, 
                    {'count': 100},
                    'RDY 100\n'),
                (nsq.nop, 
                    {},
                    'NOP\n'),
                (nsq.pub,
                    {'topic': 'test', 'data': msgs[0]},
                    'PUB test\n' + struct.pack('>l', len(msgs[0])) + msgs[0]),
                (nsq.mpub,
                    {'topic': 'test', 'data': msgs},
                    'MPUB test\n' + struct.pack('>l', len(mpub_body)) + mpub_body)
            ]:
            metafunc.addcall(funcargs=dict(cmd_method=cmd_method, kwargs=kwargs, result=result))

def test_command(cmd_method, kwargs, result):
    assert cmd_method(**kwargs) == result
