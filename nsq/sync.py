from __future__ import absolute_import
from __future__ import unicode_literals

import socket

from nsq._compat import string_types
from nsq._compat import struct_l
from nsq import protocol


class SyncConn(object):
    def __init__(self, timeout=1.0):
        self.buffer = b''
        self.timeout = timeout
        self.s = None

    def connect(self, host, port):
        assert isinstance(host, string_types)
        assert isinstance(port, int)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(self.timeout)
        self.s.connect((host, port))
        self.s.send(protocol.MAGIC_V2)

    def _readn(self, size):
        while True:
            if len(self.buffer) >= size:
                break
            packet = self.s.recv(4096)
            if not packet:
                raise Exception('failed to read %d' % size)
            self.buffer += packet
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data

    def read_response(self):
        size = struct_l.unpack(self._readn(4))[0]
        return self._readn(size)

    def send(self, data):
        self.s.send(data)
