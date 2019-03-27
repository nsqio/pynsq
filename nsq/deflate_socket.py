from __future__ import absolute_import

import zlib
import socket
import errno


class DeflateSocket(object):
    def __init__(self, socket, level):
        self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        self._bootstrapped = None
        self._socket = socket

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def bootstrap(self, data):
        if data:
            self._bootstrapped = self._decompressor.decompress(data)

    def recv(self, size):
        return self._recv(size, self._socket.recv)

    def read(self, size):
        return self._recv(size, self._socket.read)

    def recv_into(self, buf, nbytes=0):
        # no real support of efficient recv_into()
        n = nbytes or len(buf)
        data = self.recv(n)
        r = len(data)
        if r > n:
            self._bootstrapped = data[n:]
            r = n
        buf[:r] = data[:r]
        return r

    def _recv(self, size, method):
        if self._bootstrapped:
            data = self._bootstrapped
            self._bootstrapped = None
            return data
        chunk = method(size)
        if not chunk:
            return chunk
        uncompressed = self._decompressor.decompress(chunk)
        if not uncompressed:
            raise socket.error(errno.EWOULDBLOCK)
        return uncompressed

    def send(self, data):
        return self._socket.send(data)


class DeflateEncoder(object):

    def __init__(self, level):
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, -zlib.MAX_WBITS)

    def encode(self, data):
        return self._compressor.compress(data) + self._compressor.flush(zlib.Z_SYNC_FLUSH)
