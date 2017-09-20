from __future__ import absolute_import

import snappy
import socket
import errno


class SnappySocket(object):
    def __init__(self, socket):
        self._decompressor = snappy.StreamDecompressor()
        self._compressor = snappy.StreamCompressor()
        self._socket = socket
        self._bootstrapped = None

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def bootstrap(self, data):
        if data:
            self._bootstrapped = self._decompressor.decompress(data)

    def recv(self, size):
        return self._recv(size, self._socket.recv)

    def read(self, size):
        return self._recv(size, self._socket.read)

    def _recv(self, size, method):
        if self._bootstrapped:
            data = self._bootstrapped
            self._bootstrapped = None
            return data
        chunk = method(size)
        uncompressed = self._decompressor.decompress(chunk) if chunk else None
        if not uncompressed:
            raise socket.error(errno.EWOULDBLOCK)
        return uncompressed

    def send(self, data):
        if isinstance(data, memoryview):
            data = data.tobytes()
        chunk = self._compressor.add_chunk(data, compress=True)
        self._socket.send(chunk)
        return len(data)
