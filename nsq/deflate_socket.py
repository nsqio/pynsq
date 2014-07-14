from __future__ import absolute_import

import zlib
import socket
import errno


class DeflateSocket(object):
    def __init__(self, socket, level):
        wbits = -zlib.MAX_WBITS
        self._decompressor = zlib.decompressobj(wbits)
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
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

    def _recv(self, size, method):
        if self._bootstrapped:
            data = self._bootstrapped
            self._bootstrapped = None
            return data
        chunk = method(size)
        if chunk:
            uncompressed = self._decompressor.decompress(chunk)
        if not uncompressed:
            raise socket.error(errno.EWOULDBLOCK)
        return uncompressed

    def send(self, data):
        chunk = self._compressor.compress(data)
        output = chunk + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        num_bytes_sent = self._socket.send(output)

        # The caller may not know that we are compressing the data, and
        # Tornado relies on the return value to resize the transmit buffer
        # size. If the transmit buffer size decreases due to compression, then
        # NSQ commands will be broken up, triggering invalid commands on the
        # server.
        #
        # If for some reason the entire buffer was not transmitted, then
        # return the number of bytes sent. Otherwise, return the length of the
        # input data to reflect that all data was sent.

        if (num_bytes_sent != len(output)):
            return num_bytes_sent

        return len(data)
