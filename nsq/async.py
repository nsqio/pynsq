import socket
try:
    import ssl
except ImportError:
    ssl = None # pyflakes.ignore
import struct
import logging

import tornado.iostream
import tornado.ioloop
import tornado.simple_httpclient

import nsq


class AsyncConn(object):
    def __init__(self, host, port, connect_callback, data_callback, close_callback, timeout=1.0):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        assert callable(connect_callback)
        assert callable(data_callback)
        assert callable(close_callback)
        assert isinstance(timeout, float)
        
        self.connecting = False
        self.connected = False
        self.host = host
        self.port = port
        self.connect_callback = connect_callback
        self.data_callback = data_callback
        self.close_callback = close_callback
        self.timeout = timeout
    
    @property
    def id(self):
        return str(self)
    
    def __str__(self):
        return self.host + ':' + str(self.port)
    
    def connect(self):
        if self.connected or self.connecting:
            return
        
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(self.timeout)
        self.s.setblocking(0)
        
        self.stream = tornado.iostream.IOStream(self.s)
        self.stream.set_close_callback(self._socket_close)
        
        self.connecting = True
        self.stream.connect((self.host, self.port), self._connect_callback)
    
    def _connect_callback(self):
        self.connecting = False
        self.connected = True
        self.stream.write(nsq.MAGIC_V2)
        self._start_read()
        try:
            self.connect_callback(self)
        except Exception:
            logging.exception("uncaught exception in connect_callback")
    
    def _start_read(self):
        self.stream.read_bytes(4, self._read_size)
    
    def _socket_close(self):
        self.connected = False
        try:
            self.close_callback(self)
        except Exception:
            logging.exception("uncaught exception in close_callback")
    
    def close(self):
        self.connected = False
        self.stream.close()
    
    def _read_size(self, data):
        try:
            size = struct.unpack('>l', data)[0]
            self.stream.read_bytes(size, self._read_body)
        except Exception:
            self.close()
            logging.exception("failed to unpack size")
    
    def _read_body(self, data):
        try:
            self.data_callback(self, data)
        except Exception:
            logging.exception("uncaught exception in data_callback")
        tornado.ioloop.IOLoop.instance().add_callback(self._start_read)
    
    def send(self, data):
        self.stream.write(data)
    
    def upgrade_to_tls(self, options=None):
        assert ssl, "tls_v1 requires Python 2.6+ or Python 2.5 w/ pip install ssl"
        opts = {
            'cert_reqs': ssl.CERT_REQUIRED, 
            'ca_certs': tornado.simple_httpclient._DEFAULT_CA_CERTS
        }
        opts.update(options or {})
        ssl_socket = ssl.wrap_socket(self.s, ssl_version=ssl.PROTOCOL_TLSv1, 
            do_handshake_on_connect=False, **opts)
        tornado.ioloop.IOLoop.instance().remove_handler(self.s.fileno())
        self.stream = tornado.iostream.SSLIOStream(ssl_socket)
        self.stream.set_close_callback(self._socket_close)
        self.stream._do_ssl_handshake()
