from __future__ import absolute_import
from __future__ import unicode_literals

import time
import socket
import logging

from ._compat import string_types
from ._compat import struct_l
from .version import __version__

try:
    import ssl
except ImportError:
    ssl = None  # pyflakes.ignore

try:
    from .snappy_socket import SnappySocket
except ImportError:
    SnappySocket = None  # pyflakes.ignore

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

import tornado.iostream
import tornado.ioloop

try:
    from tornado.simple_httpclient import _default_ca_certs as default_ca_certs
except ImportError:
    # Tornado < 4
    from tornado.simple_httpclient import _DEFAULT_CA_CERTS

    def default_ca_certs():
        return _DEFAULT_CA_CERTS

from nsq import event, protocol
from .deflate_socket import DeflateSocket

logger = logging.getLogger(__name__)


# states
INIT = 'INIT'
DISCONNECTED = 'DISCONNECTED'
CONNECTING = 'CONNECTING'
CONNECTED = 'CONNECTED'


DEFAULT_USER_AGENT = 'pynsq/%s' % __version__


class AsyncConn(event.EventedMixin):
    """
    Low level object representing a TCP connection to nsqd.

    When a message on this connection is requeued and the requeue delay
    has not been specified, it calculates the delay automatically by an
    increasing multiple of ``requeue_delay``.

    Generates the following events that can be listened to with
    :meth:`nsq.AsyncConn.on`:

     * ``connect``
     * ``close``
     * ``error``
     * ``identify``
     * ``identify_response``
     * ``auth``
     * ``auth_response``
     * ``heartbeat``
     * ``ready``
     * ``message``
     * ``response``
     * ``backoff``
     * ``resume``

    :param host: the host to connect to

    :param port: the post to connect to

    :param timeout: the timeout for read/write operations (in seconds)

    :param heartbeat_interval: the amount of time (in seconds) to negotiate
        with the connected producers to send heartbeats (requires nsqd 0.2.19+)

    :param requeue_delay: the base multiple used when calculating requeue delay
        (multiplied by # of attempts)

    :param tls_v1: enable TLS v1 encryption (requires nsqd 0.2.22+)

    :param tls_options: dictionary of options to pass to `ssl.wrap_socket()
        <http://docs.python.org/2/library/ssl.html#ssl.wrap_socket>`_ as
        ``**kwargs``

    :param snappy: enable Snappy stream compression (requires nsqd 0.2.23+)

    :param deflate: enable deflate stream compression (requires nsqd 0.2.23+)

    :param deflate_level: configure the deflate compression level for this
        connection (requires nsqd 0.2.23+)

    :param output_buffer_size: size of the buffer (in bytes) used by nsqd
        for buffering writes to this connection

    :param output_buffer_timeout: timeout (in ms) used by nsqd before
        flushing buffered writes (set to 0 to disable).  **Warning**:
        configuring clients with an extremely low (``< 25ms``)
        ``output_buffer_timeout`` has a significant effect on ``nsqd``
        CPU usage (particularly with ``> 50`` clients connected).

    :param sample_rate: take only a sample of the messages being sent
        to the client. Not setting this or setting it to 0 will ensure
        you get all the messages destined for the client.
        Sample rate can be greater than 0 or less than 100 and the client
        will receive that percentage of the message traffic.
        (requires nsqd 0.2.25+)

    :param user_agent: a string identifying the agent for this client
        in the spirit of HTTP (default: ``<client_library_name>/<version>``)
        (requires nsqd 0.2.25+)

    :param auth_secret: a string passed when using nsq auth
        (requires nsqd 1.0+)

    :param msg_timeout: the amount of time (in seconds) that nsqd will wait
        before considering messages that have been delivered to this
        consumer timed out (requires nsqd 0.2.28+)

    :param hostname: a string identifying the host where this client runs
        (default: ``<hostname>``)
    """
    def __init__(
            self,
            host,
            port,
            timeout=1.0,
            heartbeat_interval=30,
            requeue_delay=90,
            tls_v1=False,
            tls_options=None,
            snappy=False,
            deflate=False,
            deflate_level=6,
            user_agent=DEFAULT_USER_AGENT,
            output_buffer_size=16 * 1024,
            output_buffer_timeout=250,
            sample_rate=0,
            io_loop=None,
            auth_secret=None,
            msg_timeout=None,
            hostname=None):
        assert isinstance(host, string_types)
        assert isinstance(port, int)
        assert isinstance(timeout, float)
        assert isinstance(tls_options, (dict, None.__class__))
        assert isinstance(deflate_level, int)
        assert isinstance(heartbeat_interval, int) and heartbeat_interval >= 1
        assert isinstance(requeue_delay, int) and requeue_delay >= 0
        assert isinstance(output_buffer_size, int) and output_buffer_size >= 0
        assert isinstance(output_buffer_timeout, int) and output_buffer_timeout >= 0
        assert isinstance(sample_rate, int) and sample_rate >= 0 and sample_rate < 100
        assert isinstance(auth_secret, string_types + (None.__class__,))
        assert tls_v1 and ssl or not tls_v1, \
            'tls_v1 requires Python 2.6+ or Python 2.5 w/ pip install ssl'
        assert msg_timeout is None or (isinstance(msg_timeout, (float, int)) and msg_timeout > 0)

        self.state = INIT
        self.host = host
        self.port = port
        self.timeout = timeout
        self.last_recv_timestamp = time.time()
        self.last_msg_timestamp = time.time()
        self.in_flight = 0
        self.rdy = 0
        self.rdy_timeout = None
        # for backwards compatibility when interacting with older nsqd
        # (pre 0.2.20), default this to their hard-coded max
        self.max_rdy_count = 2500
        self.tls_v1 = tls_v1
        self.tls_options = tls_options
        self.snappy = snappy
        self.deflate = deflate
        self.deflate_level = deflate_level
        self.hostname = hostname
        if self.hostname is None:
            self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        self.heartbeat_interval = heartbeat_interval * 1000
        self.msg_timeout = int(msg_timeout * 1000) if msg_timeout else None
        self.requeue_delay = requeue_delay
        self.io_loop = io_loop
        if not self.io_loop:
            self.io_loop = tornado.ioloop.IOLoop.instance()

        self.output_buffer_size = output_buffer_size
        self.output_buffer_timeout = output_buffer_timeout
        self.sample_rate = sample_rate
        self.user_agent = user_agent

        self._authentication_required = False  # tracking server auth state
        self.auth_secret = auth_secret

        self.socket = None
        self.stream = None
        self._features_to_enable = []

        self.last_rdy = 0
        self.rdy = 0

        self.callback_queue = []

        super(AsyncConn, self).__init__()

    @property
    def id(self):
        return str(self)

    def __str__(self):
        return self.host + ':' + str(self.port)

    def connected(self):
        return self.state == CONNECTED

    def connecting(self):
        return self.state == CONNECTING

    def closed(self):
        return self.state in (INIT, DISCONNECTED)

    def connect(self):
        if not self.closed():
            return

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.socket.setblocking(0)

        self.stream = tornado.iostream.IOStream(self.socket, io_loop=self.io_loop)
        self.stream.set_close_callback(self._socket_close)
        self.stream.set_nodelay(True)

        self.state = CONNECTING
        self.on(event.CONNECT, self._on_connect)
        self.on(event.DATA, self._on_data)

        self.stream.connect((self.host, self.port), self._connect_callback)

    def _connect_callback(self):
        self.state = CONNECTED
        self.stream.write(protocol.MAGIC_V2)
        self._start_read()
        self.trigger(event.CONNECT, conn=self)

    def _read_bytes(self, size, callback):
        try:
            self.stream.read_bytes(size, callback)
        except IOError:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.ConnectionClosedError('Stream is closed'),
            )

    def _start_read(self):
        self._read_bytes(4, self._read_size)

    def _socket_close(self):
        self.state = DISCONNECTED
        self.trigger(event.CLOSE, conn=self)

    def close(self):
        self.stream.close()

    def _read_size(self, data):
        try:
            size = struct_l.unpack(data)[0]
        except Exception:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.IntegrityError('failed to unpack size'),
            )
            return
        self._read_bytes(size, self._read_body)

    def _read_body(self, data):
        try:
            self.trigger(event.DATA, conn=self, data=data)
        except Exception:
            logger.exception('uncaught exception in data event')
        self._start_read()

    def send(self, data):
        self.stream.write(data)

    def upgrade_to_tls(self, options=None):
        assert ssl, 'tls_v1 requires Python 2.6+ or Python 2.5 w/ pip install ssl'

        # in order to upgrade to TLS we need to *replace* the IOStream...
        #
        # first remove the event handler for the currently open socket
        # so that when we add the socket to the new SSLIOStream below,
        # it can re-add the appropriate event handlers.
        self.io_loop.remove_handler(self.socket.fileno())

        opts = {
            'cert_reqs': ssl.CERT_REQUIRED,
            'ca_certs': default_ca_certs()
        }
        opts.update(options or {})
        self.socket = ssl.wrap_socket(self.socket, ssl_version=ssl.PROTOCOL_TLSv1,
                                      do_handshake_on_connect=False, **opts)

        self.stream = tornado.iostream.SSLIOStream(self.socket, io_loop=self.io_loop)
        self.stream.set_close_callback(self._socket_close)

        # now that the IOStream has been swapped we can kickstart
        # the SSL handshake
        self.stream._do_ssl_handshake()

    def upgrade_to_snappy(self):
        assert SnappySocket, 'snappy requires the python-snappy package'

        # in order to upgrade to Snappy we need to use whatever IOStream
        # is currently in place (normal or SSL)...
        #
        # first read any compressed bytes the existing IOStream might have
        # already buffered and use that to bootstrap the SnappySocket, then
        # monkey patch the existing IOStream by replacing its socket
        # with a wrapper that will automagically handle compression.
        existing_data = self.stream._consume(self.stream._read_buffer_size)
        self.socket = SnappySocket(self.socket)
        self.socket.bootstrap(existing_data)
        self.stream.socket = self.socket

    def upgrade_to_deflate(self):
        # in order to upgrade to DEFLATE we need to use whatever IOStream
        # is currently in place (normal or SSL)...
        #
        # first read any compressed bytes the existing IOStream might have
        # already buffered and use that to bootstrap the DefalteSocket, then
        # monkey patch the existing IOStream by replacing its socket
        # with a wrapper that will automagically handle compression.
        existing_data = self.stream._consume(self.stream._read_buffer_size)
        self.socket = DeflateSocket(self.socket, self.deflate_level)
        self.socket.bootstrap(existing_data)
        self.stream.socket = self.socket

    def send_rdy(self, value):
        try:
            self.send(protocol.ready(value))
        except Exception as e:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.SendError('failed to send RDY %d' % value, e),
            )
            return False
        self.last_rdy = value
        self.rdy = value
        return True

    def _on_connect(self, **kwargs):
        identify_data = {
            'short_id': self.short_hostname, # TODO remove when deprecating pre 1.0 support
            'long_id': self.hostname, # TODO remove when deprecating pre 1.0 support
            'client_id': self.short_hostname,
            'hostname': self.hostname,
            'heartbeat_interval': self.heartbeat_interval,
            'feature_negotiation': True,
            'tls_v1': self.tls_v1,
            'snappy': self.snappy,
            'deflate': self.deflate,
            'deflate_level': self.deflate_level,
            'output_buffer_timeout': self.output_buffer_timeout,
            'output_buffer_size': self.output_buffer_size,
            'sample_rate': self.sample_rate,
            'user_agent': self.user_agent
        }
        if self.msg_timeout:
            identify_data['msg_timeout'] = self.msg_timeout
        self.trigger(event.IDENTIFY, conn=self, data=identify_data)
        self.on(event.RESPONSE, self._on_identify_response)
        try:
            self.send(protocol.identify(identify_data))
        except Exception as e:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.SendError('failed to bootstrap connection', e),
            )

    def _on_identify_response(self, data, **kwargs):
        self.off(event.RESPONSE, self._on_identify_response)

        if data == b'OK':
            logger.warning('nsqd version does not support feature netgotiation')
            return self.trigger(event.READY, conn=self)

        try:
            data = json.loads(data.decode('utf-8'))
        except ValueError:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.IntegrityError(
                    'failed to parse IDENTIFY response JSON from nsqd - %r' %
                    data
                ),
            )
            return

        self.trigger(event.IDENTIFY_RESPONSE, conn=self, data=data)

        if self.tls_v1 and data.get('tls_v1'):
            self._features_to_enable.append('tls_v1')
        if self.snappy and data.get('snappy'):
            self._features_to_enable.append('snappy')
        if self.deflate and data.get('deflate'):
            self._features_to_enable.append('deflate')

        if data.get('auth_required'):
            self._authentication_required = True

        if data.get('max_rdy_count'):
            self.max_rdy_count = data.get('max_rdy_count')
        else:
            # for backwards compatibility when interacting with older nsqd
            # (pre 0.2.20), default this to their hard-coded max
            logger.warn('setting max_rdy_count to default value of 2500')
            self.max_rdy_count = 2500

        self.on(event.RESPONSE, self._on_response_continue)
        self._on_response_continue(conn=self, data=None)

    def _on_response_continue(self, data, **kwargs):
        if self._features_to_enable:
            feature = self._features_to_enable.pop(0)
            if feature == 'tls_v1':
                self.upgrade_to_tls(self.tls_options)
            elif feature == 'snappy':
                self.upgrade_to_snappy()
            elif feature == 'deflate':
                self.upgrade_to_deflate()
            # the server will 'OK' after these conneciton upgrades triggering another response
            return

        self.off(event.RESPONSE, self._on_response_continue)
        if self.auth_secret and self._authentication_required:
            self.on(event.RESPONSE, self._on_auth_response)
            self.trigger(event.AUTH, conn=self, data=self.auth_secret)
            try:
                self.send(protocol.auth(self.auth_secret))
            except Exception as e:
                self.close()
                self.trigger(
                    event.ERROR,
                    conn=self,
                    error=protocol.SendError('Error sending AUTH', e),
                )
            return
        self.trigger(event.READY, conn=self)

    def _on_auth_response(self, data, **kwargs):
        try:
            data = json.loads(data.decode('utf-8'))
        except ValueError:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.IntegrityError(
                    'failed to parse AUTH response JSON from nsqd - %r' % data
                ),
            )
            return

        self.off(event.RESPONSE, self._on_auth_response)
        self.trigger(event.AUTH_RESPONSE, conn=self, data=data)
        return self.trigger(event.READY, conn=self)

    def _on_data(self, data, **kwargs):
        self.last_recv_timestamp = time.time()
        frame, data = protocol.unpack_response(data)
        if frame == protocol.FRAME_TYPE_MESSAGE:
            self.last_msg_timestamp = time.time()
            self.in_flight += 1

            message = protocol.decode_message(data)
            message.on(event.FINISH, self._on_message_finish)
            message.on(event.REQUEUE, self._on_message_requeue)
            message.on(event.TOUCH, self._on_message_touch)

            self.trigger(event.MESSAGE, conn=self, message=message)
        elif frame == protocol.FRAME_TYPE_RESPONSE and data == b'_heartbeat_':
            self.send(protocol.nop())
            self.trigger(event.HEARTBEAT, conn=self)
        elif frame == protocol.FRAME_TYPE_RESPONSE:
            self.trigger(event.RESPONSE, conn=self, data=data)
        elif frame == protocol.FRAME_TYPE_ERROR:
            self.trigger(event.ERROR, conn=self, error=protocol.Error(data))

    def _on_message_requeue(self, message, backoff=True, time_ms=-1, **kwargs):
        if backoff:
            self.trigger(event.BACKOFF, conn=self)
        else:
            self.trigger(event.CONTINUE, conn=self)

        self.in_flight -= 1
        try:
            time_ms = self.requeue_delay * message.attempts * 1000 if time_ms < 0 else time_ms
            self.send(protocol.requeue(message.id, time_ms))
        except Exception as e:
            self.close()
            self.trigger(event.ERROR, conn=self, error=protocol.SendError(
                'failed to send REQ %s @ %d' % (message.id, time_ms), e))

    def _on_message_finish(self, message, **kwargs):
        self.trigger(event.RESUME, conn=self)

        self.in_flight -= 1
        try:
            self.send(protocol.finish(message.id))
        except Exception as e:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.SendError('failed to send FIN %s' % message.id, e),
            )

    def _on_message_touch(self, message, **kwargs):
        try:
            self.send(protocol.touch(message.id))
        except Exception as e:
            self.close()
            self.trigger(
                event.ERROR,
                conn=self,
                error=protocol.SendError('failed to send TOUCH %s' % message.id, e),
            )
