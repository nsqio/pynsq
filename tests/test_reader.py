from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import signal
import subprocess
import time
import ssl

import tornado.gen
import tornado.httpclient
import tornado.httpserver
import tornado.testing
import tornado.web

# shunt '..' into sys.path since we are in a 'tests' subdirectory
base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from nsq import protocol
from nsq.conn import AsyncConn
from nsq.reader import Reader
from nsq.deflate_socket import DeflateSocket
from nsq.snappy_socket import SnappySocket


class IntegrationBase(tornado.testing.AsyncTestCase):
    nsqd_command = []
    nsqlookupd_command = []

    def setUp(self):
        super(IntegrationBase, self).setUp()
        if not hasattr(self, 'processes'):
            self.processes = []

        if self.nsqlookupd_command:
            proc = subprocess.Popen(self.nsqlookupd_command)
            self.processes.append(proc)
            self.wait_ping('http://127.0.0.1:4161/ping')
        if self.nsqd_command:
            proc = subprocess.Popen(self.nsqd_command)
            self.processes.append(proc)
            self.wait_ping('http://127.0.0.1:4151/ping')

    def tearDown(self):
        super(IntegrationBase, self).tearDown()
        for proc in self.processes:
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait()

    def wait_ping(self, endpoint):
        start = time.time()
        http = tornado.httpclient.HTTPClient()
        while True:
            try:
                resp = http.fetch(endpoint)
                if resp.body == b'OK':
                    break
                continue
            except Exception:
                if time.time() - start > 5:
                    raise
                time.sleep(0.1)
                continue

    def _send_messages(self, topic, count, body):
        c = AsyncConn('127.0.0.1', 4150)
        c.connect()

        def _on_ready(*args, **kwargs):
            for i in range(count):
                c.send(protocol.pub(topic, body))

        c.on('ready', _on_ready)


class ReaderIntegrationTest(IntegrationBase):
    identify_options = {
        'user_agent': 'sup',
        'snappy': True,
        'tls_v1': True,
        'tls_options': {'cert_reqs': ssl.CERT_NONE},
        'heartbeat_interval': 10,
        'output_buffer_size': 4096,
        'output_buffer_timeout': 50
    }

    nsqd_command = ['nsqd', '--verbose', '--snappy',
                    '--tls-key=%s/tests/key.pem' % base_dir,
                    '--tls-cert=%s/tests/cert.pem' % base_dir]

    def test_bad_reader_arguments(self):
        topic = 'test_reader_msgs_%s' % time.time()
        bad_options = dict(self.identify_options)
        bad_options.update(dict(foo=10))

        def handler(msg):
            return None

        self.assertRaises(
            AssertionError,
            Reader,
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            topic=topic, channel='ch',
            message_handler=handler, max_in_flight=100,
            **bad_options)

    def test_conn_identify(self):
        c = AsyncConn('127.0.0.1', 4150)
        c.on('identify_response', self.stop)
        c.connect()
        response = self.wait()
        print(response)
        assert response['conn'] is c
        assert isinstance(response['data'], dict)

    def test_conn_identify_options(self):
        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)
        c.on('identify_response', self.stop)
        c.connect()
        response = self.wait()
        print(response)
        assert response['conn'] is c
        assert isinstance(response['data'], dict)
        assert response['data']['snappy'] is True
        assert response['data']['tls_v1'] is True

    def test_conn_socket_upgrade(self):
        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)
        c.on('ready', self.stop)
        c.connect()
        self.wait()
        assert isinstance(c.socket, SnappySocket)
        assert isinstance(c.socket._socket, ssl.SSLSocket)

    def test_conn_subscribe(self):
        topic = 'test_conn_suscribe_%s' % time.time()
        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)

        def _on_ready(*args, **kwargs):
            c.on('response', self.stop)
            c.send(protocol.subscribe(topic, 'ch'))

        c.on('ready', _on_ready)
        c.connect()
        response = self.wait()
        print(response)
        assert response['conn'] is c
        assert response['data'] == b'OK'

    def test_conn_messages(self):
        self.msg_count = 0

        topic = 'test_conn_suscribe_%s' % time.time()
        self._send_messages(topic, 5, b'sup')

        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)

        def _on_message(*args, **kwargs):
            self.msg_count += 1
            if c.in_flight == 5:
                self.stop()

        def _on_ready(*args, **kwargs):
            c.on('message', _on_message)
            c.send(protocol.subscribe(topic, 'ch'))
            c.send_rdy(5)

        c.on('ready', _on_ready)
        c.connect()

        self.wait()
        assert self.msg_count == 5

    def test_reader_messages(self):
        self.msg_count = 0
        num_messages = 500

        topic = 'test_reader_msgs_%s' % time.time()
        self._send_messages(topic, num_messages, b'sup')

        def handler(msg):
            assert msg.body == b'sup'
            self.msg_count += 1
            if self.msg_count >= num_messages:
                self.stop()
            return True

        r = Reader(nsqd_tcp_addresses=['127.0.0.1:4150'], topic=topic, channel='ch',
                   message_handler=handler, max_in_flight=100,
                   **self.identify_options)

        self.wait()
        r.close()
        assert self.msg_count == num_messages

    def test_reader_coro(self):
        self.msg_count = 0
        num_messages = 20

        topic = 'test_reader_msgs_%s' % time.time()
        self._send_messages(topic, num_messages, b'sup')

        @tornado.gen.coroutine
        def handler(msg):
            yield tornado.gen.sleep(0.1)
            self.msg_count += 1
            if self.msg_count >= num_messages:
                self.stop()
            raise tornado.gen.Return(True)

        r = Reader(nsqd_tcp_addresses=['127.0.0.1:4150'], topic=topic, channel='ch',
                   message_handler=handler, max_in_flight=10,
                   **self.identify_options)

        self.wait()
        r.close()
        assert self.msg_count == num_messages

    def test_reader_heartbeat(self):
        this = self
        this.count = 0

        def handler(msg):
            return True

        class HeartbeatReader(Reader):
            def heartbeat(self, conn):
                this.count += 1
                if this.count == 2:
                    this.stop()

        topic = 'test_reader_hb_%s' % time.time()
        HeartbeatReader(nsqd_tcp_addresses=['127.0.0.1:4150'], topic=topic, channel='ch',
                        message_handler=handler, max_in_flight=100,
                        heartbeat_interval=1)
        self.wait()


class DeflateReaderIntegrationTest(IntegrationBase):
    identify_options = {
        'user_agent': 'sup',
        'deflate': True,
        'deflate_level': 6,
        'tls_options': {'cert_reqs': ssl.CERT_NONE},
        'heartbeat_interval': 10,
        'output_buffer_size': 4096,
        'output_buffer_timeout': 50
    }

    nsqd_command = ['nsqd', '--verbose', '--deflate',
                    '--tls-key=%s/tests/key.pem' % base_dir,
                    '--tls-cert=%s/tests/cert.pem' % base_dir]

    def test_conn_identify_options(self):
        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)
        c.on('identify_response', self.stop)
        c.connect()
        response = self.wait()
        print(response)
        assert response['conn'] is c
        assert isinstance(response['data'], dict)
        assert response['data']['deflate'] is True

    def test_conn_socket_upgrade(self):
        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)
        c.on('ready', self.stop)
        c.connect()
        self.wait()
        assert isinstance(c.socket, DeflateSocket)

    def test_reader_messages(self):
        self.msg_count = 0
        num_messages = 300
        topic = 'test_reader_msgs_%s' % time.time()
        self._send_messages(topic, num_messages, b'sup')

        def handler(msg):
            assert msg.body == b'sup'
            self.msg_count += 1
            if self.msg_count >= num_messages:
                self.stop()
            return True

        r = Reader(nsqd_tcp_addresses=['127.0.0.1:4150'],
                   topic=topic, channel='ch',
                   message_handler=handler, max_in_flight=100,
                   **self.identify_options)
        self.wait()
        r.close()


class LookupdReaderIntegrationTest(IntegrationBase):
    nsqd_command = ['nsqd', '--verbose', '-lookupd-tcp-address=127.0.0.1:4160']
    nsqlookupd_command = ['nsqlookupd', '--verbose', '-broadcast-address=127.0.0.1']

    identify_options = {
        'user_agent': 'sup',
        'heartbeat_interval': 10,
        'output_buffer_size': 4096,
        'output_buffer_timeout': 50
    }

    def test_reader_messages(self):
        self.msg_count = 0
        num_messages = 1

        topic = 'test_lookupd_msgs_%s' % int(time.time())
        self._send_messages(topic, num_messages, b'sup')

        def handler(msg):
            assert msg.body == b'sup'
            self.msg_count += 1
            if self.msg_count >= num_messages:
                self.stop()
            return True

        r = Reader(lookupd_http_addresses=['http://127.0.0.1:4161'],
                   topic=topic,
                   channel='ch',
                   message_handler=handler,
                   lookupd_poll_interval=1,
                   lookupd_poll_jitter=0.01,
                   max_in_flight=100,
                   **self.identify_options)

        self.wait()
        r.close()


class AuthHandler(tornado.web.RequestHandler):
    def get(self):
        if self.get_argument('secret') == 'opensesame':
            self.finish({
                'ttl': 30,
                'identity': 'username',
                'authorizations': [{
                    'permissions': ['subscribe'],
                    'topic': 'authtopic',
                    'channels': ['ch'],
                }]
            })
        else:
            self.set_status(403)


class ReaderAuthIntegrationTest(IntegrationBase):
    identify_options = {
        'user_agent': 'supauth',
        'heartbeat_interval': 10,
        'output_buffer_size': 4096,
        'output_buffer_timeout': 50,
        'auth_secret': "opensesame",
    }

    def setUp(self):
        auth_sock, auth_port = tornado.testing.bind_unused_port()
        self.auth_sock = auth_sock
        self.nsqd_command = [
            'nsqd', '--verbose', '--auth-http-address=127.0.0.1:%d' % auth_port
        ]
        super(ReaderAuthIntegrationTest, self).setUp()

    def tearDown(self):
        super(ReaderAuthIntegrationTest, self).tearDown()
        self.auth_sock.close()

    def test_conn_identify(self):
        auth_app = tornado.web.Application([("/auth", AuthHandler)])
        auth_srv = tornado.httpserver.HTTPServer(auth_app)
        auth_srv.add_socket(self.auth_sock)

        c = AsyncConn('127.0.0.1', 4150, **self.identify_options)

        def _on_ready(*args, **kwargs):
            c.on('response', self.stop)
            c.send(protocol.subscribe('authtopic', 'ch'))

        c.on('ready', _on_ready)
        c.connect()
        response = self.wait()
        auth_srv.stop()
        print(response)
        assert response['conn'] is c
        assert response['data'] == b'OK'
