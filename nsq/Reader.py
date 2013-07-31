import logging
try:
    import simplejson as json
except ImportError:
    import json # pyflakes.ignore
import time
import socket
import functools
import urllib
import random
try:
    import ssl
except ImportError:
    ssl = None # pyflakes.ignore

import tornado.ioloop
import tornado.httpclient

import BackoffTimer
import nsq
import async


class Reader(object):
    """
    Reader provides high-level functionality for building robust NSQ consumers in Python
    on top of the async module.
    
    Reader receives messages over the specified ``topic/channel`` and calls ``message_handler`` 
    for each message (up to ``max_tries``).
    
    Multiple readers can be instantiated in a single process (to consume from multiple
    topics/channels at once).
    
    Supports various hooks to modify behavior when heartbeats are received, to temporarily
    disable the reader, and pre-process/validate messages.
    
    When supplied a list of ``nsqlookupd`` addresses, it will periodically poll those
    addresses to discover new producers of the specified ``topic``.
    
    It maintains a sufficient RDY count based on the # of producers and your configured
    ``max_in_flight``.
    
    Handlers should be defined as shown in the examples below. The handler receives a
    :class:`nsq.Message` object that has instance methods :meth:`nsq.Message.finish`, 
    :meth:`nsq.Message.requeue`, and :meth:`nsq.Message.touch` to respond to ``nsqd``.
    
    When messages are not responded to explicitly, it is responsible for sending 
    ``FIN`` or ``REQ`` commands based on return value of  ``message_handler``. When 
    re-queueing, it will backoff from processing additional messages for an increasing 
    delay (calculated exponentially based on consecutive failures up to ``max_backoff_duration``).
    
    Additionally, when message processing fails, it will defer processing the failed message 
    for an increasing multiple of ``requeue_delay``.
    
    Synchronous example::
        
        import nsq
        
        def handler(message):
            print message
            return True
        
        r = nsq.Reader(message_handler=handler,
                lookupd_http_addresses=['http://127.0.0.1:4161'],
                topic="nsq_reader", channel="asdf", lookupd_poll_interval=15)
        nsq.run()
    
    Asynchronous example::
        
        import nsq
        
        buf = []
        
        def process_message(message):
            global buf
            message.enable_async()
            # cache the message for later processing
            buf.append(message)
            if len(buf) >= 3:
                for msg in buf:
                    print msg
                    msg.finish()
                buf = []
            else:
                print 'deferring processing'
        
        r = nsq.Reader(message_handler=process_message,
                lookupd_http_addresses=['http://127.0.0.1:4161'],
                topic="nsq_reader", channel="async", max_in_flight=9)
        nsq.run()
    
    :param message_handler: the callable that will be executed for each message received
    
    :param topic: specifies the desired NSQ topic
    
    :param channel: specifies the desired NSQ channel
    
    :param name: a string that is used for logging messages (defaults to "topic:channel")
    
    :param nsqd_tcp_addresses: a sequence of string addresses of the nsqd instances this reader
        should connect to
    
    :param lookupd_http_addresses: a sequence of string addresses of the nsqlookupd instances this
        reader should query for producers of the specified topic
    
    :param max_tries: the maximum number of attempts the reader will make to process a message after
        which messages will be automatically discarded
    
    :param max_in_flight: the maximum number of messages this reader will pipeline for processing.
        this value will be divided evenly amongst the configured/discovered nsqd producers
    
    :param requeue_delay: the base multiple used when re-queueing (multiplied by # of attempts)
    
    :param lookupd_poll_interval: the amount of time in seconds between querying all of the supplied
        nsqlookupd instances.  a random amount of time based on thie value will be initially
        introduced in order to add jitter when multiple readers are running
    
    :param low_rdy_idle_timeout: the amount of time in seconds to wait for a message from a producer
        when in a state where RDY counts are re-distributed (ie. max_in_flight < num_producers)
    
    :param heartbeat_interval: the amount of time in seconds to negotiate with the connected
        producers to send heartbeats (requires nsqd 0.2.19+)
    
    :param max_backoff_duration: the maximum time we will allow a backoff state to last in seconds
    
    :param lookupd_poll_jitter: The maximum fractional amount of jitter to add to the lookupd pool loop.
        This helps evenly distribute requests even if multiple consumers restart at the same time.
    
    :param tls_v1: enable TLS v1 encryption (requires nsqd 0.2.22+)
    
    :param tls_options: dictionary of options to pass to `ssl.wrap_socket() 
        <http://docs.python.org/2/library/ssl.html#ssl.wrap_socket>`_ as **kwargs
    """
    def __init__(self, topic, channel, message_handler=None, name=None,
                nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                max_tries=5, max_in_flight=1, requeue_delay=90, lookupd_poll_interval=60,
                low_rdy_idle_timeout=10, heartbeat_interval=30, max_backoff_duration=128,
                lookupd_poll_jitter=0.3, tls_v1=False, tls_options=None):
        assert isinstance(topic, (str, unicode)) and len(topic) > 0
        assert isinstance(channel, (str, unicode)) and len(channel) > 0
        assert isinstance(max_in_flight, int) and max_in_flight > 0
        assert isinstance(heartbeat_interval, (int, float)) and heartbeat_interval >= 1
        assert isinstance(max_backoff_duration, (int, float)) and max_backoff_duration > 0
        assert isinstance(name, (str, unicode, None.__class__))
        assert isinstance(lookupd_poll_interval, int)
        assert isinstance(lookupd_poll_jitter, float)
        assert lookupd_poll_jitter >= 0 and lookupd_poll_jitter <= 1
        assert isinstance(tls_options, (dict, None.__class__))
        assert tls_v1 and ssl or not tls_v1, "tls_v1 requires Python 2.6+ or Python 2.5 w/ pip install ssl"
        
        if nsqd_tcp_addresses:
            if not isinstance(nsqd_tcp_addresses, (list, set, tuple)):
                assert isinstance(nsqd_tcp_addresses, (str, unicode))
                nsqd_tcp_addresses = [nsqd_tcp_addresses]
        else:
            nsqd_tcp_addresses = []
        
        if lookupd_http_addresses:
            if not isinstance(lookupd_http_addresses, (list, set, tuple)):
                assert isinstance(lookupd_http_addresses, (str, unicode))
                lookupd_http_addresses = [lookupd_http_addresses]
            random.shuffle(lookupd_http_addresses)
        else:
            lookupd_http_addresses = []
        
        assert nsqd_tcp_addresses or lookupd_http_addresses
        
        self.name = name or (topic + ":" + channel)
        self.message_handler = None
        if message_handler:
            self.set_message_handler(message_handler)
        self.topic = topic
        self.channel = channel
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.lookupd_http_addresses = lookupd_http_addresses
        self.lookupd_query_index = 0
        self.requeue_delay = int(requeue_delay * 1000)
        self.max_tries = max_tries
        self.max_in_flight = max_in_flight
        self.low_rdy_idle_timeout = low_rdy_idle_timeout
        self.total_rdy = 0
        self.need_rdy_redistributed = False
        self.lookupd_poll_interval = lookupd_poll_interval
        self.lookupd_poll_jitter = lookupd_poll_jitter
        self.heartbeat_interval = int(heartbeat_interval * 1000)
        self.random_rdy_ts = time.time()
        self.tls_v1 = tls_v1
        self.tls_options = tls_options
        
        self.backoff_timer = BackoffTimer.BackoffTimer(0, max_backoff_duration)
        self.backoff_timeout = None
        self.backoff_block = False
        
        self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        self.conns = {}
        self.connection_attempts = {}
        self.http_client = tornado.httpclient.AsyncHTTPClient()
        
        self.ioloop = tornado.ioloop.IOLoop.instance()
        
        # will execute when run() is called (for all Reader instances)
        self.ioloop.add_callback(self._run)
    
    def _run(self):
        assert self.message_handler, "you must specify the Reader's message_handler"
        
        logging.info("[%s] starting reader for %s/%s...", self.name, self.topic, self.channel)
        
        for addr in self.nsqd_tcp_addresses:
            address, port = addr.split(':')
            self.connect_to_nsqd(address, int(port))
        
        if not self.lookupd_http_addresses:
            return
        # trigger the first lookup query manually
        self.query_lookupd()
        
        tornado.ioloop.PeriodicCallback(self._redistribute_rdy_state, 5 * 1000).start()
        tornado.ioloop.PeriodicCallback(self._check_last_recv_timestamps, 60 * 1000).start()
        periodic = tornado.ioloop.PeriodicCallback(self.query_lookupd, self.lookupd_poll_interval * 1000)
        # randomize the time we start this poll loop so that all servers don't query at exactly the same time
        # randomize based on 30% of the interval
        delay = random.random() * self.lookupd_poll_interval * self.lookupd_poll_jitter
        self.ioloop.add_timeout(time.time() + delay, periodic.start)
    
    def set_message_handler(self, message_handler):
        """
        Assigns the callback method to be executed for each message received
        
        :param message_handler: a callable that takes a single argument
        """
        assert callable(message_handler), "message_handler must be callable"
        self.message_handler = message_handler
    
    def _message_responder(self, response, message=None, conn=None, **kwargs):
        """
        This is the underlying implementation behind a message's instance methods
        for responding to nsqd.
        
        In addition, we take care of backoff in the appropriate cases.  When this
        happens, we set a failure on the backoff timer and set the RDY count to zero.
        Once the backoff time has expired, we allow *one* of the connections let
        a single message through to test the water.  This will continue until we
        reach no backoff in which case we go back to the normal RDY count.
        
        NOTE: A calling a message's .finish() and .requeue() methods positively and
        negatively impact the backoff state, respectively.  However, sending the
        backoff=False keyword argument to .requeue() is considered neutral and
        will not impact backoff state.
        """
        start_backoff_interval = self.backoff_timer.get_interval()
        if response is nsq.FIN:
            if not self.backoff_block:
                self.backoff_timer.success()
            self._finish(conn, message)
        elif response is nsq.REQ:
            if kwargs.get('backoff', True) and not self.backoff_block:
                self.backoff_timer.failure()
            self._requeue(conn, message, time_ms=kwargs.get('time_ms', -1))
        elif response is nsq.TOUCH:
            return self._touch(conn, message)
        else:
            raise TypeError("invalid NSQ response type: %s" % response)
        
        self._enter_continue_or_exit_backoff(start_backoff_interval)
    
    def _requeue(self, conn, message, time_ms=-1):
        if message.attempts > self.max_tries:
            self.giving_up(message)
            return self._finish(conn, message)
        
        try:
            conn.in_flight -= 1
            # ms
            requeue_delay = self.requeue_delay * message.attempts if time_ms < 0 else time_ms
            conn.send(nsq.requeue(message.id, requeue_delay))
        except Exception:
            conn.close()
            logging.exception('[%s:%s] failed to send requeue %s @ %d', conn.id, self.name, message.id, requeue_delay)
    
    def _finish(self, conn, message):
        try:
            conn.in_flight -= 1
            conn.send(nsq.finish(message.id))
        except Exception:
            conn.close()
            logging.exception('[%s:%s] failed to send finish %s', conn.id, self.name, message.id)
    
    def _touch(self, conn, message):
        try:
            conn.send(nsq.touch(message.id))
        except Exception:
            conn.close()
            logging.exception('[%s:%s] failed to send touch %s', conn.id, self.name, message.id)
    
    def _connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))
    
    def is_starved(self):
        """
        Used to identify when buffered messages should be processed and responded to.
        
        When max_in_flight > 1 and you're batching messages together to perform work
        is isn't possible to just compare the len of your list of buffered messages against
        your configured max_in_flight (because max_in_flight may not be evenly divisible
        by the number of producers you're connected to, ie. you might never get that many
        messages... it's a *max*).
        
        Example::
            
            def message_handler(self, nsq_msg, reader):
                # buffer messages
                if reader.is_starved():
                    # perform work
            
            reader = nsq.Reader(...)
            reader.set_message_handler(functools.partial(message_handler, reader=reader))
            nsq.run()
        """
        for conn in self.conns.itervalues():
            if conn.in_flight > 0 and conn.in_flight >= (conn.last_rdy * 0.85):
                return True
        return False
    
    def _handle_message(self, conn, message):
        conn.rdy = max(conn.rdy - 1, 0)
        self.total_rdy = max(self.total_rdy - 1, 0)
        conn.in_flight += 1
        
        rdy_conn = conn
        if len(self.conns) > self.max_in_flight:
            # if all connections aren't getting RDY
            # occsionally randomize which connection gets RDY
            time_since_random_rdy = time.time() - self.random_rdy_ts
            if time_since_random_rdy > 30:
                self.random_rdy_ts = time.time()
                conns_with_no_rdy = [c for c in self.conns.itervalues() if not c.rdy]
                rdy_conn = random.choice(conns_with_no_rdy)
                if rdy_conn is not conn:
                    logging.info('[%s:%s] redistributing RDY to %s', conn.id, self.name, rdy_conn.id)
        
        self._maybe_update_rdy(rdy_conn)
        
        success = False
        try:
            pre_processed_message = self.preprocess_message(message)
            if not self.validate_message(pre_processed_message):
                return message.finish()
            success = self.process_message(message)
        except Exception:
            logging.exception('[%s:%s] uncaught exception while handling message %s body:%r', 
                                conn.id, self.name, message.id, message.body)
            if not message.has_responded():
                return message.requeue()
        
        if not message.is_async() and not message.has_responded():
            assert success is not None, "ambiguous return value for synchronous mode"
            if success:
                return message.finish()
            return message.requeue()
    
    def _maybe_update_rdy(self, conn):
        if self.backoff_timer.get_interval():
            return
        
        if conn.rdy <= 1 or conn.rdy < int(conn.last_rdy * 0.25):
            self._send_rdy(conn, self._connection_max_in_flight())
    
    def _finish_backoff_block(self):
        self.backoff_timeout = None
        self.backoff_block = False
        
        # test the waters after finishing a backoff round
        # if we have no connections, this will happen when a new connection gets RDY 1
        if self.conns:
            conn = random.choice(self.conns.values())
            logging.info('[%s:%s] testing backoff state with RDY 1', conn.id, self.name)
            self._send_rdy(conn, 1)
            
            # for tests
            return conn
    
    def _enter_continue_or_exit_backoff(self, start_backoff_interval):
        current_backoff_interval = self.backoff_timer.get_interval()
        
        # do nothing
        if self.backoff_block:
            return
        
        # we're out of backoff completely, return to full blast for all conns
        if start_backoff_interval and not current_backoff_interval:
            rdy = self._connection_max_in_flight()
            logging.info('[%s] backoff complete, resuming normal operation (%d connections)', self.name, len(self.conns))
            for c in self.conns.values():
                self._send_rdy(c, rdy)
            return
        
        # enter or continue a backoff iteration
        if current_backoff_interval:
            self._start_backoff_block()
    
    def _start_backoff_block(self):
        self.backoff_block = True
        backoff_interval = self.backoff_timer.get_interval()
        
        logging.info('[%s] backing off for %0.2f seconds (%d connections)', self.name, backoff_interval, len(self.conns))
        for c in self.conns.values():
            self._send_rdy(c, 0)
        
        self.backoff_timeout = self.ioloop.add_timeout(time.time() + backoff_interval, self._finish_backoff_block)
    
    def _rdy_retry(self, conn, value):
        conn.rdy_timeout = None
        self._send_rdy(conn, value)
    
    def _send_rdy(self, conn, value):
        if conn.rdy_timeout:
            self.ioloop.remove_timeout(conn.rdy_timeout)
            conn.rdy_timeout = None
        
        if value and self.disabled():
            logging.info('[%s:%s] disabled, delaying RDY state change', conn.id, self.name)
            rdy_retry_callback = functools.partial(self._rdy_retry, conn, value)
            conn.rdy_timeout = self.ioloop.add_timeout(time.time() + 15, rdy_retry_callback)
            return
        
        if value > conn.max_rdy_count:
            value = conn.max_rdy_count
        
        if (self.total_rdy + value) > self.max_in_flight:
            if not conn.rdy:
                # if we're going from RDY 0 to non-0 and we couldn't because 
                # of the configured max in flight, try again
                rdy_retry_callback = functools.partial(self._rdy_retry, conn, value)
                conn.rdy_timeout = self.ioloop.add_timeout(time.time() + 5, rdy_retry_callback)
            return
        
        try:
            conn.send(nsq.ready(value))
            self.total_rdy = max(self.total_rdy - conn.rdy + value, 0)
            conn.last_rdy = value
            conn.rdy = value
        except Exception:
            conn.close()
            logging.exception('[%s:%s] failed to send RDY', conn.id, self.name)
    
    def _data_callback(self, conn, raw_data):
        conn.last_recv_timestamp = time.time()
        frame, data  = nsq.unpack_response(raw_data)
        if frame == nsq.FRAME_TYPE_MESSAGE:
            conn.last_msg_timestamp = time.time()
            message = nsq.decode_message(data)
            message.respond = functools.partial(self._message_responder, message=message, conn=conn)
            try:
                self._handle_message(conn, message)
            except Exception:
                logging.exception('[%s:%s] failed to handle_message() %r', conn.id, self.name, message)
        elif frame == nsq.FRAME_TYPE_RESPONSE and data == "_heartbeat_":
            logging.info("[%s:%s] received heartbeat" % (conn.id, self.name))
            self.heartbeat(conn)
            conn.send(nsq.nop())
        elif frame == nsq.FRAME_TYPE_RESPONSE:
            if conn.response_callback_queue:
                callback = conn.response_callback_queue.pop(0)
                callback(conn, data)
        elif frame == nsq.FRAME_TYPE_ERROR:
            logging.error("[%s:%s] ERROR: %s" , conn.id, self.name, data)
    
    def connect_to_nsqd(self, host, port):
        """
        Adds a connection to ``nsqd`` at the specified address.
        
        :param host: the address to connect to
        :param port: the port to connect to
        """
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        
        conn = async.AsyncConn(host, port,
            self._connect_callback,
            self._data_callback,
            self._close_callback)
        
        if conn.id in self.conns:
            return
        
        # only attempt to re-connect once every 10s per destination
        # this throttles reconnects to failed endpoints
        now = time.time()
        last_connect_attempt = self.connection_attempts.get(conn.id)
        if last_connect_attempt and last_connect_attempt > now - 10:
            return
        self.connection_attempts[conn.id] = now
        
        self._initialize_conn(conn)
        logging.info("[%s:%s] connecting to nsqd", conn.id, self.name)
        conn.connect()
    
    def _initialize_conn(self, conn):
        conn.rdy_timeout = None
        conn.last_recv_timestamp = time.time()
        conn.last_msg_timestamp = time.time()
        conn.response_callback_queue = []
        conn.in_flight = 0
        conn.rdy = 0
        # for backwards compatibility when interacting with older nsqd
        # (pre 0.2.20), default this to their hard-coded max
        conn.max_rdy_count = 2500
    
    def _connect_callback(self, conn):
        try:
            identify_data = {
                'short_id': self.short_hostname,
                'long_id': self.hostname,
                'heartbeat_interval': self.heartbeat_interval,
                'feature_negotiation': True,
                'tls_v1': self.tls_v1,
            }
            logging.info("[%s:%s] IDENTIFY sent %r", conn.id, self.name, identify_data)
            conn.send(nsq.identify(identify_data))
            conn.response_callback_queue.append(self._identify_response_callback)
        except Exception:
            logging.exception('[%s:%s] failed to bootstrap connection', conn.id, self.name)
            conn.close()
    
    def _identify_response_callback(self, conn, data):
        if data == 'OK':
            return self._conn_subscribe(conn)
        
        try:
            data = json.loads(data)
        except ValueError:
            logging.error("[%s:%s] failed to parse JSON from nsqd: %r", conn.id, self.name, data)
            conn.close()
            return
        
        logging.info('[%s:%s] IDENTIFY received %r' % (conn.id, self.name, data))
        conn.max_rdy_count = data['max_rdy_count']
        if conn.max_rdy_count < self.max_in_flight:
            logging.warning("[%s:%s] max RDY count %d < reader max in flight %d, truncation possible",
                conn.id, self.name, conn.max_rdy_count, self.max_in_flight)
        
        if self.tls_v1:
            if data.get('tls_v1'):
                conn.upgrade_to_tls(self.tls_options)
                conn.response_callback_queue.append(self._upgrade_to_tls_callback)
                return
            logging.warning("[%s:%s] tls_v1 requested but disabled, could not negotiate feature", conn.id)
        
        self._conn_subscribe(conn)
    
    def _upgrade_to_tls_callback(self, conn, data):
        self._conn_subscribe(conn)
    
    def _conn_subscribe(self, conn):
        conn.send(nsq.subscribe(self.topic, self.channel))
        # re-check to make sure another connection didn't beat this one done
        if conn.id in self.conns:
            logging.warning("[%s:%s] connected to NSQ but another matching connection already exists", conn.id, self.name)
            conn.close()
            return
        
        self.conns[conn.id] = conn
        # we send an initial RDY of 1 up to our configured max_in_flight
        # this resolves two cases:
        #    1. `max_in_flight >= num_conns` ensuring that no connections are ever
        #       *initially* starved since redistribute won't apply
        #    2. `max_in_flight < num_conns` ensuring that we never exceed max_in_flight
        #       and rely on the fact that redistribute will handle balancing RDY across conns
        if not self.backoff_timer.get_interval() or len(self.conns) == 1:
            # only send RDY 1 if we're not in backoff (some other conn
            # should be testing the waters)
            # (but always send it if we're the first)
            self._send_rdy(conn, 1)
    
    def _close_callback(self, conn):
        if conn.id in self.conns:
            del self.conns[conn.id]
        
        self.total_rdy = max(self.total_rdy - conn.rdy, 0)
        
        logging.warning("[%s:%s] connection closed", conn.id, self.name)
        
        if (conn.rdy_timeout or conn.rdy) and \
            (len(self.conns) == self.max_in_flight or self.backoff_timer.get_interval()):
            # we're toggling out of (normal) redistribution cases and this conn
            # had a RDY count...
            #
            # trigger RDY redistribution to make sure this RDY is moved
            # to a new connection
            self.need_rdy_redistributed = True
        
        if conn.rdy_timeout:
            self.ioloop.remove_timeout(conn.rdy_timeout)
            conn.rdy_timeout = None
        
        if not self.lookupd_http_addresses:
            # automatically reconnect to nsqd addresses when not using lookupd
            logging.info("[%s:%s] attempting to reconnect in 15s", conn.id, self.name)
            reconnect_callback = functools.partial(self.connect_to_nsqd, host=conn.host, port=conn.port)
            self.ioloop.add_timeout(time.time() + 15, reconnect_callback)
    
    def query_lookupd(self):
        """
        Trigger a query of the configured ``nsq_lookupd_http_addresses``.
        """
        endpoint = self.lookupd_http_addresses[self.lookupd_query_index]
        self.lookupd_query_index = (self.lookupd_query_index + 1) % len(self.lookupd_http_addresses)
        lookupd_url = endpoint + "/lookup?topic=" + urllib.quote(self.topic)
        req = tornado.httpclient.HTTPRequest(lookupd_url, method="GET",
                    connect_timeout=1, request_timeout=2)
        callback = functools.partial(self._finish_query_lookupd, lookupd_url=lookupd_url)
        self.http_client.fetch(req, callback=callback)
    
    def _finish_query_lookupd(self, response, lookupd_url):
        if response.error:
            logging.warning("[%s] lookupd %s query error: %s", self.name, lookupd_url, response.error)
            return
        
        try:
            lookup_data = json.loads(response.body)
        except ValueError:
            logging.warning("[%s] lookupd %s failed to parse JSON: %r", self.name, lookupd_url, response.body)
            return
        
        if lookup_data['status_code'] != 200:
            logging.warning("[%s] lookupd %s responded with %d", self.name, lookupd_url, lookup_data['status_code'])
            return
        
        for producer in lookup_data['data']['producers']:
            # TODO: this can be dropped for 1.0
            address = producer.get('broadcast_address', producer.get('address'))
            assert address
            self.connect_to_nsqd(address, producer['tcp_port'])
    
    def _check_last_recv_timestamps(self):
        # this method takes care to get the list of stale connections then close
        # so `conn.close()` doesn't modify the list of connections while we iterate them.
        now = time.time()
        def is_stale(conn):
            timestamp = conn.last_recv_timestamp
            return (now - timestamp) > ((self.heartbeat_interval * 2) / 1000.0)
        
        stale_connections = [conn for conn in self.conns.values() if is_stale(conn)]
        for conn in stale_connections:
            timestamp = conn.last_recv_timestamp
            # this connection hasnt received data beyond
            # the configured heartbeat interval, close it
            logging.warning("[%s:%s] connection is stale (%.02fs), closing", conn.id, self.name, (now - timestamp))
            conn.close()
    
    def _redistribute_rdy_state(self):
        # We redistribute RDY counts in a few cases:
        # 
        # 1. our # of connections exceeds our configured max_in_flight
        # 2. we're in backoff mode (but not in a current backoff block)
        # 3. something out-of-band has set the need_rdy_redistributed flag (connection closed
        #    that was about to get RDY during backoff)
        # 
        # At a high level, we're trying to mitigate stalls related to low-volume
        # producers when we're unable (by configuration or backoff) to provide a RDY count
        # of (at least) 1 to all of our connections.
        
        if self.disabled() or self.backoff_block:
            return
        
        if len(self.conns) > self.max_in_flight:
            self.need_rdy_redistributed = True
            logging.debug('redistributing RDY state (%d conns > %d max_in_flight)',
                len(self.conns), self.max_in_flight)
        
        backoff_interval = self.backoff_timer.get_interval()
        if backoff_interval and len(self.conns) > 1:
            self.need_rdy_redistributed = True
            logging.debug('redistributing RDY state (%d backoff interval and %d conns > 1)',
                backoff_interval, len(self.conns))
        
        if self.need_rdy_redistributed:
            self.need_rdy_redistributed = False
            
            # first set RDY 0 to all connections that have not received a message within
            # a configurable timeframe (low_rdy_idle_timeout).
            for conn_id, conn in self.conns.iteritems():
                last_message_duration = time.time() - conn.last_msg_timestamp
                logging.debug('[%s:%s] rdy: %d (last message received %.02fs)',
                    conn.id, self.name, conn.rdy, last_message_duration)
                if conn.rdy > 0 and last_message_duration > self.low_rdy_idle_timeout:
                    logging.info('[%s:%s] idle connection, giving up RDY count', conn.id, self.name)
                    self._send_rdy(conn, 0)
            
            if backoff_interval:
                max_in_flight = 1 - self.total_rdy
            else:
                max_in_flight = self.max_in_flight - self.total_rdy
            
            # randomly walk the list of possible connections and send RDY 1 (up to our
            # calculate "max_in_flight").  We only need to send RDY 1 because in both cases described
            # above your per connection RDY count would never be higher.
            #
            # We also don't attempt to avoid the connections who previously might have had RDY 1 
            # because it would be overly complicated and not actually worth it (ie. given enough 
            # redistribution rounds it doesn't matter).
            possible_conns = self.conns.values()
            while possible_conns and max_in_flight:
                max_in_flight -= 1
                conn = possible_conns.pop(random.randrange(len(possible_conns)))
                logging.info('[%s:%s] redistributing RDY', conn.id, self.name)
                self._send_rdy(conn, 1)
            
            # for tests
            return conn
    
    #
    # subclass overwriteable
    #
    
    def process_message(self, message):
        """
        Called when a message is received in order to execute the configured ``message_handler``
        
        This is useful to subclass and override if you want to change how your
        message handlers are called.
        
        :param message: the :class:`nsq.Message` received
        """
        return self.message_handler(message)
    
    def giving_up(self, message):
        """
        Called when a message has been received where ``msg.attempts > max_tries``
        
        This is useful to subclass and override to perform a task (such as writing to disk, etc.)
        
        :param message: the :class:`nsq.Message` received
        """
        logging.warning("[%s] giving up on message %s after max tries %d %r", 
                        self.name, message.id, self.max_tries, message.body)
    
    def disabled(self):
        """
        Called as part of RDY handling to identify whether this Reader has been disabled
        
        This is useful to subclass and override to examine a file on disk or a key in cache
        to identify if this reader should pause execution (during a deploy, etc.).
        """
        return False
    
    def heartbeat(self, conn):
        """
        Called whenever a heartbeat has been received
        
        This is useful to subclass and override to perform an action based on liveness (for
        monitoring, etc.)
        
        :param conn: the :class:`nsq.AsyncConn` over which the heartbeat was received
        """
        pass
    
    def validate_message(self, message):
        return True
    
    def preprocess_message(self, message):
        return message
