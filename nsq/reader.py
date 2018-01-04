from __future__ import absolute_import
from __future__ import division

import logging
import time
import functools
import random
import warnings

try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore

from tornado.ioloop import PeriodicCallback
import tornado.httpclient

from ._compat import integer_types
from ._compat import iteritems
from ._compat import itervalues
from ._compat import string_types
from ._compat import to_bytes
from ._compat import urlencode
from ._compat import urlparse
from ._compat import parse_qs
from ._compat import func_args
from .backoff_timer import BackoffTimer
from .client import Client
from . import protocol
from . import async

logger = logging.getLogger(__name__)


class Reader(Client):
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

    Synchronous example::

        import nsq

        def handler(message):
            print message
            return True

        r = nsq.Reader(message_handler=handler,
                lookupd_http_addresses=['http://127.0.0.1:4161'],
                topic='nsq_reader', channel='asdf', lookupd_poll_interval=15)
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
                topic='nsq_reader', channel='async', max_in_flight=9)
        nsq.run()

    :param message_handler: the callable that will be executed for each message received

    :param topic: specifies the desired NSQ topic

    :param channel: specifies the desired NSQ channel

    :param name: a string that is used for logging messages (defaults to 'topic:channel')

    :param nsqd_tcp_addresses: a sequence of string addresses of the nsqd instances this reader
        should connect to

    :param lookupd_http_addresses: a sequence of string addresses of the nsqlookupd instances this
        reader should query for producers of the specified topic

    :param max_tries: the maximum number of attempts the reader will make to process a message after
        which messages will be automatically discarded

    :param max_in_flight: the maximum number of messages this reader will pipeline for processing.
        this value will be divided evenly amongst the configured/discovered nsqd producers

    :param lookupd_poll_interval: the amount of time in seconds between querying all of the supplied
        nsqlookupd instances.  a random amount of time based on thie value will be initially
        introduced in order to add jitter when multiple readers are running

    :param lookupd_poll_jitter: The maximum fractional amount of jitter to add to the
        lookupd poll loop. This helps evenly distribute requests even if multiple consumers
        restart at the same time.

    :param lookupd_connect_timeout: the amount of time in seconds to wait for
        a connection to ``nsqlookupd`` to be established

    :param lookupd_request_timeout: the amount of time in seconds to wait for
        a request to ``nsqlookupd`` to complete.

    :param low_rdy_idle_timeout: the amount of time in seconds to wait for a message from a producer
        when in a state where RDY counts are re-distributed (ie. max_in_flight < num_producers)

    :param max_backoff_duration: the maximum time we will allow a backoff state to last in seconds

    :param \*\*kwargs: passed to :class:`nsq.AsyncConn` initialization
    """
    def __init__(
            self,
            topic,
            channel,
            message_handler=None,
            name=None,
            nsqd_tcp_addresses=None,
            lookupd_http_addresses=None,
            max_tries=5,
            max_in_flight=1,
            lookupd_poll_interval=60,
            low_rdy_idle_timeout=10,
            max_backoff_duration=128,
            lookupd_poll_jitter=0.3,
            lookupd_connect_timeout=1,
            lookupd_request_timeout=2,
            **kwargs):
        super(Reader, self).__init__(**kwargs)

        assert isinstance(topic, string_types) and len(topic) > 0
        assert isinstance(channel, string_types) and len(channel) > 0
        assert isinstance(max_in_flight, int) and max_in_flight > 0
        assert isinstance(max_backoff_duration, (int, float)) and max_backoff_duration > 0
        assert isinstance(name, string_types + (None.__class__,))
        assert isinstance(lookupd_poll_interval, int)
        assert isinstance(lookupd_poll_jitter, float)
        assert isinstance(lookupd_connect_timeout, int)
        assert isinstance(lookupd_request_timeout, int)

        assert lookupd_poll_jitter >= 0 and lookupd_poll_jitter <= 1

        if nsqd_tcp_addresses:
            if not isinstance(nsqd_tcp_addresses, (list, set, tuple)):
                assert isinstance(nsqd_tcp_addresses, string_types)
                nsqd_tcp_addresses = [nsqd_tcp_addresses]
        else:
            nsqd_tcp_addresses = []

        if lookupd_http_addresses:
            if not isinstance(lookupd_http_addresses, (list, set, tuple)):
                assert isinstance(lookupd_http_addresses, string_types)
                lookupd_http_addresses = [lookupd_http_addresses]
            random.shuffle(lookupd_http_addresses)
        else:
            lookupd_http_addresses = []

        assert nsqd_tcp_addresses or lookupd_http_addresses

        self.name = name or (topic + ':' + channel)
        self.message_handler = None
        if message_handler:
            self.set_message_handler(message_handler)
        self.topic = topic
        self.channel = channel
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.lookupd_http_addresses = lookupd_http_addresses
        self.lookupd_query_index = 0
        self.max_tries = max_tries
        self.max_in_flight = max_in_flight
        self.low_rdy_idle_timeout = low_rdy_idle_timeout
        self.total_rdy = 0
        self.need_rdy_redistributed = False
        self.lookupd_poll_interval = lookupd_poll_interval
        self.lookupd_poll_jitter = lookupd_poll_jitter
        self.lookupd_connect_timeout = lookupd_connect_timeout
        self.lookupd_request_timeout = lookupd_request_timeout
        self.random_rdy_ts = time.time()

        # Verify keyword arguments
        valid_args = func_args(async.AsyncConn.__init__)
        diff = set(kwargs) - set(valid_args)
        assert len(diff) == 0, 'Invalid keyword argument(s): %s' % list(diff)

        self.conn_kwargs = kwargs

        self.backoff_timer = BackoffTimer(0, max_backoff_duration)
        self.backoff_timeout = None
        self.backoff_block = False
        self.backoff_block_completed = True

        self.conns = {}
        self.connection_attempts = {}
        self.http_client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)

        # will execute when run() is called (for all Reader instances)
        self.io_loop.add_callback(self._run)

        self.redist_periodic = None
        self.query_periodic = None

    def _run(self):
        assert self.message_handler, "you must specify the Reader's message_handler"

        logger.info('[%s] starting reader for %s/%s...', self.name, self.topic, self.channel)

        for addr in self.nsqd_tcp_addresses:
            address, port = addr.split(':')
            self.connect_to_nsqd(address, int(port))

        self.redist_periodic = PeriodicCallback(
            self._redistribute_rdy_state,
            5 * 1000,
            io_loop=self.io_loop,
        )
        self.redist_periodic.start()

        if not self.lookupd_http_addresses:
            return
        # trigger the first lookup query manually
        self.query_lookupd()

        self.query_periodic = PeriodicCallback(
            self.query_lookupd,
            self.lookupd_poll_interval * 1000,
            io_loop=self.io_loop,
        )

        # randomize the time we start this poll loop so that all
        # consumers don't query at exactly the same time
        delay = random.random() * self.lookupd_poll_interval * self.lookupd_poll_jitter
        self.io_loop.add_timeout(time.time() + delay, self.query_periodic.start)

    def close(self):
        """
        Closes all connections stops all periodic callbacks
        """
        for conn in self.conns.values():
            conn.close()

        self.redist_periodic.stop()
        if self.query_periodic is not None:
            self.query_periodic.stop()

    def set_message_handler(self, message_handler):
        """
        Assigns the callback method to be executed for each message received

        :param message_handler: a callable that takes a single argument
        """
        assert callable(message_handler), 'message_handler must be callable'
        self.message_handler = message_handler

    def _connection_max_in_flight(self):
        return max(1, self.max_in_flight // max(1, len(self.conns)))

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
        for conn in itervalues(self.conns):
            if conn.in_flight > 0 and conn.in_flight >= (conn.last_rdy * 0.85):
                return True
        return False

    def _on_message(self, conn, message, **kwargs):
        try:
            self._handle_message(conn, message)
        except Exception:
            logger.exception('[%s:%s] failed to handle_message() %r', conn.id, self.name, message)

    def _handle_message(self, conn, message):
        self._maybe_update_rdy(conn)

        success = False
        try:
            if 0 < self.max_tries < message.attempts:
                self.giving_up(message)
                return message.finish()
            pre_processed_message = self.preprocess_message(message)
            if not self.validate_message(pre_processed_message):
                return message.finish()
            success = self.process_message(message)
        except Exception:
            logger.exception('[%s:%s] uncaught exception while handling message %s body:%r',
                             conn.id, self.name, message.id, message.body)
            if not message.has_responded():
                return message.requeue()

        if not message.is_async() and not message.has_responded():
            assert success is not None, 'ambiguous return value for synchronous mode'
            if success:
                return message.finish()
            return message.requeue()

    def _maybe_update_rdy(self, conn):
        if self.backoff_timer.get_interval() or self.max_in_flight == 0:
            return

        # Update RDY in 2 cases:
        #     1. On a new connection or in backoff we start with a tentative RDY
        #        count of 1.  After successfully receiving a first message we go to
        #        full throttle.
        #     2. After a change in connection count or max_in_flight we adjust to the new
        #        connection_max_in_flight.
        conn_max_in_flight = self._connection_max_in_flight()
        if conn.rdy == 1 or conn.rdy != conn_max_in_flight:
            self._send_rdy(conn, conn_max_in_flight)

    def _finish_backoff_block(self):
        self.backoff_timeout = None
        self.backoff_block = False

        # we must have raced and received a message out of order that resumed
        # so just complete the backoff block
        if not self.backoff_timer.get_interval():
            self._complete_backoff_block()
            return

        # test the waters after finishing a backoff round
        # if we have no connections, this will happen when a new connection gets RDY 1
        if not self.conns or self.max_in_flight == 0:
            return

        conn = random.choice(list(self.conns.values()))
        logger.info('[%s:%s] testing backoff state with RDY 1', conn.id, self.name)
        self._send_rdy(conn, 1)

        # for tests
        return conn

    def _on_backoff_resume(self, success, **kwargs):
        if success:
            self.backoff_timer.success()
        elif success is False and not self.backoff_block:
            self.backoff_timer.failure()

        self._enter_continue_or_exit_backoff()

    def _complete_backoff_block(self):
        self.backoff_block_completed = True
        rdy = self._connection_max_in_flight()
        logger.info('[%s] backoff complete, resuming normal operation (%d connections)',
                    self.name, len(self.conns))
        for c in self.conns.values():
            self._send_rdy(c, rdy)

    def _enter_continue_or_exit_backoff(self):
        # Take care of backoff in the appropriate cases.  When this
        # happens, we set a failure on the backoff timer and set the RDY count to zero.
        # Once the backoff time has expired, we allow *one* of the connections let
        # a single message through to test the water.  This will continue until we
        # reach no backoff in which case we go back to the normal RDY count.

        current_backoff_interval = self.backoff_timer.get_interval()

        # do nothing
        if self.backoff_block:
            return

        # we're out of backoff completely, return to full blast for all conns
        if not self.backoff_block_completed and not current_backoff_interval:
            self._complete_backoff_block()
            return

        # enter or continue a backoff iteration
        if current_backoff_interval:
            self._start_backoff_block()

    def _start_backoff_block(self):
        self.backoff_block = True
        self.backoff_block_completed = False
        backoff_interval = self.backoff_timer.get_interval()

        logger.info('[%s] backing off for %0.2f seconds (%d connections)',
                    self.name, backoff_interval, len(self.conns))
        for c in self.conns.values():
            self._send_rdy(c, 0)

        self.backoff_timeout = self.io_loop.add_timeout(time.time() + backoff_interval,
                                                        self._finish_backoff_block)

    def _rdy_retry(self, conn, value):
        conn.rdy_timeout = None
        self._send_rdy(conn, value)

    def _send_rdy(self, conn, value):
        if conn.rdy_timeout:
            self.io_loop.remove_timeout(conn.rdy_timeout)
            conn.rdy_timeout = None

        if value and (self.disabled() or self.max_in_flight == 0):
            logger.info('[%s:%s] disabled, delaying RDY state change', conn.id, self.name)
            rdy_retry_callback = functools.partial(self._rdy_retry, conn, value)
            conn.rdy_timeout = self.io_loop.add_timeout(time.time() + 15, rdy_retry_callback)
            return

        if value > conn.max_rdy_count:
            value = conn.max_rdy_count

        new_rdy = max(self.total_rdy - conn.rdy + value, 0)
        if new_rdy > self.max_in_flight:
            return

        if conn.send_rdy(value):
            self.total_rdy = new_rdy

    def connect_to_nsqd(self, host, port):
        """
        Adds a connection to ``nsqd`` at the specified address.

        :param host: the address to connect to
        :param port: the port to connect to
        """
        assert isinstance(host, string_types)
        assert isinstance(port, int)

        conn = async.AsyncConn(host, port, **self.conn_kwargs)
        conn.on('identify', self._on_connection_identify)
        conn.on('identify_response', self._on_connection_identify_response)
        conn.on('auth', self._on_connection_auth)
        conn.on('auth_response', self._on_connection_auth_response)
        conn.on('error', self._on_connection_error)
        conn.on('close', self._on_connection_close)
        conn.on('ready', self._on_connection_ready)
        conn.on('message', self._on_message)
        conn.on('heartbeat', self._on_heartbeat)
        conn.on('backoff', functools.partial(self._on_backoff_resume, success=False))
        conn.on('resume', functools.partial(self._on_backoff_resume, success=True))
        conn.on('continue', functools.partial(self._on_backoff_resume, success=None))

        if conn.id in self.conns:
            return

        # only attempt to re-connect once every 10s per destination
        # this throttles reconnects to failed endpoints
        now = time.time()
        last_connect_attempt = self.connection_attempts.get(conn.id)
        if last_connect_attempt and last_connect_attempt > now - 10:
            return
        self.connection_attempts[conn.id] = now

        logger.info('[%s:%s] connecting to nsqd', conn.id, self.name)
        conn.connect()

        return conn

    def _on_connection_ready(self, conn, **kwargs):
        conn.send(protocol.subscribe(self.topic, self.channel))
        # re-check to make sure another connection didn't beat this one done
        if conn.id in self.conns:
            logger.warning(
                '[%s:%s] connected to NSQ but anothermatching connection already exists',
                conn.id, self.name)
            conn.close()
            return

        if conn.max_rdy_count < self.max_in_flight:
            logger.warning(
                '[%s:%s] max RDY count %d < reader max in flight %d, truncation possible',
                conn.id, self.name, conn.max_rdy_count, self.max_in_flight)

        self.conns[conn.id] = conn

        conn_max_in_flight = self._connection_max_in_flight()
        for c in self.conns.values():
            if c.rdy > conn_max_in_flight:
                self._send_rdy(c, conn_max_in_flight)

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

    def _on_connection_close(self, conn, **kwargs):
        if conn.id in self.conns:
            del self.conns[conn.id]

        self.total_rdy = max(self.total_rdy - conn.rdy, 0)

        logger.warning('[%s:%s] connection closed', conn.id, self.name)

        if (conn.rdy_timeout or conn.rdy) and \
                (len(self.conns) == self.max_in_flight or self.backoff_timer.get_interval()):
            # we're toggling out of (normal) redistribution cases and this conn
            # had a RDY count...
            #
            # trigger RDY redistribution to make sure this RDY is moved
            # to a new connection
            self.need_rdy_redistributed = True

        if conn.rdy_timeout:
            self.io_loop.remove_timeout(conn.rdy_timeout)
            conn.rdy_timeout = None

        if not self.lookupd_http_addresses:
            # automatically reconnect to nsqd addresses when not using lookupd
            logger.info('[%s:%s] attempting to reconnect in 15s', conn.id, self.name)
            reconnect_callback = functools.partial(self.connect_to_nsqd,
                                                   host=conn.host, port=conn.port)
            self.io_loop.add_timeout(time.time() + 15, reconnect_callback)

    def query_lookupd(self):
        """
        Trigger a query of the configured ``nsq_lookupd_http_addresses``.
        """
        endpoint = self.lookupd_http_addresses[self.lookupd_query_index]
        self.lookupd_query_index = (self.lookupd_query_index + 1) % len(self.lookupd_http_addresses)

        # urlsplit() is faulty if scheme not present
        if '://' not in endpoint:
            endpoint = 'http://' + endpoint

        scheme, netloc, path, query, fragment = urlparse.urlsplit(endpoint)

        if not path or path == "/":
            path = "/lookup"

        params = parse_qs(query)
        params['topic'] = self.topic
        query = urlencode(_utf8_params(params), doseq=1)
        lookupd_url = urlparse.urlunsplit((scheme, netloc, path, query, fragment))

        req = tornado.httpclient.HTTPRequest(
            lookupd_url, method='GET',
            headers={'Accept': 'application/vnd.nsq; version=1.0'},
            connect_timeout=self.lookupd_connect_timeout,
            request_timeout=self.lookupd_request_timeout)
        callback = functools.partial(self._finish_query_lookupd, lookupd_url=lookupd_url)
        self.http_client.fetch(req, callback=callback)

    def _finish_query_lookupd(self, response, lookupd_url):
        if response.error:
            logger.warning('[%s] lookupd %s query error: %s',
                           self.name, lookupd_url, response.error)
            return

        try:
            lookup_data = json.loads(response.body.decode("utf8"))
        except ValueError:
            logger.warning('[%s] lookupd %s failed to parse JSON: %r',
                           self.name, lookupd_url, response.body)
            return

        for producer in lookup_data['producers']:
            # TODO: this can be dropped for 1.0
            address = producer.get('broadcast_address', producer.get('address'))
            assert address
            self.connect_to_nsqd(address, producer['tcp_port'])

    def set_max_in_flight(self, max_in_flight):
        """dynamically adjust the reader max_in_flight count. Set to 0 to immediately disable a Reader"""
        assert isinstance(max_in_flight, int)
        self.max_in_flight = max_in_flight

        if max_in_flight == 0:
            # set RDY 0 to all connections
            for conn in itervalues(self.conns):
                if conn.rdy > 0:
                    logger.debug('[%s:%s] rdy: %d -> 0', conn.id, self.name, conn.rdy)
                    self._send_rdy(conn, 0)
            self.total_rdy = 0
        else:
            self.need_rdy_redistributed = True
            self._redistribute_rdy_state()

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
        if not self.conns:
            return

        if self.disabled() or self.backoff_block or self.max_in_flight == 0:
            return

        if len(self.conns) > self.max_in_flight:
            self.need_rdy_redistributed = True
            logger.debug('redistributing RDY state (%d conns > %d max_in_flight)',
                         len(self.conns), self.max_in_flight)

        backoff_interval = self.backoff_timer.get_interval()
        if backoff_interval and len(self.conns) > 1:
            self.need_rdy_redistributed = True
            logger.debug('redistributing RDY state (%d backoff interval and %d conns > 1)',
                         backoff_interval, len(self.conns))

        if self.need_rdy_redistributed:
            self.need_rdy_redistributed = False

            # first set RDY 0 to all connections that have not received a message within
            # a configurable timeframe (low_rdy_idle_timeout).
            for conn_id, conn in iteritems(self.conns):
                last_message_duration = time.time() - conn.last_msg_timestamp
                logger.debug('[%s:%s] rdy: %d (last message received %.02fs)',
                             conn.id, self.name, conn.rdy, last_message_duration)
                if conn.rdy > 0 and last_message_duration > self.low_rdy_idle_timeout:
                    logger.info('[%s:%s] idle connection, giving up RDY count', conn.id, self.name)
                    self._send_rdy(conn, 0)

            conns = self.conns.values()

            in_flight_or_rdy = len([c for c in conns if c.in_flight or c.rdy])
            if backoff_interval:
                available_rdy = max(0, 1 - in_flight_or_rdy)
            else:
                available_rdy = max(0, self.max_in_flight - in_flight_or_rdy)

            # if moving any connections from RDY 0 to non-0 would violate in-flight constraints,
            # set RDY 0 on some connection with msgs in flight so that a later redistribution
            # round can proceed and we don't stay pinned to the same connections.
            #
            # if nothing's in flight, then we have connections with RDY 1 that are still
            # waiting to hit the idle timeout, in which case it's ok to do nothing.
            in_flight = [c for c in conns if c.in_flight]
            if in_flight and not available_rdy:
                c = random.choice(in_flight)
                logger.info('[%s:%s] too many msgs in flight, giving up RDY count', c.id, self.name)
                self._send_rdy(c, 0)

            # randomly walk the list of possible connections and send RDY 1 (up to our
            # calculated "max_in_flight").  We only need to send RDY 1 because in both
            # cases described above your per connection RDY count would never be higher.
            #
            # We also don't attempt to avoid the connections who previously might have had RDY 1
            # because it would be overly complicated and not actually worth it (ie. given enough
            # redistribution rounds it doesn't matter).
            possible_conns = [c for c in conns if not (c.in_flight or c.rdy)]
            while possible_conns and available_rdy:
                available_rdy -= 1
                conn = possible_conns.pop(random.randrange(len(possible_conns)))
                logger.info('[%s:%s] redistributing RDY', conn.id, self.name)
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
        logger.warning('[%s] giving up on message %s after %d tries (max:%d) %r',
                       self.name, message.id, message.attempts, self.max_tries, message.body)

    def _on_connection_identify_response(self, conn, data, **kwargs):
        if not hasattr(self, '_disabled_notice'):
            self._disabled_notice = True

            def semver(v):
                def cast(x):
                    try:
                        return int(x)
                    except:
                        return x
                return [cast(x) for x in v.replace('-', '.').split('.')]

            if self.disabled.__code__ != Reader.disabled.__code__ and \
               semver(data['version']) >= semver('0.3'):
                logging.warning('disabled() deprecated and incompatible with nsqd >= 0.3. ' +
                                'It will be removed in a future release. Use set_max_in_flight(0) instead')
                warnings.warn('disabled() is deprecated and will be removed in a future release, ' +
                              'use set_max_in_flight(0) instead', DeprecationWarning)
        return super(Reader, self)._on_connection_identify_response(conn, data, **kwargs)

    @classmethod
    def disabled(cls):
        """
        Called as part of RDY handling to identify whether this Reader has been disabled

        This is useful to subclass and override to examine a file on disk or a key in cache
        to identify if this reader should pause execution (during a deploy, etc.).

        Note: deprecated. Use set_max_in_flight(0)
        """
        return False

    def validate_message(self, message):
        return True

    def preprocess_message(self, message):
        return message


def _utf8_params(params):
    """encode a dictionary of URL parameters (including iterables) as utf-8"""
    assert isinstance(params, dict)
    encoded_params = []
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, integer_types + (float,)):
            v = str(v)
        if isinstance(v, (list, tuple)):
            v = [to_bytes(x) for x in v]
        else:
            v = to_bytes(v)
        encoded_params.append((k, v))
    return dict(encoded_params)
