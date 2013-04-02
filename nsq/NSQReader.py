"""
high-level NSQ reader class built on top of a Tornado IOLoop supporting both sync and
async modes of operation.

supports various hooks to modify behavior when heartbeats are received, temporarily
disable the reader, and pre-process/validate messages.

when supplied a list of nsqlookupd addresses, a reader instance will periodically poll
the specified topic in order to discover new producers and reconnect to existing ones.

sync ex.
    import nsq
    
    def task1(message):
        print message
        return True
    
    def task2(message):
        print message
        return True
    
    all_tasks = {"task1": task1, "task2": task2}
    r = nsq.Reader(all_tasks, lookupd_http_addresses=['http://127.0.0.1:4161'],
            topic="nsq_reader", channel="asdf", lookupd_poll_interval=15)
    nsq.run()

async ex.
    import nsq
    
    buf = []
    
    def process_message(message):
        global buf
        message.enable_async()
        # cache the message for later processing
        buf.append(message)
        if len(buf) >= 3:
            print '****'
            for msg in buf:
                print msg
                msg.finish()
            print '****'
            buf = []
        else:
            print 'deferring processing'
    
    all_tasks = {"task1": process_message}
    r = nsq.Reader(all_tasks, lookupd_http_addresses=['http://127.0.0.1:4161'],
            topic="nsq_reader", channel="async", max_in_flight=9)
    nsq.run()
"""
import logging
try:
    import simplejson as json
except ImportError:
    import json # pyflakes.ignore
import time
import signal
import socket
import functools
import urllib
import random
import warnings

import tornado.ioloop
import tornado.httpclient

import BackoffTimer
import nsq
import async


class RequeueWithoutBackoff(Exception):
    """exception for requeueing a message without incrementing backoff"""
    pass


class Reader(object):
    def __init__(self, all_tasks, topic, channel,
                nsqd_tcp_addresses=None, lookupd_http_addresses=None, async=False,
                max_tries=5, max_in_flight=1, requeue_delay=90, lookupd_poll_interval=120,
                low_rdy_idle_timeout=10):
        """
        Reader receives messages over the specified ``topic/channel`` and provides an async loop
        that calls each task method provided by ``all_tasks`` up to ``max_tries``.
        
        It will handle sending FIN or REQ commands based on feedback from the task methods.  When
        re-queueing, an increasing delay will be calculated automatically.  Additionally, when
        message processing fails, it will backoff for increasing multiples of ``requeue_delay``
        between updating of RDY count.
        
        ``all_tasks`` defines the a mapping of tasks and callables that will be executed for each
            message received.
        
        ``topic`` specifies the desired NSQ topic
        
        ``channel`` specifies the desired NSQ channel
        
        ``nsqd_tcp_addresses`` a sequence of string addresses of the nsqd instances this reader
            should connect to
        
        ``lookupd_http_addresses`` a sequence of string addresses of the nsqlookupd instances this
            reader should query for producers of the specified topic
        
        ``async`` **deprecated in 0.3.2** determines whether handlers will do asynchronous processing. 
            If set to True, handlers must accept a keyword argument called ``finisher`` that will be 
            a callable used to signal message completion, taking a boolean argument indicating success.
        
        ``max_tries`` the maximum number of attempts the reader will make to process a message after
            which messages will be automatically discarded
        
        ``max_in_flight`` the maximum number of messages this reader will pipeline for processing.
            this value will be divided evenly amongst the configured/discovered nsqd producers.
        
        ``requeue_delay`` the base multiple used when re-queueing (multiplied by # of attempts)
        
        ``lookupd_poll_interval`` the amount of time in between querying all of the supplied
            nsqlookupd instances.  a random amount of time based on thie value will be initially
            introduced in order to add jitter when multiple readers are running.
        """
        assert isinstance(all_tasks, dict)
        for key, method in all_tasks.items():
            assert callable(method), "key %s must have a callable value" % key
        assert isinstance(topic, (str, unicode)) and len(topic) > 0
        assert isinstance(channel, (str, unicode)) and len(channel) > 0
        assert isinstance(max_in_flight, int) and 0 < max_in_flight < 2500
        
        if async:
            warnings.warn("async=True is deprecated: for now, this enables legacy support for passing of the finisher callable to message handlers", DeprecationWarning)
        
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
        else:
            lookupd_http_addresses = []
        
        assert nsqd_tcp_addresses or lookupd_http_addresses
        
        self.topic = topic
        self.channel = channel
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.lookupd_http_addresses = lookupd_http_addresses
        self.requeue_delay = int(requeue_delay * 1000)
        self.max_tries = max_tries
        self.max_in_flight = max_in_flight
        self.low_rdy_idle_timeout = low_rdy_idle_timeout
        self.total_ready = 0
        self.lookupd_poll_interval = lookupd_poll_interval
        self.async = async
        
        self.task_lookup = all_tasks
        
        self.backoff_timer = dict((k, BackoffTimer.BackoffTimer(0, 120)) for k in self.task_lookup.keys())
        
        self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        self.conns = {}
        self.http_client = tornado.httpclient.AsyncHTTPClient()
        
        logging.info("starting reader for topic '%s'..." % self.topic)
        
        for task in self.task_lookup:
            for addr in self.nsqd_tcp_addresses:
                address, port = addr.split(':')
                self.connect_to_nsqd(address, int(port), task)
        
        # trigger the first one manually
        self.query_lookupd()
        
        tornado.ioloop.PeriodicCallback(self.redistribute_ready_state, 5 * 1000).start()
        tornado.ioloop.PeriodicCallback(self.check_last_recv_timestamps, 60 * 1000).start()
        periodic = tornado.ioloop.PeriodicCallback(self.query_lookupd, self.lookupd_poll_interval * 1000)
        # randomize the time we start this poll loop so that all servers don't query at exactly the same time
        # randomize based on 10% of the interval
        delay = random.random() * self.lookupd_poll_interval * .1
        tornado.ioloop.IOLoop.instance().add_timeout(time.time() + delay, periodic.start)
    
    def _client_callback(self, response, message=None, task=None, conn=None, **kwargs):
        '''
        This is the method that an asynchronous nsqreader should call to indicate
        async completion of a message. This will most likely be exposed as the respond
        callable in the Message instance with some functools voodoo
        '''
        if response is nsq.FIN:
            self.backoff_timer[task].success()
            self.finish(conn, message)
        elif response is nsq.REQ:
            if kwargs.get('backoff', True):
                self.backoff_timer[task].failure()
            self.requeue(conn, message, time_ms=kwargs.get('time_ms', -1))
        elif response is nsq.TOUCH:
            self.touch(conn, message)
        else:
            raise TypeError("invalid NSQ response type: %s" % response)
    
    def requeue(self, conn, message, time_ms=-1):
        if message.attempts > self.max_tries:
            self.giving_up(message)
            return self.finish(conn, message)
        
        try:
            # ms
            requeue_delay = self.requeue_delay * message.attempts if time_ms < 0 else time_ms
            conn.send(nsq.requeue(message.id, requeue_delay))
        except Exception:
            conn.close()
            logging.exception('[%s] failed to send requeue %s @ %d' % (conn, message.id, requeue_delay))
    
    def finish(self, conn, message):
        '''
        This is an internal method for NSQReader
        '''
        try:
            conn.send(nsq.finish(message.id))
        except Exception:
            conn.close()
            logging.exception('[%s] failed to send finish %s' % (conn, message.id))
    
    def touch(self, conn, message):
        try:
            conn.send(nsq.touch(message.id))
        except Exception:
            conn.close()
            logging.exception('[%s] failed to send touch %s' % (conn, message.id))
    
    def connection_max_in_flight(self):
        return max(1, self.max_in_flight / max(1, len(self.conns)))
    
    def handle_message(self, conn, task, message):
        conn.ready = max(conn.ready - 1, 0)
        self.total_ready = max(self.total_ready - 1, 0)
        
        # update ready count if necessary...
        # if we're in a backoff state for this task
        # set a timer to actually send the ready update
        per_conn = self.connection_max_in_flight()
        if not conn.rdy_timeout and (conn.ready <= 1 or conn.ready < int(per_conn * 0.25)):
            backoff_interval = self.backoff_timer[task].get_interval()
            if self.disabled():
                backoff_interval = 15
            if backoff_interval > 0:
                logging.info('[%s] backing off for %0.2f seconds' % (conn, backoff_interval))
                send_ready_callback = functools.partial(self.send_ready, conn, per_conn)
                conn.rdy_timeout = tornado.ioloop.IOLoop.instance().add_timeout(time.time() + backoff_interval, send_ready_callback)
            else:
                self.send_ready(conn, per_conn)
        
        try:
            pre_processed_message = self.preprocess_message(message)
            if not self.validate_message(pre_processed_message):
                return message.finish()
        except Exception:
            logging.exception('[%s] caught exception while preprocessing' % conn)
            return message.requeue()
        
        success = False
        task_method = self.get_task_method(task, message)
        try:
            if self.async:
                # legacy mode - this handler accepts the finisher callable as a keyword arg
                def finisher(success):
                    warnings.warn("The finisher callable is deprecated: please call the finish or requeue methods directly on the Message instance", DeprecationWarning)
                    RESPONSE_MAP = {
                        True : nsq.FIN,
                        False: nsq.REQ
                    }
                    response = RESPONSE_MAP[success] if success in RESPONSE_MAP else success
                    self._client_callback(response, message=message, task=task, conn=conn)
                return task_method(pre_processed_message, finisher=finisher)
            
            success = task_method(pre_processed_message)
        except RequeueWithoutBackoff:
            warnings.warn("RequeueWithoutBackoff is deprecated: please use the message instance methods if you want to requeue without backing off", DeprecationWarning)
            if not message.has_responded():
                return message.requeue(backoff=False)
        except Exception:
            logging.exception('[%s] caught exception while handling %s' % (conn, task))
            if not message.has_responded():
                return message.requeue()
        
        if not message.is_async() and not message.has_responded():
            assert success is not None, "ambiguous return value for synchronous mode"
            if success:
                return message.finish()
            else:
                return message.requeue()
    
    def send_ready(self, conn, value):
        if self.disabled():
            logging.info('[%s] disabled, delaying ready state change', conn)
            send_ready_callback = functools.partial(self.send_ready, conn, value)
            conn.rdy_timeout = tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 15, send_ready_callback)
            return
        
        conn.rdy_timeout = None
        
        if (self.total_ready + value) > self.max_in_flight:
            return
        
        try:
            conn.send(nsq.ready(value))
            conn.ready = value
        except Exception:
            conn.close()
            logging.exception('[%s] failed to send ready' % conn)
    
    def _data_callback(self, conn, raw_data, task):
        conn.last_recv_timestamp = time.time()
        frame, data  = nsq.unpack_response(raw_data)
        if frame == nsq.FRAME_TYPE_MESSAGE:
            conn.last_msg_timestamp = time.time()
            message = nsq.decode_message(data)
            message.respond = functools.partial(self._client_callback, message=message, task=task, conn=conn)
            try:
                self.handle_message(conn, task, message)
            except Exception:
                logging.exception('[%s] failed to handle_message() %r' % (conn, message))
        elif frame == nsq.FRAME_TYPE_RESPONSE and data == "_heartbeat_":
            self.heartbeat(conn)
            conn.send(nsq.nop())
    
    def connect_to_nsqd(self, host, port, task):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        
        conn_id = host + ':' + str(port) + ':' + task
        if conn_id in self.conns:
            return
        
        logging.info("[%s] connecting to nsqd for task '%s'", host + ':' + str(port), task)
        
        connect_callback = functools.partial(self._connect_callback, task=task)
        data_callback = functools.partial(self._data_callback, task=task)
        close_callback = functools.partial(self._close_callback, task=task)
        
        conn = async.AsyncConn(host, port, connect_callback, data_callback, close_callback)
        conn.connect()
        
        conn.rdy_timeout = None
        conn.last_recv_timestamp = time.time()
        conn.last_msg_timestamp = time.time()
        
        initial_ready = self.connection_max_in_flight()
        if self.total_ready + initial_ready > self.max_in_flight:
            initial_ready = 0
        conn.ready = initial_ready
        self.total_ready += initial_ready
        
        self.conns[conn_id] = conn
    
    def _connect_callback(self, conn, task):
        if len(self.task_lookup) > 1:
            channel = self.channel + '.' + task
        else:
            channel = self.channel
        
        try:
            conn.send(nsq.identify({'short_id': self.short_hostname, 'long_id': self.hostname}))
            conn.send(nsq.subscribe(self.topic, channel))
            conn.send(nsq.ready(conn.ready))
        except Exception:
            conn.close()
            logging.exception('[%s] failed to bootstrap connection' % conn)
    
    def _close_callback(self, conn, task):
        conn_id = get_conn_id(conn, task)
        if conn_id in self.conns:
            del self.conns[conn_id]
        
        self.total_ready = max(self.total_ready - conn.ready, 0)
        
        logging.warning("[%s] connection closed for task '%s'", conn, task)
        
        if len(self.lookupd_http_addresses) == 0:
            # automatically reconnect to nsqd addresses when not using lookupd
            logging.info("[%s] attempting to reconnect in 15s", conn)
            reconnect_callback = functools.partial(self.connect_to_nsqd, 
                host=conn.host, port=conn.port, task=task)
            tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 15, reconnect_callback)
    
    def query_lookupd(self):
        for endpoint in self.lookupd_http_addresses:
            lookupd_url = endpoint + "/lookup?topic=" + urllib.quote(self.topic)
            req = tornado.httpclient.HTTPRequest(lookupd_url, method="GET",
                        connect_timeout=1, request_timeout=2)
            callback = functools.partial(self._finish_query_lookupd, endpoint=endpoint)
            self.http_client.fetch(req, callback=callback)
    
    def _finish_query_lookupd(self, response, endpoint):
        if response.error:
            logging.warning("[%s] lookupd error %s", endpoint, response.error)
            return
        
        try:
            lookup_data = json.loads(response.body)
        except json.JSONDecodeError:
            logging.warning("[%s] failed to parse JSON from lookupd: %r", endpoint, response.body)
            return
        
        if lookup_data['status_code'] != 200:
            logging.warning("[%s] lookupd responded with %d", endpoint, lookup_data['status_code'])
            return
        
        for task in self.task_lookup:
            for producer in lookup_data['data']['producers']:
                # TODO: this can be dropped for 1.0
                address = producer.get('broadcast_address', producer.get('address'))
                assert address
                self.connect_to_nsqd(address, producer['tcp_port'], task)
    
    def check_last_recv_timestamps(self):
        now = time.time()
        for conn_id, conn in self.conns.iteritems():
            timestamp = conn.last_recv_timestamp
            if (now - timestamp) > 60:
                # this connection hasnt received data beyond
                # the normal heartbeat interval, close it
                logging.warning("[%s] connection is stale, closing", conn)
                conn.close()
    
    def redistribute_ready_state(self):
        if self.disabled():
            return
        
        if len(self.conns) > self.max_in_flight:
            logging.debug('redistributing ready state (%d conns > %d max_in_flight)', len(self.conns), self.max_in_flight)
            for conn_id, conn in self.conns.iteritems():
                last_message_duration = time.time() - conn.last_msg_timestamp
                logging.debug('[%s] rdy: %d (last message received %.02fs)', conn, conn.ready, last_message_duration)
                if conn.ready > 0 and last_message_duration > self.low_rdy_idle_timeout:
                    logging.info('[%s] idle connection, giving up RDY count', conn)
                    self.total_ready = max(self.total_ready - conn.ready, 0)
                    self.send_ready(conn, 0)
            
            possible_conns = self.conns.values()
            max_in_flight = self.max_in_flight - self.total_ready
            while possible_conns and max_in_flight:
                max_in_flight -= 1
                conn = possible_conns.pop(random.randrange(len(possible_conns)))
                logging.info('[%s] redistributing RDY', conn)
                self.send_ready(conn, 1)
    
    #
    # subclass overwriteable
    #
    
    def get_task_method(self, task, message):
        '''
        Returns the method to process this message for this task.
        You may wish to override or extend this method if you
        wish to customize how the message is passed to the handler fxn
        '''
        return self.task_lookup[task]
    
    def giving_up(self, message):
        logging.warning("giving up on message '%s' after max tries %d", message.id, self.max_tries)
    
    def disabled(self):
        return False
    
    def heartbeat(self, conn):
        pass
    
    def validate_message(self, message):
        return True
    
    def preprocess_message(self, message):
        return message


def get_conn_id(conn, task):
    return str(conn) + ':' + task

def _handle_term_signal(sig_num, frame):
    logging.info('TERM Signal handler called with signal %r' % sig_num)
    tornado.ioloop.IOLoop.instance().stop()

def run():
    signal.signal(signal.SIGTERM, _handle_term_signal)
    tornado.ioloop.IOLoop.instance().start()
