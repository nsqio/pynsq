# -*- coding: utf-8 -*-
import logging
try:
    import simplejson as json
except ImportError:
    import json  # pyflakes.ignore
import time
import socket
import functools
import random

import tornado.ioloop

import nsq
import async


class Writer(object):
    """
    A high-level producer class built on top of the `Tornado IOLoop <http://tornadoweb.org>`_
    supporting async publishing (``PUB`` & ``MPUB``) of messages to ``nsqd`` over the TCP protocol.
    
    Example publishing a message repeatedly using a Tornado IOLoop periodic callback::
    
        import nsq
        import tornado.ioloop
        import time
        
        def pub_message():
            writer.pub('test', time.strftime('%H:%M:%S'), finish_pub)
        
        def finish_pub(conn, data):
            print data
        
        writer = nsq.Writer(["127.0.0.1:4150"])
        tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
        nsq.run()
    
    Example publshing a message from a Tornado HTTP request handler::
        
        import functools
        import tornado.httpserver
        import tornado.ioloop
        import tornado.options
        import tornado.web
        from nsq import Writer, Error
        from tornado.options import define, options
        
        class MainHandler(tornado.web.RequestHandler):
            @property
            def nsq(self):
                return self.application.nsq
            
            def get(self):
                topic = "log"
                msg = "Hello world"
                msg_cn = "Hello 世界"
                
                self.nsq.pub(topic, msg) # pub
                self.nsq.mpub(topic, [msg, msg_cn]) # mpub
                
                # customize callback
                callback = functools.partial(self.finish_pub, topic=topic, msg=msg)
                self.nsq.pub(topic, msg, callback=callback)
                
                self.write(msg)
            
            def finish_pub(self, conn, data, topic, msg):
                if isinstance(data, Error):
                    # try to re-pub message again if pub failed
                    self.nsq.pub(topic, msg)
        
        class Application(tornado.web.Application):
            def __init__(self, handlers, **settings):
                self.nsq = Writer(["127.0.0.1:4150"])
                super(Application, self).__init__(handlers, **settings)
    
    :param nsqd_tcp_addresses: a sequence of (addresses, port) of the ``nsqd`` instances this writer
        should publish to
    
    :param heartbeat_interval: the interval in seconds to configure heartbeats w/ ``nsqd``
    """
    def __init__(self, nsqd_tcp_addresses, heartbeat_interval=30):
        assert isinstance(heartbeat_interval, (int, float)) and heartbeat_interval >= 1
        if not isinstance(nsqd_tcp_addresses, (list, set, tuple)):
            assert isinstance(nsqd_tcp_addresses, (str, unicode))
            nsqd_tcp_addresses = [nsqd_tcp_addresses]
        assert nsqd_tcp_addresses
        
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.heartbeat_interval = int(heartbeat_interval * 1000)
        self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        self.conns = {}
        
        logging.info("starting writer...")
        self.connect()
        
        tornado.ioloop.PeriodicCallback(self._check_last_recv_timestamps, 60 * 1000).start()
    
    def pub(self, topic, msg, callback=None):
        self._pub("pub", topic, msg, callback)
    
    def mpub(self, topic, msg, callback=None):
        if isinstance(msg, (str, unicode)):
            msg = [msg]
        assert isinstance(msg, (list, set, tuple))
        
        self._pub("mpub", topic, msg, callback)
    
    def _pub(self, command, topic, msg, callback):
        if not callback:
            callback = functools.partial(self._finish_pub, command=command,
                                         topic=topic, msg=msg)
        
        conn = random.choice(self.conns.values())
        try:
            cmd = getattr(nsq, command)
            conn.send(cmd(topic, msg))
            
            conn.callback_queue.append(callback)
        except Exception, error:
            logging.exception('[%s] failed to send %s' % (conn.id, command))
            conn.close()
            
            callback(conn, SendError(error))
    
    def _data_callback(self, conn, raw_data):
        do_callback = False
        conn.last_recv_timestamp = time.time()
        frame, data = nsq.unpack_response(raw_data)
        if frame == nsq.FRAME_TYPE_RESPONSE and data == "_heartbeat_":
            logging.info("[%s] received heartbeat", conn.id)
            self.heartbeat(conn)
            conn.send(nsq.nop())
        elif frame == nsq.FRAME_TYPE_RESPONSE:
            do_callback = True
        elif frame == nsq.FRAME_TYPE_ERROR:
            logging.error("[%s] ERROR: %s", conn.id, data)
            data = DataError(data)
            do_callback = True
        
        if do_callback and conn.callback_queue:
            callback = conn.callback_queue.pop(0)
            callback(conn, data)
    
    def connect(self):
        for addr in self.nsqd_tcp_addresses:
            host, port = addr.split(':')
            self.connect_to_nsqd(host, int(port))
    
    def connect_to_nsqd(self, host, port):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        
        conn_id = host + ':' + str(port)
        if conn_id in self.conns:
            return
        
        logging.info("[%s] connecting to nsqd", conn_id)
        conn = async.AsyncConn(host, port, self._connect_callback,
                               self._data_callback, self._close_callback)
        conn.connect()
        
        conn.id = conn_id
        conn.last_recv_timestamp = time.time()
        conn.callback_queue = []
        
        self.conns[conn_id] = conn
    
    def _connect_callback(self, conn):
        try:
            identify_data = {
                'short_id': self.short_hostname,
                'long_id': self.hostname,
                'heartbeat_interval': self.heartbeat_interval,
                'feature_negotiation': True,
            }
            logging.info("[%s] IDENTIFY sent %r", conn.id, identify_data)
            conn.send(nsq.identify(identify_data))
            conn.callback_queue.append(self._identify_response_callback)
        except Exception:
            conn.close()
            logging.exception('[%s] failed to bootstrap connection' % conn.id)
    
    def _identify_response_callback(self, conn, data):
        if data == 'OK' or isinstance(data, nsq.Error):
            return
        
        try:
            data = json.loads(data)
        except ValueError:
            logging.warning("[%s] failed to parse JSON from nsqd: %r", conn.id, data)
            return
        
        logging.info('[%s] IDENTIFY received %r', conn.id, data)
    
    def _close_callback(self, conn):
        if conn.id in self.conns:
            del self.conns[conn.id]
            
            for callback in conn.callback_queue:
                try:
                    callback(conn, ConnectionClosedError())
                except Exception, error:
                    logging.exception("[%s] failed to callback: %s", conn.id, error)
        
        logging.warning("[%s] connection closed", conn.id)
        logging.info("[%s] attempting to reconnect in 15s", conn.id)
        reconnect_callback = functools.partial(self.connect_to_nsqd,
            host=conn.host, port=conn.port)
        tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 15, reconnect_callback)
    
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
            logging.warning("[%s] connection is stale (%.02fs), closing" % (conn.id, (now - timestamp)))
            conn.close()
    
    def _finish_pub(self, conn, data, command, topic, msg):
        if isinstance(data, nsq.Error):
            logging.error('[%s] failed to %s (%s, %s), data is %s',
                          conn.id, command, topic, msg, data)
    
    #
    # subclass overwriteable
    #
    
    def heartbeat(self, conn):
        pass


class DataError(nsq.Error):
    def __init__(self, data):
        self.data = data
    
    def __str__(self):
        return "DataError: %s" % self.data

class SendError(nsq.Error):
    def __init__(self, error):
        self.error = error
    
    def __str__(self):
        return "SendError: %s" % self.error

class ConnectionClosedError(nsq.Error):
    pass
