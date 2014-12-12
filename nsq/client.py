from __future__ import absolute_import

import time
import logging

import tornado.ioloop

logger = logging.getLogger(__name__)


class Client(object):
    def __init__(self, io_loop=None, **kwargs):
        self.io_loop = io_loop
        if not self.io_loop:
            self.io_loop = tornado.ioloop.IOLoop.instance()

        tornado.ioloop.PeriodicCallback(self._check_last_recv_timestamps,
                                        60 * 1000,
                                        io_loop=self.io_loop).start()

    def _on_connection_identify(self, conn, data, **kwargs):
        logger.info('[%s:%s] IDENTIFY sent %r' % (conn.id, self.name, data))

    def _on_connection_identify_response(self, conn, data, **kwargs):
        logger.info('[%s:%s] IDENTIFY received %r' % (conn.id, self.name, data))
        if conn.tls_v1 and not data.get('tls_v1'):
            logger.warning('[%s:%s] tls_v1 requested but disabled, could not negotiate feature',
                           conn.id, self.name)
        if conn.snappy and not data.get('snappy'):
            logger.warning('[%s:%s] snappy requested but disabled, could not negotiate feature',
                           conn.id, self.name)

    def _on_connection_auth(self, conn, data, **kwargs):
        logger.info('[%s:%s] AUTH sent' % (conn.id, self.name))

    def _on_connection_auth_response(self, conn, data, **kwargs):
        metadata = []
        if data.get('identity'):
            metadata.append("Identity: %r" % data['identity'])
        if data.get('permission_count'):
            metadata.append("Permissions: %d" % data['permission_count'])
        if data.get('identity_url'):
            metadata.append(data['identity_url'])
        logger.info('[%s:%s] AUTH accepted %s' % (conn.id, self.name, ' '.join(metadata)))

    def _on_connection_error(self, conn, error, **kwargs):
        if kwargs:
            logger.error('[%s:%s ERROR: %r]', conn.id, self.name, kwargs)
        logger.error('[%s:%s] ERROR: %r', conn.id, self.name, error)

    def _check_last_recv_timestamps(self):
        now = time.time()

        def is_stale(conn):
            timestamp = conn.last_recv_timestamp
            return (now - timestamp) > ((conn.heartbeat_interval * 2) / 1000.0)

        # first get the list of stale connections, then close
        # (`conn.close()` may modify the list of connections while we're iterating)
        stale_connections = [conn for conn in self.conns.values() if is_stale(conn)]
        for conn in stale_connections:
            timestamp = conn.last_recv_timestamp
            # this connection hasnt received data for more than
            # the configured heartbeat interval, close it
            logger.warning('[%s:%s] connection is stale (%.02fs), closing',
                           conn.id, self.name, (now - timestamp))
            conn.close()

    def _on_heartbeat(self, conn):
        logger.info('[%s:%s] received heartbeat' % (conn.id, self.name))
        self.heartbeat(conn)

    def heartbeat(self, conn):
        """
        Called whenever a heartbeat has been received

        This is useful to subclass and override to perform an action based on liveness (for
        monitoring, etc.)

        :param conn: the :class:`nsq.AsyncConn` over which the heartbeat was received
        """
        pass
