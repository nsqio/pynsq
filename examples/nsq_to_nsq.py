# nsq_to_nsq.py
# Written by Ryder Moody and Jehiah Czebotar.
# Slower than the golang nsq_to_nsq included with nsqd, but useful as a
# starting point for a message transforming client written in python.

import tornado.options
from nsq import Reader, run
from nsq import Writer, Error
import functools
import logging
from host_pool import HostPool

class NSQProxy:
    def __init__(self, topic, nsqds):
        self.topic = topic
        self.writer_pool = HostPool([Writer([nsqd]) for nsqd in nsqds])

    def relay(self, nsq_message):
        nsq_message.enable_async()
        writer = self.writer_pool.get()
        callback = functools.partial(self._on_message_response, nsq_message=nsq_message, writer=writer)
        writer.pub(self.topic, nsq_message.body, callback)

    def _on_message_response(self, conn, data, nsq_message, writer):
        if isinstance(data, Error):
            logging.warning("requeuing message: %s", nsq_message.body)
            self.writer_pool.failed(writer)
            nsq_message.requeue()
        else:
            self.writer_pool.success(writer)
            nsq_message.finish()

if __name__ == "__main__":
    tornado.options.define('destination_topic', type=str)
    tornado.options.define('topic', type=str)
    tornado.options.define('nsqd_tcp_address', type=str, multiple=True)
    tornado.options.define('destination_nsqd_tcp_address', type=str, multiple=True)
    tornado.options.define('lookupd_http_address', type=str, multiple=True)
    tornado.options.define('channel', type=str)
    tornado.options.define('max_in_flight', type=int, default=500)

    tornado.options.parse_command_line()

    assert tornado.options.options.topic
    assert tornado.options.options.destination_nsqd_tcp_address
    assert tornado.options.options.channel

    destination_topic = str(tornado.options.options.destination_topic or tornado.options.options.topic)
    lookupd_http_addresses = map(lambda addr: 'http://' + addr, tornado.options.options.lookupd_http_address)

    proxy = NSQProxy(destination_topic, tornado.options.options.destination_nsqd_tcp_address)

    Reader(
        topic=tornado.options.options.topic,
        channel=tornado.options.options.channel,
        message_handler=proxy.relay,
        max_in_flight=tornado.options.options.max_in_flight,
        lookupd_http_addresses=lookupd_http_addresses,
        nsqd_tcp_addresses=tornado.options.options.nsqd_tcp_address,
    )
    run()
