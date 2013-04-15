import signal
import tornado.ioloop
import logging

from nsq import unpack_response, decode_message, identify, subscribe, ready, finish, touch, requeue, nop
from nsq import valid_topic_name, valid_channel_name
from nsq import FRAME_TYPE_RESPONSE, FRAME_TYPE_ERROR, FRAME_TYPE_MESSAGE, TOUCH, FIN, REQ
from BackoffTimer import BackoffTimer
from sync import SyncConn
from async import AsyncConn
from Reader import Reader


def _handle_term_signal(sig_num, frame):
    logging.info('TERM Signal handler called with signal %r' % sig_num)
    tornado.ioloop.IOLoop.instance().stop()

def run():
    signal.signal(signal.SIGTERM, _handle_term_signal)
    tornado.ioloop.IOLoop.instance().start()


__version__ = '0.4.0-alpha5'

__author__ = "Matt Reiferson <snakes@gmail.com>"
__all__ = ["Reader", "run", "BackoffTimer",
           "SyncConn", "AsyncConn", "unpack_response", "decode_message",
           "identify", "subscribe", "ready", "finish", "touch", "requeue", "nop",
           "valid_topic_name", "valid_channel_name",
           "FRAME_TYPE_RESPONSE", "FRAME_TYPE_ERROR", "FRAME_TYPE_MESSAGE",
           "TOUCH", "FIN", "REQ"]
