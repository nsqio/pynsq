import signal
import tornado.ioloop
import logging

from nsq import Message, Error, unpack_response, decode_message, valid_topic_name, valid_channel_name
from nsq import identify, subscribe, ready, finish, touch, requeue, nop, pub, mpub
from nsq import FRAME_TYPE_RESPONSE, FRAME_TYPE_ERROR, FRAME_TYPE_MESSAGE, TOUCH, FIN, REQ
from BackoffTimer import BackoffTimer
from sync import SyncConn
from async import AsyncConn
from Reader import Reader
from LegacyReader import LegacyReader
from Writer import Writer


def _handle_term_signal(sig_num, frame):
    logging.info('TERM Signal handler called with signal %r' % sig_num)
    tornado.ioloop.IOLoop.instance().stop()

def run():
    """
    Starts any instantiated :class:`nsq.Reader` or :class:`nsq.Writer`
    """
    signal.signal(signal.SIGTERM, _handle_term_signal)
    tornado.ioloop.IOLoop.instance().start()

# also update in setup.py
__version__ = '0.5.0'

__author__ = "Matt Reiferson <snakes@gmail.com>"
__all__ = ["Reader", "Writer", "run", "BackoffTimer", "Message", "Error", "LegacyReader",
           "SyncConn", "AsyncConn", "unpack_response", "decode_message",
           "identify", "subscribe", "ready", "finish", "touch", "requeue", "nop","pub", "mpub",
           "valid_topic_name", "valid_channel_name",
           "FRAME_TYPE_RESPONSE", "FRAME_TYPE_ERROR", "FRAME_TYPE_MESSAGE",
           "TOUCH", "FIN", "REQ"]
