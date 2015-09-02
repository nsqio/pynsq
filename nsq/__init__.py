from __future__ import absolute_import

import signal
import tornado.ioloop
import logging

from .protocol import (
    Error,
    unpack_response,
    decode_message,
    valid_topic_name,
    valid_channel_name,
    identify,
    subscribe,
    ready,
    finish,
    touch,
    requeue,
    nop,
    pub,
    mpub,
    FRAME_TYPE_RESPONSE,
    FRAME_TYPE_ERROR,
    FRAME_TYPE_MESSAGE,
)
from .message import Message
from .backoff_timer import BackoffTimer
from .sync import SyncConn
from .async import AsyncConn
from .reader import Reader
from .legacy_reader import LegacyReader
from .writer import Writer
from .version import __version__  # NOQA


def _handle_term_signal(sig_num, frame):
    logging.getLogger(__name__).info(
        'TERM Signal handler called with signal %r', sig_num)
    tornado.ioloop.IOLoop.instance().stop()


def run():
    """
    Starts any instantiated :class:`nsq.Reader` or :class:`nsq.Writer`
    """
    signal.signal(signal.SIGTERM, _handle_term_signal)
    signal.signal(signal.SIGINT, _handle_term_signal)
    tornado.ioloop.IOLoop.instance().start()


__author__ = "Matt Reiferson <snakes@gmail.com>"
__all__ = ["Reader", "Writer", "run", "BackoffTimer", "Message", "Error", "LegacyReader",
           "SyncConn", "AsyncConn", "unpack_response", "decode_message",
           "identify", "subscribe", "ready", "finish", "touch", "requeue", "nop", "pub", "mpub",
           "valid_topic_name", "valid_channel_name",
           "FRAME_TYPE_RESPONSE", "FRAME_TYPE_ERROR", "FRAME_TYPE_MESSAGE"]
