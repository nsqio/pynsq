from __future__ import absolute_import

import warnings
from .reader import Reader


class LegacyReader(object):
    """
    In ``v0.5.0`` we dropped support for "tasks" in the :class:`nsq.Reader` API in
    favor of a single message handler.

    ``LegacyReader`` is a backwards compatible API for clients interacting with ``v0.5.0+`` that
    want to continue to use "tasks".

    Usage::

        from nsq import LegacyReader as Reader
    """
    def __init__(self, *args, **kwargs):
        warnings.warn('LegacyReader is a deprecated wrapper and will be removed in a future' +
                      ' release.  Use (multiple) Reader(s) each with their own' +
                      ' message handler.', DeprecationWarning)

        old_params = {}

        old_kwargs = ('all_tasks', 'topic', 'channel'
                      'nsqd_tcp_addresses', 'lookupd_http_addresses'
                      'max_tries', 'max_in_flight', 'requeue_delay', 'lookupd_poll_interval',
                      'low_rdy_idle_timeout', 'heartbeat_interval', 'max_backoff_duration')

        if args:
            keys = old_kwargs[:len(args)]
            old_params = dict(zip(keys, args))

        old_params.update(kwargs)

        all_tasks = old_params['all_tasks']
        topic = old_params['topic']
        channel = old_params['channel']

        del old_params['all_tasks']
        del old_params['topic']
        del old_params['channel']

        assert isinstance(all_tasks, dict)
        for key, method in all_tasks.items():
            assert callable(method), 'key %s must have a callable value' % key

        self.readers = []
        for task, method in all_tasks.items():
            if len(all_tasks) > 1:
                task_channel = channel + '.' + task
            else:
                task_channel = channel

            r = Reader(topic=topic, channel=task_channel, message_handler=method, **old_params)
            self.readers.append(r)
