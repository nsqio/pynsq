from __future__ import absolute_import

from nsq import event


class Message(event.EventedMixin):
    """
    A class representing a message received from ``nsqd``.

    If you want to perform asynchronous message processing use the
    :meth:`nsq.Message.enable_async` method, pass the message around,
    and respond using the appropriate instance method.

    Generates the following events that can be listened to with :meth:`nsq.Message.on`:

     * ``finish``
     * ``requeue``
     * ``touch``

    NOTE: A calling a message's :meth:`nsq.Message.finish()` and :meth:`nsq.Message.requeue()`
    methods positively and negatively impact the backoff state, respectively.  However,
    sending the ``backoff=False`` keyword argument to :meth:`nsq.Message.requeue()` is
    considered neutral and will not impact backoff state.

    :param id: the ID of the message
    :type id: string

    :param body: the raw message body
    :type body: string

    :param timestamp: the timestamp the message was produced
    :type timestamp: int

    :param attempts: the number of times this message was attempted
    :type attempts: int

    :ivar id: the ID of the message (from the parameter).
    :type id: string

    :ivar body: the raw message body (from the parameter).
    :type body: string

    :ivar timestamp: the timestamp the message was produced
                     (from the parameter).
    :type timestamp: int

    :ivar attempts: the number of times this message was attempted
                    (from the parameter).
    :type attempts: int
    """
    def __init__(self, id, body, timestamp, attempts):
        self._async_enabled = False
        self._has_responded = False
        self.id = id
        self.body = body
        self.timestamp = timestamp
        self.attempts = attempts

        super(Message, self).__init__()

    def enable_async(self):
        """
        Enables asynchronous processing for this message.

        :class:`nsq.Reader` will not automatically respond to the message
        upon return of ``message_handler``.
        """
        self._async_enabled = True

    def is_async(self):
        """
        Returns whether or not asynchronous processing has been enabled.
        """
        return self._async_enabled

    def has_responded(self):
        """
        Returns whether or not this message has been responded to.
        """
        return self._has_responded

    def finish(self):
        """
        Respond to ``nsqd`` that you've processed this message successfully (or would like
        to silently discard it).
        """
        assert not self._has_responded
        self._has_responded = True
        self.trigger(event.FINISH, message=self)

    def requeue(self, **kwargs):
        """
        Respond to ``nsqd`` that you've failed to process this message successfully (and would
        like it to be requeued).

        :param backoff: whether or not :class:`nsq.Reader` should apply backoff handling
        :type backoff: bool

        :param delay: the amount of time (in seconds) that this message should be delayed
            if -1 it will be calculated based on # of attempts
        :type delay: int
        """

        # convert delay to time_ms for fixing
        # https://github.com/nsqio/pynsq/issues/71 and maintaining
        # backward compatibility
        if 'delay' in kwargs and isinstance(kwargs['delay'], int) and kwargs['delay'] >= 0:
            kwargs['time_ms'] = kwargs['delay'] * 1000

        assert not self._has_responded
        self._has_responded = True
        self.trigger(event.REQUEUE, message=self, **kwargs)

    def touch(self):
        """
        Respond to ``nsqd`` that you need more time to process the message.
        """
        assert not self._has_responded
        self.trigger(event.TOUCH, message=self)
