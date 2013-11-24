from collections import defaultdict


class DuplicateListenerError(Exception):
    pass


class InvalidListenerError(Exception):
    pass


class EventedMixin(object):
    """
    Provides methods to trigger and listen for arbitrary events named as strings.
    """
    def __init__(self, *args, **kwargs):
        self.__listeners = defaultdict(list)

    def on(self, name, callback):
        """
        Listen for the named event with the specified callback.

        :param name: the name of the event
        :type name: string

        :param callback: the callback to execute when the event is triggered
        :type callback: callable
        """
        assert callable(callback), 'callback is not callable'
        if callback in self.__listeners[name]:
            raise DuplicateListenerError
        self.__listeners[name].append(callback)

    def off(self, name, callback):
        """
        Stop listening for the named event via the specified callback.

        :param name: the name of the event
        :type name: string

        :param callback: the callback that was originally used
        :type callback: callable
        """
        if callback not in self.__listeners[name]:
            raise InvalidListenerError
        self.__listeners[name].remove(callback)

    def trigger(self, name, *args, **kwargs):
        """
        Execute the callbacks for the listeners on the specified event with the
        supplied arguments.

        All extra arguments are passed through to each callback.

        :param name: the name of the event
        :type name: string
        """
        for ev in self.__listeners[name]:
            ev(*args, **kwargs)
