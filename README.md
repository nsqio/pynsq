## pynsq

`pynsq` is the official Python client library for [NSQ][nsq].

Latest stable release is **[0.4.2][latest_stable]**

[![Build Status](https://secure.travis-ci.org/bitly/pynsq.png)](http://travis-ci.org/bitly/pynsq)

It provides a high-level reader library for building consumers and two low-level modules for both
sync and async communication over the [NSQ][nsq] protocol (if you wanted to write your own
high-level functionality).

The async module is built on top of the [Tornado IOLoop][tornado] and as such requires `tornado` be
installed.

### Installation

    $ pip install pynsq

### Reader

Reader provides high-level functionality for building robust NSQ consumers in Python on top of the
async module.

Multiple reader instances can be instantiated in a single process (to consume from multiple
topics/channels at once). Each specifying a set of tasks that will be called for each message over
that channel. Tasks are defined as a dictionary of string names -> callables.

The library handles backoff as well as maintaining a sufficient RDY count based on the # of
producers and your configured `max_in_flight`.

`preprocess_method` defines an optional callable that can alter the message data before other task
functions are called.

`validate_method` defines an optional callable that returns a boolean as to weather or not this
message should be processed.

Handlers should be defined as shown in the examples below. The handler receives a message object
that has instance methods `finish()`, `requeue()`, and `touch()` to respond to `nsqd`.

If you want to perform asynchronous message processing call the message object's `enable_async()`,
pass the message around, and respond using its aforementioned instance methods.

Here is an example that demonstrates synchronous message processing:

```python
import nsq

def task1(message):
    print message
    return True

def task2(message):
    print message
    return True

all_tasks = {"task1": task1, "task2": task2}
r = nsq.Reader(all_tasks, lookupd_http_addresses=['http://127.0.0.1:4161'], 
        topic="nsq_reader", channel="asdf")
nsq.run()
```

And async:

```python
"""
This is a simple example of async processing with nsq.Reader.

It will print "deferring processing" twice, and then print
the last 3 messages that it received.

Note in particular that we cache the message instance to be used for responding
asynchronously, at a later time.
"""
import nsq

buf = []

def process_message(message):
    global buf
    message.enable_async()
    # cache the message for later processing
    buf.append(message)
    if len(buf) >= 3:
        for msg in buf:
            print msg
            msg.finish()
        buf = []
    else:
        print 'deferring processing'
    
all_tasks = {"task1": process_message}
r = nsq.Reader(all_tasks, lookupd_http_addresses=['http://127.0.0.1:4161'],
        topic="nsq_reader", channel="async", max_in_flight=9)
nsq.run()
```

[latest_stable]: https://pypi.python.org/pypi?:action=display&name=pynsq&version=0.4.2
[nsq]: https://github.com/bitly/nsq
[tornado]: https://github.com/facebook/tornado
