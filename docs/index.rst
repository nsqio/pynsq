pynsq
=====

The official Python client library for `NSQ <https://github.com/bitly/nsq>`_

It provides high-level :class:`nsq.Reader` and :class:`nsq.Writer` classes for building 
consumers and producers and two low-level modules for both sync and async communication over the 
`NSQ Protocol <https://github.com/bitly/nsq/blob/master/docs/protocol.md>`_ (if you wanted 
to write your own high-level functionality).

The async module is built on top of the `Tornado IOLoop <http://tornadoweb.org>`_ and as 
such requires ``tornado`` to be installed.

Contents:

.. toctree::
   :maxdepth: 2
   
   message
   reader
   writer
   legacy_reader

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
