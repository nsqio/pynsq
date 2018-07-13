## pynsq

[![Build Status](https://secure.travis-ci.org/nsqio/pynsq.svg)](http://travis-ci.org/nsqio/pynsq) [![GitHub release](https://img.shields.io/github/release/nsqio/pynsq.svg)](https://github.com/nsqio/pynsq/releases/latest) [![PyPI](https://img.shields.io/pypi/v/pynsq.svg)](https://pypi.org/project/pynsq)

The official Python client library for [NSQ][nsq].

### Installation

    $ pip install pynsq

### Docs

For HTML documentation, visit [https://pynsq.readthedocs.org/](https://pynsq.readthedocs.org/)

### Tests

These are the prerequisites for running tests:

     sudo apt-get install libsnappy-dev
     wget http://bitly-downloads.s3.amazonaws.com/nsq/nsq-0.3.5.linux-amd64.go1.4.2.tar.gz
     tar zxvf nsq-0.3.5.linux-amd64.go1.4.2.tar.gz
     sudo cp nsq-0.3.5.linux-amd64.go1.4.2/bin/{nsqd,nsqlookupd} /usr/local/bin

To run test cases:

     python setup.py test

[nsq]: https://github.com/nsqio/nsq
