## pynsq

The official Python client library for [NSQ][nsq].

Latest stable release is **[0.6.8][latest_stable]**

[![Build Status](https://secure.travis-ci.org/nsqio/pynsq.png)](http://travis-ci.org/nsqio/pynsq)

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

[latest_stable]: https://pypi.python.org/pypi?:action=display&name=pynsq&version=0.6.8
[nsq]: https://github.com/nsqio/nsq
