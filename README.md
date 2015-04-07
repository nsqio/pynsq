## pynsq

`pynsq` is the official Python client library for [NSQ][nsq].

Latest stable release is **[0.6.7][latest_stable]**

[![Build Status](https://secure.travis-ci.org/bitly/pynsq.png)](http://travis-ci.org/bitly/pynsq)

### Installation

    $ pip install pynsq

### Docs

For HTML documentation, visit [https://pynsq.readthedocs.org/](https://pynsq.readthedocs.org/)

## Running test cases

These are the prerequisite for running test cases.

     sudo apt-get install libsnappy-dev
     wget http://bitly-downloads.s3.amazonaws.com/nsq/nsq-0.2.28.linux-amd64.go1.2.1.tar.gz
     tar zxvf nsq-0.2.28.linux-amd64.go1.2.1.tar.gz
     sudo cp nsq-0.2.28.linux-amd64.go1.2.1/bin/nsqd nsq-0.2.28.linux-amd64.go1.2.1/bin/nsqlookupd /usr/local/bin

To run test cases:

     python setup.py test

[latest_stable]: https://pypi.python.org/pypi?:action=display&name=pynsq&version=0.6.7
[nsq]: https://github.com/bitly/nsq
