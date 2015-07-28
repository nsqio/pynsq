#!/bin/bash


pip install simplejson
export PYCURL_SSL_LIBRARY=openssl
pip install pycurl
pip install tornado==$TORNADO_VERSION

# install snappy from source so we can compile with `-fPIC` witout having to sudo install stuff
git clone https://github.com/google/snappy.git
cd snappy
sh autogen.sh
mkdir $HOME/usr/local
./configure --prefix=$HOME/usr/local
CFLAGS="-fPIC" make
make install

set -e

CPPFLAGS="-I$HOME/usr/local/include -L$HOME/usr/local/lib -fPIC" pip install python-snappy
wget http://bitly-downloads.s3.amazonaws.com/nsq/$NSQ_DOWNLOAD.tar.gz
tar zxvf $NSQ_DOWNLOAD.tar.gz
export PATH=$NSQ_DOWNLOAD/bin:$PATH

LD_LIBRARY_PATH=$HOME/usr/local/lib py.test -q -v
