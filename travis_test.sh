#!/bin/sh

set -eu

install_nsq () {
    wget "http://bitly-downloads.s3.amazonaws.com/nsq/$NSQ_DOWNLOAD.tar.gz"
    tar zxvf "$NSQ_DOWNLOAD.tar.gz"
}

install_snappy () {
    # install snappy from source so we can compile with `-fPIC` witout having to sudo install stuff
    git clone https://github.com/google/snappy.git
    cd snappy
    sh autogen.sh
    mkdir -p "$HOME/usr/local"
    ./configure --prefix="$HOME/usr/local"
    CFLAGS="-fPIC" make
    make install
    cd ..
}

echo "travis_fold:start:install.nsq"
install_nsq
echo "travis_fold:end:install.nsq"

echo "travis_fold:start:install.snappy"
install_snappy
echo "travis_fold:end:install.snappy"
echo "travis_fold:start:install.pythondeps"
pip install simplejson
pip install certifi
pip install tornado=="$TORNADO_VERSION"
PYCURL_SSL_LIBRARY=openssl pip install pycurl
CPPFLAGS="-I$HOME/usr/local/include -L$HOME/usr/local/lib -fPIC" pip install python-snappy
echo "travis_fold:end:install.pythondeps"

# Finally, run some tests!
export PATH=$NSQ_DOWNLOAD/bin:$PATH
export LD_LIBRARY_PATH=$HOME/usr/local/lib

cd "$TRAVIS_BUILD_DIR"
py.test
