#!/bin/sh
set -eu

install_snappy () {
    # install snappy from source so we can compile with `-fPIC` witout having to sudo install stuff
    git clone https://github.com/google/snappy.git
    cd snappy
    git checkout 1.1.7
    CFLAGS="-fPIC" CXXFLAGS="-fPIC" \
        cmake -DCMAKE_INSTALL_PREFIX="$HOME/usr/local"
    make install
    cd ..
}

echo "travis_fold:start:install.nsq"
wget "http://bitly-downloads.s3.amazonaws.com/nsq/$NSQ_DOWNLOAD.tar.gz"
tar zxvf "$NSQ_DOWNLOAD.tar.gz"
echo "travis_fold:end:install.nsq"

echo "travis_fold:start:install.snappy"
install_snappy
echo "travis_fold:end:install.snappy"

echo "travis_fold:start:install.pythondeps"
pip install pytest==3.6.3 flake8==3.6.0
PYCURL_SSL_LIBRARY=openssl pip install pycurl
CPPFLAGS="-I$HOME/usr/local/include -L$HOME/usr/local/lib -fPIC" pip install python-snappy
pip install tornado=="$TORNADO_VERSION"
echo "travis_fold:end:install.pythondeps"

# Finally, run some tests!
export PATH=$NSQ_DOWNLOAD/bin:$PATH
export LD_LIBRARY_PATH=$HOME/usr/local/lib

cd "$TRAVIS_BUILD_DIR"

py.test

echo
flake8
echo "flake8 OK"
