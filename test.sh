#!/bin/bash

if [ "$TRAVIS_PYTHON_VERSION" == "2.5" ] && [ "$TORNADO_VERSION" == "3.0" ]; then
    echo "skipping tests for python 2.5 and tornado 3.0 (incompatible)"
    exit 0
fi

py.test
exit $?
