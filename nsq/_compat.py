# flake8: noqa

import sys

PY2 = sys.version_info[0] == 2

# N.B. In each of the stanzas below, the first conditional branch represents
# Python 3 and higher. The fallback is Python 2.
#
# Don't ever check to see if the Python major version is 3, as such code will
# then immediately break if and when Python 4 is ever released. Instead, assume
# that Python 2 is the odd child, and make an exception accordingly.

if not PY2:
    text_type = str
    string_types = (str,)
    integer_types = (int,)

    itervalues = lambda d, *args, **kwargs: d.values(*args, **kwargs)
    iteritems = lambda d, *args, **kwargs: d.items(*args, **kwargs)

    def to_bytes(x, charset='utf-8', errors='strict'):
        if isinstance(x, (bytes, bytearray, memoryview)):
            return bytes(x)
        if isinstance(x, str):
            return x.encode(charset, errors)
        raise TypeError('expected bytes or a string, not %r' % type(x))

else:
    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)

    itervalues = lambda d, *args, **kwargs: d.itervalues(*args, **kwargs)
    iteritems = lambda d, *args, **kwargs: d.iteritems(*args, **kwargs)

    def to_bytes(x, charset='utf-8', errors='strict'):
        if isinstance(x, (bytes, bytearray, buffer)):
            return bytes(x)
        if isinstance(x, unicode):
            return x.encode(charset, errors)
        raise TypeError('expected bytes or a string, not %r' % type(x))

try:
    from urllib import parse as urlparse
    from urllib.parse import urlencode
except ImportError:
    import urlparse
    from urllib import urlencode
