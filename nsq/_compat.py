# flake8: noqa

import struct
import sys

PY2 = sys.version_info[0] == 2

# N.B. In each of the stanzas below, the first conditional branch represents
# Python 3 and higher. The fallback is Python 2.
#
# Don't ever check to see if the Python major version is 3, as such code will
# then immediately break if and when Python 4 is ever released. Instead, assume
# that Python 2 is the odd child, and make an exception accordingly.

if not PY2:
    bytes_types = (bytes, bytearray, memoryview)
    text_type = str
    string_types = (str,)
    integer_types = (int,)

    itervalues = lambda d, *args, **kwargs: d.values(*args, **kwargs)
    iteritems = lambda d, *args, **kwargs: d.items(*args, **kwargs)

    def to_bytes(x, charset='utf-8', errors='strict'):
        if isinstance(x, bytes_types):
            return bytes(x)
        if isinstance(x, str):
            return x.encode(charset, errors)
        raise TypeError('expected bytes or a string, not %r' % type(x))

    def _create_struct(fmt):
        return struct.Struct(fmt)

else:
    bytes_types = (bytes, bytearray, buffer)
    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)

    itervalues = lambda d, *args, **kwargs: d.itervalues(*args, **kwargs)
    iteritems = lambda d, *args, **kwargs: d.iteritems(*args, **kwargs)

    def to_bytes(x, charset='utf-8', errors='strict'):
        if isinstance(x, bytes_types):
            return bytes(x)
        if isinstance(x, unicode):
            return x.encode(charset, errors)
        raise TypeError('expected bytes or a string, not %r' % type(x))

    # Python 2.6 has the rather unfortunate problem of raising a TypeError if
    # you pass a unicode object as the `fmt` parameter. This shim can be
    # removed when support for Python earlier than 2.7 is dropped.
    def _create_struct(fmt):
        return struct.Struct(str(fmt))

struct_l = _create_struct('>l')
struct_q = _create_struct('>q')
struct_h = _create_struct('>h')

try:
    from urllib import parse as urlparse
    from urllib.parse import urlencode, parse_qs
except ImportError:
    import urlparse
    from urllib import urlencode
    from cgi import parse_qs

try:
    from inspect import signature
    def func_args(func):
        items = signature(func).parameters.values()
        return [param.name for param in items
                if param.kind == param.POSITIONAL_OR_KEYWORD]
except ImportError:
    # inspect.getargspec is deprecated since 3.5
    from inspect import getargspec
    def func_args(func):
        return getargspec(func).args
