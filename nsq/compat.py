from __future__ import absolute_import

import sys


PY3 = sys.version_info >= (3, 0)


if PY3:  # pragma: no cover
    unicode = str
    string_like = (str, bytes)

    def b(obj, encoding='utf8'):
        return obj.encode(encoding) if isinstance(obj, str) else obj

    from urllib.parse import urlsplit, urlunsplit
else:  # pragma: no cover
    unicode = unicode
    string_like = (unicode, str, bytes)

    def b(obj, encoding='utf8'):
        return obj if encoding == 'utf8' else obj.encode(encoding)

    from urlparse import urlsplit, urlunsplit
