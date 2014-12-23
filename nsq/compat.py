
try:
    from tornado.simple_httpclient import _default_ca_certs
    DEFAULT_CA_CERTS = _default_ca_certs()
except ImportError:  # tornado < 4.0
    from tornado.simple_httpclient import _DEFAULT_CA_CERTS as DEFAULT_CA_CERTS
