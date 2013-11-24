"""
Mock socket module, copied (and edited) from:
http://svn.python.org/projects/python/branches/pep-0384/Lib/test/mock_socket.py
"""


class MockSocket:
    def __init__(self):
        self.output = []
        self.lines = []
        self.conn = None
        self.timeout = None

    def connect(self, addr):
        pass

    def queue_recv(self, line):
        self.lines.append(line)

    def recv(self, bufsize, flags=None):
        return self.lines.pop(0)

    def settimeout(self, timeout=None):
        if timeout is None:
            self.timeout = 60
        else:
            self.timeout = timeout

    def gettimeout(self):
        return self.timeout

    def send(self, data, flags=None):
        self.output.append(data)
        return len(data)

    def close(self):
        pass

    def setblocking(self, val):
        pass


def socket(family=None, type=None, proto=None):
    return MockSocket()


# Constants
AF_INET = None
SOCK_STREAM = None
SOL_SOCKET = None
SO_REUSEADDR = None
