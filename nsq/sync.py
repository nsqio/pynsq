import socket
import struct
import json
import Queue
import ssl

from datetime import datetime

from nsq import MAGIC_V2, FRAME_TYPE_RESPONSE, FRAME_TYPE_ERROR, \
                FRAME_TYPE_MESSAGE

READ_CHUNK_SIZE = 4096


class NsqServerError(Exception):
    def __init__(self, message, *args, **kwargs):
        super(NsqServerError, self).__init__(message, *args, **kwargs)

        pivot = message.find(' ')
        self.__error_type = message[:pivot]
        self.__short_text = message[pivot + 1:]

    @property
    def error_type(self):
        return self.__error_type

    @property
    def short_message(self):
        return self.__short_text


class NsqMessage(object):
    def __init__(self, s, data):
        self.__s = s

        header = struct.unpack('!QH16s', data[:26])

        self.__timestamp = header[0] / 1000000000.0
        self.__attempts = header[1]
        self.__message_id = header[2]
        self.__body = data[26:]

    def __str__(self):
        return ('<MSG ID=[%s] TIME=[%s] ATT=[%s] LEN=(%d)>' % 
                (self.__message_id, self.timestamp_dt, self.__attempts, 
                 len(self.__body)))

    def finish(self):
        """Mark the message as processed."""
        return self.__s.send_fin(self.__message_id)

    def requeue(self, timeout):
        """Requeue the message."""
        return self.__s.send_req(self.__message_id, timeout)

    def extend(self):
        """Ask for more time."""
        return self.__s.send_touch(self.__message_id)

    @property
    def timestamp(self):
        return self.__timestamp

    @property
    def attempts(self):
        return self.__attempts

    @property
    def message_id(self):
        return self.__message_id

    @property
    def body(self):
        return self.__body

    @property
    def timestamp_dt(self):
        return datetime.fromtimestamp(int(self.__timestamp))


class SyncConn(object):
    def __init__(self, host='127.0.0.1', port=4150, features={}, timeout=1.0, 
                 message_processor=None, is_ssl=False, ssl_ca_certs=None):
        self.__buffer = ''
        self.__timeout = timeout
        self.__message_processor = message_processor
        self.__is_ssl = is_ssl

        self.__connect(host, port)
        self.__identify(**features)

    def __log(self, message):
        print(message)

    def __connect(self, host, port):
        self.__log("Connecting.")

        self.__s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.__is_ssl is True:
            self.__s = ssl.wrap_socket(self.s, ca_certs=ssl_ca_certs, 
                                     cert_reqs=ssl.CERT_REQUIRED)

        self.__s.settimeout(self.__timeout)
        self.__s.connect((host, port))
        
        self.__send(MAGIC_V2)

    def __readn(self, size):
        while True:
            if len(self.__buffer) >= size:
                break

            packet = self.__s.recv(READ_CHUNK_SIZE)
            if not packet:
                raise Exception("Failed to read (%d) bytes." % (size))

            self.__buffer += packet

        data = self.__buffer[:size]
        self.__buffer = self.__buffer[size:]
        return data
    
    def __pack_int(self, n):
        return struct.pack('>l', n)

    def __send_length(self, data):
        self.__send(str(self.__pack_int(len(data))))

    def __read_response(self):
        packed_length = self.__readn(4)
        length = struct.unpack('!l', packed_length)[0]

        response = self.__readn(length)
        offset = 0

        frame_type_packed = response[offset:offset + 4]
        frame_type = struct.unpack('!l', frame_type_packed)[0]
        offset += 4

        data = response[offset:]

        if frame_type == FRAME_TYPE_ERROR:
            raise NsqServerError(data)

        return (frame_type, data)

    def __send(self, data):
        self.__s.send(data)

    def __dump_string(self, str_):
        parts = []
        for i in range(len(str_)):
            parts.append('(%02X)' % (ord(str_[i])))

        return ' '.join(parts)

    def __send_command(self, command, parameters=None, data=None, 
                       *args, **kwargs):
        command = command.upper()

        command_line = command
        if parameters is not None:
            command_line += ' ' + ' '.join([str(p) for p in parameters])

        self.__log("Sending: %s" % (command_line))

        command_line += "\n"
        self.__send(command_line)

        if data is not None:
            self.__send_length(data)
            self.__send(data)

        return self.run_loop(reason='[' + command + ']', one_response=True)

    def __send_json_command(self, command, data, *args, **kwargs):
        data_encoded = json.dumps(data)
        return self.__send_command(command, data=data_encoded, *args, **kwargs)

    def __identify(self, **features):
        data = self.__send_json_command('IDENTIFY', features)

        if data != 'OK':
            raise ValueError("Identify returned an unexpected success "
                             "message: %s" % (data))

        self.__log("Identify successful.")

    def run_loop(self, reason, one_response=False):
        """This doesn't really serve a purpose other than to hold the 
        connection open, and response to heartbeats.
        """

        self.__log("Entering receive-loop (%s)." % (reason))

        data = None
        i = 0
        message_queue = Queue.Queue()

        def process_message(message):
            if self.__message_processor is not None:
                self.__message_processor(message)
            else:
                self.__log("Message to be automatically handled: %s "
                           "\"%s\"" % (message, message.body))

                message.finish()

        def process_messages():
            while 1:
                try:
                    message = message_queue.get(block=False)
                except Queue.Empty:
                    break

                process_message(message)

        while one_response is False or i == 0:
            # Below, we continue whereever a received-message might not 
            # correspond to the command we sent.

            try:
                (frame_type, data) = self.__read_response()
            except socket.timeout:
                pass
            else:
                if frame_type == FRAME_TYPE_MESSAGE:
                    self.__log("Received message.")

# TODO(dustin): Use greenlets.
                    message = NsqMessage(self, data)
                    if one_response is False:
                        process_message(message)
                    else:
                        message_queue.put(message)
    
                    continue

                if data == '_heartbeat_':
                    self.__log("Responding to heartbeat.")

                    self.send_nop()
                    continue

            i += 1

        # We've now received a response to a command. -Now- process any 
        # messages that we've received (which may require communication that 
        # would've interfered with the response we were expecting, before).

        if one_response is True:
            process_messages()

        if data is None:
            self.__log("No response received (%s)." % (reason))
        else:
            self.__log("Response received (%s)." % (reason))

        return data

    def send_nop(self):
        self.__send_command('NOP')

    def send_rdy(self, count):
# TODO(dustin): Track state. This can only happen after a SUB, when we're in 
#               RDY-0.
        self.__send_command('RDY', parameters=(count,))

    def send_pub(self, topic, data):
        self.__send_command('PUB', parameters=(topic,), data=data)

    def send_mpub(self, topic, messages):
        count_encoded = struct.pack('>l', len(messages))

        phrases = [(struct.pack('>l', len(message)) + message) 
                   for message 
                   in messages]

        phrase_bytes = ''.join(phrases)

        self.__send_command('MPUB', parameters=(topic,), 
                            data=(count_encoded + phrase_bytes))

    def send_sub(self, topic, channel):
        self.__send_command('SUB', parameters=(topic, channel))

    def send_fin(self, message_id):
        self.__send_command('FIN', parameters=(message_id,))

    def send_req(self, message_id, timeout_ms):
        self.__send_command('REQ', parameters=(message_id, timeout_ms))

    def send_touch(self, message_id):
        self.__send_command('TOUCH', parameters=(message_id))

    def send_cls(self):
        self.__send_command('CLS')
