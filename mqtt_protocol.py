import struct
import enum


class MQTTControlType(enum.IntEnum):
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14


class MQTTPacket:
    def __init__(self, buf):
        self._data = buf
        self._decode(buf)

    def _decode(self, buf):
        self.command = buf[0] >> 4
        self.flags = buf[0] & 0xf
        self.length = remaining_length_decode(buf)
        self._pos = next(i for i in range(1,5) if buf[i] & 128 == 0) + 1
        #print(buf, 'len:', self.length, 'pos:', pos)
        cls = MQTTCommandRegistry.get_cls(self.command)
        self.packet = cls(buf, self._pos)

    def check(self):
        assert len(self._data) == self._pos + self.length, 'invalid length'
        self.packet.check()

    def _encode(self):
        tmp = bytearray()
        self.packet._encode(tmp)
        buf = bytearray()
        #TODO: how to setup flags (for publish) ?
        buf.append(self.command << 4 | self.flags)
        remaining_length_encode(len(tmp), buf)
        buf.extend(tmp)
        return buf

    def __repr__(self):
        return '<%s: %r>' % (MQTTControlType(self.command).name, self.packet)


class MQTTCommandRegistry(type):
    register = {}

    def __new__(mcs, name, bases, attrs, **kwargs):
        cls = super().__new__(mcs, name, bases, attrs)
        try:
            cmdname = name[4:].upper()
            cmdvalue = MQTTControlType[cmdname].value
            mcs.register[cmdvalue] = cls
        except Exception as err:
            pass
        return cls

    @classmethod
    def get_cls(cls, cmd):
        return cls.register[cmd]


class MQTTBody(metaclass=MQTTCommandRegistry):
    def __init__(self, buf, pos):
        self.flags = buf[0] & 0xf
        self._decode(buf, pos)

    def check(self):
        pass    # default implementation, do nothing

    def __repr__(self):
        return '?'

class MQTTConnect(MQTTBody):
    def _decode(self, buf, pos):
        self.protocol, pos = get_utf8_string(buf, pos)
        self.protolevel, pos = get_uint8(buf, pos)
        cflags, pos = get_uint8(buf, pos)
        assert (cflags & 0x1) == 0, 'reserved flag'
        self.clean = (cflags & 0x2) >> 1
        self.wflag = (cflags & 0x4) >> 2
        self.wqos = (cflags & 0x18) >> 3
        self.wrflag = (cflags & 0x20) >> 5
        self.pflag = (cflags & 0x40) >> 6
        self.uflag = (cflags & 0x80) >> 7
        self.keepalive, pos = get_uint16(buf, pos)
        self.clientid, pos = get_utf8_string(buf, pos)

        if self.wflag:
            self.will_topic, pos = get_utf8_string(buf, pos)
            self.will_msg, pos = get_binary_string(buf, pos)
        else:
            self.will_topic = None
            self.will_msg = None

        if self.uflag:
            self.username, pos = get_utf8_string(buf, pos)
        else:
            self.username = None

        if self.pflag:
            self.userpassword, pos = get_binary_string(buf, pos)
        else:
            self.userpassword = None

    def check(self):
        assert self.flags == 0, '[MQTT-3.1.2-3]'
        assert (self.protocol == 'MQTT' and self.protolevel == 4) or (self.protocol == 'MQIsdp' and self.protolevel == 3), 'invalid or unkown protocol'
        assert (self.wflag != 0 or self.wqos == 0), '[MQTT-3.1.2-13]'
        assert self.wqos != 3, '[MQTT-3.1.2-14]'
        assert (self.wflag != 0 or self.wrflag == 0), '[MQTT-3.1.2-15]'
        assert (self.uflag != 0 or self.pflag == 0), '[MQTT-3.1.2-22]'

    def _encode(self, buf):
        set_utf8_string(self.protocol, buf)
        set_uint8(self.protolevel, buf)
        cflags = self.clean << 1 | self.wflag << 2 | self.wqos << 3 | self.wrflag << 5 | self.pflag << 6 | self.uflag << 7
        set_uint8(cflags, buf)
        set_uint16(self.keepalive, buf)
        set_utf8_string(self.clientid, buf)
        if self.wflag:
            set_utf8_string(self.will_topic or '', buf)
            set_binary_string(self.will_msg or '', buf)
        if self.uflag:
            set_utf8_string(self.username or '', buf)
        if self.pflag:
            set_binary_string(self.userpassword or '', buf)

    def __repr__(self):
        return 'protocol=%s level=%d clientid=%s clean=%d keepalive=%d' % (
            self.protocol,
            self.protolevel,
            self.clientid,
            self.clean,
            self.keepalive
        )

class MQTTConnack(MQTTBody):
    def _decode(self, buf, pos):
        cflags, pos = get_uint8(buf, pos)
        assert (cflags & 0xfe) == 0
        self.session_present = cflags & 0x1
        self.code, pos = get_uint8(buf, pos)

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        set_uint8(self.session_present, buf)
        set_uint8(self.code, buf)

    def __repr__(self):
        return 'session_present=%d return=%d' % (
            self.session_present,
            self.code
        )

class MQTTPublish(MQTTBody):
    def _decode(self, buf, pos):
        self.dup = (self.flags >> 3) & 0x1
        self.qos = (self.flags >> 1) & 0x3
        self.retain = self.flags & 0x1
        self.topic, pos = get_utf8_string(buf, pos)
        if self.qos > 0:
            self.packetid, pos = get_uint16(buf, pos)
        else:
            self.packetid = None
        self.payload = buf[pos:]

    def check(self):
        assert (self.qos != 0 or self.dup == 0), '[MQTT-3.3.1-2]'
        assert self.qos != 3, '[MQTT-3.3.1-4]'
        assert ('+' not in self.topic and '#' not in self.topic), '[MQTT-3.3.2-2]'

    def _encode(self, buf):
        # TODO: setup flags
        set_utf8_string(self.topic, buf)
        if self.qos > 0:
            set_uint16(self.packetid, buf)
        buf.extend(self.payload)

    def __repr__(self):
        return 'qos=%d retain=%d dup=%d topic=%s' % (
            self.qos,
            self.retain,
            self.dup,
            self.topic
        )

class MQTTPuback(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)

    def __repr__(self):
        return 'packet_id=%d' % (
            self.packetid
        )

class MQTTPubrec(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)

    def __repr__(self):
        return 'packet_id=%d' % (
            self.packetid
        )

class MQTTPubrel(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)

    def check(self):
        assert self.flags == 0x2, '[MQTT-3.6.1-1]'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)

    def __repr__(self):
        return 'packet_id=%d' % (
            self.packetid
        )

class MQTTPubcomp(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)

    def __repr__(self):
        return 'packet_id=%d' % (
            self.packetid
        )

class MQTTSubscribe(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)
        self.subscriptions = []
        while pos < len(buf):
            filter, pos = get_utf8_string(buf, pos)
            qos, pos = get_uint8(buf, pos)
            self.subscriptions.append((filter, qos))

    def check(self):
        assert self.flags == 0x2, '[MQTT-3.8.1-1]'
        assert len(self.subscriptions) > 0, '[MQTT-3.8.3-3]'
        for _, qos in self.subscriptions:
            assert 0 <= qos <= 2, '[MQTT-3-8.3-4]'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)
        for filter, qos in self.subscriptions:
            set_utf8_string(filter, buf)
            set_uint8(qos, buf)

    def __repr__(self):
        return 'packet_id=%d subscriptions=[%s]' % (
            self.packetid,
            ', '.join('%s:%d' % elt for elt in self.subscriptions)
        )

class MQTTSuback(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)
        self.codes = []
        while pos < len(buf):
            code, pos = get_uint8(buf, pos)
            self.codes.append(code)

    def check(self):
        assert self.flags == 0, 'reserved flags'
        for code in self.codes:
            assert code in [0x0, 0x1, 0x2, 0x80], '[MQTT-3.9.3-2]'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)
        for code in self.codes:
            set_uint8(code, buf)

    def __repr__(self):
        return 'packet_id=%d return_codes=[%s]' % (
            self.packetid,
            ', '.join('%d' % elt for elt in self.codes)
        )

class MQTTUnsubscribe(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)
        self.unsubscriptions = []
        while pos < len(buf):
            filter, pos = get_utf8_string(buf, pos)
            self.unsubscriptions.append(filter)

    def check(self):
        assert self.flags == 0x2, '[MQTT-3.10.1-1]'
        assert len(self.unsubscriptions) > 0, '[MQTT-3.10.3-2]'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)
        for filter in self.unsubscriptions:
            set_utf8_string(filter, buf)

    def __repr__(self):
        return 'packet_id=%d unsubscriptions=[%s]' % (
            self.packetid,
            ', '.join('%s' % elt for elt in self.unsubscriptions)
        )

class MQTTUnsuback(MQTTBody):
    def _decode(self, buf, pos):
        self.packetid, pos = get_uint16(buf, pos)

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        set_uint16(self.packetid, buf)

    def __repr__(self):
        return 'packet_id=%d' % (
            self.packetid
        )

class MQTTPingreq(MQTTBody):
    def _decode(self, buf, pos):
        pass

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        pass

    def __repr__(self):
        return ''

class MQTTPingresp(MQTTBody):
    def _decode(self, buf, pos):
        pass

    def check(self):
        assert self.flags == 0, 'reserved flags'

    def _encode(self, buf):
        pass

    def __repr__(self):
        return ''

class MQTTDisconnect(MQTTBody):
    def _decode(self, buf, pos):
        pass

    def check(self):
        assert self.flags == 0, '[MQTT-3.14.1-1]'

    def _encode(self, buf):
        pass

    def __repr__(self):
        return ''


def remaining_length_encode(x, buf):
    #assert x < 268435455
    while True:
        digit = x % 128
        x = x // 128
        if x > 0:
            digit |= 0x80
        buf.append(digit)
        if x == 0:
            break

def remaining_length_decode(buf):
    mult = 1
    value = 0
    for i in range(1, 5):
        digit = buf[i]
        value += (digit & 127) * mult
        mult *= 128
        if digit & 128 == 0:
            return value
    assert False, 'bad remaining length'

def get_binary_string(buf, pos=0):
    sz, = struct.unpack_from('!H', buf, pos)
    pos += 2
    nextpos = pos + sz
    return buf[pos: nextpos], nextpos

def get_utf8_string(buf, pos=0):
    data, nextpos = get_binary_string(buf, pos)
    assert b'\x00' not in data, '[MQTT-1.5.3-2]'
    try:
        data = data.decode('utf8')
    except UnicodeEncodeError:
        assert False, '[MQTT-1.5.3-1]'
    return data, nextpos

def get_uint8(buf, pos=0):
    val, = struct.unpack_from('!B', buf, pos)
    return val, pos + 1

def get_uint16(buf, pos=0):
    val, = struct.unpack_from('!H', buf, pos)
    return val, pos + 2

def set_binary_string(val, buf):
    buf.extend(struct.pack('!H', len(val)))
    buf.extend(val)

def set_utf8_string(val, buf):
    data = val.encode('utf8')
    set_binary_string(data, buf)

def set_uint8(val, buf):
    buf.extend(struct.pack('!B', val))

def set_uint16(val, buf):
    buf.extend(struct.pack('!H', val))


def get_remaining_length(sock, buf):
    for i in range(4):
        c = sock.recv(1)
        digit, = struct.unpack('B', c)
        buf.append(digit)
        if digit & 128 == 0:
            break
    assert digit & 128 == 0

def read_fixed_header(sock, buf):
    data = sock.recv(1)
    if not data:
        return
    buf.extend(data)
    get_remaining_length(sock, buf)

def read_paquet(sock):
    buf = bytearray()
    read_fixed_header(sock, buf)
    if not buf:
        return
    length = remaining_length_decode(buf)
    # read the whole packet
    while len(buf) < length + 2:
        data = sock.recv(length + 2 - len(buf))
        buf.extend(data)
    packet = MQTTPacket(buf)
    # check the received packet
    try:
        packet.check()
    except AssertionError as err:
        print(' Error:', err.args)
    print(packet)
    # 
    data = packet._encode()
    assert data == buf
    return buf
