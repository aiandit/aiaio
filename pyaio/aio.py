from ctypes import c_int, c_uint8, c_int32, c_int64, c_uint64, c_voidp
from ctypes import CDLL, POINTER, Structure
import errno
import time


c_off_t = c_int64
c_size_t = c_uint64


def getename(c):
    if c == 0: return 'SUCCESS'
    c = c & 0xff
    return errno.errorcode[c]


class POINT(Structure):
    _fields_ = [("x", c_int),
                ("y", c_int)]


class SIGEVENT(Structure):
    _fields_ = [
        ("fildes", c_int),
    ]


class AIOCB(Structure):
    _pack_ = 1
    _fields_ = [
        ("fildes", c_int),
        ("reserved1", c_uint8*0xc),
        ("buf", POINTER(c_uint8)),
        ("nbytes", c_size_t),
        ("reserved2", c_uint8*0x60),
        ("offset", c_off_t),
        ("reserved3", c_uint8*0x20),
    ]


AIOCBp = POINTER(AIOCB)

libaio = CDLL('libaio.so.1t64')

libaio.aio_read.argtypes = [AIOCBp]
libaio.aio_read.restype = c_int

libaio.aio_write.argtypes = [AIOCBp]
libaio.aio_write.restype = c_int

libaio.aio_return.argtypes = [AIOCBp]
libaio.aio_return.restype = c_int

libaio.aio_error.argtypes = [AIOCBp]
libaio.aio_error.restype = c_int


# https://stackoverflow.com/questions/661017/access-to-errno-from-python#661303
libc = CDLL("libc.so.6")

get_errno_loc = libc.__errno_location
get_errno_loc.restype = POINTER(c_int)


libmyaio = CDLL("libmyaio.so")

libmyaio.my_aio_info.argtypes = [AIOCBp]
libmyaio.my_aio_info.restype = c_int


def showcb(cb):
    print('show cb', cb)
    libmyaio.my_aio_info(cb)


class AIO:
    _reads = []
    _file = None

    def __init__(self, fname, mode, **kw):
        self._reads = []
        self._file = open(fname, mode)
        self._opts = kw

    def __del__(self):
        self._file.close()

    def checkreturn(self):
        pending = []
        for cb in self._reads:
            errc2 = 0
            rc1 = libaio.aio_error(cb)
            print(f'Check return of AIO cb error: {getename(rc1)} {showcb(cb)}')
            if rc1 == errno.EINPROGRESS:
                pending += [cb]
            else:
                rc2 = libaio.aio_return(cb)
                if rc1 != 0:
                    errc2 = get_errno_loc()[0]
                else:
                    #print(f'data: ', [c for c in cb.buf])
                    print(f'data: ', cb.buf)
                    print(f'data: ', bytes(cb.buf[0:cb.nbytes]))
                    pass
                print(f'aio returned: {rc2} error {getename(errc2)}; aio_error {getename(rc1)}')
        print(f'{len(pending)} of {len(self._reads)} tasks remain')
        self._reads = pending

    def read(self, n, offset=0):
        indata = (c_uint8 * n)()
        cb = AIOCB()
        cb.fildes = self._file.fileno()
        cb.buf = indata
        cb.offset = offset
        cb.nbytes = n
        cb.buf.value = b'Hallo Welt'
        self._reads += [cb]
        print(f'Call AIO read with {showcb(cb)}')
        rc = libaio.aio_read(cb)
        if rc != 0:
            errc = get_errno_loc()[0]
            print(f'Error aio_read: {rc} {getename(errc)}')

    def write(self, data, offset=0):
        n = len(data)
        indata = (c_uint8 * n)()
        indata[0:n] = data
        cb = AIOCB()
        cb.fildes = self._file.fileno()
        cb.buf = indata
        cb.offset = offset
        cb.nbytes = n
        self._reads += [cb]
        print(f'Call AIO write with {showcb(cb)}')
        rc = libaio.aio_write(cb)
        if rc != 0:
            errc = get_errno_loc()[0]
            print(f'Error aio_write: {rc} {getename(errc)}')


def run():
    aio = AIO('test.txt', 'r+')
    res = aio.read(8, 0)
    print(res)
    res2 = aio.write(b'Test Test test\n', 19)
    print(res2)
    for _ in range(4):
        res = aio.checkreturn()
        time.sleep(0.1)

run()
