from ctypes import c_int, c_uint8, c_int32, c_int64, c_uint64, c_voidp
from ctypes import CDLL, pointer, POINTER, Structure, CFUNCTYPE
import errno
import signal
import time
import enum
import sys
import asyncio
import argparse
import threading
import binascii
import os
import io

from . import __version__


SIGEV_SIGNAL = 0
SIGEV_NONE = 1
SIGEV_THREAD = 2

O_SYNC = 0x101000
O_DSYNC = 0x1000

c_off_t = c_int64
c_size_t = c_uint64
c_size_tp = POINTER(c_size_t)

SIGEV_CB = CFUNCTYPE(c_int, c_size_tp)


def getename(c):
    if c == 0: return 'SUCCESS'
    c = c & 0xff
    return errno.errorcode[c]


class SIGEV(Structure):
    _pack_ = 1
    _fields_ = [
        ("value", c_size_tp),
        ("signo", c_int),
        ("notify", c_int),
        ("notify_function", SIGEV_CB),
        ("reserved1", c_uint8*0x28),
        ]


class AIOCB(Structure):
    _pack_ = 1
    _fields_ = [
        ("fildes", c_int),            # + 4 == 4
        ("reserved1", c_uint8*0xc),   # + 0xc == 0x10
        ("buf", POINTER(c_uint8)),    # + 8 == 0x18
        ("nbytes", c_size_t),         # + 8 == 0x20
        ("sigevent", SIGEV),          # + 0x40 == 0x60
        ("reserved2", c_uint8*0x20),  # + 0x20 == 0x80
        ("offset", c_off_t),          # + 8    == 0x88
        ("reserved3", c_uint8*0x20),  # + 0x20 == 0xa8
    ]


AIOCBp = POINTER(AIOCB)
AIOCBpp = POINTER(AIOCBp)

libaio = CDLL('libaio.so.1t64')

libaio.aio_read.argtypes = [AIOCBp]
libaio.aio_read.restype = c_int

libaio.aio_write.argtypes = [AIOCBp]
libaio.aio_write.restype = c_int

libaio.aio_return.argtypes = [AIOCBp]
libaio.aio_return.restype = c_int

libaio.aio_error.argtypes = [AIOCBp]
libaio.aio_error.restype = c_int

libaio.aio_suspend.argtypes = [AIOCBpp, c_int, c_voidp]
libaio.aio_suspend.restype = c_int

libaio.aio_fsync.argtypes = [c_int, AIOCBp]
libaio.aio_fsync.restype = c_int


# https://stackoverflow.com/questions/661017/access-to-errno-from-python#661303
libc = CDLL("libc.so.6")

get_errno_loc = libc.__errno_location
get_errno_loc.restype = POINTER(c_int)


try:
    # Debug the aiocb layout
    libmyaio = CDLL("libmyaio.so")

    libmyaio.my_aio_info.argtypes = [AIOCBp]
    libmyaio.my_aio_info.restype = c_int

    def showcb(cb):
        sys.stdout.write(f'show cb: {cb}\n')
        libmyaio.my_aio_info(cb)
except:
    pass


class AIO:
    _readsDict = {}
    _file = None
    _fname = None
    _mode = None
    _verbose = 0

    def __init__(self, fname, mode, **kw):
        self._readsDict = {}
        self._fname = fname
        self._mode = mode
        self._opts = kw
        self._thr = None
        self._completed = asyncio.Condition()
        self._thr_empty = threading.Condition()
        self._thr_lock = threading.Lock()
        self._thr_stop = False
        self._thr_future = None

    def __del__(self):
        assert self._thr is None
        self.releaseThread()
        if self._file:
            self._file.close()

    def log(self, msg):
        print(f'{self} {msg}')

    def __str__(self):
        return f'AIO({self._fname}, {self._mode})'

    def run_suspend(self):
        with self._thr_lock:
            cbs = list(self._readsDict.values())
        cblist = (AIOCBp * len(cbs))()
        if self._verbose:
            self.log(f'aio_suspend: wait for {len(cbs)}')
            self.log(f'aio_suspend: wait for {cbs[0:3]}...')
        for i in range(len(cbs)):
            cblist[i] = pointer(cbs[i]['cb'])
        rc = libaio.aio_suspend(cblist, len(cbs), None)
        if rc != 0:
            errc = get_errno_loc()[0]
            if self._verbose:
                self.log(f'Error aio_suspend: {rc} {getename(errc)}')
        else:
            self._thr_future = 1
            self._thr_future = asyncio.run_coroutine_threadsafe(self.somecomplete_handler(), loop=self._loop)
            assert self._thr_future.result() == 0
            if self._verbose:
                self.log(f'aio_suspend completed: {rc}')
            self._thr_future = None

    def run_suspend_loop(self):
        while not self._thr_stop:
            if self._verbose:
                self.log(f'aio_suspend loop')
            if len(self._readsDict) == 0:
                with self._thr_empty:
                    if self._verbose:
                        self.log(f'aio_suspend: queue is empty')
                    if not self._thr_stop:
                        self._thr_empty.wait()
            else:
                self.run_suspend()
        if self._verbose:
            self.log(f'aio_suspend thread for {self._fname} exiting')

    def start_aio_suspend_loop(self):
        if self._thr is None:
            self._thr = threading.Thread(target=self.run_suspend_loop)
            self._thr.start()
            if self._verbose:
                self.log(f'aio_suspend thread for {self._fname} starting: {self._thr}')

    async def somecomplete_handler(self):
        await self.checkreturn()
        return 0

    def checkreturn1(self, cb):
        errc2 = 0
        rc1 = libaio.aio_error(cb)
        #sys.stdout.write(f'Check return of AIO cb {id(cb)}: {getename(rc1)}\n')
        #showcb(cb)
        if rc1 == errno.EINPROGRESS:
            pass
        else:
            rc2 = libaio.aio_return(cb)
            if rc1 != 0:
                errc2 = get_errno_loc()[0]
            else:
                #self.log(f'data: ', [c for c in cb.buf])
                #self.log(f'data: ', cb.buf)
                #self.log(f'data ({cb.nbytes} B): ', bytes(cb.buf[0:min(12, cb.nbytes)]))
                pass
            #sys.stdout.write(f'aio returned: {rc2} error {getename(errc2)}; aio_error {getename(rc1)}\n')
        return rc1 == 0, rc2

    async def checkreturn(self):
        completed = []
        with self._thr_lock:
            cblist = list(self._readsDict.values())
        for cb in cblist:
            status, code = self.checkreturn1(cb['cb'])
            if status:
                cb['code'] = code
                completed += [cb]

        if self._verbose:
            self.log(f'Remove completed: {len(completed)}')
        with self._thr_lock:
            for cb in completed:
                i = id(cb['cb'])
                #if self._verbose:
                #    self.log(f'{cb["mode"]} @{cb["offset"]} completed, remove')
                del self._readsDict[i]

        for cb in completed:
            async with cb['cond']:
                cb['cond'].notify()
#            async with cb['cond']:
#                await cb['cond'].wait()

        if self._verbose:
            self.log(f'{len(self._readsDict)} tasks remain\n')
        async with self._completed:
            self._completed.notify()

    def _read(self, n, offset=0):
        indata = (c_uint8 * n)()
        cb = AIOCB()
        cb.fildes = self._file.fileno()
        cb.buf = indata
        cb.offset = offset
        cb.nbytes = n
        #self.log(f'Call AIO read with {cb}')
        #showcb(cb)
        rc = libaio.aio_read(cb)
        if rc != 0:
            errc = get_errno_loc()[0]
            if self._verbose:
                self.log(f'Error aio_read: {rc} {getename(errc)}')
        else:
            cond = asyncio.Condition()
            with self._thr_lock:
                isempty = len(self._readsDict) == 0
                self._readsDict[id(cb)] = dict(cb=cb, cond=cond,mode='read',offset=offset)
            if isempty:
                with self._thr_empty:
                    self._thr_empty.notify()
        return self._readsDict[id(cb)]

    async def read(self, n, offset=0):
        cb = self._read(n, offset=offset)
        data = None
        async with cb['cond']:
            await cb['cond'].wait()
            nread = cb['code']
            data = bytes(cb['cb'].buf[0:nread])
#        async with cb['cond']:
#            cb['cond'].notify()
        return data

    def _write(self, data, offset=0):
        n = len(data)
        indata = (c_uint8 * n)()
        indata[0:n] = data
        cb = AIOCB()
        cb.fildes = self._file.fileno()
        cb.buf = indata
        cb.offset = offset
        cb.nbytes = n
        #self.log(f'Call AIO write with {cb}')
        #showcb(cb)
        rc = libaio.aio_write(cb)
        if rc != 0:
            errc = get_errno_loc()[0]
            if self._verbose:
                self.log(f'Error aio_write: {rc} {getename(errc)}')
        else:
            cond = asyncio.Condition()
            with self._thr_lock:
                isempty = len(self._readsDict) == 0
                self._readsDict[id(cb)] = dict(cb=cb, cond=cond,mode='write',offset=offset)
            if isempty:
                with self._thr_empty:
                    self._thr_empty.notify()
        return cond

    async def write(self, data, offset=0):
        c = self._write(data, offset=offset)
        async with c:
            await c.wait()
#        async with c:
#            c.notify()
        return len(data)

    def _fsync(self, op=O_SYNC):
        cb = AIOCB()
        cb.fildes = self._file.fileno()
        #self.log(f'Call AIO fsync with {cb}')
        #showcb(cb)
        rc = libaio.aio_fsync(op, cb)
        if rc != 0:
            errc = get_errno_loc()[0]
            if self._verbose:
                self.log(f'Error aio_fsync: {rc} {getename(errc)}')
        else:
            cond = asyncio.Condition()
            with self._thr_lock:
                isempty = len(self._readsDict) == 0
                self._readsDict[id(cb)] = dict(cb=cb, cond=cond,mode='fsync',offset=0,op=op)
            if isempty:
                with self._thr_empty:
                    self._thr_empty.notify()
        return cond

    async def fsync(self, op=O_SYNC):
        c = self._fsync(op)
        async with c:
            await c.wait()
        if self._verbose:
            self.log('fsync was notified!')
#        async with c:
#            c.notify()
        return 0

    def fileno(self):
        return self._file.fileno() if self._file and not self._file.closed else -1

    async def start(self):
        if self._file is None:
            self._file = open(self._fname, self._mode)
        else:
            if self._verbose:
                self.log('File was opened already')
        self._loop = asyncio.get_event_loop()
        self.start_aio_suspend_loop()

    def releaseThread(self):
        if self._verbose:
            self.log('releaseThread')
        if self._thr is not None:
            self._thr_stop = True
            with self._thr_empty:
                if self._verbose:
                    self.log(f'thread notify: {self._thr}')
                self._thr_empty.notify()
            self._thr.join()
            if self._verbose:
                self.log(f'thread joined: {self._thr}')
            self._thr = None

    async def release(self):
        if self._verbose:
            self.log('release')
        while len(self._readsDict):
            if self._verbose:
                self.log('there are pending threads')
            async with self._completed:
                await self._completed.wait()
#        if self._thr is not None:
#            self._thr_stop = True
        while self._thr_future is not None:
            if self._verbose:
                self.log('there is a pending future')
            await asyncio.sleep(1e-6)
        if self._file:
            self._file.close()
        self._file = None
        self.releaseThread()


def mkparser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument('-o', '--output', metavar='file', type=str, nargs='+')
    parser.add_argument('-a', '--append', action="store_true")
    parser.add_argument('-e', '--encoding', metavar='S', type=str)

    parser.add_argument('-V', '--version', action="version", version=f"%(prog)s v{__version__}")
    parser.add_argument('-v', '--verbose', type=int, metavar='N', nargs='?', const=1)

    return parser


async def arun(args=None):
    if args is None:
        parser = mkparser()
        args = parser.parse_args()

    aio = AIO('test.txt', 'r+')
    await aio.start()
    res = await aio.read(8, offset=0)
    print(res)
    await asyncio.sleep(0.1)
    data = b'Testa Testb testc\r\n'
    data = binascii.b2a_base64(os.urandom(1 << 10))
    res2 = await aio.write(data, offset=19)
    print(res2)
    await asyncio.sleep(0.1)

    tasks = [ aio.write(data, offset=19 + (i+1)*len(data)) for i in range(5000) ]
    await asyncio.gather(*tasks)

    tr = asyncio.create_task(aio.run())
    #await asyncio.sleep(3)
    await aio.release()


def run(args=None):
    asyncio.run(arun(args))


if __name__ == "__main__":
    run()
