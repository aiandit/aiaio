from ctypes import c_short, c_int, c_long, c_longlong, c_uint, c_uint8, c_int32, c_uint32, c_int64, c_uint64, c_voidp
from ctypes import CDLL, pointer, byref, POINTER, Structure, CFUNCTYPE, sizeof, addressof
import errno
import time
import sys
import asyncio


IO_CMD_PREAD = 0
IO_CMD_PWRITE = 1

IO_CMD_FSYNC = 2
IO_CMD_FDSYNC = 3

IO_CMD_POLL = 5
IO_CMD_NOOP = 6


c_off_t = c_int64
c_size_t = c_uint64
c_size_tp = POINTER(c_size_t)
c_uint8p = POINTER(c_uint8)


global_t0 = time.time()


def getename(c):
    if c == 0: return 'SUCCESS'
    c = c & 0xff
    return errno.errorcode[c]


async def find_task_by_name(task_name):
    for task in asyncio.all_tasks():
        if task.get_name().startswith(task_name):
            return task
    return None


class TIMESPEC(Structure):
    _pack_ = 1
    _fields_ = [
        ("tv_sec", c_uint64),              # + 4
        ("tv_nsec", c_uint64),             # + 4
        ]

class SIGSET(Structure):
    _pack_ = 1
    _fields_ = [
        ("buf", c_uint8*0x80),          # + 0x80
        ]

class IO_CONTEXT(Structure):
    _pack_ = 1
    _fields_ = [
        ("buf", c_voidp),               # + 8
        ]

class IOCB_COMMON(Structure):
    _pack_ = 1
    _fields_ = [
        ("buf", c_uint8p),              # + 8
        ("nbytes", c_long),             # + 8
        ("offset", c_longlong),         # + 8
        ("_pad3", c_longlong),          # + 8
        ("_pad4", c_longlong),          # + 8 == 0x28
        ]
    def __str__(self):
        res = f'IOCB_COMMON@{id(self):#x}({self.buf},\n {self.nbytes},\n {self.offset})'
        if self.buf:
            res += f'\n  buf: {bytes(self.buf[0:self.nbytes])}'
        return res


class IOCB(Structure):
    _pack_ = 1
    _fields_ = [
        ("data", c_uint64),             # + 8 == 8
        ("key", c_uint),                # + 4
        ("aio_rw", c_uint),             # + 4 == 0x10
        ("aio_lio_opcode", c_short),    # + 2 == 0x12
        ("aio_reqprio", c_short),       # + 2 == 0x14
        ("aio_fildes", c_int),          # + 4 == 0x18
        ("uc", IOCB_COMMON),            # + 0x30 == 0x48
        ]
    def __str__(self):
        return f'IOCB@{id(self):#x}({self.data:#x},\n {self.key},\n {self.aio_lio_opcode},\n fd={self.aio_fildes},\n {self.uc})'

class IO_EVENT(Structure):
    _pack_ = 1
    _fields_ = [
        ("data", c_uint64),             # + 8
        ("obj", POINTER(IOCB)),        # + 8
        ("res", c_int64),              # + 8
        ("res2", c_int64),             # + 8
        ]

    def __str__(self):
        res = f'IO_EVENT[{sizeof(self)}@{id(self):#x}]({self.data:#x},\n {self.obj},\n {self.res},\n {self.res2})'
        if self.obj:
            res += f'\n  obj: {self.obj.contents}'
        return res

TIMESPECp = POINTER(TIMESPEC)
SIGSETp = POINTER(SIGSET)
IO_CONTEXTp = POINTER(IO_CONTEXT)
IOCBp = POINTER(IOCB)
IOCBpp = POINTER(IOCBp)
IO_EVENTp = POINTER(IO_EVENT)
IO_EVENTpp = POINTER(IO_EVENTp)

libaio = CDLL('libaio.so.1t64')

libaio.io_setup.argtypes = [c_int, IO_CONTEXTp]
libaio.io_setup.restype = c_int

libaio.io_destroy.argtypes = [IO_CONTEXT]
libaio.io_destroy.restype = c_int

libaio.io_submit.argtypes = [IO_CONTEXT, c_long, IOCBpp]
libaio.io_submit.restype = c_int

libaio.io_getevents.argtypes = [IO_CONTEXT, c_long, c_long, IO_EVENTp, TIMESPECp]
libaio.io_getevents.restype = c_int

libaio.io_pgetevents.argtypes = [IO_CONTEXT, c_long, c_long, IO_EVENTp, TIMESPECp, SIGSETp]
libaio.io_pgetevents.restype = c_int

libaio.sigfillset.argtypes = [SIGSETp]
libaio.sigemptyset.argtypes = [SIGSETp]
libaio.sigaddset.argtypes = [SIGSETp, c_int]
libaio.sigdelset.argtypes = [SIGSETp, c_int]
libaio.sigprocmask.argtypes = [c_int, SIGSETp, SIGSETp]


# https://stackoverflow.com/questions/661017/access-to-errno-from-python#661303
libc = CDLL("libc.so.6")

get_errno_loc = libc.__errno_location
get_errno_loc.restype = POINTER(c_int)


try:
    # Debug the aiocb layout
    libmyaio = CDLL("libmyaio.so")

    assert sizeof(IO_CONTEXT) == 8

    libmyaio.my_io_info.argtypes = [IO_CONTEXTp, IOCBp, SIGSETp, IO_EVENTp, IO_EVENTpp, TIMESPECp]
    libmyaio.my_io_info.restype = c_int

    libmyaio.my_io_event_info.argtypes = [IO_EVENTp]
    libmyaio.my_io_event_info.restype = c_int

    libmyaio.my_io_getevents.argtypes = [IO_CONTEXT, c_long, c_long, IO_EVENTp, TIMESPECp, SIGSETp]
    libmyaio.my_io_getevents.restype = c_int

    def showcb(ctx, cb, sigset=None, event=None, timespec=None):
        libmyaio.my_io_info(ctx, cb, sigset, event, None, timespec)

    def showevent(event):
        libmyaio.my_io_event_info(event)

except BaseException as ex:
    #print('debug', ex)
    pass


global_task = None


class IOContext:

    _readsDict = {}
    _id = 0
    _maxbatch = 1000
    _verbose = 0
    _tgrp = None
    _task = None
    _loop = None
    _loops = 0

    def __init__(self, numRequests=10000):
        self.numRequests = numRequests
        self._ctx = IO_CONTEXT()
        rc = libaio.io_setup(numRequests, self._ctx)
        print(f'io_setup: {getename(-rc)}')
        if rc < 0:
            raise OSError(f'io_setup: {getename(-rc)}')
        self._readsDict = {}
        self._task = None
        self._tgrp = None
        self._thr_stop = False
        IOContext._id += 1
        self._id = IOContext._id

    def __del__(self):
        assert self._task is None
        self.releaseThread()
        self.closectx()

    def closectx(self):
        if self._ctx:
            rc = libaio.io_destroy(self._ctx)
            if -rc == errno.EINVAL:
                pass
            elif rc < 0:
                raise OSError(f'io_destroy: {getename(-rc)}')

    def __str__(self):
        return f'IOContext({self.numRequests}, {self._id})'

    def __repr__(self):
        return f'IOContext({self.numRequests}, {bytes(self._ctx).hex()})'

    def log(self, msg):
        print(f'{time.time() - global_t0: 12.3f} {self} {msg}')
        sys.stdout.flush()

    def _io_submit(self, cb):
        rc = libaio.io_submit(self._ctx, 1, pointer(cb))
        if rc < 0:
            self.log(f'Error io_submit: {rc} {getename(-rc)}')
            raise OSError(f'io_submit: {getename(-rc)}')
        if rc == 1:
            self._readsDict[id(cb)] = item = dict(cb=cb,cond=asyncio.Condition())
            if self._verbose:
                self.log(f'io_submit added task {id(cb):#x}: fd {cb.aio_fildes}, cmd {cb.aio_lio_opcode}')
            return item
        else:
            self.log(f'io_submit returned wrong code: {rc}')
            raise OSError(f'io_submit: code {rc}')

    async def run_getevents_loop1(self):
        nevents = self._maxbatch
        events = (IO_EVENT * nevents)()
        timeout = TIMESPEC()
        timeout.tv_sec = 0
        timeout.tv_nsec = 0 * int(1e6) # ms
        sigmask = SIGSET()
        libaio.sigfillset(sigmask)
        tries = 0

        while not self._thr_stop:
            if self._verbose:
                self.log(f'io_pgetevents loop')
            rc = libaio.io_getevents(self._ctx, 1, nevents, events, timeout)
            if self._verbose:
                self.log(f'io_pgetevents got {rc} events')
            if rc > 0:
                evlist = [(events[i].obj.contents.data, events[i].res, events[i].res2) for i in range(rc)]
                await self.notify_cbcomplete_list_task(evlist)
                tries = 0
            elif rc < 0:
                self.log(f'Error io_pgetevents: {rc} {getename(-rc)}')
                raise OSError(f'io_pgetevents: {getename(-rc)}')
            elif rc == 0:
                if not self._thr_stop:
                    await asyncio.sleep(1e-3)
                else:
                    self.log(f'io_pgetevents stop set!')
            if self._verbose:
                self.log(f'io_pgetevents slept a little')
        if self._verbose:
            self.log(f'task io_pgetevents exiting')

    async def run_getevents_loop(self):
        try:
            await self.run_getevents_loop1()
        except asyncio.CancelledError:
            if self._verbose:
                self.log(f'task pgetevents cancelled')
        except Exception as ex:
            self.log(f'task pgetevents raise exception')
            raise ex

    async def notify_cbcomplete_task(self, cbid, res, res2):
        if self._verbose:
            self.log(f'io_pgetevents event notify {cbid:#x}')
        dentry = self._readsDict[cbid]
        async with dentry['cond']:
            dentry['code'] = res
            dentry['cond'].notify()
        del self._readsDict[cbid]
        if self._verbose:
            self.log(f'io task {cbid:#x} completed, {len(self._readsDict)} io tasks remain')
        return 0

    async def notify_cbcomplete_list_task(self, evlist):
        tasks = [self.notify_cbcomplete_task(*entry) for entry in evlist]
        await asyncio.gather(*tasks)
        return 0

    async def start_aio_suspend_loop(self):
        global global_task
        self._loops += 1
        if self._loop:
            if self._loop != asyncio.get_event_loop():
                print('Loop has changed!')
                self._tgrp = None
                self._task = None
        self._thr_stop = False
        self._loop = asyncio.get_event_loop()
        if self._tgrp is None:
            self._tgrp = asyncio.TaskGroup()
            await self._tgrp.__aenter__()
        if self._task is None:
            self._task = self._tgrp.create_task(self.run_getevents_loop(), name=f'getevents-l{self._loops}-{id(self._loop):#x}')
            global_task = self._task
            if self._verbose:
                self.log(f'io_pgetevents task starting')

    async def start(self):
        await self.start_aio_suspend_loop()

    def releaseThread(self):
        if self._verbose:
            self.log('releaseThread')
        if self._task is not None:
            self._thr_stop = True
            try:
                self._task.cancel()
            except RuntimeError:
                pass
            self._task = None
        self._tgrp = None
        self._loop = None

    async def release(self):
        self.releaseThread()
        if self._verbose:
            self.log(f'IOContext release complete!')


global_context = IOContext(1000)


class AIO:
    _file = None
    _fname = None
    _mode = None
    _verbose = 0
    _own_ctx = False

    def __init__(self, fname, mode, numRequests=10000, **kw):
        global global_context
        self._readsDict = {}
        self._fname = fname
        self._mode = mode
        self._opts = kw
        if True:
            if numRequests > global_context.numRequests:
                print(f'Larger context: {numRequests}')
                global_context = IOContext(numRequests)
            self.ctx = global_context
        else:
            self.ctx = IOContext(numRequests)
            self._own_ctx = True

    def __del__(self):
        if self._file:
            self._file.close()
        if self._own_ctx:
            self.ctx.closectx()

    def __str__(self):
        return f'AIO(fd={self.fileno()}, {self._fname}, {self._mode}, {self.ctx})'

    def log(self, msg):
        print(f'{time.time() - global_t0: 12.3f} {self} {msg}')
        sys.stdout.flush()

    def _showcb(self, cb):
        showcb(self.ctx._ctx, cb, SIGSET(), IO_EVENT(), TIMESPEC())

    def _read(self, n, offset=0):
        indata = (c_uint8 * n)()
        cb = IOCB()
        #self._showcb(cb)
        cb.aio_fildes = self._file.fileno()
        cb.data = id(cb)
        # cb.aio_lio_opcode = IO_CMD_PREAD == 0
        cb.uc.buf = indata
        cb.uc.nbytes = n
        cb.uc.offset = offset
        return self.ctx._io_submit(cb)

    async def read(self, n, offset=0):
        cb = self._read(n, offset=offset)
        data = None
        async with cb['cond']:
            await cb['cond'].wait()
            nread = cb['code']
            data = bytes(cb['cb'].uc.buf[0:nread])
        return data

    def _write(self, data, offset=0):
        n = len(data)
        indata = (c_uint8 * n)()
        indata[0:n] = data
        cb = IOCB()
        #self._showcb(cb)
        cb.aio_fildes = self._file.fileno()
        cb.data = id(cb)
        cb.aio_lio_opcode = IO_CMD_PWRITE
        cb.uc.buf = indata
        cb.uc.nbytes = n
        cb.uc.offset = offset
        return self.ctx._io_submit(cb)

    async def write(self, data, offset=0):
        cb = self._write(data, offset=offset)
        async with cb['cond']:
            await cb['cond'].wait()
        return cb['code']

    def _fsync(self, op):
        if self._file.closed:
            raise BaseException('File closed already')
        cb = IOCB()
        cb.aio_fildes = self._file.fileno()
        cb.data = id(cb)
        cb.aio_lio_opcode = op
        return self.ctx._io_submit(cb)

    async def fsync(self):
        cb = self._fsync(IO_CMD_FSYNC)
        async with cb['cond']:
            await cb['cond'].wait()
        return cb['code']

    async def fdsync(self):
        cb = self._fsync(IO_CMD_FDSYNC)
        async with cb['cond']:
            await cb['cond'].wait()
        return cb['code']

    def fileno(self):
        return self._file.fileno() if self._file and not self._file.closed else -1

    async def start(self):
        await self.ctx.start()
        if self._file is None:
            self._file = open(self._fname, self._mode)
            if self._verbose:
                self.log(f'opened file handle')
        else:
            if self._verbose:
                self.log('File was opened already')

    async def release(self):
        if self._verbose:
            self.log('release')
        if self._file:
            if self._verbose:
                self.log(f'closing file handle')
            self._file.close()
        self._file = None
        if self._own_ctx:
            await self.ctx.release()


def mkparser(parser=None):
    import argparse
    from . import __version__
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument('-o', '--output', metavar='file', type=str, nargs='+')
    parser.add_argument('-a', '--append', action="store_true")
    parser.add_argument('-e', '--encoding', metavar='S', type=str)

    parser.add_argument('-V', '--version', action="version", version=f"%(prog)s v{__version__}")
    parser.add_argument('-v', '--verbose', type=int, metavar='N', nargs='?', const=1)

    return parser


async def arun(args=None):
    import binascii
    import os
    if args is None:
        parser = mkparser()
        args = parser.parse_args()

    aio = AIO('example.txt', 'r+', numRequests=15000)
    await aio.start()
    res = await aio.read(28, offset=0)
    print(res)
    await asyncio.sleep(0.1)
    data = b'Testa Testb testc\r\n'
    data = binascii.b2a_base64(os.urandom(1 << 10))
    res2 = await aio.write(data, offset=19)
    print(res2)
    #await asyncio.sleep(0.1)

    tasks = [ aio.write(data, offset=19 + (i+1)*len(data)) for i in range(120) ]
    await asyncio.gather(*tasks)

    #tr = asyncio.create_task(aio.start())
    #await asyncio.sleep(3)
    await aio.release()


def run(args=None):
    asyncio.run(arun(args))


if __name__ == "__main__":
    run()
