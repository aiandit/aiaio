from ctypes import c_short, c_int, c_uint, c_long, c_longlong, c_uint8, c_int64, c_uint64, c_voidp
from ctypes import CDLL, pointer, POINTER, Structure, addressof
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


class TIMESPEC(Structure):
    _pack_ = 1
    _fields_ = [
        ("tv_sec", c_uint64),           # + 8
        ("tv_nsec", c_uint64),          # + 8
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


class IOCB(Structure):
    _pack_ = 1
    _fields_ = [
        ("data", c_uint64),             # + 8 == 8
        ("key", c_uint),                # + 4
        ("aio_rw", c_uint),             # + 4 == 0x10
        ("aio_lio_opcode", c_short),    # + 2 == 0x12
        ("aio_reqprio", c_short),       # + 2 == 0x14
        ("aio_fildes", c_int),          # + 4 == 0x18
        ("uc", IOCB_COMMON),            # + 0x28 == 0x40
        ]


class IO_EVENT(Structure):
    _pack_ = 1
    _fields_ = [
        ("data", c_uint64),             # + 8
        ("obj", POINTER(IOCB)),         # + 8
        ("res", c_int64),               # + 8
        ("res2", c_int64),              # + 8 == 0x20
        ]


TIMESPECp = POINTER(TIMESPEC)
SIGSETp = POINTER(SIGSET)
IO_CONTEXTp = POINTER(IO_CONTEXT)
IOCBp = POINTER(IOCB)
IOCBpp = POINTER(IOCBp)
IO_EVENTp = POINTER(IO_EVENT)
IO_EVENTpp = POINTER(IO_EVENTp)

libnames = ['libaio.so.1t64', 'libaio.so.1']
for n in libnames:
    try:
        libaio = CDLL(n)
        break
    except Exception:
        pass


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


try:
    # Debug the iocb layout
    libmyaio = CDLL("libmyaio.so")

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
    pass


class IOContext:

    _readsDict = {}
    _id = 0
    _maxbatch = 1000
    _verbose = 0
    _task = None
    _loop = None
    _loops = 0
    _name = None

    def __init__(self, numRequests=10000, name=None):
        self.numRequests = numRequests
        self._ctx = IO_CONTEXT()
        rc = libaio.io_setup(numRequests, self._ctx)
        if rc < 0:
            raise OSError(f'io_setup: {getename(-rc)}')
        self._readsDict = {}
        self._task = None
        IOContext._id += 1
        self._id = IOContext._id
        if name is None:
            name = f'ioctx-{self._id}'
        self._name = name

    def __del__(self):
        assert self._task is None
        self.releaseThread()
        self.closectx()
        self.numRequests = -1

    def closectx(self):
        if self._ctx:
            rc = libaio.io_destroy(self._ctx)
            if -rc == errno.EINVAL:
                pass
            elif rc < 0:
                raise OSError(f'io_destroy: {getename(-rc)}')

    def __str__(self):
        return f'IOContext({self._name}, n={self.numRequests})'

    def __repr__(self):
        return f'IOContext({self._name}, n={self.numRequests}, {bytes(self._ctx).hex()})'

    def log(self, msg):
        print(f'{time.time() - global_t0: 12.3f} {self} {msg}')
        sys.stdout.flush()

    def _io_submit_handler(self, cb):
        pass

    def _io_submit(self, cb):
        rc = libaio.io_submit(self._ctx, 1, pointer(cb))
        if rc < 0:
            self.log(f'Error io_submit: {rc} {getename(-rc)}')
            raise OSError(f'io_submit: {getename(-rc)}')
        if rc == 1:
            cbid = addressof(cb)
            self._readsDict[cbid] = item = dict(cb=cb,cond=asyncio.Condition())
            self._io_submit_handler(cb)
            return item
        else:
            self.log(f'io_submit returned wrong code: {rc}')
            raise OSError(f'io_submit: code {rc}')

    async def run_getevents_loop1(self):
        nevents = self._maxbatch
        events = (IO_EVENT * nevents)()
        timeout = TIMESPEC() # == 0
        sigmask = SIGSET()
        libaio.sigfillset(sigmask)

        while True:
            rc = libaio.io_pgetevents(self._ctx, 1, nevents, events, timeout, sigmask)
            if rc > 0:
                evlist = [(addressof(events[i].obj.contents), events[i].res, events[i].res2) for i in range(rc)]
                await self.notify_cbcomplete_list_task(evlist)
            elif rc < 0:
                self.log(f'Error io_pgetevents: {rc} {getename(-rc)}')
                raise OSError(f'io_pgetevents: {getename(-rc)}')
            elif rc == 0:
                await asyncio.sleep(1e-3)
        if self._verbose:
            self.log(f'task io_pgetevents exiting')

    async def run_getevents_loop(self):
        try:
            await self.run_getevents_loop1()
        except asyncio.CancelledError:
            if self._verbose:
                self.log(f'task pgetevents cancelled')
        except Exception as ex:
            self.log(f'task pgetevents raise exception {ex}')
            raise ex

    async def notify_cbcomplete_task(self, cbid, res, res2):
        dentry = self._readsDict[cbid]
        async with dentry['cond']:
            dentry['code'] = res
            dentry['cond'].notify()
        del self._readsDict[cbid]

    async def notify_cbcomplete_list_task(self, evlist):
        async with asyncio.TaskGroup() as gr:
            [gr.create_task(self.notify_cbcomplete_task(*entry)) for entry in evlist]
        return 0

    async def start_aio_suspend_loop(self):
        self._loops += 1
        if self._loop:
            if self._loop != asyncio.get_event_loop():
                if self._verbose:
                    self.log('Loop has changed!')
                self._task = None
        self._loop = asyncio.get_event_loop()
        if self._task is None:
            self._task = asyncio.create_task(self.run_getevents_loop())
            if self._verbose:
                self.log(f'io_pgetevents task starting')

    async def start(self):
        await self.start_aio_suspend_loop()

    def releaseThread(self):
        if self._task is not None:
            self.log(f'releaseThread')
            self._task.cancel()
            self._task = None
        self._loop = None

    async def release(self):
        self.log(f'release')
        self.releaseThread()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        return await self.release()


global_context = IOContext(1000)
global_contexts = [global_context]
