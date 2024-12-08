import time
import sys
import asyncio

from .iocontext_task import c_uint8, IOCB
from .iocontext_task import IO_CMD_PREAD, IO_CMD_PWRITE, IO_CMD_FSYNC, IO_CMD_FDSYNC

from .iocontext_mt import IOContextMT as IOContext, global_context_mt as global_context, global_contexts_mt as global_contexts


global_t0 = time.time()


async def release_globals():
    for i, gctx in enumerate(global_contexts):
        await gctx.release()


class AIO:
    _file = None
    _fname = None
    _mode = None
    _verbose = 0
    ctx = None

    def __init__(self, fname, mode, numRequests=10000, io_context=None, **kw):
        global global_context, global_contexts
        self._fname = fname
        self._mode = mode
        self._opts = kw
        if io_context is not None:
            self.ctx = io_context
        else:
            if numRequests > global_context.numRequests:
                global_context = IOContext(numRequests)
                global_contexts += [global_context]
                #self.log(f'AIO: new context: {len(global_contexts)} now')
            self.ctx = global_context

    def __del__(self):
        if self._file:
            self._file.close()

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
        cb.aio_fildes = self._file.fileno()
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
        cb.aio_fildes = self._file.fileno()
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
        cb = IOCB()
        cb.aio_fildes = self._file.fileno()
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

    async def release(self):
        if self._file:
            self._file.close()
        self._file = None


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

    print(f'write tasks complete')

    #tr = asyncio.create_task(aio.start())
    #await asyncio.sleep(3)
    await aio.release()
    print(f'AIOFile closed')

    await aio.ctx.release()

def run(args=None):
    asyncio.run(arun(args))


if __name__ == "__main__":
    run()
