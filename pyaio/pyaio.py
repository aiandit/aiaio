from ctypes import c_int, c_uint8, c_int32, c_int64, c_uint64, c_voidp
from ctypes import CDLL, pointer, POINTER, Structure, CFUNCTYPE
import errno
import time
import sys
import asyncio

from .aio import AIO


async def aenumerate(asequence, start=0):
    """Asynchronously enumerate an async iterator from a given start value"""
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1


class AIOFile:

    def __init__(self, name, mode, encoding=None, **kw):
        self._file = AIO(name, mode, **kw)
        self.encoding = encoding

    def __del__(self):
        if self.fileno() != -1:
            raise BaseException(f'{self} was not properly closed: {self.fileno()}!')

    def __str__(self):
        return f'AIOFile({self._file}, {self.encoding})'

    def fileno(self):
        return self._file.fileno()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, *args):
        return await self.close()

    async def open(self):
        await self._file.start()

    async def close(self):
        await self._file.release()

    async def write(self, data, offset=0):
        if self.encoding:
            data = data.encode(self.encoding)
        return await self._file.write(data, offset=offset)

    async def read(self, n, offset=0):
        data = await self._file.read(n, offset=offset)
        if self.encoding:
            data = data.decode(self.encoding)
        return data

    async def fsync(self, offset=0):
        return await self._file.fsync()

    async def fdsync(self, offset=0):
        return await self._file.fdsync()

    async def truncate(self):
        return self._file._file.truncate()


class LineReader:
    def __init__(self, file):
        self._file = file
        self._chunksize = 2048
        self._chunk = None
        self._choffs = 0
        self._offs = 0
        self._lastLine = '' if file.encoding else b''
        self._sep = '\n' if file.encoding else b'\n'
        self._eof = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._chunk is None:
                if self._eof:
                    raise StopAsyncIteration()
                self._chunk = await self._file.read(self._chunksize, offset=self._choffs)
                self._choffs += len(self._chunk)
                if len(self._chunk) < self._chunksize:
                    self._eof = True
                self._lines = (self._lastLine + self._chunk).split(self._sep)
                self._offs = 0
            if self._offs < len(self._lines)-1:
                self._offs += 1
                return self._lines[self._offs-1] + self._sep
            else:
                self._lastLine = self._lines[-1]
                self._chunk = None
                if self._eof:
                    return self._lastLine



def mkparser(parser=None):
    from . import __version__
    import argparse
    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument('input', metavar='file', type=str)
    parser.add_argument('-o', '--output', metavar='file', type=str)
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

    infile = args.input
    outfile = args.output if args.output else args.input

    async with AIOFile(infile, 'r+', numRequests=5001) as aio:
        tasks = [aio.read(8, offset=i*8) for i in range(5000)]
        reads = await asyncio.gather(*tasks)

    lns = [len(r) for r in reads]

    async with AIOFile(outfile, 'w+', numRequests=5001) as aio:
        tasks = [aio.write(r, offset=sum(lns[0:i])) for i, r in enumerate(reads)]
        writes = await asyncio.gather(*tasks)


def run(args=None):
    asyncio.run(arun(args))


if __name__ == "__main__":
    run()
