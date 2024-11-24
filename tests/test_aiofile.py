import asyncio
import pytest

import os
import sys
import uuid
import json
import binascii

sys.path = ['.'] + sys.path

async def aenumerate(asequence, start=0):
    """Asynchronously enumerate an async iterator from a given start value"""
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1

from pyaio import pyaio

@pytest.mark.asyncio(loop_scope="class")
class TestCases1:

    async def test_upper(self):
        assert 'foo'.upper() == 'FOO'

    async def test_isupper(self):
        assert 'FOO'.isupper()
        assert not 'Foo'.isupper()

    async def test_split(self):
        s = 'hello world'
        assert s.split() == ['hello', 'world']

    async def test_aiofile1(self):
        async with pyaio.AIOFile('example.txt', 'r+') as aio:

            print('opened example.txt')

            r1 = aio.read(8, offset=0)
            print('first read started', r1)
            res = await r1
            print('read result', res)

            assert len(res) == 8

            data = b'Testa Testb testc\r\n'
            tasks = [ aio.write(i.to_bytes(4) + data, offset=19 + (i+1)*len(data)) for i in range(5000) ]
            results = await asyncio.gather(*tasks)

    async def test_aiofile2(self):
        async with pyaio.AIOFile('example.txt', 'r+') as aio:

            print('opened example.txt')

            data = b'Testa Testb testc\r\n'
            tasks = [ aio.read(12, offset=19 + (i+1)*len(data)) for i in range(5000) ]
            results = await asyncio.gather(*tasks)
        print(len(results))
        for i in range(len(results)):
            assert int().from_bytes(results[i][0:4]) == i

    async def test_aiofile3(self):
        aio = pyaio.AIOFile('example.txt', 'r+')

        await aio.open()
        print('opened example.txt')

        data = b'Testa Testb testc\r\n'
        tasks = [ aio.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        print('tasks issued')
        results = await asyncio.gather(*tasks)
        await aio.close()
        del aio

    async def test_aiofile4(self):
        aio = pyaio.AIOFile('example.txt', 'r+')

        await aio.open()
        print('opened example.txt')

        data = b'Testa Testb testc\r\n'
        tasks = [ aio.read(12, offset=(i+1)*len(data)) for i in range(5000) ]
        print('tasks issued')
        results = await asyncio.gather(*tasks)
        await aio.close()
        del aio

        print(len(results))
        for i in range(len(results)):
            assert int().from_bytes(results[i][0:4]) == i

    async def test_aiofile5(self):
        aio1 = pyaio.AIOFile('example1.txt', 'w')
        aio2 = pyaio.AIOFile('example2.txt', 'w')

        await aio1.open()
        await aio2.open()

        data = b'Testa Testb testc\r\n'
        tasks1 = [ aio1.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        tasks2 = [ aio2.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        await aio1.fsync()
        await aio2.fsync()
        print('fsyncs returned!')
        results = await asyncio.gather(*(tasks1 + tasks2))
        await aio1.close()
        await aio2.close()
        del aio1
        del aio2

    async def test_aiofile6(self):
        aio1 = pyaio.AIOFile('example1.txt', 'r+')
        aio2 = pyaio.AIOFile('example2.txt', 'r+')

        await aio1.open()
        await aio2.open()
        print('opened example.txt')

        data = b'Testa Testb testc\r\n'
        tasks1 = [ aio1.read(12, offset=(i+1)*len(data)) for i in range(5000) ]
        tasks2 = [ aio2.read(12, offset=(i+1)*len(data)) for i in range(5000) ]
        results = await asyncio.gather(*(tasks1 + tasks2))
        await aio1.close()
        await aio2.close()
        del aio1
        del aio2

        print(len(results))
        for i in range(len(results)):
            assert int().from_bytes(results[i][0:4]) == i % 5000

    async def test_aiofile7(self):
        async with pyaio.AIOFile('lines.txt', 'r+') as aio:
            lread = pyaio.LineReader(aio)
            async for line in lread:
                print(f'line {line}')


    async def test_aiofile8(self):
        lines = [ binascii.b2a_base64(os.urandom(1 << 4)) for i in range(100) ]
        async with pyaio.AIOFile('lines.txt', 'r+') as aio:
            await aio.truncate()
            lens = [len(l) for l in lines]
            tasks = [aio.write(l, offset=sum(lens[0:i])) for i, l in enumerate(lines)]
            await asyncio.gather(*tasks)

        async with pyaio.AIOFile('lines.txt', 'r+') as aio:
            lread = pyaio.LineReader(aio)
            async for i, line in aenumerate(lread):
                print(f'line {i}, {line}')
                if i == 100:
                    assert line == b''

    async def test_aiofile9(self):
        lines = [ binascii.b2a_base64(os.urandom(1 << 4)) for i in range(100) ] + [b'Incomplete']
        async with pyaio.AIOFile('lines.txt', 'r+') as aio:
            await aio.truncate()
            lens = [len(l) for l in lines]
            tasks = [aio.write(l, offset=sum(lens[0:i])) for i, l in enumerate(lines)]
            await asyncio.gather(*tasks)

        async with pyaio.AIOFile('lines.txt', 'r+') as aio:
            lread = pyaio.LineReader(aio)
            async for i, line in aenumerate(lread):
                print(f'line {i}, {line}')
                if i == 100:
                    assert line == b'Incomplete'
