import asyncio
import pytest
import pytest_asyncio

import os
import sys
import uuid
import json
import binascii

sys.path = ['.'] + sys.path

from aiaio import AIOFile, LineReader, IOContext
from aiaio import aio as aiomodule
from aiaio.aiaio import aenumerate


# cf. https://stackoverflow.com/questions/77242992/pytest-asyncio-howto-await-in-setup-and-teardown
@pytest_asyncio.fixture(loop_scope="class", scope="class", autouse=True)
async def per_class_fixture():
    yield True
    print('per_class_fixture release global context')
    await aiomodule.release_globals()


@pytest.mark.asyncio(loop_scope="class")
class TestCases1():

    async def test_upper(self, per_class_fixture):
        assert 'foo'.upper() == 'FOO'

    async def test_isupper(self):
        assert 'FOO'.isupper()
        assert not 'Foo'.isupper()

    async def test_split(self):
        s = 'hello world'
        assert s.split() == ['hello', 'world']

    async def test_aiofile01(self):
        async with AIOFile('example.txt', 'r+') as aio:

            print('opened example.txt')

            r1 = aio.read(8, offset=0)
            print('first read started', r1)
            res = await r1
            print('read result', res)

            assert len(res) == 8

            data = b'Testa Testb testc\r\n'
            tasks = [ aio.write(i.to_bytes(4) + data, offset=19 + (i+1)*len(data)) for i in range(5000) ]
            results = await asyncio.gather(*tasks)

    async def test_aiofile02(self):
        async with AIOFile('example.txt', 'r+') as aio:

            print('opened example.txt')

            data = b'Testa Testb testc\r\n'
            tasks = [ aio.read(12, offset=19 + (i+1)*len(data)) for i in range(5000) ]
            results = await asyncio.gather(*tasks)
        print(len(results))
        for i in range(len(results)):
            assert int().from_bytes(results[i][0:4]) == i

    async def test_aiofile03(self):
        aio = AIOFile('example.txt', 'r+')

        await aio.open()
        print('opened example.txt')

        data = b'Testa Testb testc\r\n'
        tasks = [ aio.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        print('tasks issued')
        results = await asyncio.gather(*tasks)
        await aio.close()
        del aio

    async def test_aiofile04(self):
        aio = AIOFile('example.txt', 'r+')

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

    async def test_aiofile05(self):
        aio1 = AIOFile('example1.txt', 'w')
        aio2 = AIOFile('example2.txt', 'w')

        await aio1.open()
        await aio2.open()

        data = b'Testa Testb testc\r\n'
        tasks1 = [ aio1.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        tasks2 = [ aio2.write(i.to_bytes(4) + data, offset=(i+1)*len(data)) for i in range(5000) ]
        await aio1.fsync()
        await aio2.fsync()
        print('fsyncs returned!')
        results = await asyncio.gather(*(tasks1 + tasks2))
        print('closing!')
        await aio1.close()
        await aio2.close()
        del aio1
        del aio2

    async def test_aiofile06(self):
        aio1 = AIOFile('example1.txt', 'r+')
        aio2 = AIOFile('example2.txt', 'r+')

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

    async def test_aiofile07(self):
        async with AIOFile('lines.txt', 'r+') as aio:
            lread = LineReader(aio)
            async for line in lread:
                print(f'line {line}')


    async def test_aiofile08(self):
        lines = [ binascii.b2a_base64(os.urandom(1 << 4)) for i in range(100) ]
        async with AIOFile('lines.txt', 'r+') as aio:
            await aio.truncate()
            lens = [len(l) for l in lines]
            tasks = [aio.write(l, offset=sum(lens[0:i])) for i, l in enumerate(lines)]
            await asyncio.gather(*tasks)

        async with AIOFile('lines.txt', 'r+') as aio:
            lread = LineReader(aio)
            async for i, line in aenumerate(lread):
                print(f'line {i}, {line}')
                if i == 100:
                    assert line == b''

    async def test_aiofile09(self):
        lines = [ binascii.b2a_base64(os.urandom(1 << 4)) for i in range(100) ] + [b'Incomplete']
        async with AIOFile('lines.txt', 'r+') as aio:
            await aio.truncate()
            lens = [len(l) for l in lines]
            tasks = [aio.write(l, offset=sum(lens[0:i])) for i, l in enumerate(lines)]
            await asyncio.gather(*tasks)

        async with AIOFile('lines.txt', 'r+') as aio:
            lread = LineReader(aio)
            async for i, line in aenumerate(lread):
                print(f'line {i}, {line}')
                if i == 100:
                    assert line == b'Incomplete'

    async def test_aiofile10(self):
        async with AIOFile('example1.txt', 'r+') as aio:
            await aio.fsync()
        async with AIOFile('example1.txt', 'r+') as aio:
            await aio.fsync()
            await aio.truncate()


@pytest.mark.asyncio(loop_scope="class")
class TestCases2:

    async def test_upper(self, per_class_fixture):
        assert 'foo'.upper() == 'FOO'

    async def test_aiofile01(self):
        async with AIOFile('example.txt', 'r+') as aio:

            r1 = aio.read(8, offset=0)
            print('first read started', r1)
            res = await r1
            print('read result', res)

            assert len(res) == 8

            data = b'Testa Testb testc\r\n'
            tasks = [ aio.write(i.to_bytes(4) + data, offset=19 + (i+1)*len(data)) for i in range(50) ]
            results = await asyncio.gather(*tasks)

    async def test_aiofile02(self):
        async with IOContext(10000, name='Testctx1') as ioctx:
            filenames = [f'example{i:02d}.txt' for i in range(100)]
            files = [AIOFile(fname, 'w+', io_context=ioctx) for fname in filenames]

            async with asyncio.TaskGroup() as tg:
                [tg.create_task(f.open()) for f in files]

            #[print(f) for f in files]
            async with asyncio.TaskGroup() as tg:
                for count in range(10):
                    [tg.create_task(f.write(os.urandom(1<<7), offset=(1<<7)*count)) for f in files]

            async with asyncio.TaskGroup() as tg:
                [tg.create_task(f.close()) for f in files]

            [os.unlink(fn) for fn in filenames]

    async def test_aiofile03(self):
        filenames = [f'example{i:02d}.txt' for i in range(100)]
        ioctx = [IOContext(15) for fname in filenames]
        files = [AIOFile(fname, 'w+', io_context=ioctx) for fname, ioctx in zip(filenames, ioctx)]

        async with asyncio.TaskGroup() as tg:
            [tg.create_task(f.open()) for f in files]

        #[print(f) for f in files]
        async with asyncio.TaskGroup() as tg:
            for count in range(10):
                [tg.create_task(f.write(os.urandom(1<<7), offset=(1<<7)*count)) for f in files]

        async with asyncio.TaskGroup() as tg:
            [tg.create_task(f.close()) for f in files]
            [tg.create_task(i.release()) for i in ioctx]

        [os.unlink(fn) for fn in filenames]


@pytest.mark.asyncio(loop_scope="class")
class TestCases3:

    async def test_upper(self, per_class_fixture):
        assert 'foo'.upper() == 'FOO'

    async def test_aiofile01(self):
        async with AIOFile('example1.txt', 'r+', numRequests=100) as aio1:
            r1 = await aio1.read(8, offset=0)
        async with AIOFile('example2.txt', 'r+', numRequests=10000) as aio2:
            r2 = await aio2.read(8, offset=0)
