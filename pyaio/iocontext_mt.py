from .iocontext_task import IOContext
import threading
import asyncio

from .iocontext_task import IO_EVENT, TIMESPEC, SIGSET, libaio, addressof


class IOContextMT(IOContext):

    _thread = None
    _thread_stop = False

    def __init__(self, numRequests=100, **kw):
        super().__init__(numRequests=numRequests, **kw)

    def __str__(self):
        return f'IOContextMT({self._name}, n={self.numRequests})'

    def __repr__(self):
        return f'IOContextMT({self._name}, n={self.numRequests}, {bytes(self._ctx).hex()})'

    def run_getevents_loop1(self):
        nevents = self._maxbatch
        events = (IO_EVENT * nevents)()
        timeout = TIMESPEC()
        timeout.tv_sec = 1
        sigmask = SIGSET()
        libaio.sigfillset(sigmask)

        while not self._thread_stop:
            self.log(f'io_pgetevents...')
            rc = libaio.io_pgetevents(self._ctx, 1, nevents, events, timeout, sigmask)
            self.log(f'io_pgetevents = {rc}')
            if rc > 0:
                evlist = [(addressof(events[i].obj.contents), events[i].res, events[i].res2) for i in range(rc)]
                self.notify_cbcomplete_list(evlist)
            elif rc < 0:
                self.log(f'Error io_pgetevents: {rc} {getename(-rc)}')
                raise OSError(f'io_pgetevents: {getename(-rc)}')
            elif rc == 0:
                pass
        if self._verbose:
            self.log(f'task io_pgetevents exiting')

    def run_getevents_loop(self):
        try:
            self.run_getevents_loop1()
        except Exception as ex:
            self.log(f'task pgetevents raise exception {ex}')
            raise ex

    def notify_cbcomplete_list(self, evlist):
         future = asyncio.run_coroutine_threadsafe(self.notify_cbcomplete_list_task(evlist), loop=self._loop)
         res = future.result()
         assert res == 0
         return res

    async def start_aio_suspend_loop(self):
        self._loop = asyncio.get_event_loop()
        if self._thread is None:
            if self._verbose:
                self.log(f'io_pgetevents thread starting...')
            self._thread_stop = False
            self._thread = threading.Thread(target=self.run_getevents_loop)
            self._thread.start()
            if self._verbose:
                self.log(f'io_pgetevents thread started')

    def waitForThread(self):
        print(f'wait for thread')
        self._thread.join()
        print(f'thread joined')

    def releaseThread(self):
        print(f'releaseThread')
        self._thread_stop = True

    async def awaitThread(self):
        if self._thread is not None:
            await asyncio.to_thread(self.waitForThread)
        self._thread = None
        self._loop = None

    async def release(self):
        self.releaseThread()
        await self.awaitThread()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        return await self.release()


global_context_mt = IOContextMT(numRequests=1000)
