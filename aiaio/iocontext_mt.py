from .iocontext_task import IOContext
import threading
import asyncio

from .iocontext_task import IO_EVENT, TIMESPEC, SIGSET, libaio, addressof


class IOContextMT(IOContext):

    _thread = None
    _thread_stop = False

    def __init__(self, numRequests=100, **kw):
        super().__init__(numRequests=numRequests, **kw)
        self._thread_empty = threading.Event()
        self._thread_stopped = asyncio.Event()

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

        while not self._thread_stop or len(self._readsDict) > 0:
            self.log(f'io_pgetevents...')
            if len(self._readsDict) == 0:
                timeout.tv_sec = 0
            else:
                timeout.tv_sec = 1
            rc = libaio.io_pgetevents(self._ctx, 1, nevents, events, timeout, sigmask)
            self.log(f'io_pgetevents = {rc}')
            if rc > 0:
                evlist = [(addressof(events[i].obj.contents), events[i].res, events[i].res2) for i in range(rc)]
                self.notify_cbcomplete_list(evlist)
            elif rc < 0:
                self.log(f'Error io_pgetevents: {rc} {getename(-rc)}')
                raise OSError(f'io_pgetevents: {getename(-rc)}')
            elif rc == 0:
                if len(self._readsDict) == 0 and not self._thread_stop:
                    self.log(f'{rc}=0 and no pending: go to sleep')
                    self._thread_empty.clear()
                    self._thread_empty.wait()
        if self._verbose:
            self.log(f'task io_pgetevents exiting')
        self.notify_thread_exit()
        if self._verbose:
            self.log(f'task io_pgetevents exiting(II)')

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

    async def notify_thread_exit_task(self):
        self.log(f'set thread stopped event')
        self._thread_stopped.set()
        return 0

    def notify_thread_exit(self):
         future = asyncio.run_coroutine_threadsafe(self.notify_thread_exit_task(), loop=self._loop)
         res = future.result()
         assert res == 0
         return res

    async def waitForThread(self):
        while self._thread.is_alive():
            if self._verbose:
                self.log(f'wait for thread...')
            self._thread_empty.set()
            await asyncio.sleep(1e-6)
        self._thread.join()
        if self._verbose:
            self.log(f'thread joined')

    def releaseThread(self):
        if self._verbose:
            self.log(f'releaseThread')
        self._thread_stop = True
        self._thread_empty.set()

    async def awaitThread(self):
        if self._thread is not None:
            print('wait for thread stop')
            await self._thread_stopped.wait()
            print('Thread has stopped')
            await self.waitForThread()
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

    def _io_submit_handler(self, cb):
        if self._verbose:
            self.log(f'set event')
        self._thread_empty.set()


global_context_mt = IOContextMT(numRequests=1000)
