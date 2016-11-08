import asyncio
import concurrent.futures
from task.common import *

class BufferedDataProcessor(object):
    def __init__(self, num_worker=1):
        self.logger = logger
        self.loop = get_common_event_loop()
        self.f_combine = None
        self.f_complete = None
        self.data = None
        self.futures = list()
        self.data_lock = asyncio.Lock()
        self.num_worker = num_worker
        self.terminate_flag = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_worker)
        when_terminate(self.terminate)

    def terminate(self):
        self.logger.info("try buffered data processor terminate")
        self.terminate_flag = True
    
    def close(self):
        self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def pop_data(self):
        async with self.data_lock:
            self.logger.debug("pop_data")
            res_data = self.data
            res_futures = self.futures
            fut = asyncio.Future()
            def call_when_done(o):
                self.logger.debug("pop_data_done")
                nonlocal res_futures
                self.logger.debug("pop_data_count: %d", len(res_futures))
                ex = o.exception()
                if ex is None:
                    for fu in res_futures:
                        fu.set_result(True)
                else:
                    for fu in res_futures:
                        fu.set_exception(ex)
            fut.add_done_callback(call_when_done)
            self.data = None
            self.futures = list()
        return res_data, fut
    
    async def proc_data(self, data):
        if data is None:
            return
        async with self.data_lock:
            self.logger.debug("proc_data")
            if self.data is None:
                self.data = data
            else:
                if self.f_combine is not None:
                    self.data = self.f_combine(self.data, data)
            fut = asyncio.Future()
            self.futures.append(fut)
        fut.add_done_callback(lambda o: self.logger.debug("proc_data_done"))
        return await asyncio.wait_for(fut, None)

    def processor(self, func):
        self.f_complete = func
        return func

    def on_combine(self, func):
        self.f_combine = func
        return func

    def run(self):
        c = asyncio.gather(*[self.worker(i) for i in range(self.num_worker)])
        return asyncio.ensure_future(c, loop=self.loop)

    @async_count
    async def worker(self, i):
        while True:
            data, fut = await self.pop_data()
            if data is None:
                if self.terminate_flag is True:
                    self.logger.info("buffer worker(%d) terminated", i)
                    return
                self.logger.debug("worker: no data available, wait...")
                fut.set_result(True)
                await asyncio.sleep(1)
                continue
            try:
                self.logger.debug("worker: process data")
                if self.f_complete is not None:
                    res = await wait_concurrent(self.loop, self.executor, self.f_complete, data)
                    fut.set_result(res)
            except Exception as e:
                fut.set_exception(e)
