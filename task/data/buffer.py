import asyncio
import concurrent.futures
from task.common import *

class BufferedDataProcessor(object):
    def __init__(self, num_worker=1):
        self.logger = logger
        self.combine = None
        self.on_complete = None
        self.data = None
        self.futures = list()
        self.data_lock = asyncio.Lock()
        self.num_worker = num_worker
        self.terminate_flag = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_worker)
        self.loop.add_signal_handler(2, self.terminate)

    def terminate(self):
        self.logger.info("try buffered data processor terminate")
        self.terminate_flag = True
    
    def close(self):
        self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def pop_data(self):
        with self.data_lock:
            res_data = self.data
            res_futures = self.futures
            fut = asyncio.Future()
            def call_when_done(o):
                nonlocal res_futures
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
    
    def add_data(self, data):
        if data is None:
            return
        with self.data_lock:
            if self.data is None:
                self.data = data
            else:
                self.data = self.combine(self.data, data)
            fut = asyncio.Future()
            self.futures.append(fut)
            return fut

    def on_complete(self, func):
        self.on_complete = func
        return func

    def on_combine(self, func):
        self.on_combine = func
        return func

    async def run(self):
        await asyncio.gather(*[self.worker(i) for i in range(self.num_worker)])

    async def worker(self, i):
        while True:
            data, fut = self.pop_data()
            if data is None:
                fut.set_result(True)
                await asyncio.sleep(1)
                continue
            try:
                await wait_concurrent(self.loop, self.executor, self.on_complete, data)
            except Exception as e:
                fut.set_exception(e)
