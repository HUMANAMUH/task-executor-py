"""
Task Executor
"""

import json
import functools
import logging
import traceback
import inspect
import concurrent.futures
import asyncio
import aiohttp
import async_timeout
import yaml

def async_count(crt_f):
    """
    count of async calls, if terminate_falg is True, it will wait all async call finish to exit
    """
    @functools.wraps(crt_f)
    async def wrapped(self, *args, **kwargs):
        """
        wrapped async call
        will automatically increase async count when start, and dcrease when exit
        """
        self.ref_cnt += 1
        logging.debug("ref_cnt: %d", self.ref_cnt)
        try:
            return await crt_f(self, *args, **kwargs)
        finally:
            self.ref_cnt -= 1
            logging.debug("ref_cnt: %d", self.ref_cnt)
            if self.ref_cnt == 0 and self.terminate_flag is True:
                self.close()
    return wrapped

def with_retry(limit=None, interval=None):
    def wrapper(crt_f):
        @functools.wraps(crt_f)
        async def wrapped(*args, **kwargs):
            try_count = 0
            exec_self = args[0] if len(args) > 0 else None
            retry_limit = exec_self.retry_limit if exec_self.retry_limit is not None else limit
            retry_interval = \
                exec_self.retry_interval if exec_self.retry_interval is not None else interval
            while limit is None or try_count < retry_limit:
                if exec_self is not None and exec_self.terminate_flag is True:
                    return None
                try_count += 1
                try:
                    if try_count > 1:
                        logging.debug("retry: %d", try_count)
                    return await crt_f(*args, **kwargs)
                except OSError as ex:
                    logging.warning("OSError: %s", ex)
                    await asyncio.sleep(retry_interval)
                except RuntimeError as ex:
                    logging.error(ex)
                    err_trace = traceback.format_exc()
                    logging.error(err_trace)
                    return None
                except:
                    err_trace = traceback.format_exc()
                    logging.error(err_trace)
                    await asyncio.sleep(retry_interval)
        return wrapped
    return wrapper


class TaskExecutor(object):
    """
    task executor
    """

    def __init__(self, config, loop=None):
        self._task_mapping = dict()
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.config = config
        self.server_url = config["server_url"]
        self.request_timeout = config["request_timeout"]
        self.pool = config["pool"]
        self.try_limit = config["try_limit"]
        self.task_timeout = config["task_timeout"]
        self.num_worker = config["num_worker"]
        self.exit_when_done = config["exit_when_done"]
        self.retry_interval = config["retry_interval"]
        self.retry_limit = config["retry_limit"]
        self.terminate_flag = False
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.num_worker)
        self.loop.add_signal_handler(2, self.terminate)
        self.ref_cnt = 0

    @staticmethod
    def load(config_file, loop=None):
        """
        load executor from a config file
        """
        with open(config_file, "r") as fobj:
            return TaskExecutor(yaml.load(fobj.read()), loop=loop)

    def register(self, task_type):
        """
        register function which will process the task in specified task_type
        """
        def wrapper(func):
            """
            simple wrapper which add function func to task_mapping
            """
            self._task_mapping[task_type] = func
            return func

        return wrapper

    def terminate(self):
        """
        terminate workers, which will wait running task being done
        """
        # TODO: block/async logic
        self.terminate_flag = True

    def close(self):
        """
        shutdown this executor
        """
        self.session.close()
        self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def run(self):
        """
        run all workers, async return when all workers exited
        """
        await asyncio.gather(*[self.worker(i) for i in range(self.num_worker)])

    async def post_json(self, path, obj):
        """
        post a json to server, return async json result
        """
        with async_timeout.timeout(self.request_timeout):
            url = "%s%s" % (self.server_url, path)
            data = json.dumps(obj).encode("utf-8")
            headers = {'content-type': 'application/json'}
            async with self.session.post(url, data=data, headers=headers) as response:
                return json.loads(await response.text())


    @async_count
    @with_retry(limit=5)
    async def task_schedule(self, task_type, key, datetime, *args, **kwargs):
        """
        create new task scheduled at a specified time
        """
        obj = {
            "pool": self.pool,
            "type": task_type,
            "key": str(key),
            "options": json.dumps({"args": args, "kwargs": kwargs}),
            "scheduledTime": datetime,
            "tryLimit": self.try_limit,
            "timeout": self.task_timeout
        }
        return await self.post_json("/task/create", obj)

    @async_count
    @with_retry(limit=5)
    async def task_create(self, task_type, key, *args, **kwargs):
        """
        create new task which scheduled immediately
        """
        obj = {
            "pool": self.pool,
            "type": task_type,
            "key": str(key),
            "options": json.dumps({"args": args, "kwargs": kwargs}),
            "tryLimit": self.try_limit,
            "timeout": self.task_timeout
        }
        return await self.post_json("/task/create", obj)

    @async_count
    @with_retry(limit=3)
    async def task_fetch(self):
        """
        fetch task from task queue
        """
        obj = {
            "pool": self.pool,
            "limit": 1
        }
        return await self.post_json("/task/start", obj)

    @async_count
    @with_retry(limit=3)
    async def task_delete(self, task_id):
        """
        delete task from task manager
        """
        obj = {
            "id": task_id
        }
        return await self.post_json("/task/delete", obj)

    @async_count
    @with_retry(limit=5)
    async def task_success(self, task_id):
        """
        succeed a specified task when task has succefully done
        """
        obj = {
            "id": task_id
        }
        return await self.post_json("/task/success", obj)

    @async_count
    @with_retry(limit=5)
    async def task_fail(self, task_id, log, delay=0):
        """
        fail a specified task when task has failed
        """
        obj = {
            "id": task_id,
            "log": log,
            "delay": delay
        }
        return await self.post_json("/task/fail", obj)

    @async_count
    @with_retry(limit=5)
    async def task_block(self, task_id):
        """
        block a pending task
        """
        obj = {
            "id": task_id
        }
        return await self.post_json("/task/block", obj)

    @async_count
    @with_retry(limit=5)
    async def task_unblock(self, task_id):
        """
        unblock a blocked task
        """
        obj = {
            "id": task_id
        }
        return await self.post_json("/task/unblock", obj)

    @async_count
    @with_retry(limit=5)
    async def task_recover(self, task_id):
        """
        recover a failed task
        """
        obj = {
            "id": task_id
        }
        return await self.post_json("/task/recover", obj)

    @async_count
    @with_retry(limit=5)
    async def pool_recover(self):
        """
        recover this pool's failed taskes
        """
        obj = {
            "pool": self.pool
        }
        return await self.post_json("/pool/recover", obj)

    @async_count
    async def worker(self, i):
        """
        start worker i on executors event loop
        """
        logging.info("worker(%d) started", i)
        while True:
            if self.terminate_flag is True:
                logging.info("worker(%d) terminated", i)
                return
            tasks = await self.task_fetch()
            if tasks is None:
                continue
            if len(tasks) == 0:
                if self.exit_when_done:
                    logging.info("worker(%d) done. exit", i)
                    return
                else:
                    logging.info("task all done, waiting")
                    await asyncio.sleep(5)
            for task in tasks:
                if task["type"] in self._task_mapping:
                    func = self._task_mapping[task["type"]]
                    opts = json.loads(task["options"])
                    try:
                        action = functools.partial(func, *opts["args"], **opts["kwargs"])
                        fut = self.loop.run_in_executor(self.executor, action)
                        func_res = await asyncio.wait_for(fut, None)
                        if inspect.isawaitable(func_res):
                            res = await func_rec
                            logging.debug("res: %s", res)
                        await self.task_success(task["id"])
                    except:
                        err_trace = traceback.format_exc()
                        logging.error(err_trace)
                        await self.task_fail(task["id"], err_trace)
                else:
                    logging.warning("Unknown task type: %s", repr(task))
                    await self.task_fail(task["id"], traceback.format_exc())
