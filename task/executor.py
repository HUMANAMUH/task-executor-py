"""
Task Executor
"""

import json
import traceback
import inspect
import concurrent.futures
import asyncio
import aiohttp
import yaml
from task.controller import TaskController
from task.common import *


class TaskExecutor(TaskController):
    """
    task executor
    """
    arg_opts = {
        "args": [],
        "kwargs": {}
    }

    def __init__(self, config, session, multi_process=False):
        super().__init__(config, session)
        self._task_mapping = dict()
        self._expand_arg_opts = dict()
        self.num_worker = config["num_worker"]
        self.exit_when_done = config["exit_when_done"]
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.num_worker * 3) \
            if multi_process is False \
            else concurrent.futures.ProcessPoolExecutor(max_workers=self.num_worker * 3)
        self.terminate_flag = False
        when_terminate(self.terminate)

    def terminate(self):
        """
        terminate workers, which will wait running task being done
        """
        self.logger.info("try task controller terminate")
        self.terminate_flag = True

    @staticmethod
    async def load(config_file, multi_process=False):
        """
        load executor from a config file
        """
        session = await aiohttp.ClientSession(loop=get_common_event_loop())
        with open(config_file, "r") as fobj:
            return TaskExecutor(yaml.load(fobj.read())["task"], session, multi_process=multi_process)

    def register(self, task_type, expand_param=True):
        """
        register function which will process the task in specified task_type
        """
        def wrapper(func):
            """
            simple wrapper which add function func to task_mapping
            """
            self._task_mapping[task_type] = func
            self._expand_arg_opts[task_type] = expand_param
            return func

        return wrapper

    async def close(self):
        """
        shutdown this executor
        """
        await self.session.close()
        self.executor.shutdown(wait=False)

    async def __aenter__(self):
        return self

    async def __exit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def run(self):
        """
        run all workers, async return when all workers exited
        """
        c = asyncio.gather(*[self.worker(i) for i in range(self.num_worker)])
        return asyncio.ensure_future(c, loop=self.loop)

    @async_count
    async def worker(self, i):
        """
        start worker i on executors event loop
        """
        self.logger.info("worker(%d) started", i)
        while True:
            if self.terminate_flag is True:
                self.logger.info("worker(%d) terminated", i)
                return
            tasks = await self.task_fetch()
            if tasks is None:
                continue
            if len(tasks) == 0:
                if self.exit_when_done:
                    self.logger.info("worker(%d) done. exit", i)
                    return
                else:
                    self.logger.info("task all done, waiting")
                    await asyncio.sleep(5)
            for task in tasks:
                if task["type"] in self._task_mapping:
                    func = self._task_mapping[task["type"]]
                    do_expand = self._expand_arg_opts.get(task["type"], False)
                    opts = json.loads(task["options"])
                    try:
                        if do_expand is True:
                            res = await wait_concurrent(self.loop, self.executor, func, *opts.get("args", []), **opts.get("kwargs", {}))
                        else:
                            res = await wait_concurrent(self.loop, self.executor, func, opts)
                        if inspect.isawaitable(res):
                            ref_count_incr("extra_job")
                            async def extra_job():
                                try:
                                    in_res = await res
                                    self.logger.debug("res: %s", in_res)
                                    await self.task_success(task["id"])
                                except:
                                    err_trace = traceback.format_exc()
                                    self.logger.error(err_trace)
                                    await self.task_fail(task["id"], err_trace)
                                finally:
                                    ref_count_decr("extra_job")
                            asyncio.ensure_future(extra_job(), loop=self.loop)
                        else:
                            await self.task_success(task["id"])
                    except:
                        err_trace = traceback.format_exc()
                        self.logger.error(err_trace)
                        await self.task_fail(task["id"], err_trace)
                else:
                    self.logger.warning("Unknown task type: %s", repr(task))
                    await self.task_fail(task["id"], traceback.format_exc())
