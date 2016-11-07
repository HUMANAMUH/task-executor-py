"""
Task Executor
"""

import json
import traceback
import inspect
import concurrent.futures
import asyncio
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

    def __init__(self, config, loop=None, multi_process=False):
        super().__init__(config, loop)
        self._task_mapping = dict()
        self._expand_arg_opts = dict()
        self.num_worker = config["num_worker"]
        self.exit_when_done = config["exit_when_done"]
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.num_worker * 3) \
            if multi_process is False \
            else concurrent.futures.ProcessPoolExecutor(max_workers=self.num_worker * 3)

    @staticmethod
    def load(config_file, loop=None, multi_process=False):
        """
        load executor from a config file
        """
        with open(config_file, "r") as fobj:
            return TaskExecutor(yaml.load(fobj.read())["task"], loop=loop, multi_process=multi_process)

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
                        self.logger.debug("res: %s", res)
                        await self.task_success(task["id"])
                    except:
                        err_trace = traceback.format_exc()
                        self.logger.error(err_trace)
                        await self.task_fail(task["id"], err_trace)
                else:
                    self.logger.warning("Unknown task type: %s", repr(task))
                    await self.task_fail(task["id"], traceback.format_exc())
