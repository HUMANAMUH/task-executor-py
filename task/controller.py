import json
import aiohttp
import yaml
import async_timeout
from task.common import *
from task.timeutil import *

class TaskController(object):
    def __init__(self, config, session):
        self.logger = logger
        self.loop = session._loop
        self.config = config
        self.server_url = config["server_url"]
        self.request_timeout = config["request_timeout"]
        self.pool = config["pool"]
        self.try_limit = config["try_limit"]
        self.task_timeout = config["task_timeout"]
        self.retry_interval = config["retry_interval"]
        self.retry_limit = config["retry_limit"]
        self.log_level = config["log_level"]
        self.log_file = config["log_file"]
        self.log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.session = session

        fh = logging.FileHandler(self.log_file)
        fh.setLevel(self.log_level)
        fh.setFormatter(logging.Formatter(self.log_format))
        logger.addHandler(fh)

    @staticmethod
    async def load(config_file, multi_process=False):
        """
        load executor from a config file
        """
        session = await aiohttp.ClientSession(loop=get_common_event_loop())
        with open(config_file, "r") as fobj:
            return TaskController(yaml.load(fobj.read())["task"], session)

    async def close(self):
        """
        shutdown this executor
        """
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def post_json(self, path, obj):
        """
        post a json to server, return async json result
        """
        with async_timeout.timeout(self.request_timeout):
            url = "%s%s" % (self.server_url, path)
            data = json.dumps(obj).encode("utf-8")
            headers = {'content-type': 'application/json'}
            async with self.session.post(url, data=data, headers=headers) as response:
                if response.status != 200:
                    raise UnexpectedResponceCode(response.status)
                txt = await response.text()
                logger.debug("%s: %s", url, txt)
                return json.loads(txt)

    @async_count
    @with_retry(limit=5)
    async def task_schedule(self, task_type, key, scheduled_at, group=None, options={}):
        """
        create new task scheduled at a specified time
        @scheduled_at datetime for task to start
        """
        obj = {
            "pool": self.pool,
            "type": task_type,
            "key": str(key),
            "group": str(group) if group is not None else None,
            "options": json.dumps(options),
            "scheduledAt": datetime_to_timestamp(scheduled_at),
            "tryLimit": self.try_limit,
            "timeout": self.task_timeout
        }
        return await self.post_json("/task/create", obj)

    @async_count
    @with_retry(limit=5)
    async def task_create(self, task_type, key, group=None, options={}):
        """
        create new task which scheduled immediately
        """
        obj = {
            "pool": self.pool,
            "type": task_type,
            "key": str(key),
            "group": str(group) if group is not None else None,
            "options": json.dumps(options),
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
    @with_retry(limit=5)
    async def last_task(self, task_type):
        """
        get last task of specified task_type
        """
        obj = {
            "pool": self.pool,
            "type": task_type
        }
        ans = await self.post_json("/task/last", obj)
        if ans is not None:
            ans["scheduledAt"] = timestamp_to_datetime(ans["scheduledAt"])
        return ans

    @async_count
    @with_retry(limit=5)
    async def group_last(self, group):
        """
        get last task of specified task group
        """
        obj = {
            "pool": self.pool,
            "group": group
        }
        ans = await self.post_json("/group/last", obj)
        if ans is not None:
            ans["scheduledAt"] = timestamp_to_datetime(ans["scheduledAt"])
        return ans