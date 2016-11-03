import asyncio
import task.executor as texec
from task.timeutil import *
import logging

import random

logging.basicConfig(level=logging.DEBUG)

loop = asyncio.get_event_loop()
tx = texec.TaskExecutor.load("conf/config.yaml", loop=loop)


@tx.register("hello")
def hello():
    print("Hello World!")
    if random.randint(0, 9) < 2:
        raise Exception("Boom")

async def add_hello_task():
    ans = await asyncio.gather(*(tx.task_create("hello", str(i), group=None, options=texec.TaskExecutor.arg_opts) for i in range(100)))
    logging.debug(ans)

with timer(lambda t: logging.debug("sleep time spend %.3f", t)):
    time.sleep(3)

loop.run_until_complete(add_hello_task())
loop.run_until_complete(tx.run())
tx.close()
