import asyncio
import task.executor as texec
import logging

import random

logging.basicConfig(level=logging.DEBUG)

loop = asyncio.get_event_loop()
tx = texec.TaskExecutor.load("conf/config.yaml", loop=loop)


@tx.arg_register("hello")
def hello():
    print("Hello World!")
    if random.randint(0, 9) < 2:
        raise Exception("Boom")

async def add_hello_task():
    ans = await asyncio.gather(*(tx.task_create("hello", str(i), group=None, options=texec.TaskExecutor.arg_opts) for i in range(100)))
    logging.debug(ans)

loop.run_until_complete(add_hello_task())
loop.run_until_complete(tx.run())
tx.close()
