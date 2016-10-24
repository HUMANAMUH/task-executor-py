import asyncio
import task.executor as texec
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
    await asyncio.gather(*(tx.task_create("hello", str(i)) for i in range(10)))

loop.run_until_complete(add_hello_task())
loop.run_until_complete(tx.run())
