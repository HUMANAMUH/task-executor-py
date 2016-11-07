import asyncio
import task.executor as texec
from task.data.buffer import BufferedDataProcessor
from task.timeutil import *
import logging
from task import event_loop

import random

logging.basicConfig(level=logging.DEBUG)

tx = texec.TaskExecutor.load("conf/config.yaml")

databuffer = BufferedDataProcessor()

@databuffer.on_combine
def test_combine(a, b):
    return a + b

import time

@databuffer.processor
def test_on_complete(a):
    time.sleep(1)
    logging.info(a)

@tx.register("hello")
async def hello():
    print("Hello World!")
    ans = databuffer.proc_data(1)
    if random.randint(0, 9) < 2:
        raise Exception("Boom")
    return ans

async def add_hello_task():
    ans = await asyncio.gather(*(tx.task_create("hello", str(i), group=None, options=texec.TaskExecutor.arg_opts) for i in range(100)))
    logging.debug(ans)

with timer(lambda t: logging.debug("sleep time spend %.3f", t)):
    time.sleep(3)

a = tx.run()
b = databuffer.run()
event_loop.run_until_complete(add_hello_task())
event_loop.run_until_complete(a)
event_loop.run_until_complete(b)
databuffer.close()
tx.close()
