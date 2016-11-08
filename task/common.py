import logging
import traceback
import asyncio
import inspect
import functools
from contextlib import contextmanager

logger = logging.getLogger("task-executor-py")
logger.propagate = False

__termiate_future = asyncio.Future()

def __terminate_call_back():
    if not __termiate_future.done():
        __termiate_future.set_result(True)

def when_terminate(func):
    __termiate_future.add_done_callback(lambda o: func())

__loop = asyncio.get_event_loop()
__loop.add_signal_handler(2, __terminate_call_back)

def get_common_event_loop():
    return __loop

def run_until(*coroutines):
    __loop.run_until_complete(asyncio.gather(*coroutines))

class UnexpectedResponceCode(Exception):
    def __init__(self, code):
        super().__init__("Unexpected responce code: %d" % code)

__ref_cnt = 0
__all_task_done = asyncio.Future()

def __try_all_task_done(*args):
    global __all_task_done
    if __ref_cnt == 0 and __termiate_future.done() and not __all_task_done.done():
        __all_task_done.set_result(True)

__termiate_future.add_done_callback(__try_all_task_done)

def wait_all_task_done():
    return __all_task_done

def ref_count_incr(key):
    global __ref_cnt
    __ref_cnt += 1
    logger.debug("ref_count_incr@%s: %d", key, __ref_cnt)

def ref_count_decr(key):
    global __ref_cnt
    __ref_cnt -= 1
    logger.debug("ref_count_decr@%s: %d", key, __ref_cnt)

def async_count(crt_f):
    """
    count of async calls, if terminate_falg is True, it will wait all async call finish to exit
    """
    @functools.wraps(crt_f)
    async def wrapped(*args, **kwargs):
        """
        wrapped async call
        will automatically increase async count when start, and dcrease when exit
        """
        with async_context():
            return await crt_f(*args, **kwargs)
    return wrapped

@contextmanager
def async_context():
    """
    a async_context will manage ref_count
    async_context can not break using ctrl-c
    we have to wait all async_context done
    """
    global __ref_cnt
    global __all_task_done
    __ref_cnt += 1
    logger.debug("ref_cnt: %d", __ref_cnt)
    yield
    __ref_cnt -= 1
    logger.debug("ref_cnt: %d", __ref_cnt)
    if __ref_cnt == 0 and __termiate_future.done() and not __all_task_done.done():
        __all_task_done.set_result(True)

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
                        logger.debug("retry: %d", try_count)
                    return await crt_f(*args, **kwargs)
                except OSError as ex:
                    logger.warning("OSError: %s", ex)
                    await asyncio.sleep(retry_interval)
                except UnexpectedResponceCode as ex:
                    logger.warning(ex)
                    await asyncio.sleep(retry_interval)
                except RuntimeError as ex:
                    logger.error(ex)
                    err_trace = traceback.format_exc()
                    logger.error(err_trace)
                    return None
                except Exception as e:
                    logger.error(e)
                    err_trace = traceback.format_exc()
                    logger.error(err_trace)
                    await asyncio.sleep(retry_interval)
        return wrapped
    return wrapper

async def wait_concurrent(loop, executor, func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        action = functools.partial(func, *args, **kwargs)
        fut = loop.run_in_executor(executor, action)
        return await asyncio.wait_for(fut, None)
