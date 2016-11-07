import logging
import traceback
import asyncio
import inspect
import functools

logger = logging.getLogger("task-executor-py")
logger.propagate = False


class UnexpectedResponceCode(Exception):
    def __init__(self, code):
        super().__init__("Unexpected responce code: %d" % code)

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
        logger.debug("ref_cnt: %d", self.ref_cnt)
        try:
            return await crt_f(self, *args, **kwargs)
        finally:
            self.ref_cnt -= 1
            logger.debug("ref_cnt: %d", self.ref_cnt)
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