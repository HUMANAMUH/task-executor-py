import time
import inspect
import functools
from datetime import datetime, time as tm, timedelta
from contextlib import contextmanager

one_day = timedelta(days=1)

def timestamp_to_datetime(nano_time):
    return datetime.fromtimestamp(nano_time * 1e-3)

def datetime_to_timestamp(dt):
    x = dt.timestamp()
    a = int(x)
    b = int((x - a) * 1e3)
    return a * 1000 + b

def date_to_datetime(dt):
    return datetime.combine(dt, tm.min)

def get_date(dt):
    return date_to_datetime(dt.date())

def date_range(start, end, step_days=1):
    '''
    last day will be returned seperated
    '''
    oend = end
    end = oend - one_day
    step = timedelta(days=step_days)
    base = [start + step * i for i in range(0, (end - start) // step + 1)]
    return [(b, b + step if b + step <= end else end) for b in base] + ([(oend, oend)] if oend >= start else [])

@contextmanager
def timer(action):
    """
    @action func(time_used in secondes)
    """
    bg = time.time()
    yield
    ed = time.time()
    action(ed - bg)

def with_timer(action):
    def wrapper(func):
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def a_func(*args, **kwargs):
                with timer(action):
                    return await func(*args, **kwargs)
            return a_func
        else:
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                with timer(action):
                    return func(*args, **kwargs)
            return wrapped
    return wrapper
