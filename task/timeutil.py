from datetime import datetime, time as tm, timedelta

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
    step = timedelta(days=step_days)
    base = [start + step * i for i in range(0, (end - start) // step + 1)]
    return [(b, b + step if b + step <= end else end) for b in base]
