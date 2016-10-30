from datetime import datetime, time as tm, timedelta

def nano_to_datetime(nano_time):
    return datetime.fromtimestamp(nano_time * 1e-9)

def datetime_to_nano(dt):
    x = dt.timestamp()
    a = int(x)
    b = int((x - a) * 1e9)
    return a * 1000000000 + b

def date_to_datetime(dt):
    return datetime.combine(dt, tm.min)

def date_range(start, end, step=timedelta(days=1)):
    base = [start + step * i for i in range(0, (end - start) // step + 1)]
    return [(b, b + step if b + step <= end else end) for b in base]
