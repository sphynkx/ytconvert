import time


def now_ts():
    return int(time.time())


def monotonic():
    return time.monotonic()