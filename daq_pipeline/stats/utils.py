from time import time

timer_cache = {}


def start_timer(timer_name):
    timer_cache[timer_name] = time()


def get_timer_delta(timer_name, update_timer=False):
    current_time = time()

    delta = current_time - timer_cache[timer_name]

    if update_timer:
        timer_cache[timer_name] = current_time

    return delta
