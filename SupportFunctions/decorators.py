import time
from time import process_time
from functools import wraps


def measure_running_time(original_function):

    @wraps(original_function)
    def wrapper_function(*args, **kvargs):
        t1 = process_time()
        of = original_function(*args, **kvargs)
        t2 = process_time()
        print(f'{original_function.__name__} function took {t2-t1} seconds to run. [{time.asctime()}]')
        return of
    return wrapper_function
