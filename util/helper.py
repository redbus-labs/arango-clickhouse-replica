from functools import wraps


def singleton(*arg, retry_func=None):
    obj = None

    def init(func):

        @wraps(func)
        def get_obj(*args, **kwargs):
            nonlocal obj
            if retry_func and obj:
                is_retry = retry_func(obj)
                if is_retry:
                    obj = None
            if obj is None:
                obj = func(*args, **kwargs)
            return obj

        return get_obj

    if not retry_func:
        return init(func=arg[0])

    return init
