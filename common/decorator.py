import sys, os, traceback
import inspect
from time import sleep

from common.msg import MessageHandle
from config.settings import TryExceptionConfig


def retry_except(func):
    """
    retry를 몇차례 시도할때 사용하는 decorator
    """
    def try_except_function(*args, **kwargs):
        name = None
        for i in range(TryExceptionConfig.RETRY_COUNT):
            try:
                results = func(*args, **kwargs)
                return results
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                _no = traceback.extract_tb(exc_tb)[-1][1]
                try:
                    is_method = inspect.getargspec(func)[0][0] == 'self'
                except:
                    is_method = False

                if is_method:
                    name = 'Watcher.{}.{}'.format(args[0].__class__.__name__, func.__name__)
                    _args = args[1:]
                else:
                    name = 'Watcher.{}'.format(func.__name__)
                    _args = args
                error = 'args: {}\nkwargs: {}\nerror: {}\n__name__: {}\nline: {}'
                error =  error.format(str(_args), str(kwargs), str(e), str(name), _no)
                print('ERROR:', error)
                MessageHandle().send_slack(name, '{} No:{}'.format(name, _no), error)
                sleep(TryExceptionConfig.SLEEP)
                MessageHandle().send_slack(name, 'Retry:{} failed'.format(TryExceptionConfig.RETRY_COUNT), str(error))
        return False
    return try_except_function


def except_console(func):
    """
    에러 방생 시 console log만 남길때 사용하는 decorator
    """
    def try_except_function(*args, **kwargs):
        try:
            results = func(*args, **kwargs)
            return results
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            _no = traceback.extract_tb(exc_tb)[-1][1]
            try:
                is_method = inspect.getargspec(func)[0][0] == 'self'
            except:
                is_method = False

            if is_method:
                _args = args[1:]
                name = 'Watcher.{}.{}'.format(args[0].__class__.__name__, func.__name__)
            else:
                _args = args
                name = 'Watcher.{}'.format(func.__name__)
            error = 'args: {}\nkwargs: {}\nerror: {}\n__name__: {}\nline: {}'
            error = error.format(str(_args), str(kwargs), str(e), str(name), _no)
            print('ERROR:', error)

            # MessageHandle().send_slack(name, '{} No:{}'.format(name, _no), error)
        return False
    return try_except_function


def except_pass(func):
    """
    에러 발생 시 pass 처리할때 사용하는 decorator
    """
    def try_except_function(*args, **kwargs):
        try:
            results = func(*args, **kwargs)
            return results
        except Exception as e:
            pass
        return False
    return try_except_function

