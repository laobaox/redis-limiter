#-*- coding: utf8 -*-

import time, re, functools
import threading
import weakref
from os import urandom
from base64 import b64encode
from datetime import datetime, timedelta
import jinja2

PERIOD_REGEXP = re.compile(r'^(\d+)(s|min|h|d|mo|y|w)$')

CODE_SUCCESS = 0
CODE_FULL = 1
CODE_PENDING_FULL = 2
CODE_DUPLICATE = 3
CODE_REFRESH_NOT_EXIST = 4


CONSTANTS = {
    'CODE_SUCCESS': CODE_SUCCESS,
    'CODE_FULL': CODE_FULL,
    'CODE_PENDING_FULL': CODE_PENDING_FULL,
    'CODE_DUPLICATE': CODE_DUPLICATE,
    'CODE_REFRESH_NOT_EXIST': CODE_REFRESH_NOT_EXIST,
}


def _template(source):
    return jinja2.Template(source, variable_start_string='{=', variable_end_string='=}')


SEMAPHORE_ACQUIRE_SCRIPT = '''
    redis.call("zremrangebyscore", KEYS[1], "-INF", ARGV[1])
    if redis.call("zcard", KEYS[1]) >= tonumber(ARGV[2]) then
        return 2
    else
        if redis.call("zadd", KEYS[1], "NX", ARGV[3], ARGV[4]) == 0 then
            return 3
        end
        redis.call("expire", KEYS[1], ARGV[5])
        return 0
    end
'''

RESOURCE_POOL_ACQUIRE_SCRIPT = _template('''
    local pending_key = KEYS[1]
    local resource_key = KEYS[2]
    local clear_t = ARGV[1]
    local quota = tonumber(ARGV[2])
    local now_t = ARGV[3]
    local target_id = ARGV[4]
    local pending_ex = ARGV[5]

    local expire_targets = redis.call("zrangebyscore", pending_key, "-INF", clear_t)
    if #expire_targets > 0 then
        redis.call("zrem", pending_key, unpack(expire_targets))
        redis.call("hdel", resource_key, unpack(expire_targets))
    end
    if redis.call("zcard", pending_key) >= quota then
        return {{=CODE_PENDING_FULL=}, 0}
    end

    local old_slots = redis.call("hvals", resource_key)
    local slot = nil
    table.sort(old_slots)
    for i=1,quota do
        if old_slots[i] == nil or tonumber(old_slots[i])>i then
            slot = i
            break
        end
    end

    if redis.call("zadd", pending_key, "NX", now_t, target_id) == 0 then
        return {{= CODE_DUPLICATE =}, 0}
    end
    redis.call("hset", resource_key, target_id, slot)
    redis.call("expire", pending_key, pending_ex)
    redis.call("expire", resource_key, pending_ex)
    return {{= CODE_SUCCESS =}, slot}
''').render(**CONSTANTS)


RESOURCE_POOL_RELEASE_SCRIPT = _template('''
    local pending_key = KEYS[1]
    local resource_key = KEYS[2]
    local signal_key = KEYS[3]
    local target_id = ARGV[1]

    redis.call("zrem", pending_key,  target_id)
    redis.call("hdel", resource_key, target_id)
    if signal_key ~= nil then
        redis.call("xadd", signal_key, "maxlen", 1, "*", "value", 1)
        redis.call("expire", signal_key, 1)
    end
    return {= CODE_SUCCESS =}
''').render(**CONSTANTS)


INTERVAL_LIMITER_ACQUIRE_SCRIPT = '''
    local pending_key = KEYS[1]
    local data_key = KEYS[2]
    local target_id = ARGV[5]
    redis.call("zremrangebyscore", pending_key, "-INF", ARGV[1])
    redis.call("zremrangebyscore", data_key, "-INF", ARGV[2])

    local quota = tonumber(ARGV[3])
    local applied_count = redis.call("zcard", data_key)
    if applied_count >= quota then
        return 1
    end

    if applied_count + redis.call("zcard", pending_key) >= quota then
        return 2
    end

    if redis.call("zscore", data_key, target_id) then
        return 3
    end
    if redis.call("zadd", pending_key, "NX", ARGV[4], ARGV[5]) == 0 then
        return 3
    end
    redis.call("expire", pending_key, ARGV[6])
    return 0
'''

INTERVAL_LIMITER_APPLY_SCRIPT = '''
    local now_timestamp = ARGV[1]
    local target_id = ARGV[2]
    redis.call("zrem", KEYS[1], target_id)
    redis.call("zadd", KEYS[2], now_timestamp, target_id)
    redis.call("expire", target_id, ARGV[3])
    if KEYS[3] ~= nil then
        redis.call("xadd", KEYS[3], "maxlen", 1, "*", "value", 1)
        redis.call("expire", KEYS[3], 1)
    end
    return 0
'''

FIXED_WINDOWN_ACQUIRE_SCRIPT = '''
    redis.call("zremrangebyscore", KEYS[1], "-INF", ARGV[1])
    local quota = tonumber(ARGV[2])

    local applied_count = redis.call("get", KEYS[2])
    if applied_count then
        applied_count = tonumber(applied_count)
    else
        applied_count = 0
    end

    if applied_count >= quota then
        return 1
    end

    if applied_count + redis.call("zcard", KEYS[1]) >= quota then
        return 2
    end

    if redis.call("zadd", KEYS[1], "NX", ARGV[3], ARGV[4]) == 0 then
        return 3
    end
    redis.call("expire", KEYS[1], ARGV[5])
    return 0
'''

SLIDE_WINDOW_ACQUIRE_SCRIPT = '''
    redis.call("zremrangebyscore", KEYS[1], "-INF", ARGV[1])

    local data_keys = {} 
    for i=2,#KEYS do
        data_keys[i-1] = KEYS[i]
    end
    local quota = tonumber(ARGV[2])
    local applied_count = 0
    local datas = redis.call("mget", unpack(data_keys))
    for i=1,#datas do
        if datas[i] then
            applied_count = applied_count + tonumber(datas[i])
        end
    end

    if applied_count >= quota then
        return 1
    end
    if applied_count + redis.call("zcard", KEYS[1]) >= quota then
        return 2
    end

    if redis.call("zadd", KEYS[1], "NX", ARGV[3], ARGV[4]) == 0 then
        return 3
    end
    redis.call("expire", KEYS[1], ARGV[5])
    return 0
'''

WINDOWN_LIMITER_APPLY_SCRIPT = '''
    redis.call("zrem", KEYS[1], ARGV[1])
    redis.call("incr", KEYS[2])
    redis.call("expire", KEYS[2], ARGV[2])
    if KEYS[3] ~= nil then
        redis.call("xadd", KEYS[3], "maxlen", 1, "*", "value", 1)
        redis.call("expire", KEYS[3], 1)
    end
    return 0
'''

RELEASE_SCRIPT = '''
    redis.call("zrem", KEYS[1], ARGV[1])
    if KEYS[2] ~= nil then
        redis.call("xadd", KEYS[2], "maxlen", 1, "*", "value", 1)
        redis.call("expire", KEYS[2], 1)
    end
    return 0
'''

REFRESH_SCRIPT = _template('''
    local pending_key = KEYS[1]
    local target_id = ARGV[1]
    local now_t = ARGV[2]
    local pending_ex = ARGV[3]

    if redis.call('zadd', pending_key, 'XX', 'CH', now_t, target_id) == 0 then
        return {= CODE_REFRESH_NOT_EXIST =}
    end
    redis.call('expire', pending_key, pending_ex)
    return {= CODE_SUCCESS =}
''').render(**CONSTANTS)


RESOURCE_POOL_REFRESH_SCRIPT = _template('''
    local pending_key = KEYS[1]
    local resource_key = KEYS[2]
    local target_id = ARGV[1]
    local now_t = ARGV[2]
    local pending_ex = ARGV[3]

    if redis.call('zadd', pending_key, 'XX', 'CH', now_t, target_id) == 0 then
        return {= CODE_REFRESH_NOT_EXIST =}
    end
    redis.call('expire', pending_key, pending_ex)
    redis.call('expire', resource_key, pending_ex)
    return {= CODE_SUCCESS =}
''').render(**CONSTANTS)

DEFAULT_LUA_SCRIPTS = {
    'semaphore_acquire_script': SEMAPHORE_ACQUIRE_SCRIPT,
    'resource_pool_acquire_script': RESOURCE_POOL_ACQUIRE_SCRIPT,
    'interval_limiter_acquire_script': INTERVAL_LIMITER_ACQUIRE_SCRIPT,
    'interval_limiter_apply_script': INTERVAL_LIMITER_APPLY_SCRIPT,
    'fixed_window_acquire_script': FIXED_WINDOWN_ACQUIRE_SCRIPT,
    'slide_window_acquire_script': SLIDE_WINDOW_ACQUIRE_SCRIPT,
    'window_limiter_apply_script': WINDOWN_LIMITER_APPLY_SCRIPT,
    'release_script': RELEASE_SCRIPT,
    'resource_pool_release_script': RESOURCE_POOL_RELEASE_SCRIPT,
    'refresh_script': REFRESH_SCRIPT,
    'resource_pool_refresh_script': RESOURCE_POOL_REFRESH_SCRIPT,
}

class PeriodError(Exception):
    pass

class OperationError(Exception):
    pass

class AcquireIdDuplicate(OperationError):
    pass

class AcquireFail(Exception):
    def __init__(self, msg, target):
        super(AcquireFail, self).__init__()
        self.msg = msg
        self.target = target
        self.args = (msg, target)

    def __str__(self):
        return str(self.msg)

class AcquireFullException(AcquireFail):
    pass

class AcquirePendingFullException(AcquireFail):
    pass

class RefreshError(Exception):
    pass

class UnknownError(Exception):
    pass

class Period(object):
    _conf = {
        's': {'desc': 'second', 'seconds': 1, 'boundary': 60},
        'min': {'desc': 'minute', 'seconds': 60, 'boundary': 60},
        'h': {'desc': 'hour', 'seconds': 3600, 'boundary': 24},
        'd': {'desc': 'day', 'seconds': 24 * 3600, 'boundary': 1},
        'mo': {'desc': 'month', 'seconds': None, 'boundary': 12, 'ex_seconds': 31*24*3600},
        'y': {'desc': 'year', 'seconds': None, 'boundary': 1, 'ex_seconds': 366*24*3600},
        'w': {'desc': 'week', 'seconds': 7 * 24 * 3600, 'boundary': 1},
    }
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value

    @property
    def ex_seconds(self):
        unit = self.kind_conf.get('seconds') or self.kind_conf.get('ex_seconds')
        return unit * self.value

    @property
    def total_seconds(self):
        if not self._conf[self.kind].get('seconds'):
            raise PeriodError('month and year period not support convert to seconds')
        return self.value * self._conf[self.kind]['seconds']

    @property
    def boundary(self):
        return self._conf[self.kind]['boundary']

    @property
    def kind_desc(self):
        return self._conf[self.kind]['desc']

    @property
    def kind_conf(self):
        return self._conf[self.kind]

    @classmethod
    def parse(cls, period):
        m = PERIOD_REGEXP.match(period)
        if not m:
            raise PeriodError('period format error')
        value, kind = m.groups()
        value = int(value)
        return cls(kind, value)

class Config(object):
    def __init__(self, client, support_blocking=True, block_max_seconds=4):
        self.client = client
        self.support_blocking = support_blocking
        self.block_max_seconds = block_max_seconds
        self._is_script_ready = False
        self._scripts = {}

    def register_scripts(self):
        if self._is_script_ready:
            return
        for k, v in DEFAULT_LUA_SCRIPTS.items():
            self._scripts[k] = self.client.register_script(v)
        self._is_script_ready = True

    def __getattr__(self, name):
        self.register_scripts()
        if name not in self._scripts:
            raise AttributeError("%s object has no attribute %s" % (self.__class__.__name__, name))
        return self._scripts[name]

class Target(object):
    def __init__(self, handler, flag=None, is_refresh=False):
        self.handler = handler
        self.flag = flag
        self.is_refresh = is_refresh
        self.now_timestamp = int(time.time())
        self.now_datetime = datetime.fromtimestamp(self.now_timestamp)
        self.id = '%x:%s' % (self.now_timestamp, b64encode(urandom(20)).decode('ascii'))
        self.is_finished = False

    def acquire(self):
        ret = self.handler.acquire(self)
        if self.is_refresh:
            self._start_refresh()
        return ret

    def _start_refresh(self):
        self._refresh_stop = threading.Event()
        self._refresh_thread = threading.Thread(target=self._lock_refresher, args=(
            weakref.ref(self), self.handler.pending_ex/2, self._refresh_stop))
        self._refresh_thread.setDaemon(True)
        self._refresh_thread.start()

    def _stop_refresh(self):
        self._refresh_stop.set()
        self._refresh_thread.join()
        self._refresh_thread = None

    @staticmethod
    def _lock_refresher(ref, interval, stop):
        while not stop.wait(timeout=interval):
            target = ref()
            if target is None:
                break
            target.handler.refresh(target)
            del target

    def apply(self):
        if not self.is_finished:
            if self._refresh_thread:
                self._stop_refresh()
            self.handler.apply(self)
            self.is_finished = True

    def release(self):
        if not self.is_finished:
            if self._refresh_thread:
                self._stop_refresh()
            self.handler.release(self)
            self.is_finished = True

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        raise NotImplementedError('need implement')

class SemaphoreTarget(Target):
    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        self.release()

class ResourcePoolTarget(SemaphoreTarget):
    def __init__(self, handler, flag=None, is_refresh=False):
        super(ResourcePoolTarget, self).__init__(handler, flag, is_refresh)
        self.slot = None

class LimiterTarget(Target):
    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        if exc_value:
            self.release()
        else:
            self.apply()

class Base(object):
    _target_cls = None
    def __init__(self, config, key_prefix, quota, block=0, pending_ex=20):
        self.config = config
        self.key_prefix = key_prefix
        self.quota = quota
        self.block = block
        self.pending_ex = pending_ex
        assert(self.block < pending_ex)

    def target(self, flag, is_refresh=False):
        return self._target_cls(self, flag, is_refresh=is_refresh)

    def default_target(self):
        return self.target('__default__')

    def acquire_with_block(self, target, acquire_func):
        signal_key = self.get_signal_key(target)
        signal_id = 0
        while True:
            ret = acquire_func()
            code, data = self.parse_acquire_result(ret)
            if not (self.config.support_blocking and self.block and code == CODE_PENDING_FULL):
                break

            block_seconds = self.block + target.now_timestamp - int(time.time())
            if block_seconds <= 0:
                break

            signal_data = self.config.client.xread({signal_key:signal_id}, count=1, block=block_seconds*1000)
            if signal_data:
                signal_id = signal_data[0][1][0]

        if code == CODE_SUCCESS:
            self.set_data(data, target)
        else:
            self.check_acquire_error(code, target)

    def parse_acquire_result(self, ret):
        return ret, None

    def set_data(self, data, target):
        pass

    def check_acquire_error(self, code, target):
        if code == 1:
            raise AcquireFullException('quota is full', target)
        elif code == 2:
            raise AcquirePendingFullException('quota with pending is full', target)
        elif code == 3:
            raise AcquireIdDuplicate('acquire id duplicate')
        else:
            raise OperationError('unkown error, return code:%s' % code)

    def acquire(self, target):
        raise NotImplementedError('need implement')

    def release(self, target):
        if self.config.support_blocking:
            self.config.release_script(
                client=self.config.client,
                keys=(self.get_pending_key(target), self.get_signal_key(target)),
                args=(target.id,))
        else:
            self.config.client.zrem(self.get_pending_key(target), target.id)

    def refresh(self, target):
        code = self.config.refresh_script(
            client=self.config.client,
            keys=(self.get_pending_key(target),),
            args=(target.id, int(time.time()), target.pending_ex+5))
        if code == CODE_REFRESH_NOT_EXIST:
            raise RefreshError('refresh fail, lock target not exist')
        if code != CODE_SUCCESS:
            raise UnknownError('refresh unkown return code')

    def get_pending_key(self, target):
        raise NotImplementedError('need implement')

    def get_signal_key(self, target):
        raise NotImplementedError('need implement')

class LimiterBase(Base):
    _target_cls = LimiterTarget
    def __init__(self, config, key_prefix, quota, period, block=0, pending_ex=20):
        super(LimiterBase, self).__init__(config, key_prefix, quota, block=block, pending_ex=pending_ex)
        self.period = Period.parse(period)
        self.init_params()

    def init_params(self):
        raise NotImplementedError('need implement')

    def apply(self, target):
        raise NotImplementedError('need implement')

    def get_data_key(self, target):
        raise NotImplementedError('need implement')

class Semaphore(Base):
    _target_cls = SemaphoreTarget
    def acquire(self, target):
        clear_t = target.now_timestamp - self.pending_ex
        func = functools.partial(self.config.semaphore_acquire_script,
            client=self.config.client,
            keys=(self.get_pending_key(target),),
            args=(clear_t, self.quota, target.now_timestamp, target.id, self.pending_ex+5))
        return self.acquire_with_block(target, func)

    def get_pending_key(self, target):
        return '%s:semaphore_pending:%s' % (self.key_prefix, target.flag)

    def get_signal_key(self, target):
        return '%s:semaphore_signal:%s' % (self.key_prefix, target.flag)


class ResourcePool(Base):
    _target_cls = ResourcePoolTarget
    def acquire(self, target):
        clear_t = target.now_timestamp - self.pending_ex
        func = functools.partial(self.config.resource_pool_acquire_script,
            client=self.config.client,
            keys=(self.get_pending_key(target), self.get_resource_key(target)),
            args=(clear_t, self.quota, target.now_timestamp, target.id, self.pending_ex+5))
        return self.acquire_with_block(target, func)

    def parse_acquire_result(self, ret):
        code, slot = ret
        return code, slot

    def set_data(self, data, target):
        target.slot = data

    def release(self, target):
        if self.config.support_blocking:
            signal_key = self.get_signal_key(target)
        else:
            signal_key = None
        self.config.resource_pool_release_script(
            client=self.config.client,
            keys=(self.get_pending_key(target), self.get_resource_key(target), signal_key),
            args=(target.id,))

    def refresh(self, target):
        code = self.config.resource_pool_refresh_script(
            client=self.config.client,
            keys=(self.get_pending_key(target), self.get_resource_key(target)),
            args=(target.id, int(time.time()), self.pending_ex+5))
        if code == CODE_REFRESH_NOT_EXIST:
            raise RefreshError('refresh fail, lock target not exist')
        if code != CODE_SUCCESS:
            raise UnknownError('refresh unkown return code')

    def get_pending_key(self, target):
        return '%s:resource_pool_pending:%s' % (self.key_prefix, target.flag)

    def get_resource_key(self, target):
        return '%s:resource_pool_resource:%s' % (self.key_prefix, target.flag)

    def get_signal_key(self, target):
        return '%s:resource_pool_signal:%s' % (self.key_prefix, target.flag)


class IntervalLimiter(LimiterBase):
    def init_params(self):
        assert self.period.kind_conf['seconds'], "%s value can not conver to exact seconds, can not as IntervalLimiter period" % self.period.kind_desc
        
    def acquire(self, target):
        pending_key = self.get_pending_key(target)
        data_key = self.get_data_key(target)
        signal_key = self.get_signal_key(target)
        pending_clear_t = target.now_timestamp - self.pending_ex
        data_clear_t = target.now_timestamp - self.period.total_seconds

        func = functools.partial(self.config.interval_limiter_acquire_script,
            client=self.config.client,
            keys=(pending_key, data_key),
            args=(pending_clear_t, data_clear_t, self.quota, target.now_timestamp, target.id, self.pending_ex))

        return self.acquire_with_block(target, func)

    def apply(self, target):
        keys = [self.get_pending_key(target), self.get_data_key(target)]
        if self.config.support_blocking:
            keys.append(self.get_signal_key(target))

        self.config.interval_limiter_apply_script(
            client=self.config.client,
            keys=tuple(keys),
            args=(target.now_timestamp, target.id, self.period.total_seconds + 5))

    def get_data_key(self, target):
        return '%s:interval:%s' % (self.key_prefix, target.flag)

    def get_pending_key(self, target):
        return '%s:interval_pending:%s' % (self.key_prefix, target.flag)

    def get_signal_key(self, target):
        return '%s:interval_signal:%s' % (self.key_prefix, target.flag)

class FixedWindowLimiter(LimiterBase):
    def init_params(self):
        assert self.period.value <= self.period.boundary and \
            self.period.boundary % self.period.value == 0, \
            "for fixed window limiter, %s value must <=%s and can exact division %s"  % (\
            self.period.kind_desc, self.period.boundary, self.period.boundary)

    def acquire(self, target):
        pending_key = self.get_pending_key(target)
        data_key = self.get_data_key(target)
        pending_clear_t = target.now_timestamp - self.pending_ex
        func  = functools.partial(self.config.fixed_window_acquire_script,
            client=self.config.client,
            keys=(pending_key, data_key),
            args=(pending_clear_t, self.quota, target.now_timestamp, target.id, self.pending_ex))
        return self.acquire_with_block(target, func)

    def apply(self, target):
        keys = [self.get_pending_key(target), self.get_data_key(target)]
        if self.config.support_blocking:
            keys.append(self.get_signal_key(target))

        self.config.window_limiter_apply_script(
            client=self.config.client,
            keys=tuple(keys),
            args=(target.id, self.period.ex_seconds+5))

    def get_data_key(self, target):
        time_key = gen_datetime_key(target.now_datetime, self.period)
        return '%s:fixed_window:%s_%s' % (self.key_prefix, target.flag, time_key)

    def get_pending_key(self, target):
        return '%s:fixed_window_pending:%s' % (self.key_prefix, target.flag)

    def get_signal_key(self, target):
        return '%s:fixed_window_signal:%s' % (self.key_prefix, target.flag)

class SlideWindowLimiter(LimiterBase):
    _max_step = 10
    def init_params(self):
        self.gap, self.step = self.calc_gap_step(self.period)
        
    def acquire(self, target):
        pending_key = self.get_pending_key(target)
        data_keys = self.get_slide_data_keys(target)
        assert len(data_keys) >= 1
        keys = [pending_key,] + data_keys
        pending_clear_t = target.now_timestamp - self.pending_ex
        func = functools.partial(self.config.slide_window_acquire_script,
            client=self.config.client,
            keys=tuple(keys),
            args=(pending_clear_t, self.quota, target.now_timestamp, target.id, self.pending_ex))
        return self.acquire_with_block(target, func)

    def apply(self, target):
        keys = [self.get_pending_key(target), self.get_data_key(target)]
        if self.config.support_blocking:
            keys.append(self.get_signal_key(target))

        ret = self.config.window_limiter_apply_script(
            client=self.config.client,
            keys=tuple(keys),
            args=(target.id, self.period.ex_seconds+5))
        assert ret == 0

    def get_data_key(self, target, step_decr=0):
        time_key = gen_datetime_key(target.now_datetime, self.period, gap=self.gap, step_decr=step_decr)
        return '%s:slide_window:%s_%s' % (self.key_prefix, target.flag, time_key)

    def get_slide_data_keys(self, target):
        decr_values = [self.gap*i for i in range(self.step)]
        return [self.get_data_key(target, v) for v in decr_values]

    def get_pending_key(self, target):
        return '%s:slide_window_pending:%s' % (self.key_prefix, target.flag)

    def get_signal_key(self, target):
        return '%s:slide_window_signal:%s' % (self.key_prefix, target.flag)

    def calc_gap_step(self, period):
        gap = None
        min_gap = (period.value - 1) // self._max_step + 1
        for i in range(min_gap, period.value//2+1):
            if period.value % i == 0 and self.period.boundary % i == 0:
                gap = i
                break
        if not gap:
            raise PeriodError('can calc a slide window count <=10 for %s value %s' % (period.kind_desc, period.value))
        return gap, period.value // gap

def gen_datetime_key(now, period, gap=None, step_decr=0, max_step=1):
    if not gap:
        gap = period.value

    def calc_month():
        month = now.month - step_decr
        year = now.year + month // 12
        month = month % 12
        return '%s%+02d' % (year, month)

    def calc_week():
        truncate = now - timedelta(now.weekday())
        return (truncate - timedelta(days=step_decr*7)).strftime('%Y%m%d')
        
    funcs = {
        's': lambda: (now - timedelta(seconds=step_decr + now.second % gap)).strftime('%Y%m%d%H%M%S'),
        'min': lambda: (now - timedelta(minutes=step_decr + now.minute % gap)).strftime('%Y%m%d%H%M'),
        'h': lambda: (now - timedelta(hours=step_decr + now.hour % gap)).strftime('%Y%m%d%H'),
        'd': lambda: (now - timedelta(days=step_decr)).strftime('%Y%m%d'),
        'mo': calc_month,
        'y': lambda: str(now.year - step_decr),
        'w': calc_week,
    }

    return '%s%s' % (period.kind, funcs[period.kind]())
