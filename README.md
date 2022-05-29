# redis-limiter
基于redis的限流器

## install
pip install redis-limiter

## intro
陆陆续续写过不少基于redis的限流逻辑，看似简单，实际写下来还是有不少要留意的问题

以一个简单的功能为例：

    限制每个用户一天内只能修改昵称3次
    
直观的想法很简单,伪代码如下：
    
    key=user_id + date
    if redis.get(key) >= 3:
        return False
        
    ... operation logic ...
    
    redis.incr(key)
    
然而仔细想下，事情没那么简单，上面的逻辑在并发请求的情况下，用户操作很容易会超出3个的限制；为解决这个问题，我们换个思路：
    
    key = user_id + date
    if redis.incr(key) > 3:
        return False
    
    ...operation logic....
    
嗯，并发情况下不会超额了，问题是如果operation logic执行失败了呢，用户是不是就丢失了操作的配额？ 这种情况是不是要把INCR的数值给减回来呢？

还有其他可能出现的问题：

* 执行中间出现了异常，已经执行的redis操作是不是要回滚？
* 3个限额，如果6个并发请求操作，前面3个进入operation logic处理中，后面3个返回失败，还是要等待？ 难道要加个分布式锁，保证业务串行执行？
* 如果并发请求，后面的请求要block住等前面的处理结果的话，block怎么实现？
* 上面的例子是限额1天内，可以用当天的日志做key，如果限额周期不是1天呢，比如是100分钟，比如是10天，用什么做key?

嗯，基于redis的限流并没有直观的看上去那么简单

redis-limiter致力于解决上面的所有问题，提供一个通用的基于redis的限流器功能

使用redis-limiter实现上面的例子：

```
import redis-limiter

quota = 3
key_prefix = "operation_limit"
config = redis_limiter.Config(redis)
limiter = redis_limiter.FixedWindowLimiter(config, key_prefix, quota, '1d')

limit_key = user_id
target = limiter.target(limit_key)

target.acquire()
is_suc = False
try:
    ...operation logic...
    is_suc = True
finally:
    if is_suc:
        target.apply()
    else:
        target.release()
```

留意上面的acquire，表示预扣一个限额指标，业务逻辑执行成功，则执行apply使用掉改指标，如果失败则release释放该指标；
如果acquire之后什么都不做，则经过一定的超时时间自动释放

acquire会先检查已扣的数量是否满额，如果是，则raise redis_limiter.AcquireFullException
acquire之后会检查已扣的数量 + 预扣的数据，看是否满额，如果是，则raise redis_limiter.AcquirePendingFullException

对于raise redis_limiter.AcquirePendingFullException，还可以加上指定的block time, 即等待其他现成release指标，等待超过指定block time，才raise该异常;
在limiter对象创建时，使用block参数指定超时时间，单位是秒：

```
limiter = redis_limiter.FixedWindowLimiter(config, key_prefix, quota, '1d', block=10)
```

需要留意的时，block功能是使用redis的stream数据结构实现的，需要redis server的版本>=5.0；
如果使用的版本低于5.0则无法使用block功能，并在需要指定config参数: support_blocking=False

eg:

```
config = redis_limiter.Config(redis, support_blocking=False)
```

limiter支持使用with语句, 上面的demo中acquire之后的代码，可以改为如下写法:

```
with limiter.target(limit_key) as target:
    ...opeation logic...
```

这种写法，执行无异常会自动apply，异常则自动release，也可以在with的代码块中，手动apply/release:

```
with limiter.target(limit_key) as target:
    is_suc = ...opeation logic...
    if is_suc:
        target.apply()
    else:
        target.release()
```

redis-limiter实现了4种类型的限流器

* IntervalLimiter
* FixedWindowLimiter
* SlideWindowLimiter
* Semaphore

## 时间周期Period

除了Semaphore，其他几种限流器都需要指定一个时间周期，比如10秒、1天。。。
reids_limiter使用了统一的字符串格式指定时间周期：数字+单位后缀，比如"10s"表示10秒，"10min"表示10分钟，所有的后缀如下：

* s: 秒
* min: 分钟
* h: 小时
* d: 天
* mo: 月
* y: 年
* w: 周

## IntervalLimiter

init函数：

```
def __init__(self, config, key_prefix, quota, period, block=0, pending_ex=20)
```

IntervalLimiter实现指定时间间隔内不超过某限额的功能，时间精确到秒；
比如指定 period="1d", quota=3, 不是表示1个自然日内的限额是3，而是表示任何24*3600秒的时间段内限额为3

## FixedWindowLimiter

init函数：

```
def __init__(self, config, key_prefix, quota, period, block=0, pending_ex=20)
```
FixedWindowLimiter实现基于固定时间窗口计数的限流功能；
period统一按照自然时间，比“1min"表示每自然分钟，"1d"表示每自然日，"1mo"表示每个自然月，所以period会有些限制；
比如日期单位的数量只能是1，即只能是"1d", 不能是"2d", "10d"或其他任意大于1的数量，因为每月或每年的天数是不固定的，没法取key；
而秒单位的则可以取，"5s", "10s"... ， 单要求必须是60(1分钟60秒)的约数
period数量完整的约束如下：

* s: 必须是60的约数
* min: 必须是60的约数
* h: 必须是24的约数
* d: 只能是1
* m: 必须是12的约数
* y: 只能是1
* w: 只能是1

## SlideWindowLimiter

init函数：

```
def __init__(self, config, key_prefix, quota, period, block=0, pending_ex=20)
```
SlideWindowLimiter实现了基于滑动窗口计数的限流功能，计数的窗口数量不需要在参数中自定，而是自动计算: 取<=10并且最接近10的值

类似FixedWindowLimiter，period也会有一些约束, 假定我们把period数量值表示为$v，则规则如下表示：

* s: $v和60存在一个公约数，并且 $v/该公约数<=10
* min: $v和60存在一个公约数，并且 $v/该公约数<=10
* h: $v和24存在一个公约数，并且 $v/该公约数<=10
* d: $v取1-10
* mo: $v必须和12存在一个公约数，并且$v/该公约数<=10
* y: $v取1-10
* w: $v取1-10

## Semaphore

init函数：     

```
def __init__(self, config, key_prefix, quota, block=0, pending_ex=20)
```

Semaphore用于限定同一时间的并发处理数量，相比其他限流器有些不同： 
* 不需要指定时间周期
* 没有apply操作，acquire之后只能release
* 不会 raise redis_limiter.AcquireFullException, 只会 raise redis_limiter.AcquirePendingFullException


