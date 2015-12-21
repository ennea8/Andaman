Andaman
=========

![](http://lxp-assets.qiniudn.com/github/Andaman.jpg?imageView2/2/w/800/q/85)

爱走的爬虫系统，基于Scrapy框架实现。

## 使用方法

### `scrapy`命令模式

支持标准的[Scrapy命令](http://doc.scrapy.org/en/latest/topics/commands.html)，比如：

* `scrapy list`: 列出可用的spider
* `scrapy craw {spider_name}`: 开始抓取
* `scrapy check {spider_name}`: 对某个spider进行contract check等

### Python脚本模式

除了通过`scrapy`命令以外，Andaman也支持使用Python脚本来启动，入口为`main.py`。该脚本使用`CrawlerRunner`来运行和管理抓取任务。比起`scrapy`命令，这种方法的好处是可以更加灵活地使用API来控制抓取流程，并且，如果在IDE（比如PyCharm等）中运行，可以很方便地启用调试器。

参数：

* `-s`: 设置自定义的`SETTINGS`，比如：`-s SPIDERS=baidu`

为了正常启动爬虫，需要指定`SPIDERS`这个设置项。该设置的值为为希望启动的爬虫的name（可以同时启动多个爬虫，用`,`进行连接）。比如：

`python main.py -s SPIDERS=baidu,qyer`

## 设置项

### 数据库相关

* `ANDAMAN_MONGO_URI`: MongoDB Connection URI，访问MongoDB所必须

### Pipeline相关

* `PIPELINE_PROXY_ENABLED`: 是否启用`ProxyPipeline`

### Proxy相关

* `DYNO_PROXY_ENABLED`
* `DYNO_PROXY_MAX_FAIL`
* `DYNO_PROXY_MAX_LATENCY`
* `DYNO_PROXY_REFRESH_INTERVAL`


## Spider

如果要使用Python脚本模式来运行爬虫，需要将spider的类定义放在`andaman.spiders`中。爬虫必须继承`Spider`类，并且有`name`属性。满足这样条件的爬虫，才能被`main.py`中的`register_spiders()`方法识别并注册。

## Pipeline

## Download middleware

### AndamanProxyMiddleware

该中间件的主要目的是应对目标网站对爬虫来源IP的封禁策略。不少网站面对同一个IP来源的大量访问时，可能会封禁该IP。为了应对这一情况，`AndamanProxyMiddleware`的策略是：维护一个较大的代理池。对于经过中间件的HTTP请求，如果该请求没有指定代理服务器，则从代理池中随机挑选一个proxy，分配给该请求。如此一来，从网站看来，每个请求来自于不同的IP，这样也就无法封禁了。

#### 一些细节问题的说明

1. 代理来源：有一个爬虫叫做`youdaili`，该爬虫会定期地抓取代理列表，验证，测速，然后放到MongoDB数据库中。该中间件的代理来源就是这些数据。
2. 在爬虫运行期间，代理池也需要定期更新。
3. 中间件不但会向HTTP请求分配代理，还会对HTTP响应进行验证。如果验证没有通过，说明这个代理服务器的工作失败了。和[`RetryMiddleware`](http://doc.scrapy.org/en/latest/topics/downloader-middleware.html#module-scrapy.downloadermiddlewares.retry)配合，该HTTP请求会被重试。如果某个代理服务器总是失败，将会被禁用，对于今后的请求，不再会分配这一代理服务器。
4. 默认情况下，如果请求过程中发生了异常，或者返回码不等于2XX，说明代理工作异常。同时，用户也可以自定义这一行为（对于有些网站，但IP被封禁以后，并不会抛出4XX/5XX等错误代码，而是跳转到一个200的错误说明网页）。在`Request`的`meta`中，`dyno_proxy_validator`这一自定义的属性用于验证代理服务器是否有效。这是一个函数，接收`Response`类型的参数，返回一个`bool`值。
5. 该中间件可能和[`RetryMiddleware`](http://doc.scrapy.org/en/latest/topics/downloader-middleware.html#module-scrapy.downloadermiddlewares.retry)，[`HttpProxyMiddleware`](http://doc.scrapy.org/en/latest/topics/downloader-middleware.html#module-scrapy.downloadermiddlewares.httpproxy)等官方中间件有相互作用。需要注意它们之间的顺序。

#### 设置项目

* `DYNO_PROXY_ENABLED`：是否启用中间件。
* `DYNO_PROXY_MAX_FAIL`：最多的连续失败次数（连续失败超过这一数目，代理将被禁用），默认为3次。
* `DYNO_PROXY_MAX_LATENCY`：最大延迟（在获取代理来源的时候，限定在延迟小于该设置值的代理），单位为秒。默认为1。
* `DYNO_PROXY_REFRESH_INTERVAL`：代理池的刷新频率，单位为秒。默认为1800秒（半小时）。
