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


## Spider

如果要使用Python脚本模式来运行爬虫，需要将spider的类定义放在`andaman.spiders`中。爬虫必须继承`Spider`类，并且有`name`属性。满足这样条件的爬虫，才能被`main.py`中的`register_spiders()`方法识别并注册。

## Pipeline

## Download middleware

