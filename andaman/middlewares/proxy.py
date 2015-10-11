# coding=utf-8
import logging
import threading
from scrapy.exceptions import NotConfigured
from mongoengine import connect

from andaman.middlewares.dynoproxy import DynoProxyMiddleware
from andaman.pipelines.proxy import ProxyDocument

__author__ = 'zephyre'


def set_interval(interval):
    """
    定时执行某个函数
    :param interval:
    :return:
    """

    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():  # executed in another thread
                while not stopped.wait(interval):  # until stopped
                    function(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = True  # stop if the program exits
            t.start()
            return stopped

        return wrapper

    return decorator


class AndamanProxyMiddleware(DynoProxyMiddleware):
    """
    设置项：

    * DYNO_PROXY_VAL_SRC: 代理服务器的验证对象
    * DYNO_PROXY_REFRESH_INTERVAL: 代理服务器的最大延迟（秒）
    * DYNO_PROXY_REFRESH_INTERVAL: 代理服务器的刷新频率（秒）
    """

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('DYNO_PROXY_ENABLED'):
            raise NotConfigured

        return cls(crawler.settings)

    @staticmethod
    def init_db(settings):
        """
        Initializing the database connection
        :param settings:
        :return:
        """
        mongos = settings.getdict('ANDAMAN_SERVICES')['mongo']
        endpoints = ['%s:%d' % (server['host'], server['port']) for server in mongos.values()]
        mongo_conf = settings.getdict('ANDAMAN_CONF')['mongo']
        user = mongo_conf['user']
        password = mongo_conf['password']
        db = mongo_conf['db']
        mongo_uri = 'mongodb://%s:%s@%s/%s' % (user, password, ','.join(endpoints), db)
        return connect(host=mongo_uri)

    @staticmethod
    def _fetch_proxies(validation_src, max_latency):
        """
        Fetch proxies from the database
        :param validation_src: the source of the validation, e.g. baidu, google...
        :param max_latency: the maximal latency. Proxies with higher latencies will be filtered out
        :return:
        """
        ops = {'validation__%s__latency__lte' % validation_src: max_latency}

        def build_proxy_uri(host, port, scheme='http', user=None, password=None):
            if user and password:
                return '%s://%s:%s@%s:%d' % (scheme, user, password, host, port)
            else:
                return '%s://%s:%d' % (scheme, host, port)

        return [(build_proxy_uri(entry.host, entry.port, entry.scheme, entry.user, entry.password),
                 entry['validation'][validation_src]['latency']) for entry in ProxyDocument.objects(**ops)]

    def __init__(self, settings):
        max_fail = settings.getint('DYNO_PROXY_MAX_FAIL', 3)
        super(AndamanProxyMiddleware, self).__init__(max_fail=max_fail)

        # Initializing the database connection
        self._conn = self.init_db(settings)

        self.validation_src = settings.get('DYNO_PROXY_VAL_SRC', 'baidu')
        self.max_latency = settings.getfloat('DYNO_PROXY_MAX_LATENCY', 1)
        self.refresh_interval = settings.getint('DYNO_PROXY_REFRESH_INTERVAL', 1800)

        # Initializing the proxy pool
        # self.update_proxy_pool()
        self.schedule_proxies_refresh()

    def schedule_proxies_refresh(self):
        self.update_proxy_pool()

        interval = self.refresh_interval
        stopped = threading.Event()

        def loop():  # executed in another thread
            while not stopped.wait(interval):  # until stopped
                self.update_proxy_pool()

        t = threading.Thread(target=loop)
        t.daemon = True  # stop if the program exits
        t.start()

    def update_proxy_pool(self):
        proxy_map = {entry[0]: {'fail_cnt': 0, 'latency': entry[1]} for entry in
                     self._fetch_proxies(self.validation_src, self.max_latency) if
                     entry[0] not in self.disabled_proxies}

        for k, v in proxy_map.items():
            self.proxy_pool[k] = v

        logging.getLogger('scrapy').info(
            'Proxy pool has been updated. There are %d proxies in all' % len(self.proxy_pool))
