# coding=utf-8
from scrapy.exceptions import NotConfigured

__author__ = 'zephyre'


class DynoProxyMiddleware(object):
    """
    Downloader middleware: randomly select a proxy from the pool, and apply it to passing-by requests

    Settings:
    * DYNO_PROXY_ENABLED: whether the middleware is enabled or not. The default value is True.
    * DYNO_PROXY_MAX_FAIL: normally when a request fails, the middleware will apply another proxy server from the pool
    and try again. If a proxy server fails in succession for a certain number of times, it will be removed from the
    pool. The default value is 3.
    """

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('DYNO_PROXY_ENABLED'):
            raise NotConfigured
        return cls(max_fail=crawler.settings.getint('DYNO_PROXY_MAX_FAIL', 3))

    def __init__(self, max_fail):
        self.max_fail = max_fail
        # Pool of proxy servers, e.g. {'http://224.224.224.224:3128': {'fail_cnt': 3, 'latency': 0.2}}
        self.proxy_pool = {}
        self.disabled_proxies = set([])

    def update_proxy_pool(self):
        """
        Update the existing proxy pool
        """
        raise NotImplementedError

    def process_request(self, request, spider):
        # Conditions in which the dyno-proxy mechanism is bypassed
        if 'proxy' in request.meta or getattr(request.meta, 'dyno_proxy_ignored', False):
            return

        import random

        try:
            proxy = random.choice(self.proxy_pool.keys())
            spider.logger.debug('Randomly selecting a proxy: %s' % proxy)
            request.meta['proxy'] = proxy
            # This flag indicates that the middleware has successfully applied a proxy server on the request
            request.meta['dyno_proxy_flag'] = True
        except IndexError:
            spider.logger.warning('The middleware is bypassed because the proxy pool is empty')

    @staticmethod
    def _strip_meta(meta):
        """
        Remove related fields from request.meta. This is critical to work with other middlewares e.g. RetryMiddleware
        :param meta:
        :return:
        """
        if 'dyno_proxy_flag' in meta:
            del meta['dyno_proxy_flag']
            try:
                del meta['proxy']
            except KeyError:
                pass

        return meta

    def reset_fail_cnt(self, proxy, spider):
        self.proxy_pool[proxy]['fail_cnt'] = 0

    def add_fail_cnt(self, proxy, spider):
        fail_cnt = self.proxy_pool[proxy]['fail_cnt'] + 1
        if fail_cnt > self.max_fail:
            self.deregister_proxy(proxy, spider)

    def deregister_proxy(self, proxy, spider):
        """
        Remove a proxy from the pool
        :param proxy:
        :param spider:
        :return:
        """
        spider.logger.warn('Removing %s from the proxy pool' % proxy)
        self.disabled_proxies.add(proxy)
        try:
            del self.proxy_pool[proxy]
        except KeyError:
            pass

    def process_response(self, request, response, spider):
        meta = request.meta

        # For the condition that the requests haven't been processed by the middleware
        if 'dyno_proxy_flag' not in meta:
            return response

        proxy = meta['proxy']

        # A response is considered to be valid if:
        # * the status code is less than 400
        # * the response body is not empty
        # * the validator_func returns true
        validator_func = getattr(meta, 'dyno_proxy_validator', lambda _: True)
        is_valid = getattr(response, 'status', 500) < 400 and response.body.strip() and validator_func(response)

        if is_valid:
            self.reset_fail_cnt(proxy, spider)
        else:
            self.add_fail_cnt(proxy, spider)
            response.status = 400

        self._strip_meta(request.meta)
        return response

    def process_exception(self, request, exception, spider):
        if 'dyno_proxy_flag' in request.meta:
            proxy = request.meta['proxy']
            self.add_fail_cnt(proxy, spider)

        self._strip_meta(request.meta)
        return
