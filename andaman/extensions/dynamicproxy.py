# coding=utf-8
__author__ = 'zephyre'


class DynamicProxyExtension(object):
    def __init__(self, item_count):
        self.item_count = item_count
        self.items_scraped = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if settings.getbool('DYNAMIC_PROXY_ENABLED', False):
            # 404也应该添加到重试错误代码中
            codes = set(settings.getlist('RETRY_HTTP_CODES', []))
            codes.add(401)
            codes.add(403)
            codes.add(404)
            settings.set('RETRY_HTTP_CODES', list(codes))
