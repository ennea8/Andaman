# coding=utf-8
import scrapy

__author__ = 'zephyre'


class ProxyItem(scrapy.Item):
    host = scrapy.Field()
    port = scrapy.Field()
    scheme = scrapy.Field()
    desc = scrapy.Field()
    latency = scrapy.Field()
    verifiedTime = scrapy.Field()

    # discard: 删除该代理服务器
    # update_latency: 更新延迟
    # default:
    action = scrapy.Field()
