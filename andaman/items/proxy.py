# coding=utf-8
import scrapy

__author__ = 'zephyre'


class ProxyItem(scrapy.Item):
    # 主机地址
    host = scrapy.Field()

    # 端口
    port = scrapy.Field()

    # http或者https
    scheme = scrapy.Field()

    # 描述
    desc = scrapy.Field()

    # 验证代理服务器的来源，比如：baidu等
    validate_by = scrapy.Field()

    # 验证的时间
    validate_time = scrapy.Field()

    # 代理服务器的延迟
    latency = scrapy.Field()

    # discard: 删除该代理服务器
    # update_latency: 更新延迟
    # default:
    action = scrapy.Field()
