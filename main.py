# coding=utf-8

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy import log

from andaman.spiders.proxy import YoudailiSpider


__author__ = 'zephyre'


def spider_closing(self):
    """Activates on spider closed signal"""
    log.msg("Closing spider", level=log.INFO)
    reactor.stop()


def main():
    from scrapy.utils.log import configure_logging

    configure_logging()
    runner = CrawlerRunner()
    d = runner.crawl(YoudailiSpider)
    d.addBoth(lambda _: reactor.stop())

    reactor.run()


if __name__ == "__main__":
    main()
