# -*- coding: utf-8 -*-

# Scrapy settings for TravelCrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import scrapy
BOT_NAME = 'TravelCrawler'

SPIDER_MODULES = ['TravelCrawler.spiders']
NEWSPIDER_MODULE = 'TravelCrawler.spiders'

#ITEM_PIPELINES = {'scrapy.contrib.pipeline.images.ImagesPipeline': 1}
#ITEM_PIPELINES = {'pipelines.MofengwoPipeline': 800}
#IMAGES_STORE = './images/data'

DOWNLOADER_MIDDLEWARES = {
    #'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware'
}
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'TravelCrawler (+http://www.yourdomain.com)'
