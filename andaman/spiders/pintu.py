# coding=utf-8
import json
import logging
import scrapy

from andaman.items.pintu import PintuItem

class PintuSpider(scrapy.Spider):
    name = "pintu"
    allowed_domains = ["pintour.com"]

    def start_requests(self):
        url = "http://www.pintour.com/list/0-0-0-0-2-1-s-0_1"
        yield scrapy.Request(url)

    def parse(self, response):
        pass