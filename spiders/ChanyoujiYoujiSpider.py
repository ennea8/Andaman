__author__ = 'wdx'

import json
import re

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiYoujiItem


class ChanyoujiYoujiSpider(CrawlSpider):
    name = "chanyouji_youji"

    def start_requests(self):
        template_url = "http://chanyouji.com/trips/%d"
        for trips_id in range(1, 400000):
            url = template_url % trips_id
            m = {'trips_id': trips_id, 'url': url}
            yield Request(url=url, callback=self.parse, meta={"data": m})

    def parse(self, response):

        items = []
        sel = Selector(response)
        item = ChanyoujiYoujiItem()
        trips_data = response.meta['data']
        url = trips_data['url']
        item['trips_id'] = trips_data['trips_id']
        path = sel.xpath('//div/script/text()').extract()
        text = re.compile("TripsCollection[\s\S]*}]")
        for content in path:
            m1 = text.search(content)
            if m1:
                contents = m1.group()
                data = contents[16:]
                try:
                    data = json.loads(data)
                    item['data'] = data

                except:
                    item['data'] = None

        items.append(item)
        return items













