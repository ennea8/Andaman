import pymongo

__author__ = 'wdx'

import json
import re

from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiYoujiItem


class ChanyoujiYoujiSpider(CrawlSpider):
    name = "chanyouji_note"

    def start_requests(self):
        template_url = "http://chanyouji.com/trips/%d"

        lower = 1
        upper = 400000
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])

        for trips_id in range(lower, upper):
            url = template_url % trips_id
            m = {'trips_id': trips_id, 'url': url}
            yield Request(url=url, callback=self.parse, meta={"data": m})

    def parse(self, response):
        item = ChanyoujiYoujiItem()
        item['trips_id'] = response.meta['data']['trips_id']

        match = re.search(
            r'_G_trip_collection\s*=\s*new\s*tripshow\.TripsCollection\((?=\[)(.+?),\s*\{\s*parse\s*:\s*(true|false)',
            response.body)
        if not match:
            return
        try:
            item['data'] = json.loads(match.groups()[0])
        except ValueError:
            return

        yield item


class ChanyoujiYoujiPipline(object):
    def process_item(self, item, spider):
        if not isinstance(item, ChanyoujiYoujiItem):
            return item

        col = pymongo.Connection().raw_data.ChanyoujiNote
        note = {'noteId': item['trips_id'], 'note': item['data']}
        ret = col.find_one({'noteId': note['noteId']})
        if not ret:
            ret = {}
        for k in note:
            ret[k] = note[k]
        col.save(ret)

        return item










