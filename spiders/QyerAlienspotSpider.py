# coding: utf-8
__author__ = 'zwh'
import copy
import re
import json

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider
import pymongo

from items import QyerAlienpoiItem


class QyerAlienSpotSpider(CrawlSpider):
    name = 'qyer_alien'

    def __init__(self, *a, **kw):
        super(QyerAlienSpotSpider, self).__init__(*a, **kw)
        self.param = {}

    def start_requests(self):
        if 'param' in dir(self):
            self.param = getattr(self, 'param')

        continent_list = ["asia", "europe", "africa", "north-america", "south-america",
                          "oceania"] if 'region' not in self.param else [self.param['region'][0].lower()]

        for continent in continent_list:
            url = 'http://place.qyer.com/%s' % continent
            yield Request(url=url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        sel = Selector(response)
        lines = sel.xpath('//div[@id="allcitylist"]/div[contains(@class,"line")]/ul')
        for line in lines:
            items = line.xpath('./li')
            for item in items:
                country_tmp = item.xpath('./a/@href').extract()
                if country_tmp:
                    country_url = country_tmp[0]
                country = item.xpath('./a/text()').extract()

                hot = False
                if country:
                    country_name = country[0]
                    hot = False

                eng_name = item.xpath('./a/span/text()').extract()
                country_engname = eng_name[0].lower() if eng_name else None

                country_tmp = item.xpath('./p[@class="hot"]/a/@href').extract()
                if country_tmp:
                    country_url = country_tmp[0]
                country = item.xpath('./p[@class="hot"]/a/text()').extract()
                if country:
                    country_name = country[0]
                    hot = True
                eng_name = item.xpath('./p[@class="hot"]/a/span/text()').extract()
                if eng_name:
                    country_engname = eng_name[0]

                # if 'country' in self.param:
                # country_param = self.param['country'][0].lower()
                # if not country_engname or country_engname != country_param:
                # continue

                if not country_name:
                    continue

                sights_url = country_url + 'sight'
                m = {"country_name": country_name, "country_url": country_url, "country_popular": hot,
                     "country_engname": country_engname, "sights_url": sights_url}
                yield Request(url=sights_url, callback=self.parse_countrysights, meta={"country": m})

    def parse_countrysights(self, response):
        sel = Selector(response)
        country = response.meta["country"]
        sights = sel.xpath('//div[@id="allpoiContent"]/div[@id="poilistdiv"]/ul[contains(@class,"plaPoiListB")]/li')
        for sight in sights:
            country_info = copy.deepcopy(country)
            m = {"country_info": country_info}
            tmp = sight.xpath('./p/a/@href').extract()
            if tmp:
                m["poi_url"] = tmp[0]
                m["poi_id"] = re.search(r'/([0-9]+)/', m["poi_url"]).groups()[0]
            tmp = sight.xpath('./p/a/img/@src').extract()
            if tmp:
                m["poi_cover"] = tmp[0]
            else:
                m["poi_cover"] = None
            tmp = sight.xpath('./h3/a/text()').extract()
            if tmp:
                m["poi_name"] = tmp[0]
            tmp = sight.xpath('./div/p[@class="score"]/text()').extract()
            if tmp:
                tmp = tmp[0]
                match = re.search('^\s*\d+(\.\d+)?', tmp)
                if match:
                    m["poi_score"] = float(match.group())
            else:
                m["poi_score"] = None
            yield Request(url=m["poi_url"], callback=self.parse_poi, meta={"poi_info": m})
        tmp = sel.xpath(
            '//div[@id="place_memu_fix"]/div/div[@class="pla_topbtns"]/a[@class="ui_button yelp"]/@onclick').extract()
        if tmp:
            # cid = re.search(r'[0-9]+',tmp[0]).group()
            country_id = int(re.search(r'[0-9]+', tmp[0]).group())
        tmp = sel.xpath('//div/ul[@id="tab"]/li/a[@data-id="allpoiContent"]/text()').extract()
        if tmp:
            num = int(re.search(r'[0-9]+', tmp[0]).group())
            pagenum = int(num / 16)
            if pagenum > 1:
                for page in range(pagenum):
                    country_info = copy.deepcopy(country)
                    body = 'action=ajaxpoi&page=%d&pagesize=16&id=%d&typename=country&cateid=32&orderby=0&tagid=0' % (
                        page + 2, country_id)
                    yield Request(url="http://place.qyer.com/ajax.php", method='POST', body=body,
                                  headers={'Content-Type': 'application/x-www-form-urlencoded',
                                           'X-Requested-With': 'XMLHttpRequest'},
                                  callback=self.parse_list, meta={"country": country_info}, dont_filter=True)

    def parse_list(self, response):
        country = response.meta["country"]
        data = json.loads(response.body)
        sel = Selector(text=data['data']['html'])
        sights = sel.xpath('//ul/li')
        for sight in sights:
            country_info = copy.deepcopy(country)
            m = {"country_info": country_info}
            tmp = sight.xpath('./p/a/@href').extract()
            if tmp:
                m["poi_url"] = tmp[0]
                m["poi_id"] = re.search(r'/([0-9]+)/', m["poi_url"]).groups()[0]
            else:
                m["poi_url"] = None
                m["poi_id"] = None
            tmp = sight.xpath('./p/a/img/@src').extract()
            if tmp:
                m["poi_cover"] = tmp[0]
            else:
                m["poi_cover"] = None
            tmp = sight.xpath('./h3/a/text()').extract()
            if tmp:
                m["poi_name"] = tmp[0]
            tmp = sight.xpath('./div/p[@class="score"]/text()').extract()
            if tmp:
                m["poi_score"] = tmp[0]
            else:
                m["poi_score"] = None
            if m["poi_url"]:
                yield Request(url=m["poi_url"], callback=self.parse_poi, meta={"poi_info": m})

    def parse_poi(self, response):
        sel = Selector(response)
        poi_info = response.meta["poi_info"]
        tmp = sel.xpath('//div[@class="pla_topbars"]/div/div/div[@class="pla_topbar_names"]/p/a/text()').extract()
        if tmp:
            poi_info["poi_englishName"] = tmp[0]
        tmp = sel.xpath('//div[@class="pla_main"]/div[@id="summary_fixbox"]/div[@id="summary_box"]/p/text()').extract()
        if tmp:
            poi_info["poi_summary"] = tmp[0]
        detail_list = sel.xpath(
            '//div[contains(@class,"pla_wrap")]/div[@class="pla_main"]/div/ul[@class="pla_textdetail_list"]/li')
        detail = []
        for tem in detail_list:
            tmp = tem.xpath('./span/text()').extract()
            if tmp:
                tit = tmp[0]
            else:
                tit = None
            tmp = tem.xpath('./div/p/text()').extract()
            if tmp:
                content = tmp[0]
            else:
                tmp = tem.xpath('./div/a/text()').extract()
                if tmp:
                    content = tmp[0]
                else:
                    content = None
            detail.append({"title": tit, "content": content})
        poi_info["poi_detail"] = detail
        poi_url = poi_info["poi_url"]
        poi_photo_url = poi_url + "/photo"
        yield Request(url=poi_photo_url, callback=self.parse_photo, meta={"poi_info": poi_info})

    def parse_photo(self, response):
        item = QyerAlienpoiItem()
        sel = Selector(response)
        poi_info = response.meta["poi_info"]
        item["country_info"] = poi_info["country_info"]
        item["poi_url"] = poi_info["poi_url"]
        item["poi_id"] = int(poi_info["poi_id"])
        item["poi_cover"] = poi_info["poi_cover"]
        item["poi_name"] = poi_info["poi_name"]
        item["poi_score"] = poi_info["poi_score"]
        item["poi_englishName"] = poi_info["poi_englishName"]
        item["poi_summary"] = poi_info["poi_summary"]
        item["poi_detail"] = poi_info["poi_detail"]
        poi_photo = sel.xpath('//div/ul[@class="pla_photolist clearfix"]/li/p[@class="pic"]/a/img/@src').extract()
        if poi_photo:
            item["poi_photo"] = []
            for url in poi_photo:
                item["poi_photo"].append(url[:-7])
        yield item


class QyerAlienPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, QyerAlienpoiItem):
            return item
        col = pymongo.Connection().raw_data.QyerAliensSpot
        data = col.find_one({'poi_id': item['poi_id']})
        if not data:
            data = {}
        data['poi_id'] = item['poi_id']
        for key in item.keys():
            data[key] = item[key]
        col.save(data)
        return item