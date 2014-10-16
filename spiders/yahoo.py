# encoding=utf-8
import json

__author__ = 'lxf'

import re

from scrapy import Request, Selector, Item, Field, log
from scrapy.contrib.spiders import CrawlSpider

from utils import get_mongodb


# -----------------------------------define field------------------------
class YahooCityItem(Item):
    country = Field()  # 国家信息
    state = Field()  # 州/省份
    city = Field()  # 城市
    coords = Field()
    woeid = Field()
    abroad=Field()

# ----------------------------------define spider------------------------------------
class YahooCitySpider(CrawlSpider):
    name = 'citynamespider'  # define the spider name

    # ---------------------------draw the url-----------------------------------------
    def start_requests(self):  # send request
        country_list = []
        if 'param' in dir(self):
            param = getattr(self, 'param', [])
            if 'country' in param:
                country_list = param['country']

        if not country_list:
            country_list = list([tmp['code'] for tmp in get_mongodb('geo', 'Country').find({}, {'code': 1})])

        first_url = 'https://weather.yahoo.com/'
        for country_code in country_list:
            abroad = (country_code.lower() == 'cn')
            temp_url = first_url + country_code
            data = {'countrycode': country_code, 'abroad': abroad}
            yield Request(url=temp_url, callback=self.parse_state_url, meta={'data': data})

    # ------------------------draw the state url-------------------------------------
    def parse_state_url(self, response):  # draw the state
        sel = Selector(response)
        tempcountryname = sel.xpath(
            '//div[@id="MediaWeatherRegion"]/div[@class="hd"]/div[@class="yom-bread"]/text()').extract()
        match = re.search(r'[\w\s]+$', tempcountryname[0])
        if match:
            countryname = match.group().strip()
        else:
            self.log('没有国家名', log.WARNING)
            return


        state_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # state list
        url_state_list = sel.xpath('//div[@id="page1"]/ul/li/a/@href').extract()  # url for state_list
        data_1 = response.meta['data']

        if state_list:
            for i in range(0, len(state_list)):
                state_url = 'https://weather.yahoo.com' + url_state_list[i]
                data = {'data_1': data_1, 'countryname': countryname, 'state': state_list[i]}
                yield Request(url=state_url, callback=self.parse_city, meta={'data': data})
        else:
            return

    # ------------------------draw the city url-------------------------------------
    def parse_city(self, response):
        sel = Selector(response)
        city_list = sel.xpath('//div[@id="page1"]/ul/li/a/span/text()').extract()  # city list
        city_list_url = sel.xpath('//div[@id="MediaWeatherRegion"]/div[@class="bd"]/'
                                  'div[@class="weather-regions"]/div/ul/li/a/@href').extract()
        data_2 = response.meta['data']
        if city_list:
            for i in range(0, len(city_list)):
                city_url = 'https://weather.yahoo.com' + city_list_url[i]
                data = {'data_2': data_2, 'cityname': city_list[i], 'city_url': city_url}
                yield Request(url=city_url, callback=self.parse_city_info, meta={'data': data})
        else:
            return

    def parse_city_info(self, response):
        sel = Selector(response)
        city_url = response.meta['data']['city_url']
        match = re.search(r'\d{1,}', city_url)
        if match:
            woeid = int(match.group())
        else:
            woeid = None

        match = re.search(r'Y\.Media\.Location\.SearchAssist\((.+?)\)', response.body)
        if match:
            loc_data = json.loads(match.groups()[0])
            coords = {'lat': float(loc_data['lat']), 'lng': float(loc_data['lon'])}
        else:
            coords = None
        data_3 = response.meta['data']
        country = {'countrycode': data_3['data_2']['data_1']['countrycode'],
                   'countryname': data_3['data_2']['countryname']}
        item = YahooCityItem()
        item['country'] = country
        item['state'] = data_3['data_2']['state']
        item['city'] = data_3['cityname']
        item['abroad'] = data_3['data_2']['data_1']['abroad']
        if coords:
            item['coords'] = coords
        item['woeid'] = woeid
        return item
        # -----------------------pipeline--------------------------------------------------


class YahooCityPipeline(object):
    # 向pipline注册
    spiders = [YahooCitySpider.name]

    def process_item(self, item, spider):
        data = {}
        if 'country' in item:
            data['country'] = item['country']
        if 'state' in item:
            data['state'] = item['state']
        if 'city' in item:
            data['city'] = item['city']
        if 'abroad' in item:
            data['abroad'] = item['abroad']
        if 'coords' in item:
            data['coords'] = item['coords']
        if 'woeid' in item:
            data['woeid'] = item['woeid']
        col = get_mongodb('raw_data', 'CityInfo', profile='mongo-crawler')
        col.save(data)
        return item