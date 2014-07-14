# coding=utf-8
import copy
import json
from scrapy import Request
from scrapy.contrib.spiders import CrawlSpider
from items import WeatherItem


class WeatherSpider(CrawlSpider):
    prov_name = ["北京", "上海", "天津", "重庆", "黑龙江", "吉林", "辽宁", "内蒙古", "河北", "山西",
                 "陕西", "山东", "新疆", "西藏", "青海", "甘肃", "宁夏", "河南", "江苏", "湖北", "浙江", "安徽", "福建", "江西", "湖南", "贵州", "四川", "广东",
                 "云南", "广西", "海南", "香港", "澳门", "台湾"]
    prov_id = [10100 + tem + 1 for tem in xrange(34)]
    prov_list = {str(tem): 'http://bj.weather.com.cn/data/city3jdata/provshi/%s.html'
                      % str(tem) for tem in prov_id}


    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(WeatherSpider, self).__init__(*a, **kw)


    def start_requests(self):
        for prov_code, url in self.prov_list.items():
            m = {}
            m['prov_code'] = prov_code
            m['prov_name'] = self.prov_name[int(prov_code)-10101]
            yield Request(url=url, callback=self.parse_prov,
                          meta={'WeatherData': m})


    def parse_prov(self, response):
        prov = response.meta['WeatherData']
        # {"01":"北京"}
        data = json.loads(response.body)

        for city_code, city_name in data.items():
            m = copy.deepcopy(prov)
            m['city_code'] = city_code
            m['city_name'] = city_name
            yield Request(url='http://bj.weather.com.cn/data/city3jdata/station/%s%s.html'
                              % (m['prov_code'], m['city_code']),
                          callback=self.parse_city,
                          meta={'WeatherData': m})

    def parse_city(self, response):
        city = response.meta['WeatherData']
        # {'prov_code':10101, 'city_code':01}  1010101
        # {"02":"兴国县"}
        data = json.loads(response.body, encoding='utf-8')

        for county_code, county_name in data.items():
            m = copy.deepcopy(city)
            m['county_code'] = county_code
            m['county_name'] = county_name
            yield Request(url='http://m.weather.com.cn/data/%s%s%s.html'
                              % (m['prov_code'], m['city_code'], m['county_code']),
                          callback=self.parse_final,
                          meta={'WeatherData': m})

    def parse_final(self, response):
        allInf = response.meta['WeatherData']
        item = WeatherItem()
        item['data'] = json.loads(response.body, encoding='utf-8')

        item['province'] = allInf['prov_name']
        item['city'] = allInf['city_name']
        item['county'] = allInf['county_name']

        item['id'] = '%s%s%s' % (allInf['prov_code'], allInf['city_code'],
                  allInf['county_code'])
        yield item



