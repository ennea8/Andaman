import re

__author__ = 'zwh'
import json
import copy
import pymongo
import scrapy
from scrapy.contrib.spiders import CrawlSpider
from scrapy import Request, Selector
from scrapy import log


class BreadTripItem(scrapy.Item):
        city_name = scrapy.Field()
        city_id = scrapy.Field()
        trip_info = scrapy.Field()
        blog = scrapy.Field()
        trip_days = scrapy.Field()


class BreadTripSpider(CrawlSpider):
    name = 'BreadTripSpider'
    def __init__(self, *a, **kw):
        super(BreadTripSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url="http://breadtrip.com/destinations", callback=self.parse_city)

    def parse_city(self, response):
        sel=Selector(response)
        for item in sel.xpath('//div[@id="domestic-dest-popup"]//div[@class="level-2 float-left"]/a'):
              m = {}
              next_url = item.xpath('./@href').extract()
              m["city_name"] = item.xpath('./span[@class = "ellipsis_text"]/text()').extract()[0]

              yield Request(url="http://breadtrip.com/%s" % next_url[0],
                            callback=self.parse_trip,meta={'cityInfo': m})

    def parse_trip(self,response):
        prov = response.meta['cityInfo']
        sel=Selector(response)
        m = copy.deepcopy(prov)
        city_id_list = sel.xpath('//div[@id="content"]/@data-id').extract()
        m["city_id"] = city_id_list
        m["trips_list"] = []
        yield Request(url="http://breadtrip.com/scenic/3/%s/trip/more/?next_start=0" % city_id_list[0],
                      callback=self.parse_intro,meta={'cityInfo': m})

    def parse_intro(self,response):
        prov = response.meta['cityInfo']
        city_id = prov["city_id"]
        data = json.loads(response.body, encoding='utf-8')
        if data["more"]:
            next_start = data["next_start"]
            for trip_dict in data["trips"]:
                prov["trips_list"].append(trip_dict)
            yield Request(url="http://breadtrip.com/scenic/3/%s/trip/more/?next_start=%d" % (city_id,next_start),
                          callback=self.parse_intro, meta ={'m_cycle': prov})
        else:
            for trip_dict in data["trips"]:
                prov["trips_list"].append(trip_dict)
            for trip in prov["trips_list"]:
                day_count=trip["day_count"]
                if day_count<10:
                    Info = {}
                    Info["day_count"] = day_count
                    Info["trip_info"] = trip
                    Info["city_name"] = prov["city_name"]
                    Info["city_id"] = city_id
                    encrypt_id = trip["encrypt_id"]
                    yield Request(url = "http://breadtrip.com/trips/%d/" % encrypt_id,
                                  callback=self.parse_blog,meta ={'tripsInfo': Info})

    def parse_blog(self, response):
        allInf = response.meta['tripsInfo']
        item = BreadTripItem()
        item["city_name"] = allInf["city_name"]
        item["city_id"] = allInf["city_id"]
        item["trip_info"] = allInf["trip_info"]
        item["trip_days"] = allInf["day_count"]
        sel = Selector(response)
        alltravles = []
        for day_node in sel.xpath('//div[@class="trip-wps"]/div[@class="trip-days" and @id]'):
            # tmp = day_node.xpath('./@id').extract()[0]
            # matcher = re.search(r'(\d+)$', tmp)
            # if not matcher:
            #     continue
            # day_idx = int(matcher.groups()[0])
            blogs_list=[]
            for wp_node in day_node.xpath('./div[@class="waypoint "]'):
                url = wp_node.xpath('./div[@class="photo-ctn"]/a/@href').extract()[0]
                url_list = url.split('?')
                spot_img = url_list[0]
                blog = wp_node.xpath('./div[@class="photo-ctn"]/a/@data-caption').extract()
                if blog:
                    spot_record = blog[0]
                else:
                    spot_record = None
                like = wp_node.xpath('./div[@class="stats-wrapper"]/div[@class="wp-stats fn-clear"]/'
                                     'div[@class="wp-btns float-right"]/a/@data-time').extract()[0]
                time = wp_node.xpath('./div[@class="stats-wrapper"]/div[@class="wp-stats fn-clear"]/'
                                     'div[@class="time float-left"]/text()').extract()[0]
                href_next = wp_node.xpath('./div[@class="stats-wrapper"]/div[@class="wp-stats fn-clear"]/'
                                          'a/@href').extract()
                if href_next:
                    spot_href = href_next[0]
                else:
                    spot_href = None
                locality = wp_node.xpath('./div[@class="stats-wrapper"]/div[@class="wp-stats fn-clear"]/'
                                         'a[@class="location location-icon float-left"]/'
                                         'span/text()').extract()
                if locality:
                    spot_locality = locality[0]
                else:
                    locality = wp_node.xpath('./div[@class="stats-wrapper"]/div[@class="wp-stats fn-clear"]/'
                                             'a[@class="wp-poi-name float-left"]/span/text()').extract()
                    if locality:
                        spot_locality = locality[0]
                    else:
                        spot_locality = None
                spot_blog = {'spot_travel_time': time, 'spot_locality': spot_locality, 'spot_locality_href': spot_href,
                             'spot_img': spot_img, 'spot_record': spot_record, 'like_num': like}
                blogs_list.append(spot_blog)
            alltravles.append(blogs_list)
        item["blog"] = alltravles
        yield item

class BreadTripPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.geo

    def process_item(self, item, spider):
        if not isinstance(item, BreadTripItem):
            return item
        city_name = item["city_name"]
        city_id = item["city_id"]
        trips_info = item["trip_info"]
        blog = item["blog"]
        trip_days = item["trip_days"]
        trip_entry = ({'city_name': city_name, 'city_id': city_id[0], 'trips_info': trips_info,
                                'trip_days_num': trip_days, 'blog': blog})
        entry_exist = self.db.BreadTrip.find_one({'city_id':city_id[0]})
        if entry_exist:
            trip_entry['_id'] = entry_exist['_id']
        self.db.BreadTrip.save(trip_entry)
        return item
