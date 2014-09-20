__author__ = 'zwh'
import json
import copy

import pymongo

import scrapy
from scrapy.contrib.spiders import CrawlSpider
from scrapy import Request, Selector
from items import BreadTripItem

class BreadTripSpider(CrawlSpider):
    name = 'breadtrip_spider'

    def __init__(self, *a, **kw):
        super(BreadTripSpider, self).__init__(*a, **kw)
        self.item_class = BreadTripItem

    def start_requests(self):
        yield Request(url="http://breadtrip.com/destinations", callback=self.parse_city)

    def parse_city(self, response):
        sel = Selector(response)

        for item in sel.xpath('//div[@id="domestic-dest-popup"]//div[contains(@class, "level-2")]/a'):
            m = {}
            next_url = item.xpath('./@href').extract()
            m["city_name"] = item.xpath('./span[@class = "ellipsis_text"]/text()').extract()[0]
            yield Request(url="http://breadtrip.com/%s" % next_url[0],
                          callback=self.parse_trip, meta={'cityInfo': m})

    def parse_trip(self, response):
        prov = response.meta['cityInfo']
        sel = Selector(response)
        m = copy.deepcopy(prov)
        city_id_list = sel.xpath('//div[@id="content"]/@data-id').extract()
        m["city_id"] = city_id_list[0]
        m["trips_list"] = []
        yield Request(url="http://breadtrip.com/scenic/3/%s/trip/more/?next_start=0" % city_id_list[0],
                      callback=self.parse_intro, meta={'cityInfo': m})

    def parse_intro(self, response):
        prov = response.meta['cityInfo']
        city_id = prov["city_id"]
        data = json.loads(response.body, encoding='utf-8')
        if data["more"]:
            next_start = data["next_start"]
            for trip_dict in data["trips"]:
                prov["trips_list"].append(trip_dict)
            yield Request(url="http://breadtrip.com/scenic/3/%s/trip/more/?next_start=%d" % (city_id, next_start),
                          callback=self.parse_intro, meta={'cityInfo': prov})
        else:
            for trip_dict in data["trips"]:
                prov["trips_list"].append(trip_dict)
            for trip in prov["trips_list"]:
                day_count = trip["day_count"]
                if day_count < 3:
                    Info = {}
                    Info["day_count"] = day_count
                    Info["trip_info"] = trip
                    Info["city_name"] = prov["city_name"]
                    Info["city_id"] = city_id
                    encrypt_id = trip["encrypt_id"]
                    yield Request(url="http://breadtrip.com/trips/%d/" % encrypt_id,
                                  callback=self.parse_blog, meta={'tripsInfo': Info})

    def parse_blog(self, response):
        allInf = response.meta['tripsInfo']
        item = BreadTripItem()
        item["city_name"] = allInf["city_name"]
        item["city_id"] = allInf["city_id"]
        item["trip_info"] = allInf["trip_info"]
        item["trip_days"] = allInf["day_count"]
        sel = Selector(response)
        item["user_url"] = sel.xpath('//div[@id="trip-info"]/a[contains(@class, "trip-user")]/@href').extract()[0]
        user_img_url = sel.xpath('//div[@id="trip-info"]/a[contains(@class, "trip-user")]/img/@src').extract()[0]
        img_url = user_img_url.split('-')
        item["user_img"] = img_url[0]
        alltravles = []
        for day_node in sel.xpath('//div[@class="trip-wps"]/div[@class="trip-days" and @id]'):
            # tmp = day_node.xpath('./@id').extract()[0]
            # matcher = re.search(r'(\d+)$', tmp)
            # if not matcher:
            # continue
            # day_idx = int(matcher.groups()[0])
            blogs_list = []
            for wp_node in day_node.xpath('./div[@class="waypoint "]'):
                url = wp_node.xpath('./div[@class="photo-ctn"]/a/@href').extract()[0]
                url_list = url.split('?')
                spot_img = url_list[0]
                blog = wp_node.xpath('./div[@class="photo-ctn"]/a/@data-caption').extract()
                if blog:
                    spot_record = blog[0]
                else:
                    spot_record = None
                like = wp_node.xpath('./div[@class="stats-wrapper"]/div[contains(@class,"wp-stats")]/'
                                     'div[@class="wp-btns float-right"]/a/@data-time').extract()[0]
                time = wp_node.xpath('./div[@class="stats-wrapper"]/div[contains(@class,"wp-stats")]/'
                                     'div[@class="time float-left"]/text()').extract()[0]
                href_next = wp_node.xpath('./div[@class="stats-wrapper"]/div[contains(@class,"wp-stats")]/'
                                          'a/@href').extract()
                if href_next:
                    spot_href = href_next[0]
                else:
                    spot_href = None
                locality = wp_node.xpath('./div[@class="stats-wrapper"]/div[contains(@class,"wp-stats")]/'
                                         'a[@class="location location-icon float-left"]/'
                                         'span/text()').extract()
                if locality:
                    spot_locality = locality[0]
                else:
                    locality = wp_node.xpath('./div[@class="stats-wrapper"]/div[contains(@class,"wp-stats")]/'
                                             'a[@class="wp-poi-name float-left"]/span/text()').extract()
                    if locality:
                        spot_locality = locality[0]
                    else:
                        spot_locality = None
                spot_blog = {'SpotTravelTime': time, 'SpotLocality': spot_locality, 'SpotLocalityUrl': spot_href,
                             'SpotImg': spot_img, 'SpotRecord': spot_record, 'SpotPraiseNum': like}
                blogs_list.append(spot_blog)

            alltravles.append(blogs_list)

        item["blog"] = alltravles
        yield item


class BreadTripPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BreadTripItem):
            return item

        city_name = item["city_name"]
        city_id = item["city_id"]
        trips_info = item["trip_info"]
        blog = item["blog"]
        trip_days = item["trip_days"]
        user_homepage_url = "http://breadtrip.com/%s/" % item["user_url"]
        user_head_img = item["user_img"]
        blog_url = "http://breadtrip.com/trips/%d" % trips_info["encrypt_id"]
        trip_entry = ({"Author": {'AuthorHomepageUrl': user_homepage_url, 'AuthorHeadImg': user_head_img},
                       'CityName': city_name, 'CityId': city_id, 'TripsInfo': trips_info,
                       'TripDaysNum': trip_days, 'TripBlog': blog, "TripBlogUrl": blog_url})
        col = pymongo.MongoClient('localhost', 27017).geo.BreadTrip
        entry_exist = col.find_one({'TripsInfo.encrypt_id': trips_info["encrypt_id"]})
        if entry_exist:
            trip_entry['_id'] = entry_exist['_id']
        col.save(trip_entry)

        return item
