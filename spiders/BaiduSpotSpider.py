__author__ = 'zwh'
import json
import copy
import re
import pymongo
from scrapy.contrib.spiders import CrawlSpider
from scrapy import Request, Selector
from items import BaiduTripItem


class BaiduTripSpider(CrawlSpider):
    name = 'BaiduTripSpider'

    def __init__(self, *a, **kw):
        super(BaiduTripSpider, self).__init__(*a, **kw)

    def start_requests(self):
        locality_col = pymongo.MongoClient().geo.Locality
        city_list = list(locality_col.find({"level": 2}, {"pinyin": 1}))
        for row in city_list:
            m = {}
            city_pinyin = row['pinyin'][0]
            m = {'scene_name': city_pinyin, "page": 1}
            url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=1' % city_pinyin
            yield Request(url=url, callback=self.parse_city, meta={'cityInfo': m})

    def parse_city(self, response):
        prov = response.meta['cityInfo']
        data = json.loads(response.body, encoding='utf-8')["data"]
        scene_list = data["scene_list"]
        if scene_list:
            for scene in scene_list:
                mm = {}
                mm["page"] = 1
                mm["scene_name"] = scene["surl"]
                url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=%d' % (
                    scene["surl"], mm["page"])
                yield Request(url=url, callback=self.parse_city, meta={'cityInfo': mm})
                # if scene_list:
            m = copy.deepcopy(prov)
            m["page"] += 1
            url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=%d' % (
                m["scene_name"], m["page"])
            yield Request(url=url, callback=self.parse_city, meta={'cityInfo': m})
        else:
            m = copy.deepcopy(prov)
            scene_meta = {"sid": data["sid"], "parent_sid": data["parent_sid"], "surl": data["surl"],
                          "sname": data["sname"], "place_name": data["place_name"],
                          "ambiguity_sname": data["ambiguity_sname"], "scene_layer": data["scene_layer"],
                          "ext": data["ext"], "content": data["content"], "scene_path": data["scene_path"],
                          "nav": data["nav"], "rating": data["rating"], "rating_count": data["rating_count"],
                          "scene_total": data["scene_total"]}
            scene_url = 'http://lvyou.baidu.com/%s/fengjing' % (m["scene_name"])
            yield Request(url=scene_url, callback=self.parse_scene, meta={"scene_meta": scene_meta})


    def parse_scene(self, response):
        tmp_info = copy.deepcopy(response.meta["scene_meta"])
        sel = Selector(response)
        items = sel.xpath('//div[@id="J_photo-wrapper"]/ul/li/a/img/@src').extract()
        if items:
            imgurl_list = []
            for img_url in items:
                temp = re.search(r'/([0-9a-f]+)\.jpg', img_url)
                if temp:
                    url = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % temp.groups()[0]
                    imgurl_list.append(url)
            tmp_info["scene_img"] = imgurl_list
        else:
            tmp_info["scene_img"] = None
        yield Request(url='http://lvyou.baidu.com/%s/ditu' % tmp_info["surl"],
                      callback=self.parse_scene_map, meta={"tmp_info": tmp_info})


    def parse_scene_map(self, response):
        item = BaiduTripItem()
        scene_info = copy.deepcopy(response.meta["tmp_info"])
        sel = Selector(response)
        items = sel.xpath(
            '//div[@class="mod-scene-view-map"]/div[@class="public-group-slider"]/ul/li/a/span/img/@src').extract()
        if items:
            scene_map_list = []
            for img_url in items:
                tmp = re.search(r'/([0-9a-f]+)\.jpg', img_url)
                if tmp:
                    url = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % tmp.groups()[0]
                    scene_map_list.append(url)
            scene_info["scene_map"] = scene_map_list
        else:
            scene_info["scene_map"] = None
        item["scene_info"] = scene_info
        yield item


class BaiduTripPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduTripItem):
            return item
        scene = item["scene_info"]
        scene["ext"]["more_desc"].strip()
        scene["ext"]["abs_desc"].strip()
        scene["ext"]["sketch_desc"].strip()
        scene_entry = ({"Scene": scene})
        col = pymongo.MongoClient('localhost', 27017).geo.BaiduTrip
        entry_exist = col.find_one({'Scene.sid': scene["sid"]})
        if entry_exist:
            scene_entry['_id'] = entry_exist['_id']
        col.save(scene_entry)
        return item