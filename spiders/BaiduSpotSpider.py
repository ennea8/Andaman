__author__ = 'zwh'
import json
import copy
import re
import pymongo
from scrapy.contrib.spiders import CrawlSpider
from scrapy import Request, Selector
from items import BaiduTripItem


class BreadTripSpider(CrawlSpider):
    name = 'baidu_scene_spider'

    def __init__(self, *a, **kw):
        super(BreadTripSpider, self).__init__(*a, **kw)

    def start_requests(self):
        locality_col = pymongo.MongoClient().geo.Locality
        city_list = list(locality_col.find({"level": 2}, {"pinyin": 1}))
        for row in city_list:
            m = {}
            scene_all_lists = []
            city_pinyin = row['pinyin'][0]
            m = {'scene_name': city_pinyin, "scene_all_lists": scene_all_lists, "page": 1}
            url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=1' % city_pinyin
            yield Request(url=url, callback=self.parse_city, meta={'cityInfo': m})

    def parse_city(self, response):
        prov = response.meta['cityInfo']
        data = json.loads(response.body, encoding='utf-8')["data"]
        scene_list = data["scene_list"]
        # scene_total = data["scene_total"]
        if scene_list:
            m = copy.deepcopy(prov)
            for scene_abstact in scene_list:
                m["scene_all_lists"].append(scene_abstact["surl"])
            m["page"] += 1
            url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=%d' % (
            m["scene_name"], m["page"])
            yield Request(url=url, callback=self.parse_city, meta={'cityInfo': m})
        else:
            m = copy.deepcopy(prov)
            scene_meta = {"sid": data["sid"], "parent_sid": data["parent_sid"], "surl": data["surl"],
                          "sname": data["sname"],
                          "ext": data["ext"], "content": data["content"], "scene_path": data["scene_path"],
                          "nav": data["nav"],
                          "scene_all_lists": m["scene_all_lists"]}
            scene_url = 'http://lvyou.baidu.com/%s/fengjing' % (m["scene_name"])
            yield Request(url=scene_url, callback=self.parse_scene, meta={"scene_meta": scene_meta})

    def parse_scene(self, response):
        item = BaiduTripItem()
        scene_info = response.meta["scene_meta"]
        scene_all_lists = scene_info["scene_all_lists"]
        item["scene_info"] = scene_info
        sel = Selector(response)
        items = sel.xpath('//div[@id="J_photo-wrapper"]/ul/li/a/img/@src').extract()
        if items:
            imgurl_list = []
            for img_url in items:
                m = re.search(r'/([0-9a-f]+)\.jpg', img_url)
                if m:
                    url = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % m.groups()[0]
                    imgurl_list.append(url)
            item["scene_img"] = imgurl_list
        else:
            item["scene_img"] = None
        yield item
        for scene in scene_all_lists:
            mm = {}
            mm["page"] = 1
            mm["scene_name"] = scene
            mm["scene_all_lists"] = []
            url = 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&surl=%s&cid=0&pn=%d' % (
            scene, mm["page"])
            yield Request(url=url, callback=self.parse_city, meta={'cityInfo': mm})


class BaiduTripPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduTripItem):
            return item

        scene_img_list = item["scene_img"]
        scene = item["scene_info"]
        scene_entry = ({"Scene": scene, "SceneImg": scene_img_list})
        col = pymongo.MongoClient('localhost', 27017).geo.BaiduTrip
        entry_exist = col.find_one({'Scene.sid': scene["sid"]})
        if entry_exist:
            scene_entry['_id'] = entry_exist['_id']
        col.save(scene_entry)

        return item

