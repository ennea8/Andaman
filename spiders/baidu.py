# coding=utf-8
import json
import os
import random
import re
import urlparse
import copy
import hashlib
import urllib2
import socket
import time

import MySQLdb
from MySQLdb.cursors import DictCursor

import pymongo
from scrapy import Request, Selector, log
from scrapy.contrib.spiders import CrawlSpider

from items import BaiduNoteItem, BaiduPoiItem, BaiduWeatherItem
import qiniu_utils


__author__ = 'zephyre'


class BaiduNoteSpider(CrawlSpider):
    name = 'baidu_note'

    def __init__(self, *a, **kw):
        super(BaiduNoteSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col = pymongo.Connection().geo.Locality
        url_base = 'http://lvyou.baidu.com/search/ajax/search?format=ajax&word=%s&pn=%d'
        ret_list = list(col.find({'level': 2}, {'zhName': 1}))
        for ret in ret_list:
            url = url_base % (ret['zhName'], 0)
            yield Request(url=url, callback=self.parse_loc,
                          meta={'target': ret['zhName'], 'pn': 0, 'urlBase': url_base})
            # yield Request(url='http://chanyouji.com/users/1', callback=self.parse)

            # yield Request(url='http://lvyou.baidu.com/notes/dc6b5c3d1354b88b3b405e2e/d-0', callback=self.parse,
            # meta={'pageIdx': 0, 'noteId': 'dc6b5c3d1354b88b3b405e2e',
            # 'urlBase': 'http://lvyou.baidu.com/notes/dc6b5c3d1354b88b3b405e2e/d-'})

    def parse_loc(self, response):
        target = response.meta['target']
        pn = response.meta['pn'] + 10
        try:
            data = json.loads(response.body)

            if data['data']['search_res']['notes_list']:
                # 读取下一页
                url_base = response.meta['urlBase']
                url = url_base % (target, pn)
                yield Request(url=url, callback=self.parse_loc, meta={'target': target, 'pn': pn, 'urlBase': url_base})

            url_base = 'http://lvyou.baidu.com/notes/%s/d-%d'
            for entry in data['data']['search_res']['notes_list']:
                url = entry['loc']
                m = re.search(r'/notes/([0-9a-f]+)', url)
                if not m:
                    continue
                note_id = m.groups()[0]
                url = url_base % (note_id, 0)
                title = entry['title']
                yield Request(url=url, callback=self.parse, meta={'title': title, 'target': target, 'pageIdx': 0,
                                                                  'noteId': note_id, 'urlBase': url_base,
                                                                  'note': {'url': url, 'summary': entry}})

        except (ValueError, KeyError, TypeError):
            pass

    def parse(self, response):
        note = response.meta['note'] if 'note' in response.meta else {}
        note_id = response.meta['noteId']
        page_idx = response.meta['pageIdx']
        sel = Selector(response)

        if page_idx == 0:
            note['id'] = response.meta['noteId']
            note['target'] = response.meta['target']
            note['title'] = response.meta['title']
            # # 标题
            # ret = sel.xpath('//span[@id="J_notes-title"]/text()').extract()
            # if ret:
            # tmp = ret[0].strip()
            # if tmp:
            # note['title'] = tmp

            ret = sel.xpath('//ul[@id="J_basic-info-container"]')
            info_node = ret[0] if ret else None
            if info_node:
                ret = info_node.xpath('./li/span[contains(@class, "author-icon")]/a')
                if ret:
                    user_node = ret[0]
                    note['authorName'] = user_node.xpath('./text()').extract()[0]
                    tmp = user_node.xpath('./@href').extract()[0]
                    m = re.compile(r'[^/]+$').search(tmp)
                    if m:
                        note['authorId'] = m.group()

                ret = info_node.xpath('./li//span[contains(@class, "start_time")]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    if tmp:
                        note['startTime'] = tmp

                ret = info_node.xpath('./li/span[contains(@class, "path-icon")]/span[@class="infos"]/text()').extract()
                if ret and len(ret) == 2:
                    tmp = ret[0].strip()
                    m = re.search(ur'^从(.+)', tmp)
                    if m:
                        note['fromLoc'] = m.groups()[0]
                    tmp = ret[1].strip()
                    if tmp:
                        note['toLoc'] = filter(lambda val: val and not re.match(ur'^\s*\.+\s*$', val),
                                               list(tmp.strip() for tmp in tmp.split(u'、')))
                ret = info_node.xpath('./li/span[contains(@class, "time-icon")]/span[@class="infos"]/text()').extract()
                if ret:
                    m = re.search(ur'(\d+)天', ret[0].strip())
                    if m:
                        note['timeCost'] = int(m.groups()[0])

                ret = info_node.xpath('./li/span[contains(@class, "cost-icon")]/span[@class="infos"]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    if tmp:
                        note['cost'] = tmp

                ret = info_node.xpath('./li[contains(@class, "notes-info-foot")]/div[@class="fl"]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    m = re.search(ur'回\s*复\s*(\d+)', tmp)
                    if m:
                        note['replyCnt'] = int(m.groups()[0])
                    m = re.search(ur'浏\s*览\s*(\d+)', tmp)
                    if m:
                        note['viewCnt'] = int(m.groups()[0])

        if 'contents' not in note:
            note['contents'] = []
        contents = note['contents']
        contents_list = sel.xpath(
            '//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]/div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]').extract()
        if contents_list:
            contents.extend(contents_list)

        # 是否存在下一页？
        tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
        if not tmp or not contents_list:
            url_t = 'http://lvyou.baidu.com/notes/%s-%d'
            url = url_t % (note['id'], len(note['contents']))
            yield Request(url=url, callback=self.parse_comments, meta={'urlT': url_t, 'note': note})
        else:
            page_idx += 1
            url_base = response.meta['urlBase']
            url = url_base % (note_id, page_idx)
            yield Request(url=url, callback=self.parse,
                          meta={'pageIdx': page_idx, 'noteId': note_id, 'urlBase': url_base, 'note': note})

    def parse_comments(self, response):
        note = response.meta['note']

        if 'comments' not in note:
            note['comments'] = []
        comments = note['comments']
        author = note['authorName']

        sel = Selector(response)

        node_list = sel.xpath('//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]')
        for node in node_list:
            ret = node.xpath('./div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]')
            if not ret:
                continue
            c_node = ret[0]
            ret = c_node.xpath('./@nickname').extract()
            if not ret or (ret[0] == author and not comments):
                continue
            c_author = ret[0]
            ret = c_node.xpath('./@uid').extract()
            if not ret:
                continue
            c_author_id = ret[0]

            tmp = c_node.extract()
            if tmp:
                comments.append({'authorName': c_author, 'authorId': c_author_id, 'comment': tmp})

        # 检查是否有下一页
        tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
        tmp = tmp[-1] if tmp else None
        if tmp:
            try:
                tmp = int(tmp)
            except ValueError:
                tmp = None

        if not tmp:
            tmp_href = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/@href').extract()
            if tmp_href:
                href = tmp_href[-1]
                parts = urlparse.urlparse(response.url)
                url = urlparse.urlunparse((parts[0], parts[1], href, '', '', ''))
                return Request(url=url, callback=self.parse_comments,
                               meta={'urlT': response.meta['urlT'], 'note': note})

        item = BaiduNoteItem()
        item['note'] = note
        return item


class BaiduNotePipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduNoteItem):
            return item

        col = pymongo.Connection().raw_notes.BaiduNote
        ret = col.find_one({'id': item['note']['id']}, {'_id': 1})
        if ret:
            item['note']['_id'] = ret['_id']

        col.save(item['note'])


class BaiduPoiSpider(CrawlSpider):
    name = 'baidu_poi'

    def __init__(self, *a, **kw):
        super(BaiduPoiSpider, self).__init__(*a, **kw)

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
        item = BaiduPoiItem()
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


class BaiduPoiPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduPoiItem):
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


class BaiduPoiImageSpider(CrawlSpider):
    name = 'baidu_poi_image'

    def __init__(self, *a, **kw):
        # self.name = 'baidu_poi_image'
        super(BaiduPoiImageSpider, self).__init__(*a, **kw)
        self.start = kw['start'] if 'start' in kw else 0
        self.count = kw['count'] if 'count' in kw else 0

    def start_requests(self):
        # yield Request(url='http://chanyouji.com/users/1', callback=self.parse)

        self.log('START: %d, COUNT: %d' % (self.start, self.count), level=log.INFO)

        conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='07996019Zh', db='vxp_restore_poi',
                               cursorclass=DictCursor, charset='utf8')
        cursor = conn.cursor()
        cursor.execute(
            'SELECT p1.surl, p1.sid FROM baidu_poi as p1 join qunar_city as p2 on p1.qid=p2.id where p2.abroad=0 LIMIT %d, %d' % (
                self.start, self.count))
        for row in cursor:
            py = row['surl']
            url = 'http://lvyou.baidu.com/%s/fengjing' % py
            yield Request(url=url, callback=self.parse, meta={'sid': row['sid']})

    def parse(self, response):
        sel = Selector(response)

        col_im = pymongo.Connection().imagestore.Image
        checksum_map = {}
        for img_url in sel.xpath('//ul[@id="photo-list"]/li[contains(@class, "photo-item")]/a/img/@src').extract():
            m = re.search(r'/([0-9a-f]+)\.jpg', img_url)
            if m:
                url = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % m.groups()[0]
                checksum_map[url] = hashlib.md5(url).hexdigest()

        ret = col_im.find({'url_hash': {'$in': checksum_map.values()}}, {'url_hash': 1})
        in_db_hash = set(tmp['url_hash'] for tmp in ret)
        for u, h in checksum_map.items():
            if h in in_db_hash:
                continue
            yield Request(url=u, callback=self.parse_image, meta={'sid': response.meta['sid']})

    def parse_image(self, response):
        if not re.match(r'^image/', response.headers['Content-Type']):
            return

        ext = os.path.splitext(response.url)[-1]
        checksum = hashlib.md5(response.url).hexdigest()
        fname = '%s%s' % (checksum, ext)
        key = 'assets/images/%s' % fname

        col_im = pymongo.Connection().imagestore.Image
        col_vs = pymongo.Connection().poi.ViewSpot

        im_entry = col_im.find_one({'url_hash': checksum})
        if not im_entry or 'fmt' not in im_entry:
            local_file = 'tmp/%s' % fname
            with open(local_file, 'wb') as f:
                f.write(response.body)

            up_suc = False
            stat_suc = False
            for try_cnt in xrange(3):
                try:
                    ret, err = qiniu_utils.upload('lvxingpai-img-store', key, local_file)
                    if err:
                        continue
                    up_suc = True
                    break
                except socket.error:
                    time.sleep(2)

            if not up_suc:
                return

            if im_entry:
                im_entry = {
                    "_id": im_entry['_id'],
                    "url_hash": checksum,
                    "url": response.url,
                    "key": key,
                    "ret_hash": ret['hash'],
                    "size": len(response.body)
                }
            else:
                im_entry = {
                    "url_hash": checksum,
                    "url": response.url,
                    "key": key,
                    "ret_hash": ret['hash'],
                    "size": len(response.body)
                }

            for try_cnt in xrange(3):
                try:
                    data = json.loads(
                        urllib2.urlopen('http://lvxingpai-img-store.qiniudn.com/%s?stat' % key, timeout=5).read())
                    im_entry['type'] = data['mimeType']
                    stat_suc = True
                    break
                except socket.error:
                    time.sleep(2)
                except (ValueError, KeyError):
                    return

            if not stat_suc:
                return
            else:
                stat_suc = False

            for try_cnt in xrange(3):
                try:
                    data = json.loads(
                        urllib2.urlopen('http://lvxingpai-img-store.qiniudn.com/%s?imageInfo' % key, timeout=5).read())
                    im_entry['fmt'] = data['format']
                    im_entry['cm'] = data['colorModel']
                    im_entry['w'] = data['width']
                    im_entry['h'] = data['height']
                    stat_suc = True
                    break
                except socket.error:
                    time.sleep(2)
                except (ValueError, KeyError):
                    return

            if not stat_suc:
                return

            col_im.save(im_entry)

        baidu_id = response.meta['sid']
        vs = col_vs.find_one({'source.baidu.id': baidu_id}, {'images': 1})
        if not vs:
            return

        images = vs['images']
        existed = False
        for tmp in images:
            if checksum in tmp['url']:
                existed = True
                break
        if not existed:
            try:
                images.append({'url': 'http://lvxingpai-img-store.qiniudn.com/%s' % im_entry['key'],
                               'h': im_entry['h'], 'w': im_entry['w'], 'fSize': im_entry['size']})
                images = sorted(images, key=lambda val: val['w'] if 'w' in val else 0, reverse=True)
                col_vs.update({'_id': vs['_id']}, {'$set': {'images': images}})
            except (KeyError):
                return


class BaiduWeatherSpider(CrawlSpider):
    name = 'baidu_weather'

    def __init__(self, *a, **kw):
        super(BaiduWeatherSpider, self).__init__(*a, **kw)

        data = None
        for retry_idx in xrange(3):
            try:
                response = urllib2.urlopen('http://cms.lvxingpai.cn/baidu-key.json')
                data = json.loads(response.read())
                break
            except IOError:
                if retry_idx < 3:
                    time.sleep(2)
                else:
                    break

        self.baidu_key = data

    def start_requests(self):
        col = pymongo.MongoClient().geo.Locality
        all_obj = list(col.find({"level": {"$in": [2, 3]}}, {"zhName": 1, 'coords': 1}))
        ak_list = self.baidu_key.values() if self.baidu_key else []

        for county_code in all_obj:
            m = {"county_name": county_code['zhName'], "county_id": county_code["_id"]}

            idx = random.randint(0, len(ak_list) - 1)
            ak = ak_list[idx]

            s = None
            if 'coords' in county_code:
                coords = county_code['coords']
                if 'blat' in coords and 'blng' in coords:
                    s = '%f,%f' % (coords['blng'], coords['blat'])
                elif 'lat' in coords and 'lng' in coords:
                    s = '%f,%f' % (coords['lng'], coords['lat'])
            if not s:
                s = county_code['zhName']
            yield Request(url='http://api.map.baidu.com/telematics/v3/weather?location=%s&output='
                              'json&ak=%s' % (s, ak), callback=self.parse, meta={'WeatherData': m})

    def parse(self, response):
        try:
            data = json.loads(response.body, encoding='utf-8')
            if data['status'] != 'success':
                self.log('ERROR PARSING: %s, RESULT=%s' % (response.url, response.body), level=log.WARNING)
                return
        except ValueError:
            self.log('ERROR PARSING: %s, RESULT=%s' % (response.url, response.body), level=log.WARNING)
            return

        allInf = response.meta['WeatherData']
        item = BaiduWeatherItem()
        item['data'] = data['results'][0]
        item['loc'] = {'id': allInf['county_id'],
                       'zhName': allInf['county_name']}

        return item


class BaiduWeatherPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduWeatherItem):
            return item

        weather_entry = {'loc': item['loc']}
        for k in item['data']:
            weather_entry[k] = item['data'][k]

        col = pymongo.MongoClient().misc.Weather
        ret = col.find_one({'loc.id': item['loc']['id']}, {'_id': 1})
        if ret:
            weather_entry['_id'] = ret['_id']

        col.save(weather_entry)
        return item