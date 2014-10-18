# coding=utf-8
import json
import os
import random
import re
import copy
import hashlib
import urllib2
import socket
import time
import utils
import datetime
import pysolr

import MySQLdb
from MySQLdb.cursors import DictCursor
import pymongo
from scrapy import Request, Selector, log, Field, Item
from scrapy.contrib.spiders import CrawlSpider

from items import BaiduPoiItem, BaiduWeatherItem, BaiduNoteProcItem
import qiniu_utils



__author__ = 'zephyre'


class BaiduNoteItem(Item):
    # define the fields for your item here like:
    note = Field()


class BaiduNoteSpider(CrawlSpider):
    name = 'baidu_note'

    def __init__(self, *a, **kw):
        super(BaiduNoteSpider, self).__init__(*a, **kw)

    def start_requests(self):
        start_locs = []
        if 'param' in dir(self):
            param = getattr(self, 'param', [])
            if 'c' in param:
                start_locs = param['c']

        if not start_locs:
            col = pymongo.Connection().geo.Locality
            start_locs = list(tmp['zhName'] for tmp in col.find({'level': 2}, {'zhName': 1}))

        url_base = 'http://lvyou.baidu.com/search/ajax/search?format=ajax&word=%s&pn=%d&rn=10'

        for ret in start_locs:
            url = url_base % (ret, 0)
            yield Request(url=url, callback=self.parse_loc,
                          meta={'target': ret, 'pn': 0, 'urlBase': url_base})

    def parse_loc(self, response):
        target = response.meta['target']
        try:
            data = json.loads(response.body)

            days_map = data['data']['search_res']['days']
            costs_map = data['data']['search_res']['costs']

            if data['data']['search_res']['notes_list']:
                # 读取下一页
                url_base = response.meta['urlBase']
                pn = response.meta['pn'] + 10
                url = url_base % (target, pn)
                yield Request(url=url, callback=self.parse_loc, meta={'target': target, 'pn': pn, 'urlBase': url_base})
            else:
                return

            url_base = 'http://lvyou.baidu.com/notes/%s/d-%d'
            for entry in data['data']['search_res']['notes_list']:
                url = url_base % (entry['nid'], 0)
                entry['url'] = url
                entry['target'] = target

                cost_id = entry['avg_cost']
                ret = filter(lambda val: val['id'] == cost_id, costs_map)
                if ret:
                    ret = ret[0]
                    entry['lower_cost'] = int(ret['buildrange'][0])
                    entry['upper_cost'] = int(ret['buildrange'][1])

                yield Request(url=url, callback=self.parse,
                              meta={'target': target, 'pageIdx': 0, 'urlBase': url_base, 'note': entry,
                                    'days': days_map, 'costs': costs_map})

        except (ValueError, KeyError, TypeError):
            pass

    def parse(self, response):
        note = response.meta['note'] if 'note' in response.meta else {}
        page_idx = response.meta['pageIdx']
        note_id = note['nid']
        sel = Selector(response)

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
            item = BaiduNoteItem()
            item['note'] = note
            yield item
            #
            # url_t = 'http://lvyou.baidu.com/notes/%s-%d'
            # url = url_t % (note['nid'], len(note['contents']))
            # yield Request(url=url, callback=self.parse_comments, meta={'urlT': url_t, 'note': note})
        else:
            page_idx += 1
            url_base = response.meta['urlBase']
            url = url_base % (note_id, page_idx)
            yield Request(url=url, callback=self.parse,
                          meta={'pageIdx': page_idx, 'noteId': note_id, 'urlBase': url_base, 'note': note})

            # def parse_comments(self, response):
            # note = response.meta['note']
            #
            # if 'comments' not in note:
            # note['comments'] = []
            #     comments = note['comments']
            #     author = note['authorName']
            #
            #     sel = Selector(response)
            #
            #     node_list = sel.xpath('//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]')
            #     for node in node_list:
            #         ret = node.xpath('./div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]')
            #         if not ret:
            #             continue
            #         c_node = ret[0]
            #         ret = c_node.xpath('./@nickname').extract()
            #         if not ret or (ret[0] == author and not comments):
            #             continue
            #         c_author = ret[0]
            #         ret = c_node.xpath('./@uid').extract()
            #         if not ret:
            #             continue
            #         c_author_id = ret[0]
            #
            #         tmp = c_node.extract()
            #         if tmp:
            #             comments.append({'authorName': c_author, 'authorId': c_author_id, 'comment': tmp})
            #
            #     # 检查是否有下一页
            #     tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
            #     tmp = tmp[-1] if tmp else None
            #     if tmp:
            #         try:
            #             tmp = int(tmp)
            #         except ValueError:
            #             tmp = None
            #
            #     if not tmp:
            #         tmp_href = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/@href').extract()
            #         if tmp_href:
            #             href = tmp_href[-1]
            #             parts = urlparse.urlparse(response.url)
            #             url = urlparse.urlunparse((parts[0], parts[1], href, '', '', ''))
            #             return Request(url=url, callback=self.parse_comments,
            #                            meta={'urlT': response.meta['urlT'], 'note': note})
            #
            #     item = BaiduNoteItem()
            #     item['note'] = note
            #     return item


class BaiduNotePipeline(object):

    spiders = [BaiduNoteSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != BaiduNoteItem.__name__:
            return item

        col = pymongo.Connection().raw_data.BaiduNote
        ret = col.find_one({'nid': item['note']['nid']}, {'_id': 1})
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
    spiders = [BaiduWeatherSpider.name]

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

class BaiduNoteProcSpider(CrawlSpider):
    """
    对百度游记数据进行清洗
    """
    name = 'baidu_note_proc'  # name of spider

    def __init__(self, *a, **kw):
        super(BaiduNoteProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        item=BaiduNoteProcItem()
        col = utils.get_mongodb('raw_data', 'BaiduNote', profile='mongodb-crawler')
        part = col.find()
        for entry in part:
            content_list = []
            content_m = entry['contents']
            #part_u=part.decode('gb2312')
            if not content_m:
                continue
            for i in range(len(content_m)):
                content = content_m[i].replace('<p', '<img><p')
                #content=content.replace('%','i')
                content = content.replace('<div', '<img><div')
                zz = re.compile(ur"<(?!img)[\s\S][^>]*>")  #|(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
                content = zz.sub('', content)
                content_v = re.split('[<>]', content)
                content_list.extend(content_v)

            list_data = []

            for i in range(len(content_list)):
                part_c = re.compile(r'http[\s\S][^"]*.jpg')
                part1 = part_c.search(content_list[i].strip())
                if part1:
                    list_data.append(part1.group())
                elif (content_list[i].strip() == '') | (content_list[i].strip() == 'img'):
                    pass
                else:
                    list_data.append(content_list[i].strip())

            items=[]

            item['id'] = entry['_id']
            item['title'] = entry['title']
            item['authorName'] = entry['uname']
            item['favorCnt'] = entry['recommend_count']
            item['commentCnt'] = entry['common_posts_count']
            item['viewCnt'] = int(entry['view_count'])
            item['costNorm'] = None
            item['contents'] = list_data
            item['source'] = 'baidu'
            item['sourceUrl'] = entry['url']
            item['endDate'] = None

            '''
            item = {
                'id': entry['_id'],
                'title': entry['title'],
                'authorName': entry['uname'],
                'authorAvatar': None,
                'publishDate': None,
                'favorCnt': entry['recommend_count'],
                'commentCnt': entry['common_posts_count'],
                'viewCnt': int(entry['view_count']),
                'costLower': None,
                'costUpper': None,
                'costNorm': None,  #旅行开支
                'days': None,
                'fromLoc': None,
                'toLoc': None,
                'summary': None,
                'contents': list_data,
                'startDate': None,
                'endDate': None,
                'source': 'baidu',
                'sourceUrl': entry['url'],
                'elite': False
            }
            '''
            if 'avatar_small' in entry:
                if entry['avatar_small']:
                    avatar_small = 'himg.bdimg.com/sys/portrait/item/%s.jpg' % entry['avatar_small']
                    item['authorAvatar'] = avatar_small

            if 'create_time' in entry:
                x = time.localtime(int(entry['create_time']))
                publishDate = time.strftime('%Y-%m-%d', x)
                publishDate_v = re.split('[-]', publishDate)
                item['publishDate'] = datetime.datetime(int(publishDate_v[0]), int(publishDate_v[1]), int(publishDate_v[2]))

            if 'lower_cost' in entry:  #最低价格
                item['costLower'] = entry['lower_cost']
                if item['costLower'] == 0:
                    item['costLower'] = None

            if 'upper_cost' in entry:
                item['costUpper'] = entry['upper_cost']
                if item['costUpper'] == 0:
                    item['costUpper'] = None

            if 'days' in entry:  #花费时间
                item['days'] = int(entry['days'])
                if item['days'] == 0:
                    item['days'] = None

            if 'departure' in entry:  #出发地
                item['fromLoc'] = entry['departure']  #_from string

            if 'destinations' in entry:  #目的地
                item['toLoc'] = entry['destinations']  #_to string

            if 'content' in entry:
                item['summary'] = entry['content']

            if 'start_time' in entry:
                x = time.localtime(int(entry['start_time']))
                startDate = time.strftime('%Y-%m-%d', x)
                startDate_v = re.split('[-]', startDate)
                item['startDate'] = datetime.datetime(int(startDate_v[0]), int(startDate_v[1]), int(startDate_v[2]))

            elite = entry['is_good'] + entry['is_praised'] + int(entry['is_set_guide'])
            if elite > 0:
                item['elite'] = True
            else:
                item['elite'] = False

            items.append(item)
            yield item


class BaiduNoteProcPipeline(object):
    """
    对穷游的国家数据进行清洗
    """

    spiders = [BaiduNoteProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != BaiduNoteProcItem.__name__:
            return item

        solr_s = pysolr.Solr('http://localhost:8983/solr')
        doc=[{'id':str(item['id']),
        'title': item['title'],
        'authorName': item['authorName'],
        'authorAvatar':item['authorAvatar'],
        'publishDate':item['publishDate'],
        'favorCnt':item['favorCnt'],
        'commentCnt': item['commentCnt'],
        'viewCnt': item['viewCnt'],
        'costLower':item['costLower'],
        'costUpper':item['costUpper'],
        'costNorm':item['costNorm'],
        'days':item['days'],
        'fromLoc': item['fromLoc'],
        'toLoc': item['toLoc'],
        'summary':item['summary'],
        'contents': item['contents'],
        'startDate':item['startDate'],
        'endDate':item['endDate'],
        'source':item['source'],
        'sourceUrl': item['sourceUrl'],
        'elite':item['elite']
         }]

        solr_s.add(doc)

        return item