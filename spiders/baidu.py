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
import math

import MySQLdb
from MySQLdb.cursors import DictCursor
from bson import ObjectId
import pymongo
from scrapy import Request, Selector, log, Field, Item
from scrapy.contrib.spiders import CrawlSpider
import datetime
import pysolr

import conf
from spiders import AizouCrawlSpider, AizouPipeline, ProcImagesMixin
from spiders.mafengwo_mixin import MafengwoSugMixin
import utils
from items import BaiduPoiItem, BaiduWeatherItem, BaiduNoteProcItem, BaiduNoteKeywordItem
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
            # comments = note['comments']
            # author = note['authorName']
            #
            # sel = Selector(response)
            #
            # node_list = sel.xpath('//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]')
            # for node in node_list:
            # ret = node.xpath('./div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]')
            # if not ret:
            # continue
            # c_node = ret[0]
            # ret = c_node.xpath('./@nickname').extract()
            # if not ret or (ret[0] == author and not comments):
            # continue
            # c_author = ret[0]
            # ret = c_node.xpath('./@uid').extract()
            # if not ret:
            # continue
            # c_author_id = ret[0]
            #
            # tmp = c_node.extract()
            # if tmp:
            # comments.append({'authorName': c_author, 'authorId': c_author_id, 'comment': tmp})
            #
            # # 检查是否有下一页
            # tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
            # tmp = tmp[-1] if tmp else None
            # if tmp:
            # try:
            # tmp = int(tmp)
            # except ValueError:
            # tmp = None
            #
            # if not tmp:
            # tmp_href = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/@href').extract()
            # if tmp_href:
            # href = tmp_href[-1]
            # parts = urlparse.urlparse(response.url)
            # url = urlparse.urlunparse((parts[0], parts[1], href, '', '', ''))
            # return Request(url=url, callback=self.parse_comments,
            # meta={'urlT': response.meta['urlT'], 'note': note})
            #
            # item = BaiduNoteItem()
            # item['note'] = note
            # return item


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
        item = BaiduNoteProcItem()
        col = utils.get_mongodb('raw_data', 'BaiduNote', profile='mongodb-crawler')
        part = col.find()
        for entry in part:
            content_list = []
            content_m = entry['contents']
            # part_u=part.decode('gb2312')
            if not content_m:
                continue
            for i in range(len(content_m)):
                content = content_m[i].replace('<p', '<img><p')
                # content=content.replace('%','i')
                content = content.replace('<div', '<img><div')
                zz = re.compile(
                    ur"<(?!img)[\s\S][^>]*>")  # |(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
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

            items = []

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
                    avatar_small = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % entry['avatar_small']
                    item['authorAvatar'] = avatar_small

            if 'create_time' in entry:
                x = time.localtime(int(entry['create_time']))
                publishDate = time.strftime('%Y-%m-%d', x)
                publishDate_v = re.split('[-]', publishDate)
                item['publishDate'] = datetime.datetime(int(publishDate_v[0]), int(publishDate_v[1]),
                                                        int(publishDate_v[2]))

            if 'lower_cost' in entry:  # 最低价格
                item['costLower'] = entry['lower_cost']
                if item['costLower'] == 0:
                    item['costLower'] = None

            if 'upper_cost' in entry:
                item['costUpper'] = entry['upper_cost']
                if item['costUpper'] == 0:
                    item['costUpper'] = None

            if 'days' in entry:  # 花费时间
                item['days'] = int(entry['days'])
                if item['days'] == 0:
                    item['days'] = None

            if 'departure' in entry:  # 出发地
                item['fromLoc'] = entry['departure']  # _from string

            if 'destinations' in entry:  # 目的地
                item['toLoc'] = entry['destinations']  # _to string

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
    上传到solr
    """

    spiders = [BaiduNoteProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != BaiduNoteProcItem.__name__:
            return item

        solr_conf = conf.global_conf['solr']
        solr_s = pysolr.Solr('http://%s:%s/solr' % (solr_conf['host'], solr_conf['port']))
        doc = [{'id': str(item['id']),
                'title': item['title'],
                'authorName': item['authorName'],
                'authorAvatar': item['authorAvatar'],
                'publishDate': item['publishDate'],
                'favorCnt': item['favorCnt'],
                'commentCnt': item['commentCnt'],
                'viewCnt': item['viewCnt'],
                'costLower': item['costLower'],
                'costUpper': item['costUpper'],
                'costNorm': item['costNorm'],
                'days': item['days'],
                'fromLoc': item['fromLoc'],
                'toLoc': item['toLoc'],
                'summary': item['summary'],
                'contents': item['contents'],
                'startDate': item['startDate'],
                'endDate': item['endDate'],
                'source': item['source'],
                'sourceUrl': item['sourceUrl'],
                'elite': item['elite']
               }]

        solr_s.add(doc)

        return item


class BaiduNoteKeywordSpider(CrawlSpider):
    """
    对百度游记数据提取景点
    """
    name = 'baidu_note_keyword'  # name of spider

    def __init__(self, *a, **kw):
        super(BaiduNoteKeywordSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        item = BaiduNoteKeywordItem()
        col = utils.get_mongodb('raw_data', 'BaiduNote', profile='mongodb-crawler')
        part = col.find()
        i = 1
        for entry in part:
            print i
            keyword = []
            content_m = entry['contents']
            content_v = []
            # part_u=part.decode('gb2312')
            if not content_m:
                continue
            for i in range(len(content_m)):
                keyword_raw = re.compile(r'>[\s\S][^>]*</a>')
                contents = keyword_raw.findall(content_m[i])
                for content in contents:
                    content_v.append(content[1:-4])
                keyword.extend(content_v)

            item['title'] = entry['title']
            item['keyword'] = keyword
            item['url'] = entry['url']
            i += 1

            yield item


# class BaiduNoteProcPipeline(object):
# """
# 存到数据库
# """
# spiders = [BaiduNoteKeywordSpider.name]
#
# def process_item(self, item, spider):
# if type(item).__name__ != BaiduNoteKeywordItem.__name__:
# return item
#
# col = utils.get_mongodb('clean_data', 'BaiduView', profile='mongodb-general')
# view = {}
# view['title'] = item['title']
# view['keyword'] = item['keyword']
# view['url'] = item['url']
# col.save(view)
# return item


class BaiduSceneItem(Item):
    data = Field()


class BaiduSceneSpider(AizouCrawlSpider):
    name = 'baidu-scene'
    uuid = 'a1cf345b-1f4a-403c-aa01-b7ab81b61b3c'

    def __init__(self, *a, **kw):
        super(BaiduSceneSpider, self).__init__(*a, **kw)

    def start_requests(self):
        start_url = 'http://lvyou.baidu.com/scene/'
        yield Request(url=start_url, callback=self.parse_url)

    def parse_url(self, response):
        sel = Selector(response)
        sdata = sel.xpath('//ul[@id="J-head-menu"]/li/textarea/text()').extract()
        spot_data = (json.loads(tmp) for tmp in sdata)
        node_list = []
        url_list = []
        for node in spot_data:
            node_list.extend(tmp['sub'] for tmp in node)
        for tmp in node_list:
            url_list.extend([node['surl'] for node in tmp])
        for url in url_list:
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=1' % url,
                          meta={'surl': url, 'page_idx': 1, 'item': BaiduSceneItem()}, callback=self.parse)

    def parse(self, response):
        """
        解析每一次请求的数据

        :param response:
        """
        page_idx = response.meta['page_idx']
        curr_surl = response.meta['surl']
        item = response.meta['item']

        # 解析body
        json_data = json.loads(response.body)['data']

        # 抽取字段景点列表
        scene_list = [tmp['surl'] for tmp in json_data['scene_list']]
        next_surls = scene_list

        # 是否为第一页
        if page_idx == 1:
            # 整合url进行投递
            for key in ['relate_scene_list', 'around_scene_list', 'scene_path']:
                if key in json_data:
                    next_surls.extend([tmp['surl'] for tmp in json_data[key]])

            json_data.pop('scene_list')
            item['data'] = json_data

        item_data = item['data']

        for surl in next_surls:
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=1' % surl,
                          meta={'surl': surl, 'page_idx': 1, 'item': BaiduSceneItem()}, callback=self.parse)

        if 'scene_list' not in item_data:
            item_data['scene_list'] = []
        item_data['scene_list'].extend(scene_list)

        # 判断到达最后一页
        if not scene_list:
            yield Request(url='http://lvyou.baidu.com/%s' % item['data']['surl'], callback=self.parse_level,
                          meta={'item': item})
        else:
            page_idx += 1
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=%d' % (
                curr_surl, page_idx), callback=self.parse, meta={'item': item, 'surl': curr_surl, 'page_idx': page_idx})

    def parse_level(self, response):
        item = response.meta['item']

        # 解析原网页，判断是poi还是目的地
        sel = Selector(response)

        nav_list = [tmp.strip() for tmp in sel.xpath('//div[@id="J-sceneViewNav"]/a/span/text()').extract()]
        if not nav_list:
            nav_list = [tmp.strip() for tmp in sel.xpath(
                '//div[contains(@class,"scene-navigation")]//div[contains(@class,"nav-item")]/span/text()').extract()]

        item['data']['type'] = 'locality' if u'景点' in nav_list else 'poi'

        yield item

        sid = item['data']['sid']
        sname = item['data']['sname']
        surl = item['data']['surl']

        # 去哪吃item
        if item['data']['type'] == 'locality':
            # 抓取去哪吃的信息
            eatwhere_url = 'http://lvyou.baidu.com/destination/ajax/poi/dining?' \
                           'sid=%s&type=&poi=&order=overall_rating&flag=0&nn=0&rn=5&pn=1' % sid

            rest_item = BaiduSceneItem()
            rest_data = {'sid': sid, 'sname': sname, 'surl': surl, 'type': 'restaurant', 'restaurant': []}
            rest_item['data'] = rest_data
            yield Request(url=eatwhere_url, callback=self.parse_restaurant,
                          meta={'sid': sid, 'page_idx': 1, 'item': rest_item})

            # 住宿item
            hotel_item = BaiduSceneItem()
            hotel_data = {'sid': sid, 'sname': sname, 'type': 'hotel'}
            hotel_item['data'] = hotel_data

            yield Request(url='http://lvyou.baidu.com/%s/zhusu' % item['data']['surl'], callback=self.parse_hotel,
                          meta={'item': hotel_item})


    # 解析去哪里吃
    def parse_restaurant(self, response):
        item = response.meta['item']

        sid = response.meta['sid']

        page_idx = response.meta['page_idx']

        rest_data = json.loads(response.body)['data']

        tmp_list = rest_data['restaurant']['list']

        tmp_data = item['data']

        # 没有到达最后一页
        if tmp_list:
            tmp_data['restaurant'].extend(tmp_list)
            item['data'] = tmp_data
            page_idx += 1
            yield Request(url='http://lvyou.baidu.com/destination/ajax/poi/dining?' \
                              'sid=%s&type=&poi=&order=overall_rating&flag=0&nn=0&rn=5&pn=%d' % (sid, page_idx),
                          callback=self.parse_restaurant, meta={'item': item, 'sid': sid, 'page_idx': page_idx})
        # 到达最后一页或者没有信息
        else:
            yield item

    # 住宿信息
    def parse_hotel(self, response):
        item = response.meta['item']

        hotel_list = []
        match = re.search(r'var\s+opiList\s*=\s*(.+?);\s*var\s+', response.body)
        if match:
            hotel_list.extend(json.loads(match.group(1)))

        item['data']['hotel'] = hotel_list
        return item


class BaiduScenePipeline(AizouPipeline):
    spiders = [BaiduSceneSpider.name]
    spiders_uuid = [BaiduSceneSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        type = data['type']
        if not data:
            return item

        if type == 'locality':
            colname = 'BaiduLocality'
        elif type == 'poi':
            colname = 'BaiduPoi'
        elif type == 'restaurant':
            colname = 'BaiduRestaurant'
        elif type == 'hotel':
            colname = 'BaiduHotel'
        else:
            return item

        col = self.fetch_db_col('raw_data', colname, 'mongodb-crawler')
        col.update({'sid': data['sid']}, {'$set': data}, upsert=True)

        return item


class BaiduSceneProItem(Item):
    data = Field()
    db_name = Field()
    col_name = Field()


class BaiduSceneProcSpider(AizouCrawlSpider, MafengwoSugMixin):
    """
    百度目的地、景点数据的整理
    """

    name = 'baidu-scene-proc'
    uuid = '3d66f9ad-4190-4d7e-a392-e11e29e9b670'

    def __init__(self, *a, **kw):
        super(BaiduSceneProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    # 通过id拼接图片url
    @staticmethod
    def images_pro(urls):
        return [{'url': 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % tmp} for tmp in (urls if urls else [])]

    # 文本格式的处理
    @staticmethod
    def text_pro(text):
        if text:
            text = re.split(r'\n+', text)
            tmp_text = ['<p>%s</p>' % tmp.strip() for tmp in text]
            return '<div> %s </div>' % ('\n'.join(tmp_text))
            # return text
        else:
            return ''

    def gen_mfw_sug_req(self, item, proximity, sug_type):
        data = item['data']
        kw_list = []
        zh_name = data['zhName']
        kw_list.append(utils.get_short_loc(zh_name))
        if zh_name not in kw_list:
            kw_list.append(zh_name)
        for alias in data['alias']:
            if alias not in [tmp.lower() for tmp in kw_list]:
                kw_list.append(alias)
            alias = re.sub(ur'风?景区$', '', alias)
            if alias not in [tmp.lower() for tmp in kw_list]:
                kw_list.append(alias)

        keyword = kw_list[0]
        kw_list = kw_list[1:]
        req = self.mfw_sug_req(keyword, callback=self.bind_mfw_scene,
                               meta={'item': item, 'kw_list': kw_list, 'proximity': proximity, 'sug_type': sug_type})
        self.log('Yielding %s for BaiduSugMixin. Remaining: %s' % (keyword, ', '.join(kw_list)), log.DEBUG)

        return req

    def bind_mfw_scene(self, response):
        item = response.meta['item']
        proximity = response.meta['proximity']
        sug_type = response.meta['sug_type']
        data = item['data']

        if 'location' not in data:
            return item

        source = data['source']
        lng, lat = data['location']['coordinates']

        col_loc = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        col_vs = self.fetch_db_col('poi', 'ViewSpot', 'mongodb-general')

        def find_counterpart(sug):
            if sug['type'] == 'mdd':
                col = col_loc
            elif sug['type'] == 'vs':
                col = col_vs
            else:
                return

            mfw_cp = col.find_one({'source.mafengwo.id': sug['id']}, {'location': 1})
            if not mfw_cp:
                return

            coords = mfw_cp['location']['coordinates']
            if utils.haversine(coords[0], coords[1], lng, lat) > proximity:
                return

            if sug['name'] not in data['alias']:
                return

            return {'mfw_id': sug['id'], 'mfw_name': sug['name']}

        ret = filter(lambda val: val, map(find_counterpart, self.parse_mfw_sug(response)))
        if ret:
            mfw_item = ret[0]
            source['mafengwo'] = {'id': mfw_item['mfw_id']}
            self.log('Binding: mfw(%d, %s) => baidu(%s, %s)' % (
                mfw_item['mfw_id'], mfw_item['mfw_name'], source['baidu']['id'], data['zhName']), log.INFO)
            return item
        else:
            kw_list = response.meta['kw_list']
            if not kw_list:
                self.log(
                    'Mafengwo counterparts not found: id=%s, name=%s' % (source['baidu']['id'], data['zhName']),
                    log.INFO)
                return item

            keyword = kw_list[0]
            kw_list = kw_list[1:]
            req = self.mfw_sug_req(keyword, callback=self.bind_mfw_scene,
                                   meta={'item': item, 'kw_list': kw_list, 'proximity': proximity,
                                         'sug_type': sug_type})
            return req

    def proc_traffic(self, data, contents, is_locality):
        # 处理交通
        traffic = []
        traffic_intro = ''
        traffic_details = {}

        if 'traffic' in contents:
            traffic_intro = contents['traffic']['desc'] if 'desc' in contents['traffic'] else ''
            for key in ['remote', 'local']:
                if key in contents['traffic']:
                    for node in contents['traffic'][key]:
                        traffic.append({
                            'title': node['name'],
                            'contents': self.text_pro(node['desc'])
                        })
                    traffic_details[key + 'Traffic'] = traffic
                else:
                    traffic_details[key + 'Traffic'] = traffic

        if is_locality:
            data['trafficIntro'] = traffic_intro
            for key in traffic_details:
                data[key] = traffic_details[key]
        else:
            tmp = [traffic_intro.strip()]
            for key, value in traffic_details.items():
                info_entry = '%s\n\n%s' % (key, value)
                tmp.append(info_entry)
            tmp = filter(lambda val: val, tmp)
            data['trafficInfo'] = '\n\n'.join(tmp) if tmp else ''

    def proc_locality_misc(self, data, contents):
        # 示例：func('shoppingIntro', 'commodities', 'shopping', 'goods')
        def func(h1, h2, t1, t2):
            item_lists = []
            if t1 in contents:
                data[h1] = self.text_pro(contents[t1]['desc']) if 'desc' in contents[t1] else ''
                if t2 in contents[t1]:
                    for node in contents[t1][t2]:
                        # 图片
                        images = []
                        if 'pic_url' in node:
                            pic_url = node['pic_url'].strip()
                            if pic_url:
                                images = self.images_pro([pic_url])
                        item_lists.append({
                            'title': node['name'],
                            'desc': self.text_pro(node['desc']),
                            'images': images
                        })
            else:
                data[h1] = ''
            data[h2] = item_lists

        # 购物
        func('shoppingIntro', 'commodities', 'shopping', 'goods')
        # 美食
        func('diningIntro', 'cuisine', 'dining', 'food')
        # 活动
        func('activityIntro', 'activities', 'entertainment', 'activity')
        # 小贴士
        func('activityIntro', 'activities', 'entertainment', 'activity')
        # 活动
        func('tipsIntro', 'tips', 'attention', 'list')
        # 地理文化
        func('geoHistoryIntro', 'geoHistory', 'geography_history', 'list')

        data['miscInfo'] = []

    @staticmethod
    def proc_vs_misc(data, tmp):
        # 门票信息
        if 'ticket_info' in tmp:
            price_desc = tmp['ticket_info']['price_desc'] if 'price_desc' in tmp['ticket_info'] else ''
            open_time_desc = tmp['ticket_info']['open_time_desc'] if 'open_time_desc' in tmp[
                'ticket_info'] else ''
            data['priceDesc'] = price_desc
            data['openTime'] = open_time_desc
        else:
            data['priceDesc'] = ''
            data['openTime'] = ''

    def parse(self, response):
        targets = self.param['targets'] if 'targets' in self.param else ['mdd', 'vs']
        col_list = [{'mdd': 'BaiduLocality', 'vs': 'BaiduPoi'}[tmp] for tmp in targets]
        col_country = self.fetch_db_col('geo', 'Country', 'mongodb-general')
        col_loc = self.fetch_db_col('geo', 'Locality', 'mongodb-general')

        for col_name in col_list:
            col_raw_scene = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')

            query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
            cursor = col_raw_scene.find(query)

            if 'limit' in self.param:
                cursor.limit(int(self.param['limit'][0]))

            self.log('%d records to process...' % cursor.count(), log.INFO)
            for entry in cursor:
                is_locality = entry['type'] == 'locality'

                self.log('Yielding %s: %s, %s' % tuple([entry[key] for key in ['sid', 'surl', 'sname']]), log.INFO)

                data = {'abroad': True if entry['is_china'] == '0' else False,
                        'commentCnt': int(entry['rating_count']) if 'rating_count' in entry else None,
                        'visitCnt': int(entry['gone_count']) if 'gone_count' in entry else None,
                        'favorCnt': int(entry['going_count']) if 'going_count' in entry else None,
                        'hotness': float(entry['star']) / 5 if 'star' in entry else None}

                # 别名
                alias = set()
                for key in ['sname', 'ambiguity_sname']:
                    if key in entry:
                        data['zhName'] = entry['sname']  # 中文名
                        alias.add(entry[key].strip().lower())
                    else:
                        continue

                # 源
                data['source'] = {'baidu': {'id': entry['sid']}}

                loc_list = []
                # 层级结构
                if 'scene_path' in entry:
                    country_fetched = False
                    for scene_path in entry['scene_path']:
                        if country_fetched:
                            ret = col_loc.find_one({'alias': scene_path['sname']}, {'zhName': 1, 'enName': 1})
                            if ret:
                                loc_list.append({key: ret[key] for key in ['_id', 'zhName', 'enName']})
                        else:
                            ret = col_country.find_one({'alias': scene_path['sname']}, {'zhName': 1, 'enName': 1})
                            if ret:
                                data['country'] = {key: ret[key] for key in ['_id', 'zhName', 'enName']}
                                loc_list.append({key: ret[key] for key in ['_id', 'zhName', 'enName']})
                                country_fetched = True

                data['locList'] = loc_list
                data['targets'] = [loc_tmp['_id'] for loc_tmp in loc_list]

                data['tags'] = []

                if 'ext' in entry:
                    tmp = entry['ext']
                    data['desc'] = tmp['more_desc'] \
                        if 'more_desc' in tmp else tmp['abs_desc']
                    data['rating'] = float(tmp['avg_remark_score']) / 5 \
                        if 'avg_remark_score' in tmp else None
                    data['enName'] = tmp['en_sname'] if 'en_sname' in tmp else ''
                    # 位置信息
                    # if 'map_info' in tmp and tmp['map_info']:
                    map_info = filter(lambda val: val,
                                      [c_tmp for c_tmp in re.split(ur'[,\uff0c]', tmp['map_info'])])
                    coord = [float(node) for node in map_info]
                    if len(coord) == 2:
                        data['location'] = {'type': 'Point', 'coordinates': coord}

                else:
                    data['desc'] = ''
                    data['rating'] = None
                    data['enName'] = ''
                    data['location'] = None

                # 设置别名
                if data['enName']:
                    alias.add(data['enName'])
                data['alias'] = list(set(filter(lambda val: val, [tmp.strip().lower() for tmp in alias])))

                # 字段
                contents = entry['content'] if 'content' in entry else {}

                # 处理图片
                data['images'] = []
                if 'highlight' in contents:
                    if 'list' in contents['highlight']:
                        data['images'] = self.images_pro(contents['highlight']['list'])

                # 交通信息
                self.proc_traffic(data, contents, is_locality)

                # 旅行时间
                if 'besttime' in contents:
                    best_time = contents['besttime']
                    travel_month = best_time['more_desc'] if 'more_desc' in best_time else ''
                    if not travel_month:
                        travel_month = best_time['simple_desc'] if 'simple_desc' in best_time else ''
                    data['travelMonth'] = travel_month

                    tmp_time_cost = best_time['recommend_visit_time'] if 'recommend_visit_time' in best_time else ''
                    data['timeCostDesc'] = tmp_time_cost

                if is_locality:
                    self.proc_locality_misc(data, contents)
                else:
                    self.proc_vs_misc(data, contents)

                # 返回item
                item = BaiduSceneProItem()
                item['data'] = data

                if is_locality:
                    item['db_name'] = 'geo'
                    item['col_name'] = 'BaiduLocality'
                else:
                    item['db_name'] = 'poi'
                    item['col_name'] = 'BaiduPoi'

                proximity = 400 if is_locality else 100
                sug_type = 'mdd' if is_locality else 'vs'

                yield self.gen_mfw_sug_req(item, proximity, sug_type)


class BaiduSceneProcPipeline(AizouPipeline, ProcImagesMixin):
    spiders = [BaiduSceneProcSpider.name]
    spiders_uuid = [BaiduSceneProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        db_name = item['db_name']
        col_name = item['col_name']
        col = spider.fetch_db_col(db_name, col_name, 'mongodb-general')

        src = data.pop('source')
        alias = data.pop('alias')
        image_list = data.pop('images')

        ops = {'$set': data}
        for key in src:
            ops['$set']['source.%s' % key] = src[key]
        ops['$addToSet'] = {'alias': {'$each': alias}}

        mdd = col.find_and_modify({'source.baidu.id': src['baidu']['id']}, ops, upsert=True, new=True,
                                  fields={'_id': 1, 'isDone': 1})

        images_formal = self.process_image_list(image_list, mdd['_id'])
        if ('isDone' not in mdd or not mdd['isDone']) and images_formal:
            col.update({'_id': mdd['_id']}, {'$set': {'images': images_formal[:10]}})

        return item


class BaiduRestaurantProcSpider(AizouCrawlSpider):
    """
    百度餐厅、酒店数据的整理
    """

    name = 'baidu-rh-proc'
    uuid = 'D061397C-6615-D85D-E2B2-C7253E9BED42'

    # 墨卡托坐标转换
    def coord_trans(self, lng, lat):
        if lng and lat:
            lng = lng / 20037508.34 * 180
            lat = lat / 20037508.34 * 180
            lat = 180 / math.pi * (2 * math.atan(math.exp(lat * math.pi / 180)) - math.pi / 2)
            return lng, lat
        else:
            return ()

    def __init__(self, *a, **kw):
        super(BaiduRestaurantProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        for col_name in ['BaiduRestaurant', 'BaiduHotel']:
            col_raw_scene = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
            for entry in col_raw_scene.find():
                if entry['type'] == 'restaurant':
                    if 'restaurant' in entry:
                        restaurants_info = entry['restaurant']
                        if not restaurants_info:
                            continue
                    else:
                        continue
                    data = {}
                    # 取得locality的id,根据id获得locality的相应字段
                    data['sid'] = entry['sid']  # 'sid'为保存的locality的sid

                    # 从清洗后的数据中找
                    doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one(
                        {'sid': data['sid']})
                    if not doc:
                        continue
                    # 别名
                    data['alias'] = []
                    # 源
                    data['source'] = {
                        'baidu': {
                            'id': data['sid']
                        }
                    }

                    data['country'] = doc['country']
                    data['locList'] = doc['locList']

                    for node in restaurants_info:
                        try:
                            data['tags'] = node['tag'].split(',')[1:-1] \
                                if 'tag' in node and node['tag'] is not None else []
                        except:
                            data['tags']
                        data['address'] = node['addr'] if 'addr' in node else ''

                        data['telephone'] = node['phone'] if 'phone' in node else ''

                        data['zhName'] = node['name'] if 'name' in node else ''
                        data['enName'] = ''

                        data['desc'] = node['description'] if 'description' else ''

                        images = {'url': node['image'] if 'image' in node else ''}
                        data['images'] = [images]

                        try:
                            if 'map_x' in node and node['map_x']:
                                lng = float(node['map_x'])
                                lat = float(node['map_y'])
                                coord = self.coord_trans(lng, lat)
                                coord = [coord[0], coord[1]]
                            else:
                                coord = []
                        except:
                            node['map_x']
                        data['location'] = {'type': 'Point', 'coordinates': coord}
                        data['prikey'] = ObjectId()
                        miscInfo = [{'title': 'rec_reason',
                                     'contents': node['rec_reason'] if 'rec_reason' in node else ''},
                                    {'title': 'special_dishes',
                                     'contents': node['special_dishes'] if 'special_dishes' in node else ''}]
                        data['miscInfo'] = miscInfo

                        data['alias'] = []

                        item = BaiduSceneProItem()
                        item['data'] = data
                        item['col_name'] = 'BaiduRestaurant'
                        yield item

                if entry['type'] == 'hotel':
                    if 'hotel' in entry:
                        hotels_info = entry['hotel']
                        if not hotels_info:
                            continue
                    else:
                        continue
                    data = {}

                    # 取得locality的id,根据id获得locality的相应字段
                    data['sid'] = entry['sid']

                    # 从清洗后的数据中找
                    doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one(
                        {'sid': data['sid']})
                    if not doc:
                        continue
                    # 别名
                    data['alias'] = []

                    # 源
                    data['source'] = {
                        'baidu': {
                            'id': data['sid']
                        }
                    }

                    data['county'] = doc['country']
                    data['locList'] = doc['locList']

                    for node in hotels_info:
                        data['zhName'] = node['name']

                        if 'ext' in node:
                            ext_text = node['ext']
                            data['enName'] = ext_text['en_name']
                            data['address'] = ext_text['address']
                            data['desc'] = ext_text['rec_reason']
                            data['telephone'] = ext_text['phone']
                            data['tags'] = []
                            images = {'url': ext_text['pic_url']}
                            data['images'] = [images]
                            if 'map_x' in node and node['map_x']:
                                lng = float(node['map_x'])
                                lat = float(node['map_y'])
                                coord = self.coord_trans(lng, lat)
                                coord = [coord[0], coord[1]]
                            else:
                                coord = []
                            data['location'] = {'type': 'Point', 'coordinates': coord}
                            data['prikey'] = ObjectId()
                            miscInfo = [{'title': 'traffic',
                                         'contents': ext_text['traffic'] if 'traffic' in node else ''},
                                        {'title': 'open_time',
                                         'contents': ext_text[
                                             'open_time'] if 'open_time' in node else ''}]
                            data['miscInfo'] = miscInfo
                        else:
                            continue
                        item = BaiduSceneProItem()
                        item['data'] = data
                        item['col_name'] = 'BaiduHotel'
                        yield item


class BaiduRestaurantProcSpiderPipeline(AizouPipeline):
    spiders = [BaiduRestaurantProcSpider.name]
    spiders_uuid = [BaiduRestaurantProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        col_name = item['col_name']
        col = self.fetch_db_col('poi', col_name, 'mongodb-general')

        entry = col.find_one({'prikey': data['prikey']})

        col.update({'prikey': data['prikey']}, {'$set': entry}, upsert=True)
        return item


class BaiduCommentItem(Item):
    data = Field()


class BaiduCommentSpider(AizouCrawlSpider):
    """
    百度评论数据
    """
    name = 'baidu-comment'
    uuid = '91B443B6-91F7-E277-402A-350F5592407E'

    def __init__(self, *a, **kw):
        super(BaiduCommentSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        for col_name in ['BaiduLocality', 'BaiduPoi']:
            col = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
            for entry in col.find():
                # 存在评论里面,以便进行关联
                itemId = entry['_id']
                sid = entry['sid']  # sid locality、poi的标志
                sname = entry['sname']
                surl = entry['surl']
                # type = 'destination' if col_name == 'BaiduLocality' else 'viewspot'
                data = {'sid': sid, 'sname': sname, 'surl': surl, 'itemId': itemId}
                yield Request(
                    url='http://lvyou.baidu.com/user/ajax/remark/getsceneremarklist?'
                        'xid=%s&score=0&pn=0&rn=500&format=ajax' % (sid),
                    callback=self.parse_comment, meta={'col_name': col_name, 'data': data})

    def parse_comment(self, response):
        data = response.meta['data']
        json_data = json.loads(response.body)['data']
        comment_list = json_data['list']
        tmp_comment = []
        for node in comment_list:
            node.pop('from')
            tmp_comment.append(node)
        data['comment_list'] = tmp_comment

        item = BaiduCommentItem()
        item['data'] = data
        return item


class BaiduCommentSpiderPipeline(AizouPipeline):
    spiders = [BaiduCommentSpider.name]
    spiders_uuid = [BaiduCommentSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        if not data:
            return item

        col = self.fetch_db_col('raw_data', 'BaiduComment', 'mongodb-crawler')
        ret = col.find_one({'sid': data['sid']})
        if not ret:
            ret = {}
        for key in data:
            ret[key] = data[key]
        col.save(ret)

        return item


class BaiduCommentProcSpider(AizouCrawlSpider):
    """
    百度评论(景点、目的地)数据的清洗
    """
    name = 'baidu-comment-proc'
    uuid = '87FF4575-EA3F-959C-7CCD-4E6392AF7A8B'

    # 文本格式的处理
    def text_pro(self, text):
        if text:
            text = re.split(r'\n+', text)
            tmp_text = filter(lambda val: val, ['<p>%s</p>' % tmp.strip() for tmp in text])
            return '<div> %s </div>' % ('\n'.join(tmp_text))
            # return text
        else:
            return ''

    def __init__(self, *a, **kw):
        super(BaiduCommentProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        col = self.fetch_db_col('raw_data', 'BaiduComment', 'mongodb-crawler')
        for entry in col.find():

            data = {'itemId': entry['itemId']}

            if 'comment_list' in entry:
                comment_list = entry['comment_list']
                for node in comment_list:
                    if 'user' in node:
                        data['userAvatar'] = 'http://hiphotos.baidu.com/lvpics/abpic/item/%s.jpg' % node['user'][
                            'avatar_large']
                        data['userName'] = node['user']['nickname']
                    else:
                        continue
                    data['mTime'] = node['update_time'] if 'update_time' in node else None
                    data['cTime'] = node['create_time'] if 'create_time' in node else None
                    data['contents'] = node['content'].strip() if 'content' in node else ''
                    data['rating'] = node['score'] / 5.0
                    data['useId'] = None
                    data['remarkId'] = node['remark_id'] if 'remark_id' in node else ''
                    miscInfo = {'commentCount': node['comment_count'] if 'comment_count' in node else 0}
                    data['miscInfo'] = miscInfo
                    if 'pics' in node and node['pics']:
                        data['images'] = [{'url': tmp['full_url']} for tmp in node['pics']]

                    item = BaiduCommentItem()
                    item['data'] = data

                    yield item
            else:
                continue


class BaiduCommentProcSpiderPipeline(AizouPipeline):
    spiders = [BaiduCommentProcSpider.name]
    spiders_uuid = [BaiduCommentProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        col = self.fetch_db_col('misc', 'Comment', 'mongodb-general')
        remark_id = data.pop('remarkId')
        data['source.baidu'] = {'id': remark_id}

        col.update({'source.baidu.id': remark_id}, {'$set': data}, upsert=True)

        return item


class BaiduRestaurantCommentSpider(AizouCrawlSpider):
    """
    百度餐厅评论信息的抓取
    """
    name = 'baidu-rest-comment'
    uuid = '258EBC1C-5C75-AFC7-2C8F-BC31395D0317'

    def __init__(self, *a, **kw):
        super(BaiduRestaurantCommentSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        col = self.fetch_db_col('raw_data', 'BaiduRestaurant', 'mongodb-crawler')
        for entry in col.find():
            data = {}
            surl = entry['surl']
            sid = entry['sid']
            doc = self.fetch_db_col('poi', 'BaiduRestaurant', 'mongodb-general').find_one(
                {'sid': sid})
            if doc:
                data['itemId'] = doc['_id']  # 拿到评论项目的id
            else:
                continue

            if 'restaurant' in entry and entry['restaurant']:
                for node in entry['restaurant']:
                    place_uid = node['uid']
                    if place_uid:
                        yield Request(
                            url='http://lvyou.baidu.com/scene/poi/restaurant?surl=%s&place_uid=%s' % (
                                surl, place_uid),
                            callback=self.parse_comment, meta={'data': data})
                    else:
                        continue
            else:
                continue


    def parse_comment(self, response):
        data = response.meta['data']
        sel = Selector(response)
        comment_list = sel.xpath('//ul[contains(@class,"comment")]/li')
        for node in comment_list:
            if node:
                data['rating'] = float(
                    node.xpath('./p[@class="detail-header"]/span[@class="rating"]/mark/text()').extract()[0]) / 5.0
                data['contents'] = node.xpath('./p[@class="content"]/text()').extract()[0]
                data['userName'] = node.xpath('./p[@class="detail-footer"]/span[1]/text()').extract()[0]
                data['userAvatar'] = ''
                data['images'] = []
                data['userId'] = ''
                data['cTime'] = long(
                    1000 * time.mktime(
                        time.strptime(node.xpath('./p[@class="detail-footer"]/span[2]/text()').extract()[0],
                                      '%Y-%m-%d %H:%M')))
                data['mTime'] = data['cTime']
                data['miscInfo'] = {}
                data['prikey'] = ObjectId()
                item = BaiduCommentItem()
                item['data'] = data
                return item
            else:
                break


class BaiduRestaurantCommentSpiderPipeline(AizouPipeline):
    spiders = [BaiduRestaurantCommentSpider.name]
    spiders_uuid = [BaiduRestaurantCommentSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        if not data:
            return item

        col = self.fetch_db_col('raw_data', 'BaiduRestaurantComment', 'mongodb-crawler')
        ret = col.find_one({'prikey': data['prikey']})
        col.update({'prikey': data['prikey']}, {'$set': ret}, upsert=True)

        return item


class BaiduRestaurantRecommend(Item):
    data = Field()


class BaiduRestaurantRecSpider(AizouCrawlSpider):
    """
    百度美食店铺推荐
    """
    name = 'restaurant_rec'
    uuid = '68B7252E-B688-7615-227A-B8ED9FF9920C'

    def __init__(self, *a, **kw):
        super(BaiduRestaurantRecSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col = self.fetch_db_col('raw_data', 'BaiduLocality', 'mongodb-crawler')
        for entry in col.find():
            surl = entry['surl']
            sid = entry['sid']
            sname = entry['sname']
            data = {'surl': surl, 'sid': sid, 'sname': sname}
            tmp_url = 'http://lvyou.baidu.com/%s/meishi/' % surl
            yield Request(url=tmp_url, callback=self.parse, meta={'data': data})

    def parse(self, response):
        sel = Selector(response)
        data = response.meta['data']
        food_list = sel.xpath('//div[contains(@id,"food-list")]/div')
        if not food_list:
            return
        for node in food_list:
            food_name = node.xpath('.//h3/text()').extract()[0]
            shop_list = node.xpath('.//ul/li')
            shop = []
            if shop_list:
                for shop_node in shop_list:
                    # 店名
                    tmp_shop_name = shop_node.xpath('./p[contains(@class,"clearfix")]//a/text()').extract()
                    if tmp_shop_name:
                        shop_name = tmp_shop_name[0]
                    else:
                        continue
                    # 均价
                    tmp_shop_price = shop_node.xpath(
                        './p[contains(@class,"clearfix")]//span[contains(@class,"price")]/text()').extract()
                    if tmp_shop_price:
                        match = re.search(r'\d+', tmp_shop_price[0])
                        if match:
                            shop_price = float(match.group())
                        else:
                            shop_price = None
                    else:
                        shop_price = None

                    # 店铺描述
                    tmp_shop_desc = shop_node.xpath('./p[contains(@class,"comment")]/text()').extract()
                    if tmp_shop_desc:
                        shop_desc = tmp_shop_desc[0]
                    else:
                        shop_desc = None
                    # 店铺地址
                    tmp_shop_addr = shop_node.xpath('./p[contains(@class,"f12")]/span/text()').extract()
                    if tmp_shop_addr:
                        shop_addr = tmp_shop_addr[0]
                    else:
                        shop_addr = ''
                    tmp_data = {'shop_name': shop_name, 'shop_price': shop_price,
                                'shop_desc': shop_desc, 'shop_addr': shop_addr}
                    shop.append(tmp_data)
            data['food_name'] = food_name
            data['shop_list'] = shop
            data['prikey'] = ObjectId()
            item = BaiduRestaurantRecommend()
            item['data'] = data
            return item


class BaiduRestaurantRecSpiderPipeline(AizouPipeline):
    spiders = [BaiduRestaurantRecSpider.name]
    spiders_uuid = [BaiduRestaurantRecSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        if not data:
            return item

        col = self.fetch_db_col('raw_data', 'BaiduRestaurantRecommend', 'mongodb-crawler')
        ret = col.find_one({'prikey': data['prikey']})
        if not ret:
            ret = {}
        for key in data:
            ret[key] = data[key]
        col.save(ret)
        return item