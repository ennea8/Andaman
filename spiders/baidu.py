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
from scrapy.utils import spider

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
    type = Field()


class BaiduSceneSpider(AizouCrawlSpider):
    name = 'baidu-scene'
    uuid = 'a1cf345b-1f4a-403c-aa01-b7ab81b61b3c'

    def __init__(self, *a, **kw):
        super(BaiduSceneSpider, self).__init__(*a, **kw)
        if 'targets' not in self.param:
            self.param['targets'] = []
        if 'all' in self.param['targets']:
            self.param['targets'] = ['scene', 'scene-comment', 'dining', 'hotel', 'note']

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

        # 如果抓取目标有scene，则需要读取完整的scene_list信息
        if 'scene' not in self.param['targets'] or not scene_list:
            # 最后一页，或者不需要完整scene
            yield Request(url='http://lvyou.baidu.com/%s' % item['data']['surl'], callback=self.parse_scene,
                          meta={'item': item})
        else:
            page_idx += 1
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=%d' % (
                curr_surl, page_idx), callback=self.parse, meta={'item': item, 'surl': curr_surl, 'page_idx': page_idx})

    def parse_scene(self, response):
        item = response.meta['item']

        # 解析原网页，判断是poi还是目的地
        sel = Selector(response)

        nav_list = [tmp.strip() for tmp in sel.xpath('//div[@id="J-sceneViewNav"]/a/span/text()').extract()]
        if not nav_list:
            nav_list = [tmp.strip() for tmp in sel.xpath(
                '//div[contains(@class,"scene-navigation")]//div[contains(@class,"nav-item")]/span/text()').extract()]

        item['type'] = 'locality' if u'景点' in nav_list else 'poi'

        # 返回scene本身
        if 'scene' in self.param['targets']:
            yield item

        sid = item['data']['sid']
        sname = item['data']['sname']
        surl = item['data']['surl']
        base_data = {'sid': sid, 'sname': sname, 'surl': surl}

        if 'scene-comment' in self.param['targets']:
            yield Request(url='http://lvyou.baidu.com/user/ajax/remark/getsceneremarklist?xid=%s&score=0&pn=0&rn=500'
                              '&format=ajax' % sid, callback=self.parse_scene_comment, meta={'data': base_data})

        # 去哪吃item
        if item['type'] == 'locality':
            if 'dining' in self.param['targets']:
                # 抓取去哪吃的信息
                dining_tmpl = 'http://lvyou.baidu.com/destination/ajax/poi/dining?' \
                              'sid=%s&type=&poi=%s&order=overall_rating&flag=0&nn=%d&rn=10&pn=%d'
                page_idx = 0
                eatwhere_url = dining_tmpl % (sid, sname, 0, page_idx * 10)
                yield Request(url=eatwhere_url, callback=self.parse_dining,
                              meta={'page_idx': page_idx, 'data': base_data, 'tmpl': dining_tmpl})

                # 抓取吃什么的店铺推荐
                yield Request(url='http://lvyou.baidu.com/%s/meishi/' % surl, callback=self.parse_cuisine,
                              meta={'data': base_data})

            if 'hotel' in self.param['targets']:
                # 住宿item
                yield Request(url='http://lvyou.baidu.com/%s/zhusu' % surl, callback=self.parse_hotel,
                              meta={'data': base_data})

            # 抓取游记
            if 'note' in self.param['targets']:
                idx = 0
                yield Request(url='http://lvyou.baidu.com/search/ajax/search?format=ajax&word=%s&pn=%d&rn=10' %
                                  (sname, idx), callback=self.parse_note, meta={'data': base_data, 'idx': idx})

    # 游记解析
    def parse_note(self, response):
        json_data = json.loads(response.body)
        source_data = json_data['data']
        tmp_data = response.meta['data']
        sname = tmp_data['sname']
        sid = tmp_data['sid']
        idx = response.meta['idx']
        # log.msg('抓取地区游记列表,sname:%s,idx:%d' % (sname, idx), level=log.INFO)
        # 首次进行计算
        if idx == 0:
            total = source_data['search_res']['page']['total']

            for pn in xrange(10, total + 1, 10):
                yield Request(url='http://lvyou.baidu.com/search/ajax/search?format=ajax&word=%s&pn=%d&rn=10' %
                                  (sname, pn), callback=self.parse_note,
                              meta={'data': copy.deepcopy(tmp_data), 'idx': idx})

        # 地区首页notes列表
        # 子页
        note_list = source_data['search_res']['notes_list']
        if not note_list:
            return

        for node in note_list:
            item = BaiduSceneItem()
            note_id = node['nid']
            # note_id = 'b032cd1cdbc0cdda9f42f954'
            node['sid'] = sid
            item['data'] = node
            item['type'] = 'note_abs'
            # self.log('Yielding note_abs: nid=%s, url=%s' % (note_id, response.url), log.INFO)
            yield item

            # 某一游记的具体交互
            yield Request(url='http://lvyou.baidu.com/notes/%s-%d' % (note_id, 0), callback=self.parse_note_floor,
                          meta={'note_id': note_id})

            yield Request(url='http://lvyou.baidu.com/notes/%s/d-%d' % (note_id, 0),
                          callback=self.parse_note_floor,
                          meta={'note_id': note_id, 'main_post': True})

            # # 只看游记
            # idx_ath = 0
            # flag = 0
            # post_id_list = []
            # yield Request(url='http://lvyou.baidu.com/notes/%s/d-%d' % (note_id, idx_ath),
            # callback=self.parse_note_floor, meta={'note_id': note_id, 'idx_ath': idx_ath,
            # 'post_id_list': post_id_list, 'flag': flag,
            # 'main_post': True})

    # 具体论坛抓贴
    def parse_note_floor(self, response):
        note_id = response.meta['note_id']
        sel = Selector(response)
        note_floor = sel.xpath('//div[@id="building-container"]//div[contains(@class,"grid-s5m0")]')
        note_area_list = sel.xpath('//div[@id="building-container"]//textarea[@class="textarea-hide"]/text()').extract()

        main_post = 'main_post' in response.meta

        for sel_list in [Selector(text=tmp).xpath('//div[contains(@class,"grid-s5m0")]') for tmp in note_area_list]:
            note_floor.extend(sel_list)

        if note_floor:
            for node in note_floor:
                try:
                    floor_id = node.xpath('.//div[@class="col-main"]//div[@class="floor"]/div/@id').extract()[0]
                except IndexError:
                    continue
                item = BaiduSceneItem()
                data = {'floor_id': floor_id, 'node': node.extract(), 'nid': note_id}
                if main_post:
                    data['main_post'] = True

                item['data'] = data
                item['type'] = 'note_floor'
                # self.log('Yielding note post: post_id=%s, url=%s' % (floor_id, response.url), log.INFO)
                yield item

        # 翻页
        for href in sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog" and @href]/@href').extract():
            m = {'note_id': note_id}
            if main_post:
                m['main_post'] = True
            yield Request(url=self.build_href(response.url, href), callback=self.parse_note_floor, meta=m)

    # # 只存放游记发帖id
    # def parse_youji(self, response):
    # sel = Selector(response)
    # youji_list = sel.xpath('//div[@id="building-container"]//div[@class="detail-bd"]/div')
    # idx_ath = response.meta['idx_ath']
    # note_id = response.meta['note_id']
    # # log.msg('抓游记id列表,note_id:%s,idx:%d' % (note_id, idx_ath), level=log.INFO)
    #     post_id_list = response.meta['post_id_list']
    #     flag = response.meta['flag']
    #
    #     # 首页判断是否可以翻页
    #     if idx_ath == 0:
    #         page_list = sel.xpath('//div[@class="detail-ft clearfix"]//div[@class="pagelist-wrapper"]')
    #         if not page_list:
    #             tmp_post_id_list = youji_list.xpath('.//div[@class="floor"]/div/@id').extract()
    #             post_id_list.extend(tmp_post_id_list)
    #             data = {'nid': note_id, 'post_id_list': post_id_list}
    #             item = BaiduSceneItem()
    #             item['data'] = data
    #             item['type'] = 'post_id_list'
    #             yield item
    #         else:
    #             flag = 1
    #     else:
    #         flag = response.meta['flag']
    #
    #     if youji_list and flag:
    #         tmp_post_id_list = youji_list.xpath('.//div[@class="floor"]/div/@id').extract()
    #         post_id_list.extend(tmp_post_id_list)
    #         # 向后翻页
    #         idx_ath += 1
    #         yield Request(url='http://lvyou.baidu.com/notes/%s/d-%d' % (note_id, idx_ath),
    #                       callback=self.parse_youji,
    #                       meta={'note_id': note_id, 'idx_ath': idx_ath, 'post_id_list': post_id_list, 'flag': flag})
    #     # 到达最后一页
    #     elif flag == 1:
    #         data = {'nid': note_id, 'post_id_list': post_id_list}
    #         item = BaiduSceneItem()
    #         item['data'] = data
    #         item['type'] = 'post_id_list'
    #         yield item

    def parse_cuisine(self, response):
        data = response.meta['data']
        # 获得JSON结构
        match = re.search(r'var\s+opiList\s*=\s*(\[.*?\])', response.body)
        if match:
            restaurants = {tmp['poid']: tmp for tmp in
                           json.loads(match.group(1))}
        else:
            restaurants = {}

        for node in Selector(response).xpath('//div[contains(@id,"food-list")]/div'):
            food_name = node.xpath('.//h3/text()').extract()[0]
            for shop_node in node.xpath('.//ul/li'):
                # id
                shop_id = shop_node.xpath('.//div[@data-poid]/@data-poid').extract()[0]

                entry = restaurants[shop_id]
                if 'place_uid' not in entry or not entry['place_uid']:
                    continue

                if 'special_dishes' not in entry:
                    entry['special_dishes'] = []
                entry['special_dishes'].append(food_name)

                item = BaiduSceneItem()
                item['type'] = 'dining'
                for key in ('sid', 'sname', 'surl'):
                    entry[key] = data[key]
                item['data'] = entry
                yield item

                # 抓取餐厅的评论信息
                place_uid = entry['place_uid']
                comment_url = 'http://lvyou.baidu.com/scene/poi/restaurant?surl=%s&place_uid=%s' % \
                              (data['surl'], place_uid)
                base_data = copy.deepcopy(data)
                base_data['place_uid'] = place_uid
                yield Request(url=comment_url, callback=self.parse_dining_comment, meta={'data': base_data})

    @staticmethod
    def parse_scene_comment(response):
        rdata = response.meta['data']
        for entry in json.loads(response.body)['data']['list']:
            item = BaiduSceneItem()
            item['type'] = 'scene-comment'
            for key in ('sid', 'sname', 'surl'):
                entry[key] = rdata[key]
            item['data'] = entry
            yield item

    def parse_dining(self, response):
        rdata = response.meta['data']
        page_idx = response.meta['page_idx']
        tmpl = response.meta['tmpl']

        try:
            dining_data = json.loads(response.body)['data']['restaurant']
        except KeyError:
            return

        # 第一页的时候，判断有多少个页面
        if page_idx == 0:
            tot = dining_data['total']
            for idx in xrange(1, int(math.ceil(tot / 10.0))):
                yield Request(url=tmpl % (rdata['sid'], rdata['sname'], 0, idx * 10),
                              callback=self.parse_dining,
                              meta={'page_idx': idx, 'data': {key: rdata[key] for key in ('sid', 'sname', 'surl')},
                                    'tmpl': tmpl})

        for entry in dining_data['list']:
            for key in ('sid', 'sname', 'surl'):
                entry[key] = rdata[key]

            # 有的餐厅没有uid信息，构造一个
            if 'uid' not in entry or not entry['uid']:
                continue
            item = BaiduSceneItem()
            item['type'] = 'dining'
            item['data'] = entry
            yield item

            # if 'fake_uid' not in entry or not entry['fake_uid']:
            # 抓取餐厅的评论信息
            place_uid = entry['uid']
            comment_url = 'http://lvyou.baidu.com/scene/poi/restaurant?surl=%s&place_uid=%s' % \
                          (rdata['surl'], place_uid)
            base_data = copy.deepcopy(rdata)
            base_data['place_uid'] = place_uid
            yield Request(url=comment_url, callback=self.parse_dining_comment, meta={'data': base_data})

    @staticmethod
    def parse_dining_comment(response):
        rdata = response.meta['data']
        for node in Selector(response).xpath('//ul[contains(@class,"comment")]/li'):
            entry = copy.deepcopy(rdata)
            tmp = node.xpath('./p[@class="content"]/text()').extract()
            if not tmp:
                continue
            entry['contents'] = tmp[0]

            tmp = node.xpath('./p[@class="detail-header"]/span[@class="rating"]/mark/text()').extract()
            if tmp:
                entry['rating'] = float(tmp[0]) / 5.0

            tmp = node.xpath('./p[@class="detail-footer"]/span[1]/text()').extract()
            entry['userName'] = tmp[0] if tmp else ''
            entry['userAvatar'] = ''
            entry['images'] = []
            entry['userId'] = ''
            date_text = node.xpath('./p[@class="detail-footer"]/span[2]/text()').extract()[0]
            for fmt in ['%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']:
                try:
                    entry['cTime'] = long(1000 * time.mktime(time.strptime(date_text, fmt)))
                    break
                except ValueError:
                    pass

            entry['mTime'] = entry['cTime']
            entry['miscInfo'] = {}
            entry['prikey'] = hashlib.md5('%s|%s|%s' % (entry['userName'], rdata['place_uid'], date_text)).hexdigest()

            item = BaiduSceneItem()
            item['type'] = 'dining-comment'
            item['data'] = entry
            yield item

    def parse_hotel(self, response):
        rdata = response.meta['data']

        match = re.search(r'var\s+opiList\s*=\s*(.+?);\s*var\s+', response.body)
        if not match:
            return

        for entry in json.loads(match.group(1)):
            if not entry['ext']['address']:
                continue
            item = BaiduSceneItem()
            item['type'] = 'hotel'
            for key in ('sid', 'sname', 'surl'):
                entry[key] = rdata[key]
            item['data'] = entry
            yield item


class BaiduScenePipeline(AizouPipeline):
    spiders = [BaiduSceneSpider.name]
    spiders_uuid = [BaiduSceneSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        col_map = {'locality': ('BaiduLocality', 'sid'),
                   'poi': ('BaiduPoi', 'sid'),
                   'dining': ('BaiduRestaurant', 'place_uid'),
                   'hotel': ('BaiduHotel', 'place_uid'),
                   'dining-comment': ('BaiduDiningCmt', 'prikey'),
                   'scene-comment': ('BaiduSceneCmt', 'remark_id'),
                   'note_abs': ('BaiduNoteAbs', 'nid'),
                   'note_floor': ('BaiduNoteFloor', 'floor_id'),
                   'post_id_list': ('BaiduPostIdList', 'nid')}

        if item_type in col_map:
            col_name, pk = col_map[item_type]
            col = self.fetch_db_col('raw_baidu', col_name, 'mongodb-crawler')
            col.update({pk: data[pk]}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
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
    def images_proc(urls):
        return [{'url': 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % tmp} for tmp in (urls if urls else [])]

    # 文本格式的处理
    @staticmethod
    def text_pro(text):
        if text:
            text = filter(lambda val: val, [tmp.strip() for tmp in re.split(r'\n+', text)])
            tmp_text = ['<p>%s</p>' % tmp for tmp in text]
            return '<div>%s</div>' % (''.join(tmp_text))
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
        traffic_intro = ''
        traffic_details = {}

        if 'traffic' in contents:
            traffic_intro = contents['traffic']['desc'] if 'desc' in contents['traffic'] else ''
            for key in ['remote', 'local']:
                traffic = []
                if key in contents['traffic']:
                    for node in contents['traffic'][key]:
                        traffic.append({
                            'title': node['name'],
                            'contents_html': self.text_pro(node['desc']),
                            'contents': node['desc']
                        })
                traffic_details[key + 'Traffic'] = traffic

        if is_locality:
            data['trafficIntro'] = self.text_pro(traffic_intro)
            for key in traffic_details:
                data[key] = []
                for tmp in traffic_details[key]:
                    title = tmp['title']
                    desc = tmp['contents_html']
                    data[key].append({'title': title, 'desc': desc})
        else:
            tmp = [traffic_intro.strip()]
            for value in (traffic_details[t_type] for t_type in ['localTraffic', 'remoteTraffic'] if
                          t_type in traffic_details):
                info_entry = ['%s：\n\n%s' % (value_tmp['title'], value_tmp['contents']) for value_tmp in value]
                tmp.extend(info_entry)
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
                                images = self.images_proc([pic_url])
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
        func('diningIntro', 'cuisines', 'dining', 'food')
        # 活动
        func('activityIntro', 'activities', 'entertainment', 'activity')
        # 小贴士
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
            is_locality = (col_name == 'BaiduLocality')
            col_raw_scene = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')

            query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
            cursor = col_raw_scene.find(query)

            if 'limit' in self.param:
                cursor.limit(int(self.param['limit'][0]))

            self.log('%d records to process...' % cursor.count(), log.INFO)
            for entry in cursor:
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
                                      [c_tmp for c_tmp in re.split(ur'[,/\uff0c]', tmp['map_info'])])
                    try:
                        coord = [float(node) for node in map_info]
                        if len(coord) == 2:
                            # 有时候经纬度反了
                            ret = utils.guess_coords(*coord)
                            if ret:
                                data['location'] = {'type': 'Point', 'coordinates': ret}
                    except (ValueError, UnicodeEncodeError):
                        self.log(map_info, log.ERROR)
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
                        data['images'] = self.images_proc(contents['highlight']['list'])

                # 交通信息
                self.proc_traffic(data, contents, is_locality)

                # 旅行时间
                if 'besttime' in contents:
                    best_time = contents['besttime']
                    travel_month = best_time['more_desc'] if 'more_desc' in best_time else ''
                    if not travel_month:
                        travel_month = best_time['simple_desc'] if 'simple_desc' in best_time else ''
                    data['travelMonth'] = travel_month.strip()

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

                if 'bind' in self.param:
                    proximity = 400 if is_locality else 100
                    sug_type = 'mdd' if is_locality else 'vs'

                    yield self.gen_mfw_sug_req(item, proximity, sug_type)
                else:
                    yield item


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


class BaiduRestaurantHotelItem(Item):
    data = Field()
    col_name = Field()


class BaiduRestaurantProcSpider(AizouCrawlSpider):
    """
    百度餐厅数据的整理
    """
    name = 'baidu-rest-hotel-proc'
    uuid = 'D061397C-6615-D85D-E2B2-C7253E9BED42'

    def __init__(self, param, *a, **kw):
        super(BaiduRestaurantProcSpider, self).__init__(param, *a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        for col_name in ['BaiduRestaurant', 'BaiduHotel']:
            col = self.fetch_db_col('raw_data', 'BaiduRestaurant', 'mongodb-crawler')
            for entry in col.find({'name': '蓬莱依海渔家乐'}):
                data = {}
                if 'ext' not in entry:
                    sid = entry['sid']
                    doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one(
                        {'source.baidu.id': sid})
                    if not doc:
                        continue
                    # 别名
                    data['alias'] = None
                    # 源
                    data['source.baidu.id'] = entry['place_uid']
                    # 层级控制
                    data['country'] = doc['country']
                    data['targets'] = [tmp['_id'] for tmp in doc['locList']]
                    # 特色菜
                    special_dishes = entry['special_dishes'] if 'special_dishes' in entry else None
                    if special_dishes:
                        data['specialDishes'] = filter(lambda val: val, re.split(ur'[,\uff0c]', special_dishes))
                    else:
                        data['specialDishes'] = None
                    # 标签
                    tags = entry['tag'] if 'tag' in entry else None
                    data['tags'] = filter(lambda val: val, re.split(ur'[,\uff0c;\s]', tags))
                    # 地址
                    data['address'] = entry['addr'] if 'addr' in entry else ''
                    # 电话
                    data['telephone'] = entry['phone'] if 'phone' in entry else ''
                    # 店铺中文名
                    data['zhName'] = entry['name'] if 'name' in entry else ''
                    data['enName'] = None
                    # 评分
                    data['rating'] = float(entry['overall_rating']) / 5.0 if 'overall_rating' in entry and entry[
                        'overall_rating'] else None
                    # 评论次数
                    data['commentCnt'] = entry['comment_num'] if 'comment_num' in entry else None
                    # 热度
                    data['hotness'] = None
                    # 浏览次数
                    data['visitCnt'] = None
                    # 收藏次数
                    data['favorCnt'] = None
                    # 价格描述
                    try:
                        data['price'] = float(entry['price']) if 'price' in entry and entry['price'] else None
                    except ValueError:
                        data['priceDesc'] = entry['price']
                    # 店铺描述
                    data['desc'] = entry['description'] if 'description' else ''
                    # 店铺图片
                    images = [{'url': entry['image'] if 'image' in entry else ''}]
                    data['images'] = images
                    # 位置信息
                    if 'map_x' in entry and entry['map_x']:
                        lng = float(entry['map_x'])
                        lat = float(entry['map_y'])
                        coord = utils.guess_coords(lng, lat)
                    else:
                        coord = None
                    data['location'] = {'type': 'Point', 'coordinates': coord}
                    misc_info = [{'title': 'rec_reason',
                                  'contents': entry['rec_reason'] if 'rec_reason' in entry else None}]
                    data['miscInfo'] = misc_info

                    item = BaiduRestaurantHotelItem()
                    item['data'] = data
                    item['col_name'] = col_name
                    yield item
                else:
                    ext = entry['ext']
                    sid = entry['sid']
                    doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one(
                        {'source.baidu.id': sid})
                    if not doc:
                        continue
                    # 别名
                    data['alias'] = None
                    # 源
                    data['source.baidu.id'] = entry['place_uid']
                    # 层级控制
                    data['country'] = doc['country']
                    data['targets'] = [tmp['_id'] for tmp in doc['locList']]
                    # TODO 标签
                    tags = ext['recommendation'] if 'recommendation' in ext and ext['recommendation'] else None
                    if tags:
                        data['tags'] = filter(lambda val: val, re.split(ur'[,\uff0c;\s]', tags))
                    else:
                        data['tags'] = None
                    # 地址
                    data['address'] = ext['address'] if 'address' in ext else ''
                    # 电话
                    data['telephone'] = ext['phone'] if 'phone' in ext else ''
                    # 店铺中文名
                    data['zhName'] = entry['name'] if 'name' in entry else ''
                    data['enName'] = ext['en_name']
                    # 评分
                    data['rating'] = float(entry['overall_rating']) / 5.0 if 'overall_rating' in entry and entry[
                        'overall_rating'] else None
                    # 评论次数
                    data['commentCnt'] = ext['remark_count'] if 'remark_count' in ext else None
                    # 热度
                    data['hotness'] = None
                    # 浏览次数
                    data['visitCnt'] = None
                    # 收藏次数
                    data['favorCnt'] = None
                    # 价格描述
                    try:
                        data['price'] = float(entry['price']) if 'price' in entry and entry['price'] else None
                    except ValueError:
                        data['priceDesc'] = entry['price']
                    # 店铺描述
                    data['desc'] = ext['desc'] if 'desc' in ext and ext['desc'] else ext['rec_reason']
                    # 店铺图片
                    images = [
                        {'url': utils.images_pro(ext['pic_url']) if 'pic_url' in entry and ext['pic_url']else None}]
                    data['images'] = images
                    # 位置信息
                    if 'map_x' in entry and entry['map_x']:
                        lng = float(entry['map_x'])
                        lat = float(entry['map_y'])
                        coord = utils.guess_coords(lng, lat)
                    else:
                        coord = None
                    data['location'] = {'type': 'Point', 'coordinates': coord}
                    misc_info = [{'title': 'tips',
                                  'contents': ext['tips'] if 'tips' in ext else None},
                                 {'title': 'traffic', 'contents': ext['contents'] if 'contents' in ext else None}]
                    data['miscInfo'] = misc_info

                    item = BaiduRestaurantHotelItem()
                    item['data'] = data
                    item['col_name'] = col_name
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
        col.update({'source.baidu.id': data['source.baidu.id']}, {'$set': data}, upsert=True)
        spider.log('%s' % data['zhName'], log.INFO)
        return item


class BaiduHotelProcSpider(AizouCrawlSpider):
    """
    百度酒店数据的整理
    """
    name = 'baidu-hotel-proc'
    uuid = '295C846A-7DB4-FB1A-2E64-AADABA44D022'

    def __init__(self, *a, **kw):
        super(BaiduHotelProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        col = self.fetch_db_col('raw_data', 'BaiduHotel', 'mongodb-crawler')
        for entry in col.find():
            if 'ext' not in entry:
                continue
            ext = entry['ext']
            data = {}
            sid = entry['sid']
            doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one({'source.baidu.id': sid})
            if not doc:
                continue
            # 别名
            data['alias'] = None
            # 源
            data['source.baidu.id'] = entry['place_uid']
            # 层级控制
            data['country'] = doc['country']
            data['targets'] = [tmp['_id'] for tmp in doc['locList']]
            # 酒店级别
            data['level'] = entry['type']
            # TODO 标签
            tags = ext['rec_tag'] if 'rec_tag' in ext and ext['rec_tag'] else None
            # data['tags'] = filter(lambda val: val, re.split(ur'[,\uff0c;\s]', tags))
            # 地址
            data['address'] = ext['address'] if 'address' in ext else ''
            # 电话
            data['telephone'] = ext['phone'] if 'phone' in ext else ''
            # 店铺中文名
            data['zhName'] = entry['name'] if 'name' in entry else ''
            data['enName'] = ext['en_name']
            # 评分
            data['rating'] = float(entry['overall_rating']) / 5.0 if 'overall_rating' in entry and entry[
                'overall_rating'] else None
            # 评论次数
            data['commentCnt'] = ext['remark_count'] if 'remark_count' in ext else None
            # 热度
            data['hotness'] = None
            # 浏览次数
            data['visitCnt'] = None
            # 收藏次数
            data['favorCnt'] = None
            # 价格描述
            try:
                data['price'] = float(entry['price']) if 'price' in entry and entry['price'] else None
            except ValueError:
                data['priceDesc'] = entry['price']
            # 店铺描述
            data['desc'] = ext['rec_reason'] if 'rec_reason' else ''
            # 店铺图片
            images = [{'url': utils.images_pro(ext['pic_url']) if 'pic_url' in entry and ext['pic_url']else None}]
            data['images'] = images
            # 位置信息
            if 'map_x' in entry and entry['map_x']:
                lng = float(entry['map_x'])
                lat = float(entry['map_y'])
                coord = utils.mercator2wgs(lng, lat)
            else:
                coord = None
            data['location'] = {'type': 'Point', 'coordinates': coord}
            misc_info = [{'title': 'tips',
                          'contents': ext['tips'] if 'tips' in ext else None},
                         {'title': 'traffic', 'contents': ext['contents'] if 'contents' in ext else None}]
            data['miscInfo'] = misc_info

            item = BaiduRestaurantHotelItem()
            item['data'] = data
            item['col_name'] = 'BaiduHotel'
            yield item


class BaiduHotelProcSpiderPipeline(AizouPipeline):
    spiders = [BaiduHotelProcSpider.name]
    spiders_uuid = [BaiduHotelProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        col_name = item['col_name']
        col = self.fetch_db_col('poi', col_name, 'mongodb-general')
        col.update({'source.baidu.id': data['source.baidu.id']}, {'$set': data}, upsert=True)
        spider.log('%s' % data['zhName'], log.INFO)
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
                        'xid=%s&score=0&pn=0&rn=500&format=json' % (sid),
                    callback=self.parse_comment, meta={'col_name': col_name, 'data': data})


    def parse_comment(self, response):

        data = response.meta['data']

        tmp_data = json.loads(response.body)
        if 'data' in tmp_data:
            json_data = tmp_data['data']
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
        col.update({'sid': data['sid']}, {'$set': ret}, upsert=True)

        return item


class BaiduCommentProcSpider(AizouCrawlSpider):
    """
    百度评论(景点、目的地)数据的清洗
    """
    name = 'baidu-comment-proc'
    uuid = '87FF4575-EA3F-959C-7CCD-4E6392AF7A8B'

    # 文本格式的处理
    @staticmethod
    def text_pro(text):
        if text:
            text = re.split(r'\n+', text)
            tmp_text = filter(lambda val: val, ['<p>%s</p>' % tmp.strip() for tmp in text])
            return '<div> %s </div>' % (''.join(tmp_text))
            # return text
        else:
            return ''

    def __init__(self, *a, **kw):
        super(BaiduCommentProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        col = self.fetch_db_col('raw_data', 'BaiduSceneCmt', 'mongodb-crawler')
        for entry in col.find():
            data = {}
            sid = entry['sid']
            # 取得项目id
            loc_doc = self.fetch_db_col('geo', 'BaiduLocality', 'mongodb-general').find_one({'source.baidu.id': sid})
            if loc_doc:
                data['itemId'] = loc_doc['_id']
            else:
                vspot_doc = self.fetch_db_col('poi', 'BaiduPoi', 'mongodb-general').find_one({'source.baidu.id': sid})
                if vspot_doc:
                    data['itemId'] = vspot_doc['_id']
                else:
                    data['itemId'] = None

            # 取得用户信息
            if 'user' in entry:
                data['userAvatar'] = 'http://hiphotos.baidu.com/lvpics/abpic/item/%s.jpg' % entry['user'][
                    'avatar_large']
                data['userName'] = entry['user']['nickname']
            else:
                continue
            data['mTime'] = entry['update_time'] if 'update_time' in entry else None
            data['cTime'] = entry['create_time'] if 'create_time' in entry else None
            data['contents'] = entry['content'].strip() if 'content' in entry else ''
            data['rating'] = entry['score'] / 5.0
            data['useId'] = None
            data['remarkId'] = entry['remark_id'] if 'remark_id' in entry else ''
            misc_info = {'commentCount': entry['comment_count'] if 'comment_count' in entry else 0}
            data['miscInfo'] = misc_info
            if 'pics' in entry and entry['pics']:
                data['images'] = [{'url': tmp['full_url']} for tmp in entry['pics']]
            item = BaiduCommentItem()
            item['data'] = data

            yield item


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
        # spider.log('%s' % data['userName'], log.INFO)
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


class BaiduDiningCmtItem(Item):
    data = Field()


class BaiduDiningCmtProcSpider(AizouCrawlSpider):
    """
    百度餐厅评论数据的清洗
    """
    name = 'baidu-dining-cmt-proc'
    uuid = '25E1FE27-11A0-7960-6B76-2C8EDDEF749A'

    def __init__(self, *a, **kw):
        super(BaiduDiningCmtProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        col = self.fetch_db_col('raw_data', 'BaiduDiningCmt', 'mongodb-crawler')
        for entry in col.find({'place_uid': 'f4a33da99c1586103010afd6'}):
            data = {}
            place_uid = entry['place_uid']
            # 取得项目id
            rest_doc = self.fetch_db_col('poi', 'BaiduRestaurant', 'mongodb-general').find_one(
                {'source.baidu.id': place_uid})
            if rest_doc:
                data['itemId'] = rest_doc['_id']
            else:
                continue

            # 取得用户信息
            data['userAvatar'] = 'http://hiphotos.baidu.com/lvpics/abpic/item/%s.jpg' % entry['userAvatar']
            data['userName'] = entry['userName'] if 'userName' in entry else ''
            data['mTime'] = entry['mTime'] if 'mTime' in entry else None
            data['cTime'] = entry['cTime'] if 'cTime' in entry else None
            data['contents'] = entry['content'].strip() if 'content' in entry else ''
            data['rating'] = entry['rating'] if 'rating' in entry else None
            data['useId'] = None
            data['miscInfo'] = entry['miscInfo'] if 'miscInfo' in entry and entry['miscInfo'] else None
            data['images'] = entry['images'] if 'images' in entry and entry['images'] else None
            item = BaiduCommentItem()
            item['data'] = data

            yield item


class BaiduDiningCmtProcSpiderPipeline(AizouPipeline):
    spiders = [BaiduDiningCmtProcSpider.name]
    spiders_uuid = [BaiduDiningCmtProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        data['prikey'] = data['userName'] + str(data['cTime'])
        col = self.fetch_db_col('misc', 'RestaurantComment', 'mongodb-general')
        col.update({'prikey': data['prikey']}, {'$set': data}, upsert=True)
        # spider.log('%s' % data['userName'], log.INFO)
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
        for entry in col.find({'sname': '北京'}):
            surl = entry['surl']
            sname = entry['sname']
            tmp_url = 'http://lvyou.baidu.com/%s/meishi/' % surl
            data = {'sname': sname, 'surl': surl}
            yield Request(url=tmp_url, callback=self.parse, meta={'data': data})

    def parse(self, response):
        # sel = Selector(response)
        data = response.meta['data']
        # food_list = sel.xpath('//div[contains(@id,"food-list")]/div')
        # if not food_list:
        # return

        # 网页中获得原始结构
        match = re.search(r'var\s+opiList\s*=\s*(\[.+?\])', response.body)
        if match:
            tmp = match.group(1)
        try:
            source_data = json.loads(tmp)
        except ValueError:
            spider.log('name:%s,surl:%s' % (data['sname'], data['surl']), log.INFO)
            return

        # for node in food_list:
        # temp = {'surl': data['surl'], 'sid': data['sid'], 'sname': data['sname']}
        # food_name = node.xpath('.//h3/text()').extract()[0]
        # shop_list = node.xpath('.//ul/li')
        # shop = []
        # if shop_list:
        # for shop_node in shop_list:
        # # id
        # shop_id = shop_node.xpath('.//div[@data-poid]/@data-poid').extract()[0]
        # # 店名
        # tmp_shop_name = shop_node.xpath('./p[contains(@class,"clearfix")]//a/text()').extract()
        # if tmp_shop_name:
        # shop_name = tmp_shop_name[0]
        # else:
        # continue
        # # 均价
        # tmp_shop_price = shop_node.xpath(
        # './p[contains(@class,"clearfix")]//span[contains(@class,"price")]/text()').extract()
        # if tmp_shop_price:
        # match = re.search(r'\d+', tmp_shop_price[0])
        # if match:
        # shop_price = float(match.group())
        # else:
        # shop_price = None
        # else:
        # shop_price = None
        #
        # # 店铺描述
        # tmp_shop_desc = shop_node.xpath('./p[contains(@class,"comment")]/text()').extract()
        # if tmp_shop_desc:
        # shop_desc = tmp_shop_desc[0]
        # else:
        # shop_desc = None
        # # 店铺地址
        # tmp_shop_addr = shop_node.xpath('./p[contains(@class,"f12")]/span/text()').extract()
        # if tmp_shop_addr:
        # shop_addr = tmp_shop_addr[0]
        # else:
        # shop_addr = ''
        # tmp_data = {'shop_name': shop_name, 'shop_price': shop_price,
        # 'shop_desc': shop_desc, 'shop_addr': shop_addr,
        # 'shop_id': shop_id}
        # shop.append(tmp_data)
        # else:
        # continue
        # # self.log(food_name, log.INFO)
        # temp['food_name'] = food_name
        # temp['shop_list'] = shop
        # temp['prikey'] = food_name + (data['sid'])
        #
        for node in source_data:
            item = BaiduRestaurantRecommend()
            item['data'] = node
            yield item


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
        col.update({'poid': data['poid']}, {'$set': data}, upsert=True)
        # digest = hashlib.md5(data['prikey']).hexdigest()
        spider.log('%s' % (data['name']), log.INFO)
        return item