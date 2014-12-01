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

import MySQLdb
from MySQLdb.cursors import DictCursor
from bson import ObjectId
import pymongo
from scrapy import Request, Selector, log, Field, Item
from scrapy.contrib.spiders import CrawlSpider
import datetime
import pysolr

import conf
from spiders import AizouCrawlSpider, AizouPipeline
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
        node_list = list()
        url_list = list()
        for node in spot_data:
            node_list.extend(tmp['sub'] for tmp in node)
        for tmp in node_list:
            url_list.extend([node['surl'] for node in tmp])
        for url in url_list[:1]:
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


        # TODO http://lvyou.baidu.com/destination/ajax/poi/dining?sid=b935706693d2d06f5707d5da&type=&poi=&order=overall_rating&flag=0&nn=0&rn=10&pn=0

        # TODO 住宿：http://lvyou.baidu.com/bali/zhusu，找到var opiList

        # 查找住宿
        yield item

        if item['data']['type'] == 'locality':
            yield Request(url='http://lvyou.baidu.com/%s/zhusu' % item['data']['surl'], callback=self.parse_hotel,
                          meta={'item': item})

    def parse_hotel(self, response):
        match = re.search(r'var\s+opiList\s*=\s*(.+?);\s*var\s+', response.body)
        if match:
            hotel_data = json.loads(match.group(1))
            for hotel_entry in hotel_data:
                item = BaiduSceneItem()
                hotel_entry['type'] = 'hotel'
                item['data'] = hotel_entry


class BaiduScenePipeline(AizouPipeline):
    spiders = [BaiduSceneSpider.name]
    spiders_uuid = [BaiduSceneSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        if not data:
            return item

        col = self.fetch_db_col('raw_data', 'BaiduScene', 'mongodb-crawler')
        ret = col.find_one({'surl': data['surl']})
        if not ret:
            ret = {}
        for key in data:
            ret[key] = data[key]
        col.save(ret)

        return item


class BaiduSceneProItem(Item):
    data = Field()
    col_name = Field()


class BaiduSceneProcSpider(AizouCrawlSpider):
    """
    百度目的地数据的整理
    """

    name = 'baidu-scene-proc'
    uuid = '3d66f9ad-4190-4d7e-a392-e11e29e9b670'

    def __init__(self, *a, **kw):
        super(BaiduSceneProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col_raw_scene = self.fetch_db_col('raw_data', 'BaiduScene', 'mongodb-crawler')

        for entry in col_raw_scene.find():
            yield Request(url='http://www.baidu.com', meta={'entry': entry})

    # 通过id拼接图片url
    def images_pro(self, urls):
<<<<<<< HEAD
        urls_list = list()
        if urls:
            tmp_list = [('http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % tmp) for tmp in urls]
            for tmp in tmp_list:
                urls_list.append({'url': tmp})
        return urls_list
=======
        return [{'url': 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % tmp} for tmp in (urls if urls else [])]
>>>>>>> origin/baidu_spider_pro

    # 文本格式的处理
    def text_pro(self, text):
        if text:
            text = re.split(r'\n+', text)
            text = ['<p>%s</p>' % tmp.strip for tmp in text]
            return '<div> %s </div>' % ('\n'.join(text))
        else:
            return ''

    def parse(self, response):
        # col_raw_scene = utils.get_mongodb('raw_data', 'BaiduScene', profile='mongodb-crawler')
        #
        # for entry in col_raw_scene.find():
        entry = response.meta['entry']
        data = {}

        data['id'] = entry['sid']  # 设置id

        # 国内外字段
        data['abroad'] = 'true' if entry['is_china'] == '0' else 'false'

        # 评价次数
        data['commentCnt'] = entry['rating_count'] if 'rating_count' in entry else None

        # 多少人去过该景点
        data['visitCnt'] = entry['gone_count'] if 'gone_count' in entry else None

        # 收藏次数
        data['favorCnt'] = int(entry['going_count']) if 'going_count' in entry else None

        # 热门程度
        data['hotness'] = float(entry['star']) / 5 if 'star' in entry else None

        # 别名
        alias = set()
        for key in ['sname', 'ambiguity_sname']:
            if key in entry:
                data['zhName'] = entry['sname']  # 中文名
                alias.add(entry[key])
            else:
                continue

        # 源
        data['source'] = {
            'name': 'baidulvyou',
            'url': 'http://lvyou.baidu.com/destination/ajax/jingdian?format=ajax&'
                   'surl=%s&cid=0&pn=1' % entry['surl'],
            'id': entry['sid']
        }

        # 层级结构
        if 'scene_path' in entry:
            length = len(entry['scene_path'])
            if length > 2:
                tmp = entry['scene_path'][1]
                data['country'] = {
                    'id': ObjectId(),
                    'zhName': tmp['sname'],
                    'enName': ''
                }
                locList = list()  # 存放层级列表
                for key in entry['scene_path'][:-1]:
                    tmp_loc = {
                        'id': ObjectId(),
                        'zhName': key['sname'],
                        'enName': ''
                    }
                    locList.append(tmp_loc)
                data['locList'] = locList
            else:
                # log.WARNING('not a city')
                data['country'] = list()
                data['locList'] = list()

        data['tags'] = list()

        if 'ext' in entry:
            tmp = entry['ext']
            data['desc'] = self.text_pro(tmp['more_desc']) if 'more_desc' in tmp else self.text_pro(tmp['abs_desc'])
            data['rating'] = float(tmp['avg_remark_score']) / 5 if 'avg_remark_score' in tmp else None
            data['enName'] = tmp['en_sname'] if 'en_sname' in tmp else ''
        else:
            data['desc'] = ''
            data['rating'] = None
            data['enName'] = ''

        # 设置别名
        if data['enName'] == '':
            pass
        else:
            alias.add(data['enName'])
        data['alias'] = alias

        # 字段
        tmp = dict(entry['content'] if 'content' in entry else '')

        # 处理图片
        if 'highlight' in tmp:
            if 'list' in tmp['highlight']:
                data['images'] = [self.images_pro(pic) for pic in tmp['highlight']['list']]
            else:
                data['images'] = list()
        else:
            data['images'] = list()

        # 处理交通
        traffic = list()
        if 'traffic' in tmp:
            data['trafficIntro'] = self.text_pro(tmp['traffic']['desc']) if 'desc' in tmp['traffic'] else ''
            for key in ['remote', 'local']:
                if key in tmp['traffic']:
                    for node in tmp['traffic'][key]:
                        data = {
                            'name': node['name'],
                            'desc': self.text_pro(node['desc'])
                        }
                        traffic.append(data)
                    data[key + 'Traffic'] = traffic
                else:
                    data[key + 'Traffic'] = traffic
        else:
            data['remoteTraffic'] = traffic
            data['localTraffic'] = traffic
            data['trafficIntro'] = ''

        # 旅行时间
        if 'besttime' in tmp:
            data['travelMonth'] = tmp['besttime']['simple_desc'] \
                if 'simple_desc' in tmp['besttime'] else ''
            # TODO 小时
            tmp_time_cost = tmp['besttime']['recommend_visit_time'] \
                if 'recommend_visit_time' in tmp['besttime'] else ''
            data['timeCost'] = int(re.search('\d', tmp_time_cost).group()) \
                if re.search('\d', tmp_time_cost) else None
            data['timeCostDesc'] = tmp['besttime']['more_desc'] \
                if 'more_desc' in tmp['besttime'] else ''

        # 购物
        goods_list = list()
        if 'shopping' in tmp:
            data['shoppingIntro'] = self.text_pro(tmp['shopping']['desc']) if 'desc' in tmp['shopping'] else ''
            if 'goods' in tmp['shopping']:
                for node in tmp['shopping']['goods']:
                    # 图片
                    images = self.images_pro(list(node['pic_url'])) if 'pic_url' in node else list()
                    goods_tmp = {
                        'zhName': node['name'],
                        'enName': '',
                        'desc': self.text_pro(node['desc']),
                        'images': images
                    }
                    goods_list.append(goods_tmp)
        else:
            data['shoppingIntro'] = ''
        data['commodities'] = goods_list

        # 美食
        food_list = list()
        if 'dining' in tmp:
            data['dinningIntro'] = tmp['dining']['desc'] if 'desc' in tmp['dining'] else ''
            if 'food' in tmp['dining']:
                for node in tmp['dining']['food']:
                    # 图片
                    images = self.images_pro(list(node['pic_url'])) if 'pic_url' in node else list()
                    food_tmp = {
                        'zhName': node['name'],
                        'enName': '',
                        'desc': self.text_pro(node['desc']),
                        'images': images
                    }
                    food_list.append(food_tmp)
        else:
            data['dinningIntro'] = ''
        data['cuisine'] = food_list

        # 活动
        activity_list = list()
        if 'entertainment' in tmp:
            data['activityIntro'] = tmp['entertainment']['desc'] if 'desc' in tmp['entertainment'] else ''
            if 'activity' in tmp['entertainment']:
                for node in tmp['entertainment']['activity']:
                    # 图片
                    images = self.images_pro(list(node['pic_url'])) if 'pic_url' in node else list()
                    activity_tmp = {
                        'zhName': node['name'],
                        'enName': "",
                        'desc': self.text_pro(node['desc']),
                        'images': images
                    }
                    activity_list.append(activity_tmp)
        else:
            data['activityIntro'] = ''
        data['activities'] = activity_list

        # 小贴士
        tips_list = list()
        if 'attention' in tmp:
            if 'list' in tmp['attention']:
                for node in tmp['attention']['list']:
                    # 图片
                    images = self.images_pro(list(node['pic_url'])) if 'pic_url' in node else list()
                    tips_tmp = {
                        'title': node['name'],
                        'desc': self.text_pro(node['desc']),
                        'images': images
                    }
                    tips_list.append(tips_tmp)
        data['activities'] = tips_list

        # 地理文化
        geo_list = list()
        if 'geography_history' in tmp:
            if 'list' in tmp['geography_history']:
                for node in tmp['geography_history']['list']:
                    geo_tmp = {
                        'title': node['name'],
                        'desc': self.text_pro(node['desc']),
                    }
                    geo_list.append(geo_tmp)
        # 杂项信息
        miscInfo = list()
        miscInfo.append(geo_list)
        data['miscInfo'] = miscInfo

        item = BaiduSceneProItem()
        item['data'] = data
        if entry['type'] == 'locality':
            col_name = 'BaiduScene'
        elif entry['type'] == 'poi':
            col_name = 'BaiduPoi'
        elif entry['type'] == 'hotel':
            col_name = 'BaiduHotel'
        else:
            return item

        item['col_name'] = col_name

        return item


class BaiduSceneProPipeline(AizouPipeline):
    spiders = [BaiduSceneProcSpider.name]
    spiders_uuid = [BaiduSceneProcSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        col_name = item['col_name']
        col = self.fetch_db_col('geo', col_name, 'mongodb-general')

        entry = col.find_one({'geo.%s.id' % col_name: data['id']})
        if not entry:
            entry = {}
        for k in data:
            entry[k] = data[k]

        col.save(entry)
