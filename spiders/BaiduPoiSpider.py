import hashlib
import json
import os
import re
import socket
import time
import urllib2

import MySQLdb
from MySQLdb.cursors import DictCursor
import pymongo
from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider
from scrapy import log

import qiniu_utils


__author__ = 'zephyre'

# Crawler


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



