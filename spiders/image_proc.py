# coding=utf-8
import hashlib
import json
import os
import random
import re
import time

import qiniu.conf
import qiniu.rs
import qiniu.io
from scrapy import Request, Item, Field, log

from spiders import AizouCrawlSpider
import utils


__author__ = 'zephyre'


class ImageProcItem(Item):
    # define the fields for your item here like:
    db = Field()
    col = Field()
    list1 = Field()
    list1_name = Field()
    list2 = Field()
    list2_name = Field()
    doc_id = Field()
    stat = Field()
    image_info = Field()


class ImageProcSpider(AizouCrawlSpider):
    """
    将imageList中的内容，上传到七牛，然后更新images列表
    """
    name = 'image-proc'

    # 获得上传权限
    qiniu.conf.ACCESS_KEY = utils.cfg_entries('qiniu', 'ak')
    qiniu.conf.SECRET_KEY = utils.cfg_entries('qiniu', 'sk')

    def __init__(self, *a, **kw):
        super(ImageProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com')

    def parse(self, response):
        param = getattr(self, 'param', {})
        db = param['db'][0]
        col_name = param['col'][0]
        list1_name = param['from'][0] if 'from' in param else 'imageList'
        list2_name = param['to'][0] if 'to' in param else 'images'

        col = utils.get_mongodb(db, col_name, profile='mongodb-general')
        col_im = utils.get_mongodb('imagestore', 'Images', profile='mongodb-general')
        for entry in col.find({list1_name: {'$ne': None}}, {list1_name: 1, list2_name: 1}):
            # 从哪里取原始url？比如：imageList
            list1 = entry[list1_name] if list1_name in entry else []
            # 往哪里存？默认：images
            list2 = entry[list2_name] if list2_name in entry else []

            item = ImageProcItem()
            item['doc_id'] = entry['_id']
            item['db'] = db
            item['col'] = col_name
            item['list1'] = list1
            item['list1_name'] = list1_name
            item['list2'] = list2
            item['list2_name'] = list2_name

            upload_list = []
            modified = False
            for url in list1:
                url_set = set([tmp['url'] for tmp in list2])
                url1 = 'http://lvxingpai-img-store.qiniudn.com/assets/images/%s' % hashlib.md5(url).hexdigest()
                if url in url_set or url1 in url_set:
                    continue

                # 是否已经在数据库中存在
                match = re.search(r'http://lvxingpai-img-store\.qiniudn\.com/(.+)', url)
                if match:
                    image = col_im.find_one({'key': match.groups()[0]})
                else:
                    image = col_im.find_one({'url_hash': hashlib.md5(url).hexdigest()})

                if image:
                    url2 = 'http://lvxingpai-img-store.qiniudn.com/' + image['key']
                    if url2 not in url_set:
                        modified = True
                        list2.append({'url': url2, 'h': image['h'], 'w': image['w'], 'fSize': image['size'],
                                      'enabled': True})
                else:
                    modified = True
                    upload_list.append(url)

            # if not modified:
            # continue

            if not upload_list:
                yield item
            else:
                # 开始链式下载
                url = upload_list.pop()
                yield Request(url=url, meta={'src': url, 'item': item, 'upload': upload_list},
                              headers={'Referer': None}, callback=self.parse_img)

    def parse_img(self, response):
        self.log('DOWNLOADED: %s' % response.url, log.INFO)
        # 配置上传策略。
        # 其中lvxingpai是上传空间的名称（或者成为bucket名称）
        bucket = 'lvxingpai-img-store'
        policy = qiniu.rs.PutPolicy(bucket)
        # 取得上传token
        uptoken = policy.token()

        # 上传的额外选项
        extra = qiniu.io.PutExtra()
        # 文件自动校验crc
        extra.check_crc = 1

        fname = './tmp/%d' % (long(time.time() * 1000) + random.randint(1, 10000))
        with open(fname, 'wb') as f:
            f.write(response.body)
        key = 'assets/images/%s' % hashlib.md5(response.meta['src']).hexdigest()

        sc = False
        self.log('START UPLOADING: %s <= %s' % (key, response.url), log.INFO)
        for idx in xrange(5):
            ret, err = qiniu.io.put_file(uptoken, key, fname, extra)
            if err:
                self.log('UPLOADING FAILED #1: %s' % key, log.INFO)
                continue
            else:
                sc = True
                break
        if not sc:
            raise IOError
        self.log('UPLOADING COMPLETED: %s' % key, log.INFO)

        # 删除上传成功的文件
        os.remove(fname)

        # 统计信息
        url = 'http://%s.qiniudn.com/%s?stat' % (bucket, key)
        meta = response.meta
        yield Request(url=url, meta={'src': meta['src'], 'item': meta['item'], 'upload': meta['upload'], 'key': key,
                                     'bucket': bucket}, callback=self.parse_stat)

    def parse_stat(self, response):
        stat = json.loads(response.body)
        meta = response.meta
        item = meta['item']
        upload = meta['upload']
        key = meta['key']
        bucket = meta['bucket']
        src = meta['src']

        url = 'http://%s.qiniudn.com/%s?imageInfo' % (bucket, key)
        yield Request(url=url, meta={'item': item, 'upload': upload, 'key': key, 'bucket': bucket, 'stat': stat,
                                     'src': src}, callback=self.parse_image_info)

    def parse_image_info(self, response):
        image_info = json.loads(response.body)
        meta = response.meta
        item = meta['item']
        upload = meta['upload']
        key = meta['key']
        bucket = meta['bucket']
        stat = meta['stat']
        src = meta['src']

        entry = {'url_hash': hashlib.md5(src).hexdigest(),
                 'cTime': long(time.time() * 1000),
                 'cm': image_info['colorModel'],
                 'h': image_info['height'],
                 'w': image_info['width'],
                 'fmt': image_info['format'],
                 'size': stat['fsize'],
                 'url': src,
                 'key': key,
                 'type': stat['mimeType'],
                 'hash': stat['hash']}
        col_im = utils.get_mongodb('imagestore', 'Images', profile='mongodb-general')
        col_im.save(entry)

        # 修正list
        item['list2'].append({
            'url': 'http://%s.qiniudn.com/%s' % (bucket, key),
            'h': image_info['height'],
            'w': image_info['width'],
            'fSize': stat['fsize'],
            'enabled': True
        })

        if not upload:
            yield item
        else:
            url = upload.pop()
            yield Request(url=url, meta={'src': url, 'item': item, 'upload': upload},
                          headers={'Ref erer': None}, callback=self.parse_img)


class ImageProcPipeline(object):
    spiders = [ImageProcSpider.name]

    def process_item(self, item, spider):
        db = item['db']
        col_name = item['col']
        list1_name = item['list1_name']
        list2_name = item['list2_name']
        list1 = item['list1']
        list2 = item['list2']
        doc_id = item['doc_id']

        # list1中有一些项目，如果已经在list2中存在，则可以删除了
        new_list1 = []
        for url in list1:
            url2 = url if re.search(r'http://lvxingpai-img-store\.qiniudn\.com/(.+)', url) \
                else 'http://lvxingpai-img-store.qiniudn.com/assets/images/%s' % hashlib.md5(url).hexdigest()
            if url2 not in [tmp['url'] for tmp in list2]:
                new_list1.append(url)

        col = utils.get_mongodb(db, col_name, profile='mongodb-general')
        ops = {'$set': {list2_name: list2}}
        if new_list1:
            ops['$set'][list1_name] = new_list1
        else:
            spider.log('Unset %s for document: _id=%s' % (list1_name, doc_id))
            ops['$unset'] = {list1_name: 1}
        col.update({'_id': doc_id}, ops)

        return item