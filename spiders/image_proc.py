# coding=utf-8
import hashlib
import json
import os
import random
import re
import time

from scrapy import Request, Item, Field, log

import conf
from spiders import AizouCrawlSpider, AizouPipeline


__author__ = 'zephyre'


class ImageProcItem(Item):
    # define the fields for your item here like:
    image = Field()


# class ImageProcSpider(AizouCrawlSpider):
#     """
#     将imageList中的内容，上传到七牛，然后更新images列表
#     """
#     name = 'image-proc'
#     uuid = 'ccef9d95-7b40-441c-a6d0-2c7fb293a4ef'
#
#     handle_httpstatus_list = [400, 403, 404]
#
#     def __init__(self, *a, **kw):
#         self.ak = None
#         self.sk = None
#         self.min_width = 100
#         self.min_height = 100
#         super(ImageProcSpider, self).__init__(*a, **kw)
#
#     def start_requests(self):
#         yield Request(url='http://www.baidu.com')
#
#     def check_img(self, fname):
#         """
#         检查fname是否为有效的图像（是否能打开，是否能加载，内容是否有误）
#         :param fname:
#         :return:
#         """
#         from PIL import Image
#
#         try:
#             with open(fname, 'rb') as f:
#                 img = Image.open(f, 'r')
#                 img.load()
#                 w, h = img.size
#                 if w < self.min_width or h < self.min_height:
#                     return False
#                 else:
#                     return True
#         except IOError:
#             return False
#
#     def parse(self, response):
#         db = self.param['db'][0] if 'db' in self.param else None
#         col_name = self.param['col'][0] if 'col' in self.param else None
#         profile = self.param['profile'][0] if 'profile' in self.param else 'mongodb-general'
#         query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
#
#         col_im_c = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')
#         if db and col_name:
#             col = self.fetch_db_col(db, col_name, profile)
#             cursor = col.find(query, {'_id': 1}, snapshot=True)
#             if 'limit' in self.param:
#                 cursor.limit(int(self.param['limit'][0]))
#
#             for entry in cursor:
#                 for img in col_im_c.find({'itemIds': entry['_id']}, snapshot=True):
#                     item = ImageProcItem()
#                     item['image'] = img
#                     url = img['url']
#                     yield Request(url=url, meta={'item': item}, headers={'Referer': None}, callback=self.parse_img)
#         else:
#             cursor = col_im_c.find(query, snapshot=True)
#             if 'limit' in self.param:
#                 cursor.limit(int(self.param['limit'][0]))
#
#             self.log('Estiname: %d images to process...' % cursor.count(), log.INFO)
#             for img in cursor:
#                 item = ImageProcItem()
#                 item['image'] = img
#                 url = img['url']
#                 yield Request(url=url, meta={'item': item}, headers={'Referer': None}, callback=self.parse_img)
#
#     def get_upload_token(self, key, bucket='lvxingpai-img-store', overwrite=True):
#         """
#         获得七牛的上传凭证
#         :param key:
#         :param bucket:
#         :param overwrite: 是否为覆盖模式
#         """
#         from qiniu import Auth
#
#         if not self.ak or not self.sk:
#             # 获得上传权限
#             section = conf.global_conf.get('qiniu', {})
#             self.ak = section['ak']
#             self.sk = section['sk']
#         q = Auth(self.ak, self.sk)
#         return q.upload_token(bucket, key)
#
#     def parse_img(self, response):
#         from qiniu import put_file
#
#         if response.status not in [400, 403, 404]:
#             self.log('DOWNLOADED: %s' % response.url, log.INFO)
#             meta = response.meta
#
#             fname = './tmp/%d' % (long(time.time() * 1000) + random.randint(1, 10000))
#             with open(fname, 'wb') as f:
#                 f.write(response.body)
#
#             if not self.check_img(fname):
#                 os.remove(fname)
#                 return
#             else:
#                 key = 'assets/images/%s' % meta['item']['image']['url_hash']
#                 sc = False
#                 self.log('START UPLOADING: %s <= %s' % (key, response.url), log.INFO)
#
#                 uptoken = self.get_upload_token(key)
#                 for idx in xrange(5):
#                     ret, err = put_file(uptoken, key, fname, check_crc=True)
#                     if err:
#                         self.log('UPLOADING FAILED #%d: %s, reason: %s, file=%s' % (idx, key, err, fname), log.INFO)
#                         continue
#                     else:
#                         sc = True
#                         break
#                 if not sc:
#                     raise IOError
#                 self.log('UPLOADING COMPLETED: %s' % key, log.INFO)
#
#                 # 删除上传成功的文件
#                 os.remove(fname)
#
#                 # 统计信息
#                 bucket = 'lvxingpai-img-store'
#                 url = 'http://%s.qiniudn.com/%s?stat' % (bucket, key)
#                 yield Request(url=url, meta={'item': meta['item'], 'key': key, 'bucket': bucket},
#                               callback=self.parse_stat)
#
#     def parse_stat(self, response):
#         stat = json.loads(response.body)
#         meta = response.meta
#         item = meta['item']
#         key = meta['key']
#         bucket = meta['bucket']
#
#         url = 'http://%s.qiniudn.com/%s?imageInfo' % (bucket, key)
#         yield Request(url=url, callback=self.parse_image_info,
#                       meta={'item': item, 'key': key, 'bucket': bucket, 'stat': stat})
#
#     def parse_image_info(self, response):
#         image_info = json.loads(response.body)
#         if 'error' not in image_info:
#             meta = response.meta
#             item = meta['item']
#             key = meta['key']
#             stat = meta['stat']
#
#             img = item['image']
#             entry = {'url_hash': hashlib.md5(img['url']).hexdigest(),
#                      'cTime': long(time.time() * 1000),
#                      'cm': image_info['colorModel'],
#                      'h': image_info['height'],
#                      'w': image_info['width'],
#                      'fmt': image_info['format'],
#                      'size': stat['fsize'],
#                      'url': img['url'],
#                      'key': key,
#                      'type': stat['mimeType'],
#                      'hash': stat['hash']}
#             for k, v in entry.items():
#                 img[k] = v
#             if '_id' in img:
#                 img.pop('_id')
#
#             yield item
#
#
# class ImageProcPipeline(AizouPipeline):
#     spiders = [ImageProcSpider.name]
#     spiders_uuid = [ImageProcSpider.uuid]
#
#     def __init__(self, param):
#         super(ImageProcPipeline, self).__init__(param)
#
#     def process_item(self, item, spider):
#         if not self.is_handler(item, spider):
#             return item
#
#         img = item['image']
#
#         col_im = self.fetch_db_col('imagestore', 'Images', 'mongodb-general')
#         col_im_c = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')
#         if 'itemIds' in img:
#             item_ids = img.pop('itemIds')
#         else:
#             item_ids = None
#         ops = {'$set': img}
#         if item_ids:
#             ops['$addToSet'] = {'itemIds': {'$each': item_ids}}
#
#         col_im.update({'key': img['key']}, ops, upsert=True)
#         col_im_c.remove({'url_hash': img['url_hash']})
#
#         return item


class UniversalImageSpider(AizouCrawlSpider):
    """
    将images里面的内容，转换成album
    调用参数：--col geo:Locality
    """
    name = 'univ-image'
    uuid = '81122b9c-d445-4f89-939f-e3a51c30512f'

    def __init__(self, param, *a, **kw):
        super(UniversalImageSpider, self).__init__(param, *a, **kw)

        image_det_names = self.param['image-detector']

        self.min_width = int(self.param['min-width'][0]) if 'min-width' in self.param else 0
        self.min_height = int(self.param['min-height'][0]) if 'min-height' in self.param else 0

        section = conf.global_conf.get('qiniu', {})
        self.ak = section['ak']
        self.sk = section['sk']

        # globals()
        def is_detector((name, val)):
            if not name.endswith('ImageDetector'):
                return False
            try:
                c = val
                if c.name in image_det_names and 'get_images' in dir(c):
                    return True
            except AttributeError:
                pass
            return False

        self.detectors = [cls[1]() for cls in filter(is_detector, globals().items())]

    def start_requests(self):
        yield Request(url='http://www.baidu.com')

    def parse(self, response):
        db_name = self.param['db'][0]
        col_name = self.param['col'][0]
        profile = self.param['profile'][0]

        col = self.fetch_db_col(db_name, col_name, profile)

        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        cursor = col.find(query, {'_id': 1})

        if 'limit' in self.param:
            cursor.limit(int(self.param['limit'][0]))

        if 'skip' in self.param:
            cursor.skip(int(self.param['skip'][0]))

        col_im = self.fetch_db_col('imagestore', 'Images', 'mongodb-general')

        def walk_tree(node):
            for det in self.detectors:
                image_urls.extend(det.get_images(node))

            # 进入子节点
            children = []
            if isinstance(node, dict):
                children = node.values()
            elif isinstance(node, list):
                children = node

            for subnode in filter(lambda val: val and (isinstance(val, dict) or isinstance(val, list)), children):
                walk_tree(subnode)

        for entry in list(cursor):
            entry = col.find_one({'_id': entry['_id']})
            image_urls = []
            walk_tree(entry)

            for url in image_urls:
                # 查看是否在数据库中已存在
                k = hashlib.md5(url).hexdigest()
                key = 'assets/images/%s' % k

                ret = col_im.find_one({'key': {'$in': [re.compile(r'^%s' % key), k]}}, {'_id': 1})
                if not ret:
                    item = ImageProcItem()
                    item['image'] = {'url': url, 'key': k, 'bucket': 'aizou', 'url_hash': k}
                    yield Request(url=url, callback=self.img_downloaded, meta={'item': item})

    def check_img(self, fname=None, data=None):
        """
        检查fname是否为有效的图像（是否能打开，是否能加载，内容是否有误）
        :param fname:
        :return:
        """
        from PIL import Image

        try:
            if fname:
                with open(fname, 'rb') as f:
                    img = Image.open(f, 'r')
            elif data:
                import cStringIO
                img = Image.open(cStringIO.StringIO(data), 'r')
            else:
                raise ValueError

            img.load()
            w, h = img.size
            if w < self.min_width or h < self.min_height:
                return False
            else:
                return True
        except IOError:
            return False

    def get_upload_token(self, key, bucket='aizou'):
        """
        获得七牛的上传凭证
        :param key:
        :param bucket:
        """
        from qiniu import Auth

        if not self.ak or not self.sk:
            # 获得上传权限
            section = conf.global_conf.get('qiniu', {})
            self.ak = section['ak']
            self.sk = section['sk']
        q = Auth(self.ak, self.sk)
        return q.upload_token(bucket, key)

    def parse_stat(self, response):
        stat = json.loads(response.body)
        meta = response.meta
        item = meta['item']
        key = item['image']['key']
        bucket = item['image']['bucket']

        url = 'http://%s.qiniudn.com/%s?imageInfo' % (bucket, key)
        yield Request(url=url, callback=self.parse_image_info,
                      meta={'item': item, 'stat': stat})

    @staticmethod
    def parse_image_info(response):
        image_info = json.loads(response.body)
        if 'error' not in image_info:
            meta = response.meta
            item = meta['item']
            stat = meta['stat']
            img = item['image']
            bucket = img['bucket']
            key = img['key']

            entry = {'url_hash': img['url_hash'],
                     'cTime': long(time.time() * 1000),
                     'cm': image_info['colorModel'],
                     'h': image_info['height'],
                     'w': image_info['width'],
                     'fmt': image_info['format'],
                     'size': stat['fsize'],
                     'url': img['url'],
                     'bucket': bucket,
                     'key': key,
                     'type': stat['mimeType'],
                     'hash': stat['hash']}
            for k, v in entry.items():
                img[k] = v
            if '_id' in img:
                img.pop('_id')

            yield item

    def img_downloaded(self, response):
        from qiniu import put_data

        if response.status not in [400, 403, 404]:
            self.log('DOWNLOADED: %s' % response.url, log.INFO)
            meta = response.meta

            if not self.check_img(data=response.body):
                return
            else:
                image = meta['item']['image']
                key = image['key']
                bucket = image['bucket']
                sc = False
                self.log('START UPLOADING: %s <= %s' % (key, response.url), log.INFO)

                uptoken = self.get_upload_token(key, bucket)

                for idx in xrange(5):
                    ret, info = put_data(uptoken, key, response.body, check_crc=True)
                    if not ret:
                        self.log('UPLOADING FAILED #%d: %s, reason: %s' % (idx, key, info.error), log.INFO)
                        continue
                    else:
                        sc = True
                        break
                if not sc:
                    raise IOError
                self.log('UPLOADING COMPLETED: %s' % key, log.INFO)

                # 统计信息
                url = 'http://%s.qiniudn.com/%s?stat' % (bucket, key)
                yield Request(url=url, meta={'item': meta['item']}, callback=self.parse_stat)


class BaiduSceneImageDetector(object):
    name = 'baidu-scene'

    @staticmethod
    def get_images(node):
        images = []
        if isinstance(node, dict):
            candidates = []
            if 'pic_url' in node:
                tmp = node['pic_url']
                if tmp:
                    candidates.append(tmp)

            if 'highlight' in node:
                try:
                    candidates.extend(filter(lambda val: val, node['highlight']['list']))
                except KeyError:
                    pass

            for c in candidates:
                if re.search(r'[0-9a-f]{40}', c):
                    images.append('http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c)

        return images


class UniversalImagePipeline(AizouPipeline):
    spiders = [UniversalImageSpider.name]
    spiders_uuid = [UniversalImageSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        img = item['image']

        col_im = self.fetch_db_col('imagestore', 'Images', 'mongodb-general')
        if 'itemIds' in img:
            item_ids = img.pop('itemIds')
        else:
            item_ids = None
        ops = {'$set': img}
        if item_ids:
            ops['$addToSet'] = {'itemIds': {'$each': item_ids}}

        col_im.update({'key': img['key']}, ops, upsert=True)

        return item