# coding=utf-8
import hashlib
import json
import re
import time

from scrapy import Request, Item, Field, log

import conf
from spiders import AizouCrawlSpider, AizouPipeline


__author__ = 'zephyre'


class ImageProcItem(Item):
    # define the fields for your item here like:
    image = Field()


class UniversalImageSpider(AizouCrawlSpider):
    """
    通过调用detector，将collection中的图像找出来，并放在ImageCandidates中。
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

        self.detectors = [cls[1](self) for cls in filter(is_detector, globals().items())]

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

        if 'batch-size' in self.param:
            cursor.batch_size(int(self.param['batch-size'][0]))

        tot = cursor.count(with_limit_and_skip=True)
        self.log('%d documents to process...' % tot, log.INFO)

        def walk_tree(node):
            for det in self.detectors:
                for walker_entry in det.get_images(node):
                    image_items[walker_entry.pop('id')] = walker_entry

            # 进入子节点
            children = []
            if isinstance(node, dict):
                children = node.values()
            elif isinstance(node, list):
                children = node

            for subnode in filter(lambda val: val and (isinstance(val, dict) or isinstance(val, list)), children):
                walk_tree(subnode)

        col_im = self.fetch_db_col('imagestore', 'Images', 'mongodb-general')
        col_cand = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')

        for entry in cursor:
            entry = col.find_one({'_id': entry['_id']})
            image_items = {}
            walk_tree(entry)

            for image_entry in image_items.values():
                key = image_entry['key']
                key_old = 'assets/images/%s' % key
                # 查看是否在数据库中已 存在
                ret = col_im.find_one({'key': {'$in': [re.compile(r'^%s' % key_old), key]}}, {'_id': 1})
                if ret:
                    continue
                ret = col_cand.find_one({'key': {'$in': [re.compile(r'^%s' % key_old), key]}}, {'_id': 1})
                if ret:
                    continue

                # 不存在，添加item
                if not ret:
                    item = ImageProcItem()
                    item['image'] = {'url': image_entry['src'], 'key': key, 'bucket': 'aizou', 'url_hash': key}
                    for k, v in image_entry['metadata'].items():
                        item['image'][k] = v

                    yield item

    def parse_sub(self, response):
        cursor = response.meta['cursor']

        db_name = self.param['db'][0]
        col_name = self.param['col'][0]
        profile = self.param['profile'][0]

        col = self.fetch_db_col(db_name, col_name, profile)

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

        for entry in cursor:
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

    def __init__(self, spider):
        self.spider = spider

    @staticmethod
    def get_images(node):
        images = []
        if isinstance(node, dict):
            candidates = []
            if 'pic_url' in node:
                tmp = node['pic_url']
                if tmp:
                    candidates.append(tmp)

            if 'image' in node and node['image']:
                match = re.search(r'hiphotos\.baidu\.com/lvpics/pic/item/([0-9a-f]+)\.jpg', node['image'])
                if match:
                    candidates.append(match.group(1))

            if 'highlight' in node:
                try:
                    candidates.extend(filter(lambda val: val, node['highlight']['list']))
                except KeyError:
                    pass

            for c in candidates:
                if re.search(r'[0-9a-f]{40}', c):
                    images.append({'id': c, 'metadata': {},
                                   'url': 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c})

        return images


class MfwImageExtractor(object):
    def __init__(self):
        def helper(image_id, src):
            key = hashlib.md5(src).hexdigest()
            url = 'http://aizou.qiniudn.com/%s' % key

            return {'id': image_id, 'metadata': {}, 'src': src, 'url': url, 'key': key, 'url_hash': key}

        def f1(src):
            pattern = r'([^\./]+)\.\w+\.[\w\d]+\.(jpeg|bmp|png)$'
            match = re.search(pattern, src)
            if not match:
                return None
            c = match.group(1)
            ext = match.group(2)
            src = re.sub(pattern, '%s.%s' % (c, ext), src)
            return helper(c, src)

        self.extractor = [f1]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret


class BaiduImageExtractor(object):
    def __init__(self):
        def helper(image_id, src):
            key = hashlib.md5(src).hexdigest()
            url = 'http://aizou.qiniudn.com/%s' % key

            return {'id': image_id, 'metadata': {}, 'src': src, 'url': url, 'key': key, 'url_hash': key}

        def f1(src):
            match = re.search(r'hiphotos\.baidu\.com/lvpics/pic/item/([0-9a-f]{40})\.jpg', src)
            if not match:
                return None
            c = match.group(1)
            src = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c
            return helper(c, src)

        def f2(src):
            match = re.search(r'himg\.bdimg\.com/sys/portrait/item/(\w+)\.jpg', src)
            if not match:
                return None
            return helper(match.group(1), src)

        def f3(src):
            match = re.search(r'hiphotos\.baidu\.com/lvpics/abpic/item/([0-9a-f]{40})\.jpg', src)
            if not match:
                return None
            c = match.group(1)
            src = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c
            return helper(c, src)

        def f4(src):
            match = re.search(r'hiphotos\.baidu\.com/lvpics/.+sign=[0-9a-f]+/([0-9a-f]{40})\.jpg', src)
            if not match:
                return None
            c = match.group(1)
            src = 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % c
            return helper(c, src)

        self.extractor = [f1, f2, f3, f4]

    def retrieve_image(self, src):
        for func in self.extractor:
            ret = func(src)
            if ret:
                return ret


class BaiduNoteImageDetector(BaiduImageExtractor):
    name = 'baidu-note'

    def __init__(self, spider):
        BaiduImageExtractor.__init__(self)
        self.spider = spider

    def get_images(self, node):
        images = []

        if isinstance(node, dict):
            for image_src in re.findall(r'<img\s+[^<>]*src="(.+?)"', node['node'] if 'node' in node else ''):
                ret = self.retrieve_image(image_src)
                if ret:
                    images.append(ret)

        return images


class ImageCandidatesSpider(AizouCrawlSpider):
    """
    将ImageCandidates里面的内容，上传七牛
    """
    name = 'cand-image'
    uuid = 'c3f718e6-f175-4e72-8056-18b67a11007e'

    def __init__(self, param, *a, **kw):
        super(ImageCandidatesSpider, self).__init__(param, *a, **kw)

        self.min_width = int(self.param['min-width'][0]) if 'min-width' in self.param else 0
        self.min_height = int(self.param['min-height'][0]) if 'min-height' in self.param else 0

        section = conf.global_conf.get('qiniu', {})
        self.ak = section['ak']
        self.sk = section['sk']

    def start_requests(self):
        yield Request(url='http://www.baidu.com')

    def parse(self, response):
        col = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')

        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        cursor = col.find(query, snapshot=True)

        if 'limit' in self.param:
            cursor.limit(int(self.param['limit'][0]))

        if 'skip' in self.param:
            cursor.skip(int(self.param['skip'][0]))

        tot = cursor.count(with_limit_and_skip=True)
        self.log('%d documents to process...' % tot, log.INFO)
        for entry in cursor:
            item = ImageProcItem()
            entry['col_name'] = 'Images'
            item['image'] = entry
            yield Request(url=entry['url'], callback=self.img_downloaded, meta={'item': item})

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
                match = re.search(r'assets/images/([0-9a-f]{32})', key)
                if match:
                    key = match.group(1)
                    image['key'] = key
                image['bucket'] = 'aizou'
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


class UniversalImagePipeline(AizouPipeline):
    spiders = [UniversalImageSpider.name, ImageCandidatesSpider.name]
    spiders_uuid = [UniversalImageSpider.uuid, ImageCandidatesSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        img = item['image']
        if 'col_name' in img:
            col_name = img.pop('col_name')
        else:
            col_name = 'ImageCandidates'

        image_id = img.pop('_id') if '_id' in img else None

        col_im = self.fetch_db_col('imagestore', col_name, 'mongodb-general')
        if 'itemIds' in img:
            item_ids = img.pop('itemIds')
        else:
            item_ids = None
        ops = {'$set': img}
        if item_ids:
            ops['$addToSet'] = {'itemIds': {'$each': item_ids}}

        col_im.update({'$or': [{'key': img['key']}, {'url_hash': img['url_hash']}]}, ops, upsert=True)

        if image_id:
            col_cand = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')
            col_cand.remove({'_id': image_id})

        return item