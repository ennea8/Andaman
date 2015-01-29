# coding=utf-8
# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import hashlib
import random
import time
import sys
import urlparse
import re

from scrapy.contrib.spiders import CrawlSpider

import utils
from utils.database import get_mongodb


class AizouCrawlSpider(CrawlSpider):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings=settings)

    def __init__(self, *a, **kw):
        super(CrawlSpider, self).__init__(*a, **kw)
        settings = kw['settings']
        self.param = settings['USER_PARAM']
        self.args = settings['USER_ARGS']

        self.col_dict = {}

        # 每个爬虫需要分配一个唯一的爬虫id，用来在日志文件里面作出区分。
        r = long(time.time() * 1000) + random.randint(0, sys.maxint)
        h = hashlib.md5('%d' % r).hexdigest()
        self.name = '%s:%s' % (self.name, h[:16])

    def build_href(self, url, href):
        c = urlparse.urlparse(href)
        if c.netloc:
            return href
        else:
            c1 = urlparse.urlparse(url)
            return urlparse.urlunparse((c1.scheme, c1.netloc, c.path, c.params, c.query, c.fragment))

    def fetch_db_col(self, db, col, profile):
        sig = '%s.%s.%s' % (db, col, profile)
        if sig not in self.col_dict:
            self.col_dict[sig] = get_mongodb(db, col, profile)
        return self.col_dict[sig]


class ProcImagesMixin(object):
    """
    在清洗数据的时候，处理images列表
    """

    def process_image_list(self, image_list, item_id):
        col_im_c = self.fetch_db_col('imagestore', 'ImageCandidates', 'mongodb-general')
        col_im = self.fetch_db_col('imagestore', 'Images', 'mongodb-general')
        # 正式的，供POI、目的地等使用的images字段
        images_formal = []

        def is_qiniu(url):
            """
            判断是否为存储在七牛上的照片，同时返回key
            :param url:
            """
            match = re.search(r'http://lvxingpai-img-store\.qiniudn\.com/(.+)', url)
            if match:
                return True, match.group(1)
            else:
                return False, 'assets/images/%s' % img['url_hash']

        def fetch_qiniu_pic(key, item_id):
            """
            通过key在Images中查找相应的记录，同时登记item_id
            :param key:
            :param item_id:
            :return:
            """
            return col_im.find_and_modify({'key': key}, {'$addToSet': {'itemIds': item_id}}, new=True)

        def append_image(img):
            """
            往images_formal中添加一个项目
            :param img:
            """
            if not img:
                return

            img_set = set([tmp['key'] for tmp in images_formal])
            if img['key'] in img_set:
                return

            new_img = {}
            for key in ['key', 'w', 'h', 'size', 'title', 'user_name', 'favor_cnt']:
                if key in img:
                    new_img[key] = img[key]
            new_img['url'] = 'http://lvxingpai-img-store.qiniudn.com/%s' % new_img['key']
            images_formal.append(new_img)

        # 先一次性把item_id在Images和ImageCandidates中对应的图像查找出来
        im_map = {tmp['key']: tmp for tmp in col_im.find({'itemIds': item_id})}
        imc_map = {tmp['key']: tmp for tmp in col_im_c.find({'itemIds': item_id})}

        for img in image_list:
            url = img['url']
            if 'url_hash' not in img:
                img['url_hash'] = hashlib.md5(url).hexdigest()
            qiniu_flag, key = is_qiniu(url)

            if qiniu_flag:
                # 如果已经是七牛格式，说明按理说应该已经存在于库里
                if key in im_map:
                    ret = im_map[key]
                else:
                    ret = fetch_qiniu_pic(key, item_id)
                append_image(ret)
            else:
                if key in im_map:
                    ret = im_map[key]
                    src = 'im'
                elif key in imc_map:
                    ret = imc_map[key]
                    src = 'imc'
                else:
                    # 既不存在于Images中，也不存在与ImageCandidates中
                    ret = fetch_qiniu_pic(key, item_id)
                    src = 'im'

                if ret and src == 'im':
                    # 已经存在于数据库中，直接添加到images_formal
                    append_image(ret)

                if not ret:
                    # 尚不存在，添加到ImageCandidates
                    new_img = {}
                    for tmp in img:
                        if tmp in ['itemIds', '_id']:
                            continue
                        new_img[tmp] = img[tmp]
                    new_img['key'] = key
                    col_im_c.update({'url_hash': img['url_hash']},
                                    {'$setOnInsert': new_img, '$addToSet': {'itemIds': item_id}}, upsert=True)

        def images_cmp(img1, img2):
            f1 = img1['favor_cnt'] if 'favor_cnt' in img1 else 0
            f2 = img2['favor_cnt'] if 'favor_cnt' in img2 else 0

            if f1 != f2:
                return f1 - f2
            else:
                s1 = img1['size'] if 'size' in img1 else 0
                s2 = img2['size'] if 'size' in img2 else 0
                return s1 - s2

        return sorted(images_formal, cmp=images_cmp, reverse=True)


class AizouPipeline(object):
    spiders = []
    spiders_uuid = []

    @staticmethod
    def fetch_db_col(db, col, profile):
        return get_mongodb(db, col, profile)

    def __init__(self):
        self.col_dict = {}

    def is_handler(self, item, spider):
        return spider.uuid in self.spiders_uuid