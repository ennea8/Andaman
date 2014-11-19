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

import conf
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

    handle_httpstatus_list = [400, 403, 404]

    def __init__(self, *a, **kw):
        self.ak = None
        self.sk = None
        self.min_width = 400
        self.min_height = 400
        super(ImageProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        self.param = getattr(self, 'param', {})
        yield Request(url='http://www.baidu.com')

    def check_img(self, fname):
        """
        检查fname是否为有效的图像（是否能打开，是否能加载，内容是否有误）
        :param fname:
        :return:
        """
        from PIL import Image

        try:
            with open(fname, 'rb') as f:
                img = Image.open(f, 'r')
                img.load()
                w, h = img.size
                if w < self.min_width and h < self.min_height:
                    return False
                else:
                    return True
        except IOError:
            return False

    def parse(self, response):
        param = getattr(self, 'param', {})
        db = param['db'][0]
        col_name = param['col'][0]
        list1_name = param['from'][0] if 'from' in param else 'imageList'
        list2_name = param['to'][0] if 'to' in param else 'images'
        profile = param['profile'][0] if 'profile' in param else 'mongodb-general'

        col = utils.get_mongodb(db, col_name, profile=profile)
        col_im = utils.get_mongodb('imagestore', 'Images', profile='mongodb-general')
        for entry in col.find({list1_name: {'$ne': None}}, {list1_name: 1, list2_name: 1}):
            # 从哪里取原始url？比如：imageList
            list1 = entry[list1_name] if list1_name in entry else []

            # list1有两种格式，一种是原始的plain string list，另一种是带有metadata的，形如：[{'url': '', 'author': ''}]
            # 这里需要做转换
            if list1 and (isinstance(list1[0], str) or isinstance(list1[0], unicode)):
                list1 = [{'url': tmp} for tmp in list1]

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
            url_set = set([tmp['url'] for tmp in list2])
            for list1_entry in list1:
                url = list1_entry['url']
                url1 = 'http://lvxingpai-img-store.qiniudn.com/assets/images/%s' % hashlib.md5(url).hexdigest()
                if url in url_set or url1 in url_set:
                    continue

                # 原始的元数据
                img_meta = {k.encode('utf-8'): list1_entry[k] for k in list1_entry if k != 'url'}

                # 是否已经在数据库中存在
                match = re.search(r'http://lvxingpai-img-store\.qiniudn\.com/(.+)', url)
                if match:
                    image = col_im.find_one({'key': match.groups()[0]})
                else:
                    image = col_im.find_one({'url_hash': hashlib.md5(url).hexdigest()})

                if image:
                    url2 = 'http://lvxingpai-img-store.qiniudn.com/' + image['key']
                    if url2 not in url_set:
                        tmp = {'url': url2, 'h': image['h'], 'w': image['w'], 'fSize': image['size'],
                               'enabled': True}
                        for k in img_meta:
                            if k not in tmp:
                                tmp[k] = img_meta[k]
                        list2.append(tmp)
                else:
                    # 是否下载缺失的图像
                    if 'skip-upload' not in self.param:
                        upload_list.append((url, img_meta))

            if not upload_list:
                yield item
            else:
                # 开始链式下载
                url, img_meta = upload_list[0]
                upload_list = upload_list[1:]
                yield Request(url=url, meta={'src': url, 'item': item, 'upload': upload_list, 'img_meta': img_meta},
                              headers={'Referer': None}, callback=self.parse_img)

    def get_upload_token(self, key, bucket='lvxingpai-img-store', overwrite=True):
        """
        获得七牛的上传凭证
        :param key:
        :param bucket:
        :param overwrite: 是否为覆盖模式
        """
        if not self.ak or not self.sk:
            # 获得上传权限
            section = conf.global_conf.get('qiniu', {})
            self.ak = section['ak']
            self.sk = section['sk']
        qiniu.conf.ACCESS_KEY = self.ak
        qiniu.conf.SECRET_KEY = self.sk

        # 配置上传策略。
        scope = '%s:%s' % (bucket, key) if overwrite else bucket
        policy = qiniu.rs.PutPolicy(scope)
        return policy.token()

    def parse_img(self, response):
        if response.status not in [400, 403, 404]:
            self.log('DOWNLOADED: %s' % response.url, log.INFO)

            fname = './tmp/%d' % (long(time.time() * 1000) + random.randint(1, 10000))
            with open(fname, 'wb') as f:
                f.write(response.body)

            if not self.check_img(fname):
                os.remove(fname)
                for entry in self.next_proc(response):
                    yield entry
            else:
                key = 'assets/images/%s' % hashlib.md5(response.meta['src']).hexdigest()

                sc = False
                self.log('START UPLOADING: %s <= %s' % (key, response.url), log.INFO)

                uptoken = self.get_upload_token(key)
                # 上传的额外选项
                extra = qiniu.io.PutExtra()
                # 文件自动校验crc
                extra.check_crc = 1

                for idx in xrange(5):
                    ret, err = qiniu.io.put_file(uptoken, key, fname, extra)
                    if err:
                        self.log('UPLOADING FAILED #%d: %s, reason: %s, file=%s' % (idx, key, err, fname), log.INFO)
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
                bucket = 'lvxingpai-img-store'
                url = 'http://%s.qiniudn.com/%s?stat' % (bucket, key)
                meta = response.meta
                yield Request(url=url, meta={'src': meta['src'], 'item': meta['item'], 'upload': meta['upload'],
                                             'key': key, 'bucket': bucket, 'img_meta': meta['img_meta']},
                              callback=self.parse_stat)
        else:
            for entry in self.next_proc(response):
                yield entry

    def next_proc(self, response):
        """
        放弃当前的item chain，处理下一个item
        :param response:
        """
        meta = response.meta
        upload = meta['upload']
        item = meta['item']
        # 从list1中去掉这个url
        list1 = item['list1']
        item['list1'] = filter(lambda val: val['url'] != meta['src'], list1)

        if not upload:
            yield item
        else:
            url, img_meta = upload[0]
            upload = upload[1:]
            yield Request(url=url, meta={'src': url, 'item': item, 'upload': upload, 'img_meta': img_meta},
                          headers={'Ref erer': None}, callback=self.parse_img)

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
                                     'img_meta': meta['img_meta'], 'src': src}, callback=self.parse_image_info)

    def parse_image_info(self, response):
        image_info = json.loads(response.body)
        if 'error' in image_info:
            for entry in self.next_proc(response):
                yield entry
        else:
            meta = response.meta
            item = meta['item']
            upload = meta['upload']
            key = meta['key']
            bucket = meta['bucket']
            stat = meta['stat']
            src = meta['src']
            img_meta = meta['img_meta']

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
            for k, v in img_meta.items():
                if k not in entry:
                    entry[k] = v
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
                url, img_meta = upload[0]
                upload = upload[1:]
                yield Request(url=url, meta={'src': url, 'item': item, 'upload': upload, 'img_meta': img_meta},
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
        for list1_entry in list1:
            url = list1_entry['url']
            url2 = url if re.search(r'http://lvxingpai-img-store\.qiniudn\.com/(.+)', url) \
                else 'http://lvxingpai-img-store.qiniudn.com/assets/images/%s' % hashlib.md5(url).hexdigest()
            if url2 not in [tmp['url'] for tmp in list2]:
                new_list1.append(list1_entry)

        col = utils.get_mongodb(db, col_name, profile='mongodb-general')
        ops = {'$set': {list2_name: list2}}
        if new_list1:
            ops['$set'][list1_name] = new_list1
        else:
            spider.log('Unset %s for document: _id=%s' % (list1_name, doc_id))
            ops['$unset'] = {list1_name: 1}
        col.update({'_id': doc_id}, ops)

        return item