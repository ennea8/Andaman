# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import random
import re
from os.path import getsize
import sys
import time

import pymongo
from qiniu import io
import qiniu
from qiniu.rs import rs


reload(sys)


class QunarApiPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.QunarPoiRaw

    def process_item(self, item, spider):
        data = item['data']
        ret = self.db.Poi.find_one({'id': data['id']}, {'id': 1})
        if not ret:
            self.db.Poi.insert(data)


class BaiduPoiPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.BaiduPoiRaw

    def process_item(self, item, spider):
        data = item['data']
        if 'scene_total' in data:
            ret = self.db.LocInfo.find_one({'sid': data['sid']}, {'sid': 1})
            if not ret:
                self.db.LocInfo.insert(data)
        else:
            ret = self.db.Poi.find_one({'sid': data['sid']}, {'sid': 1})
            if not ret:
                self.db.Poi.insert(data)


class TravelcrawlerPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('zephyre.me', 27017)
        self.db = client.geo

    def process_item(self, item, spider):
        province = item['province']
        city = item['city']
        county = item['county']

        id = item['id']
        data = item['data']

        # 先找到city，验证其parent为province
        city_obj = self.db.locality.find_one({'zhName': re.compile(r'^' + city),
                                              'parent.name': re.compile(r'^' + province)})
        if city_obj:
            # 找到对应的county
            if 'siblings' in city_obj:
                for t in city_obj['siblings']:
                    if (re.compile(r'^' + county)).match(t['name']):
                        county_id = t['_id']
                        self.db.weather.save({'weatherId': id, 'localityId': county_id,
                                              'Data': data, 'province': province, 'city': city, 'county': county})
                        return item

            # 找不到则为对应的城市Id eg. 长沙 长沙
            if '_id' in city_obj and city == county:
                city_id = city_obj['_id']
                self.db.weather.save({'weatherId': id, 'localityId': city_id,
                                      'Data': data, 'province': province, 'city': city, 'county': county})
                return item
        return item


class QiniuyunPipeline(object):
    def process_item(self, item, spider):
        # 获得上传权限
        qiniu.conf.ACCESS_KEY = "QBsaz_MsErywKS2kkQpwJlIIvBYmryNuPzoGvHJF"
        qiniu.conf.SECRET_KEY = "OTi4GrXf8CQQ0ZLit6Wgy3P8MxFIueqMOwBJhBti"

        # 配置上传策略。
        # 其中lvxingpai是上传空间的名称（或者成为bucket名称）
        policy = qiniu.rs.PutPolicy('lvxingpai-img-store')
        # 取得上传token
        uptoken = policy.token()

        # 上传的额外选项
        extra = io.PutExtra()
        # 文件自动校验crc
        extra.check_crc = 1

        upload_stream = False

        # 将相应的数据存入mongo中
        client = pymongo.MongoClient('zephyre.me', 27017)
        db = client.imagestore

        # 检查是否已经入mongo库
        if db.Hotel.find_one({'url_hash': str(item['hash_value'])}) is None:
            # 先生成本地文件
            localfile = str(time.time()) + str(random.random())

            with open(localfile, 'wb') as f:
                f.write(item['pic'])
            # 上传
            if upload_stream:
                # 上传流
                with open(localfile, 'rb') as f:
                    body = f.read()
                ret, err = io.put(uptoken, str(item['key']), body, extra)
            else:
                # 上传本地文件
                ret, err = io.put_file(uptoken, str(item['key']), localfile, extra)

            if err is not None:
                sys.stderr.write('error: %s ' % err)
                return

            # 计算文件大小，进入mongo
            file_size = int(getsize(localfile))
            db.Hotel.save({'url': item['url'], 'key': item['key'],
                           'url_hash': item['hash_value'], 'ret_hash': ret['hash'], 'size': file_size})
            # 增加索引
            db.Hotel.create_index('url_hash')


            # 删除上传成功的文件
            os.remove(localfile)
        return item


class TravelNotesPipeline(object):
    def process_item(self, item, spider):
        # 将相应的数据存入mongo中
        client = pymongo.MongoClient('zephyre.me', 27017)
        db = client.travel_notes

        # 检查是否入库
        if db.baidu_notes.find_one({'note_url': str(item['note_url'])}) is None:
            # 增加索引
            db.baidu_notes.create_index('note_url')
            db.baidu_notes.save({'user_name': item['user_name'],
                                 'user_url': item['user_url'],
                                 'note_url': item['note_url'],
                                 'note_list': item['note_list'],
                                 'start_year': item['start_year'],
                                 'start_month': item['start_month'],
                                 'origin': item['origin'],
                                 'destination': item['destination'],
                                 'time': item['time'],
                                 'cost': item['cost'],
                                 'quality': item['quality'],
                                 'title': item['title'],
                                 'reply': item['reply'],
                                 'view': item['view'],
                                 'recommend': item['recommend'],
                                 'favourite': item['favourite'],
                                 'sub_note': item['sub_note']})

        return item




