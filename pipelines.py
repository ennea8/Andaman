# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import random
import re
from os.path import getsize
import pymongo
import sys
from qiniu import io
import qiniu
from qiniu.rs import rs
import time

reload(sys)
sys.setdefaultencoding("utf-8")


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
            qiniu.conf.ACCESS_KEY = "gsLdbmFPOFdSAcAp46Vm8c654spHn975sopg8_jP"
            qiniu.conf.SECRET_KEY = "VvsEmqEBagbFa7LkkJfNchxJ7LQRfygPJxKazFzC"

            # 配置上传策略。
            # 其中lvxingpai是上传空间的名称（或者成为bucket名称）
            policy = qiniu.rs.PutPolicy('thefirsttest')
            # 取得上传token
            uptoken = policy.token()

            # 上传的额外选项
            extra = io.PutExtra()
            # 文件自动校验crc
            extra.check_crc = 1

            upload_stream = False

            # 先生成本地文件
            localfile = str(time.time()) + str(random.random())
            with open(localfile, 'wb') as f:
                f.write(item['pic'])

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

            # 将相应的数据存入mongo中
            client = pymongo.MongoClient('zephyre.me', 27017)
            db = client.pic

            # 计算文件大小
            file_size = int(getsize(localfile))
            db.pic_test.save({'url': item['url'], 'key': item['key'],
                              'url_hash':item['hash_value'], 'ret_hash': ret['hash'], 'size': file_size})

            #删除上传成功的文件
            os.remove(localfile)
            return item



