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

from qiniu import io
import qiniu
from qiniu.rs import rs
import pymongo

from items import QunarPoiItem, BaiduPoiItem, ChanyoujiUser, ChanyoujiYoujiItem, MafengwoYoujiItem


reload(sys)
sys.setdefaultencoding("utf-8")
# from scrapy.exceptions import DropItem

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


class MofengwoPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        web_name = item['web_name']
        if web_name == 'mafengwo':
            author_id = item['author_id']
            author_name = item['author_name']
            author_url = item['author_url']
            blog_url = item['blog_url']
            date = item['date']
            title = item['title']
            tag = item['tag']
            hot = item['hot']
            img_url = item['image_urls']
            desc = item['desc']
            type = item['type']
            person = item['person']
            play_time = item['play_time']
            days = item['days']
            cost = item['cost']
            is_recommend = item['is_recommend']
            self.db.mafengwo.save(
                {'author_id': author_id, 'author_name': author_name, 'author_url': author_url, 'blog_url': blog_url,
                 'date': date, 'title': title, 'tag': tag, 'hot': hot, 'img_url': img_url, 'desc': desc, 'date': date,
                 'type': type, 'play_time': play_time, 'person': person, 'cost': cost, 'is_recommend': is_recommend,
                 'days': days})
            return item


class ZailushangPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        web_name = item['web_name']
        if web_name == 'zailushang':
            author_url = item['author_url']
            author_id = item['author_id']
            author_name = item['author_name']
            title = item['title']
            date = item['date']
            tag = item['tag']
            like_num = item['like_num']
            cmt_num = item['cmt_num']
            preface = item['preface']
            sights = item['sights']
            content = item['content']
            image_urls = item['image_urls']
            self.db.zailushang.save(
                {'author_url': author_url, 'author_id': author_id, 'author_name': author_name, 'title': title,
                 'date': date, ' tag': tag, 'like_num': like_num, 'cmt_num': cmt_num, 'preface': preface,
                 ' sights': sights, ' content': content, 'image_urls': image_urls})
            return item


class BreadtripPipeline(object):
    def __init__(self):
        # print 'hell000000'
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        # print 'hell1111111'
        web_name = item['web_name']
        if web_name == 'breadtrip':
            blog_url = item['blog_url']
            author_url = item['author_url']
            title = item['title']
            date = item['date']
            like_num = item['like_num']
            cmt_num = item['cmt_num']
            share_num = item['share_num']
            days = item['days']
            sights = item['sights']
            content = item['content']
            image_urls = item['image_urls']
            self.db.breadtrip.save(
                {'blog_url': blog_url, 'author_url': author_url, 'title': title, 'date': date, 'like_num': like_num,
                 'cmt_num': cmt_num, 'days': days, 'share_num': share_num, ' sights': sights, ' content': content,
                 'image_urls': image_urls})
            return item


class YiqiquPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        # print 'hello'
        web_name = item['web_name']
        if web_name == 'yiqiqu':
            sight_url = item['sight_url']
            reasons = item['reasons']
            culture = item['culture']
            name = item['name']
            province = item['province']
            city = item['city']
            theme = item['theme']
            rate = item['rate']
            address = item['address']
            price = item['price']
            phone = item['phone']
            opentime = item['opentime']
            image_urls = item['image_urls']
            intro = item['intro']
            notice = item['notice']
            desc = item['desc']
            spots = item['spots']
            food = item['food']
            traffic = item['traffic']
            self.db.yiqiyou.save(
                {'sight_url': sight_url, 'reasons': reasons, 'culture': culture, 'name': name, 'province': province,
                 'city': city, 'theme': theme, 'rate': rate, 'image_urls': image_urls, 'address': address,
                 'price': price,
                 'phone': phone, 'opentime': opentime, 'intro': intro, 'notice': notice, 'desc': desc, 'spots': spots,
                 'food': food, 'traffic': traffic})
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


class QunarPoiPipeline(object):
    def __init__(self):
        self.db = None

    def connect(self):
        self.db = pymongo.MongoClient().QunarPoiRaw

    def process_item(self, item, spider):
        if not isinstance(item, QunarPoiItem):
            return item

        if not self.db:
            self.connect()

        data = item['data']
        ret = self.db.Poi.find_one({'id': data['id']}, {'id': 1})
        if not ret:
            self.db.Poi.insert(data)

        return item


class BaiduPoiPipeline(object):
    def __init__(self):
        self.db = None

    def connect(self):
        self.db = pymongo.MongoClient().BaiduPoiRaw

    def process_item(self, item, spider):
        if not isinstance(item, BaiduPoiItem):
            return item

        if not self.db:
            self.db = pymongo.MongoClient().BaiduPoiRaw

        data = item['data']
        if 'scene_total' in data:
            ret = self.db.LocInfo.find_one({'sid': data['sid']}, {'sid': 1})
            if not ret:
                self.db.LocInfo.insert(data)
        else:
            ret = self.db.Poi.find_one({'sid': data['sid']}, {'sid': 1})
            if not ret:
                self.db.Poi.insert(data)

        return item


class ChanyoujiYoujiPipline(object):
    def __init__(self):
        self.db = pymongo.MongoClient('dev.lvxingpai.cn',27019).ChanyoujiYoujidb

    '''
    def connect(self):
        self.db = pymongo.MongoClient('dev.lvxingapi.cn',27019).ChanyoujiYoujidb
    '''
    def process_item(self, item, spider):
        if not isinstance(item, ChanyoujiYoujiItem):
            return item
        if not self.db:
            self.db = pymongo.MongoClient().ChanyoujiYoujidb
        trips_id = item['trips_id']
        contents = item['data']

        youji_data = {'trips_id': trips_id, 'contents': contents}
        ret = self.db.youji.find_one({'trips_id': trips_id})
        if not ret:
            self.db.youji.insert(youji_data)
        return item


class MafengwoYoujiPipline(object):
    def __init__(self):
        self.db = None

    def connect(self):
        self.db = pymongo.MongoClient().MafengwoYoujidb

    def process_item(self, item, spider):
        if not isinstance(item, MafengwoYoujiItem):
            return item
        if not self.db:
            self.db = pymongo.MongoClient().MafengwoYoujidb
        title = item['title']
        place = item['place']
        author = item['author']
        title = item['title']
        public_time = item['public_time']
        cost = item['cost']
        way = item['way']
        days = item['days']
        contents = item['contents']
        reply = item['reply']
        youji_data = {'title': title, 'place': place, 'author': author, 'title': title, 'public_time': public_time,
                      'cost': cost, 'way': way, 'days': days, 'contents': contents, 'reply': reply}
        self.db.youji.insert(youji_data)

