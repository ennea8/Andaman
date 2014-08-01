# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import re
import pymongo
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import scrapy
from scrapy.contrib.pipeline.images import ImagesPipeline
#from scrapy.exceptions import DropItem

class MyImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)
    '''def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        item['image_paths'] = image_paths
        return item'''


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

        author_id = item['author_id']
        author_name= item['author_name']
        author_url=item['author_url']
        blog_url = item['blog_url']
        date = item['date']
        title = item['title']
        tag= item['tag']
        hot=item['hot']
        img_url= item['image_urls']
        desc= item['desc']
        type= item['type']
        person= item['person']
        play_time= item['play_time']
        days= item['days']
        cost= item['cost']
        is_recommend= item['is_recommend']
        self.db.mafengwo.save({'author_id':author_id,'author_name':author_name,'author_url':author_url,'blog_url':blog_url,'date':date,'title':title,'tag':tag,'hot':hot,'img_url':img_url,'desc':desc,'date':date,'type':type,'play_time':play_time,'person':person,'cost':cost,'is_recommend':is_recommend,'days':days})
        return item
class YiqiquPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        print 'hello'
        name = item['name']
        province= item['province']
        city = item['city']
        theme = item['theme']
        rate = item['rate']
        address= item['address']
        price= item['price']
        phone= item['phone']
        opentime= item['opentime']
        img= item['img']
        intro= item['intro']
        notice= item['notice']
        desc= item['desc']
        spots= item['spots']
        food= item['food']
        traffic= item['traffic']
        self.db.yiqiyou.save({'name':name,'province':province,'city':city,'theme':theme,'rate':rate,'img':img,'address':address,'price':price,
                               'phone':phone,'opentime':opentime,'intro':intro,'notice':notice,'desc':desc,'spots':spots,'food':food,'traffic':traffic})
        return item
class ZailushangPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb
    def process_item(self,item,spider):
        author_url=item['author_url']
        author_id=item['author_id']
        author_name=item['author_name']
        title=item['title']
        date=item['date']
        tag=item['tag']
        like_num=item['like_num']
        cmt_num=item['cmt_num']
        preface=item['preface']
        sights=item['sights']
        content=item['content']
        image_urls=item['image_urls']
        self.db.zailushang.save({'author_url':author_url,'author_id':author_id,'author_name':author_name,'title':title,'date':date,' tag': tag,'like_num':like_num,'cmt_num':cmt_num,'preface':preface,' sights': sights,' content':content,'image_urls':image_urls})
        return item

