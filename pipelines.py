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
        web_name=item['web_name']
        if web_name=='mafengwo':
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

class ZailushangPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb
    def process_item(self,item,spider):
        web_name=item['web_name']
        if web_name=='zailushang':
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

class BreadtripPipeline(object):
    def __init__(self):
        #print 'hell000000'
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self,item,spider):
        #print 'hell1111111'
        web_name=item['web_name']
        if web_name=='breadtrip':
            blog_url=item['blog_url']
            author_url=item['author_url']
            title=item['title']
            date=item['date']
            like_num=item['like_num']
            cmt_num=item['cmt_num']
            share_num=item['share_num']
            days=item['days']
            sights=item['sights']
            content=item['content']
            image_urls=item['image_urls']
            self.db.breadtrip.save({'blog_url':blog_url,'author_url':author_url,'title':title,'date':date,'like_num':like_num,'cmt_num':cmt_num,'days':days,'share_num':share_num,' sights': sights,' content':content,'image_urls':image_urls})
            return item
class YiqiquPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.mydb

    def process_item(self, item, spider):
        #print 'hello'
        web_name=item['web_name']
        if web_name=='yiqiqu':
            sight_url=item['sight_url']
            reasons=item['reasons']
            culture=item['culture']
            name = item['name']
            province= item['province']
            city = item['city']
            theme = item['theme']
            rate = item['rate']
            address= item['address']
            price= item['price']
            phone= item['phone']
            opentime= item['opentime']
            image_urls= item['image_urls']
            intro= item['intro']
            notice= item['notice']
            desc= item['desc']
            spots= item['spots']
            food= item['food']
            traffic= item['traffic']
            self.db.yiqiyou.save({'sight_url':sight_url,'reasons':reasons,'culture':culture,'name':name,'province':province,'city':city,'theme':theme,'rate':rate,'image_urls':image_urls,'address':address,'price':price,
                                   'phone':phone,'opentime':opentime,'intro':intro,'notice':notice,'desc':desc,'spots':spots,'food':food,'traffic':traffic})
            return item
