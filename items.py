# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field


class SightItem(Item):
    web_name = Field()
    sight_url = Field()
    name = Field()
    province = Field()
    city = Field()
    address = Field()
    theme = Field()
    price = Field()
    phone = Field()
    opentime = Field()
    rate = Field()
    intro = Field()
    image_urls = Field()
    images = Field()


class YiqiqusightItem(SightItem):
    web_name = Field(SightItem.fields['web_name'])
    sight_url = Field(SightItem.fields['sight_url'])
    name = Field(SightItem.fields['name'])
    province = Field(SightItem.fields['province'])
    city = Field(SightItem.fields['city'])
    address = Field(SightItem.fields['address'])
    theme = Field(SightItem.fields['theme'])
    price = Field(SightItem.fields['price'])
    phone = Field(SightItem.fields['phone'])
    opentime = Field(SightItem.fields['opentime'])
    rate = Field(SightItem.fields['rate'])
    intro = Field(SightItem.fields['intro'])
    image_urls = Field(SightItem.fields['image_urls'])
    images = Field(SightItem.fields['images'])
    reasons = Field()
    notice = Field()
    traffic = Field()
    food = Field()
    desc = Field()
    spots = Field()
    culture = Field()


class BlogItem(Item):
    web_name = Field()
    author_url = Field()
    author_id = Field()
    author_name = Field()
    blog_url = Field()
    date = Field()
    title = Field()
    tag = Field()
    hot = Field()
    image_urls = Field()
    images = Field()


class MafengwoblogItem(BlogItem):
    web_name = Field(BlogItem.fields['web_name'])
    author_id = Field(BlogItem.fields['author_id'])
    author_name = Field(BlogItem.fields['author_name'])
    author_url = Field(BlogItem.fields['author_url'])
    blog_url = Field(BlogItem.fields['blog_url'])
    date = Field(BlogItem.fields['date'])
    title = Field(BlogItem.fields['title'])
    tag = Field(BlogItem.fields['tag'])
    hot = Field(BlogItem.fields['hot'])
    image_urls = Field(BlogItem.fields['image_urls'])
    images = Field(BlogItem.fields['images'])
    desc = Field()
    is_recommend = Field()
    play_time = Field()
    type = Field()
    days = Field()
    cost = Field()
    person = Field()


class ZailushangItem(BlogItem):
    web_name = Field(BlogItem.fields['web_name'])
    author_url = Field(BlogItem.fields['author_url'])
    author_name = Field(BlogItem.fields['author_name'])
    author_id = Field(BlogItem.fields['author_id'])
    blog_url = Field(BlogItem.fields['blog_url'])
    title = Field(BlogItem.fields['title'])
    date = Field(BlogItem.fields['date'])
    tag = Field(BlogItem.fields['tag'])
    like_num = Field(BlogItem.fields['hot'])
    image_urls = Field(BlogItem.fields['image_urls'])
    images = Field(BlogItem.fields['images'])
    cmt_num = Field()
    preface = Field()
    sights = Field()
    content = Field()


class BreadtripItem(BlogItem):
    web_name = Field(BlogItem.fields['web_name'])
    author_url = Field(BlogItem.fields['author_url'])
    blog_url = Field(BlogItem.fields['blog_url'])
    title = Field(BlogItem.fields['title'])
    date = Field(BlogItem.fields['date'])
    like_num = Field(BlogItem.fields['hot'])
    image_urls = Field(BlogItem.fields['image_urls'])
    images = Field(BlogItem.fields['images'])
    cmt_num = Field()
    share_num = Field()
    sights = Field()
    content = Field()
    days = Field()


class JsonItem(scrapy.Item):
    data = scrapy.Field()


class BaiduPoiItem(JsonItem):
    pass


class QunarPoiItem(JsonItem):
    pass


class QiniuyunItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pic = scrapy.Field()
    url = scrapy.Field()
    key = scrapy.Field()
    hash_value = scrapy.Field()


class TravelNotesItem(scrapy.Item):
    user_name = scrapy.Field()
    user_url = scrapy.Field()

    note_url = scrapy.Field()
    note_list = scrapy.Field()

    start_year = scrapy.Field()
    start_month = scrapy.Field()
    origin = scrapy.Field()
    destination = scrapy.Field()
    time = scrapy.Field()
    cost = scrapy.Field()

    quality = scrapy.Field()

    title = scrapy.Field()
    reply = scrapy.Field()
    view = scrapy.Field()
    recommend = scrapy.Field()
    favourite = scrapy.Field()

    sub_note = scrapy.Field()


class ChanyoujiUser(scrapy.Item):
    user_id = scrapy.Field()
    user_name = scrapy.Field()
    gender = scrapy.Field()
    num_notes = scrapy.Field()
    weibo_url = scrapy.Field()
    weibo_uid = scrapy.Field()
    douban_url = scrapy.Field()
    douban_uid = scrapy.Field()
    renren_url = scrapy.Field()
    renren_uid = scrapy.Field()
    traveled = scrapy.Field()
    avatar = scrapy.Field()


class ChanyoujiYoujiItem(scrapy.Item):
    trips_id = scrapy.Field()
    data = scrapy.Field()


class MafengwoYoujiItem(scrapy.Item):
    place = scrapy.Field()  # 景观地点
    author = scrapy.Field()  # 游记作者
    title = scrapy.Field()  # 游记标题
    public_time = scrapy.Field()  # 游记发表时间
    cost = scrapy.Field()  # 人均费用
    way = scrapy.Field()  # 旅行方法
    days = scrapy.Field()  # 旅行用的时间
    contents = scrapy.Field()  # 内容
    reply = scrapy.Field()  # 回复


class WeatherItem(scrapy.Item):
    data = scrapy.Field()
    loc = scrapy.Field()


class BreadTripItem(scrapy.Item):
    city_name = scrapy.Field()
    city_id = scrapy.Field()
    trip_info = scrapy.Field()
    blog = scrapy.Field()
    trip_days = scrapy.Field()
    user_url = scrapy.Field()
    user_img = scrapy.Field()


class BaiduTripItem(scrapy.Item):
    scene_info = scrapy.Field()


class YikuaiquSpotItem(scrapy.Item):
    spot_id = scrapy.Field()
    name = scrapy.Field()
    locality = scrapy.Field()
    address = scrapy.Field()
    details = scrapy.Field()
    price = scrapy.Field()
    price_details = scrapy.Field()
    cover = scrapy.Field()
    image_list = scrapy.Field()
    test = scrapy.Field()
    url = scrapy.Field()
