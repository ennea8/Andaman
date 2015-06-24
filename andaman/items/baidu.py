# coding=utf-8
import scrapy

__author__ = 'zephyre'


class BaiduNoteItem(scrapy.Item):
    # 游记标题
    title = scrapy.Field()
    # 作者名称
    author_name = scrapy.Field()
    # 作者头像
    author_avatar = scrapy.Field()
    # 阅读次数
    view_cnt = scrapy.Field()
    # 评论次数
    comment_cnt = scrapy.Field()
    # upvote次数
    vote_cnt = scrapy.Field()
    # 收藏次数
    favor_cnt = scrapy.Field()
    # 是否为精华
    is_elite = scrapy.Field()
    # 摘要
    abstract = scrapy.Field()
