# coding=utf-8
from scrapy import Field, Item

__author__ = 'zephyre'


class QAItem(Item):
    # 蚂蜂窝：mafengwo，穷游：qyer，携程：ctrip
    source = Field()

    # 种类：question / answer
    type = Field()

    # 问题的id
    qid = Field()

    # 回答的id
    aid = Field()

    title = Field()
    author_nickname = Field()
    author_id = Field()
    author_avatar = Field()
    timestamp = Field()

    # 问题的主题（比如：来自“美国”板块这类说法）
    topic = Field()
    contents = Field()
    tags = Field()

    # 被浏览的次数
    view_cnt = Field()
    # 被赞同的次数
    vote_cnt = Field()
    # 是否被采纳
    accepted = Field()

    file_urls = Field()
    files = Field()

