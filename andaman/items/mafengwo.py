# coding=utf-8
from scrapy import Field, Item

__author__ = 'zephyre'


class MafengwoQuestion(Item):
    qid = Field()
    title = Field()
    author_nickname = Field()
    author_id = Field()
    author_avatar = Field()
    timestamp = Field()
    topic = Field()
    contents = Field()
    tags = Field()
    view_cnt = Field()
    file_urls = Field()
    files = Field()


class MafengwoAnswer(Item):
    qid = Field()
    aid = Field()
    author_nickname = Field()
    author_id = Field()
    author_avatar = Field()
    timestamp = Field()
    contents = Field()
    vote_cnt = Field()
    accepted = Field()
    file_urls = Field()
    files = Field()

