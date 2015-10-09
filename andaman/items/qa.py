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

    # 标题
    title = Field()

    # 作者的昵称
    author_nickname = Field()

    # 作者的id
    author_id = Field()

    # 作者的头像
    author_avatar = Field()

    # 帖子的发布时间
    timestamp = Field()

    # 问题的主题（比如：来自“美国”板块这类说法）
    topic = Field()

    # 帖子的内容
    contents = Field()

    # 帖子的标签
    tags = Field()

    # 被浏览的次数
    view_cnt = Field()

    # 被赞同的次数
    vote_cnt = Field()

    # 是否被采纳
    accepted = Field()

    file_urls = Field()
    files = Field()
