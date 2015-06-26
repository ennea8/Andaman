# coding=utf-8
import scrapy

__author__ = 'zephyre'


class BaiduNoteItem(scrapy.Item):
    """
    一篇游记的整体属性
    """
    # 游记的主键
    note_id = scrapy.Field()
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
    # 全部的数据
    raw_data = scrapy.Field()
    # 游记出发的地点
    departure = scrapy.Field()
    # 游记到达的地点
    destinations = scrapy.Field()
    # 旅程持续的时间
    durationtime = scrapy.Field()
    # 持续时间的单位
    durationtime_unit = scrapy.Field()
    # 出发的月份
    start_month = scrapy.Field()
    # 游记发布的时间，unix格式
    publish_time = scrapy.Field()
    # 简略的相册，只有四张图的链接
    brief_album = scrapy.Field()

class BaiduNotePostItem(scrapy.Item):
    """
    游记的一篇篇帖子（楼层）
    """
    # 游记的主键
    note_id = scrapy.Field()
    # 楼层的id
    floor_id = scrapy.Field()
    # 正文
    contents = scrapy.Field()


class BaiduNotePathPlace(scrapy.Item):
    """
    游记摘要中出现的地方及其介绍链接
    """
    # 游记的主键
    note_id = scrapy.Field()
    # 游记的地方及链接，一一对应
    path_place = scrapy.Field()
