# coding=utf-8
from itertools import imap
import json

from scrapy import log
import scrapy


__author__ = 'zephyre'


def image_builder(key):
    return 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % key


class BaiduNoteSpider(scrapy.Spider):
    name = 'baidu-note'

    def __init__(self, **kwargs):
        super(BaiduNoteSpider, self).__init__(**kwargs)
        self.note_offset = int(getattr(self, 'note_offset', 0))
        self.note_limit = int(getattr(self, 'note_count', 20))
        self.note_type = int(getattr(self, 'note_type', 3))

    def start_requests(self):
        step_size = 20
        self.note_limit = (self.note_limit / step_size + 1) * step_size
        self.log('Spider started. Note type: %d, offset: %d, count: %d'
                 % (self.note_type, self.note_offset, self.note_limit), level=log.INFO)
        return imap(lambda pn: scrapy.Request(
            'http://lvyou.baidu.com/search/ajax/searchnotes?format=ajax&type=3&pn=%d&rn=20' % pn),
                    xrange(self.note_offset, self.note_limit, step_size))

    def parse(self, response):
        """
        @url http://lvyou.baidu.com/search/ajax/searchnotes?format=ajax&type=3&pn=0&rn=20
        @returns items 20 20
        @returns requests 0 0
        @scrapes title author_name author_avatar abstract view_cnt vote_cnt comment_cnt favor_cnt
        """

        def build_item(entry):
            """
            从response中的一段数据，生成一个BaiduNoteItem

            :param entry:
            :return:
            """
            from andaman.items.baidu import BaiduNoteItem

            item = BaiduNoteItem()
            item['title'] = entry['title'].strip()
            item['author_name'] = entry['user_nickname'].strip()
            item['author_avatar'] = image_builder(entry['avatar_small'].strip())
            item['abstract'] = entry['content'].strip()
            item['view_cnt'] = int(entry['view_count'])
            item['vote_cnt'] = int(entry['recommend_count'])
            item['comment_cnt'] = int(entry['common_posts_count'])
            item['favor_cnt'] = int(entry['favorite_count'])

            return item

        return imap(build_item, json.loads(response.body_as_unicode())['data']['notes_list'])


