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
        self.note_limit = int(getattr(self, 'note_limit', 100))
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
            从response中的一段数据，生成一个BaiduNoteItem，以及访问游记详情的Request

            :param entry:
            :return:
            """
            from andaman.items.baidu import BaiduNoteItem

            item = BaiduNoteItem()
            item['note_id'] = entry['nid']
            item['title'] = entry['title'].strip()
            item['author_name'] = entry['user_nickname'].strip()
            item['author_avatar'] = image_builder(entry['avatar_small'].strip())
            item['abstract'] = entry['content'].strip()
            item['view_cnt'] = int(entry['view_count'])
            item['vote_cnt'] = int(entry['recommend_count'])
            item['comment_cnt'] = int(entry['common_posts_count'])
            item['favor_cnt'] = int(entry['favorite_count'])
            item['raw_data'] = entry
            item['departure'] = entry['departure']
            try:
                destinations = [x['surl'] for x in entry['destinations']]
            except TypeError:
                destinations = [x for x in entry['destinations']]
            item['destinations'] = destinations
            item['durationtime'] = int(entry['time'])
            item['durationtime_unit'] = entry['time_unit']
            item['start_month'] = int(entry['start_month'])
            item['publish_time'] = long(entry['publish_time']) * 1000L
            item['brief_album'] = map(image_builder, [x['pic_url'] for x in entry['album_pic_list']])

            # 生成正文的请求
            req = scrapy.Request(url='http://lvyou.baidu.com/notes/%s/d-0' % entry['nid'], callback=self.parse_detail,
                                 meta={'nid':entry['nid'], 'page': 0})

            return [item, req]

        # 游记的综合信息
        return [item for pair in imap(build_item, json.loads(response.body_as_unicode())['data']['notes_list']) for item
                in pair]

    def parse_detail(self, response):
        nid = response.meta['nid']
        current_page = response.meta['page']
        sel = scrapy.Selector(response)
        # 由meta中的page来判定是否是第一页，当其它页时，meta也需要设定page
        if current_page == 0:
            # 先抓取路线中的地点
            place_name = sel.xpath('//div[@class="detail-floor1-path"]//span[@class="path-detail"]/a/text()').extract()
            # 若没有地点则不需要抓取
            if place_name:
                place_href = sel.xpath('//div[@class="detail-floor1-path"]//span[@class="path-detail"]/a/@href').extract()
                place_url = ['http://lvyou.baidu.com%s' % x for x in place_href]
                path_place = dict(zip(place_name, place_url))

                from andaman.items.baidu import BaiduNotePathPlace

                path_item = BaiduNotePathPlace()
                path_item['note_id'] = nid
                path_item['path_place'] = path_place
                yield path_item

            # Parse the pagination section
            # 若找不到xpath会返回:空list,即[]
            # 加上.extract()返回的才是list,否则是Secletor
            nodes = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@href]/text()').extract()

            # 写法要会，思维要习惯，自己要写一写，
            # 功能：将val转换成int格式，不能转换则返回0
            def conv_int(val):
                try:
                    return int(val)
                except ValueError:
                    return 0

            max_page = max(map(conv_int, nodes)) if nodes else 0
            # 由maxpage生成要访问的翻页,从第二页开始
            for page_num in xrange(1, max_page):
                yield scrapy.Request(url='http://lvyou.baidu.com/notes/%s/d-%d' % (nid, page_num),
                                     callback=self.parse_detail,
                                     meta={'nid': nid, 'page': page_num})

        #由xpath选出每页中的所有的游记内容
        post_list = sel.xpath('//div[@class="detail-post-item"]')
        for node in post_list:
            # 每一楼都要有note_id、floor_id and contents
            floor_id = node.xpath('./@id').extract()
            contents = node.extract()

            from andaman.items.baidu import BaiduNotePostItem

            post_item = BaiduNotePostItem()
            post_item['note_id'] = nid
            post_item['floor_id'] = floor_id[0]
            post_item['contents'] = contents
            yield post_item


