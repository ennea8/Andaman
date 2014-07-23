# coding=utf-8
import copy
import json
import re
import urllib
from scrapy import Selector
from scrapy import Request
from items import TravelNotesItem

__author__ = 'Jerry'

import scrapy


class TravelNote(scrapy.Spider):
    name = 'notes'
    start_urls = []
    # 发送ajax请求，获得nid(note_id)
    for i in xrange(0, 1001):
        start_urls = start_urls + ['http://lvyou.baidu.com/search/ajax/searchnotes?format=ajax&type=0&pn=%d&rn=24' % i]


    def parse(self, response):
        data = json.loads(response.body)
        note_list = data['data']['notes_list']

        for note in note_list:
            m = {}
            nid = note['nid']
            # 游记url
            note_url = 'http://lvyou.baidu.com/notes/%s/d' % str(nid)
            m['note_url'] = note_url
            yield Request(url=note_url, callback=self.parse_note, meta={'note_data': m})

    def parse_note(self, response):
        d = response.meta['note_data']
        m = copy.deepcopy(d)

        m['user_name'] = response.xpath \
            ('//a[contains(@href, "/user")]/text()')[0].extract()
        m['user_url'] = 'http://lvyou.baidu.com%s' % \
                        response.xpath('//a[contains(@href, "/user")]/@href')[0].extract()

        # 出发时间
        st = response.xpath \
            ('//span[@class="start_time infos"]/text()').extract()
        if len(st) == 0:
            m['start_year'] ="None"
            m['start_month'] ="None"
        else:
            start_time=st[0]
            m['start_year'] = re.search("(\d*)\D*\d*\D*", start_time).group(1)
            m['start_month'] = re.search("\d*\D*(\d*)\D*", start_time).group(1)

        # 出发地
        od = response.xpath \
            ("//span[@class='notes-info-icon path-icon']//span[@class='infos']/text()").extract()
        if len(od) == 0:
            m['origin']="None"
            m['destination']="None"
        else:
            m['origin'] = response.xpath \
                          ("//span[@class='notes-info-icon path-icon']//span[@class='infos']/text()")[0].extract()[1:]
            if len(od) > 1:
                m['destination'] = response.xpath \
                    ("//span[@class='notes-info-icon path-icon']//span[@class='infos']/text()")[-1].extract()
            else:
                m['destination'] = None

        # 旅行时间
        time = response.xpath \
            ("//span[@class='notes-info-icon time-icon']//span[@class='infos']/text()").extract()
        if len(time) == 0:
            m['time'] = "None"
        else:
            m['time'] = time[0]
        # 花费
        cost = response.xpath \
            ("//span[@class='notes-info-icon cost-icon']//span[@class='infos']/text()").extract()
        if len(cost) == 0:
            m['cost'] = "None"
        else:
            m['cost'] = re.search("\D*(\d*-?\d*)\D*", cost[0]).group(1)

        # 文章质量
        quality = response.xpath \
            ("//div[@class='notes-stamp']/img/@title").extract()
        if len(quality) == 0:
            m['quality'] = 'None'
        else:
            m['quality'] = response.xpath \
                ("//div[@class='notes-stamp']/img/@title")[0].extract()

        # 游记标题
        title = response.xpath \
            ("//span[@id='J_notes-title']/text()").extract()
        if len(title) == 0:
            m['title'] = "None"
        else:
            m['title'] = title[0]
        # 游记回复/浏览数
        r_v = response.xpath \
            ("//li[@class='clearfix notes-info-foot']//div[@class='fl']/text()").extract()
        if len(r_v) == 0:
            m['reply'] = "None"
            m['view'] = "None"
        else:
            rep_view=r_v[0]
            m['reply'] = re.search("\D*(\d*)\D*\d*\D*", rep_view).group(1)
            m['view'] = re.search("\D*\d*\D*(\d*)\D*", rep_view).group(1)

        # 游记点赞/收藏数
        rec = response.xpath \
            ("//em[@class='recommend-btn-count']/text()").extract()
        if len(rec) == 0:
            m['recommend'] = "None"
        else:
            m['recommend'] = rec[0]

        fav = response.xpath \
            ("//em[@class='favorite-btn-count']/text()").extract()
        if len(fav) == 0:
            m['favourite'] = "None"
        else:
            m['favourite'] = fav[0]

        # 游记list(title, url, path)
        lis = {}
        list_title = response.xpath \
            (
                "//li[@class='paths-item width540 is-good-notes clearfix']//a[@class='path-name path-nslog-name nslog']/text()").extract()
        l_url = response.xpath \
            (
                "//li[@class='paths-item width540 is-good-notes clearfix']//a[@class='path-name path-nslog-name nslog']/@href").extract()
        if len(l_url) >0:
            list_url = "http://lvyou.baidu.com%s" % str(l_url[0])

        lsit_path = response.xpath \
            ("//li[@class='paths-item width540 is-good-notes clearfix']//span[@class='path-detail']").extract()
        for i in xrange(len(list_title)):
            lis['list_%d' % i] = {"title": list_title[i],
                                  "url": list_url,
                                  "path": lsit_path[i]}
        m['note_list'] = lis

        pages = response.xpath \
            ("//span[@id='J_notes-view-pagelist']//a[@class='nslog']/text()").extract()
        # 如果只有一页
        if len(pages) == 0:
            end_page = 0
        else:
            # 有尾页，取尾页数
            if pages.count("尾页") > 0:
                last_href = response.xpath \
                    ("//span[@id='J_notes-view-pagelist']//a[@class='nslog']/@href")[-1].extract()
                end_page = re.search("\S*\D(\d*$)", last_href).group(1)
            # 没有尾页，取最大页数
            else:
                last_href = response.xpath \
                    ("//span[@id='J_notes-view-pagelist']//a[@class='nslog']/@href")[-2].extract()
                end_page = re.search("\S*\D(\d*$)", last_href).group(1)

        sub_note = {}
        # k为子游记标号
        k = 1
        for i in xrange(int(end_page) + 1):
            url = "%s-%d" % (str(m['note_url']), i)
            wp = urllib.urlopen(url)
            body = wp.read()

            # 子游记标题
            subnote_title = Selector(text=body).xpath \
                ("//div[@class='grid-s5m0 position pt20']//span[@class='bigger path-disabled']/text()").extract()
            # 发表时间
            subnote_date = Selector(text=body).xpath \
                ("//div[@class='grid-s5m0 position pt20']//div[@class='floor-content']/@updatetime").extract()
            # 子游记内容
            subnote_content = Selector(text=body).xpath \
                ("//div[@class='grid-s5m0 position pt20']//div[@class='html-content']").extract()

            for j in xrange(len(subnote_title)):
                sub_note['subnote_%d' % k] = {'title': subnote_title[j], 'date': subnote_date[j],
                                              'content': subnote_content[j], 'url': url}
                k += 1
        m['sub_note'] = sub_note

        item = TravelNotesItem()
        item['user_name'] = m['user_name']
        item['user_url'] = m['user_url']

        item['note_url'] = m['note_url']
        item['note_list'] = m['note_list']

        item['start_year'] = m['start_year']
        item['start_month'] = m['start_month']
        item['origin'] = m['origin']
        item['destination'] = m['destination']
        item['time'] = m['time']
        item['cost'] = m['cost']

        item['quality'] = m['quality']
        item['title'] = m['title']
        item['reply'] = m['reply']
        item['view'] = m['view']
        item['recommend'] = m['recommend']
        item['favourite'] = m['favourite']

        item['sub_note'] = m['sub_note']

        yield item
