# coding=utf-8
import json
import random

from scrapy.contrib.spiders import CrawlSpider
from scrapy import Request
from bs4 import BeautifulSoup

from items import YiqiqusightItem


class YiqiquSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(YiqiquSpider, self).__init__(*a, **kw)


    def start_requests(self):
        # print 'hello'
        with open('proxy.json', 'r') as file:
            t = file.readline()
            proxy_list = json.loads(t)
            i = random.randint(0, len(proxy_list) - 1)
            max_request_nums = 5
            for id in range(12749, 12750):
                url = "http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id
                yield Request(url="http://www.yikuaiqu.com/mudidi/photo.php?scenery_id=%s" % id,
                              callback=self.parse_images,
                              meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                    'max_request_nums': max_request_nums, 'id': id, 'url': url},
                              errback=self.SetProxy(max_request_nums))

    def parse_judge(self, response):
        url = response.meta['url']
        # print url
        id = response.meta['id']
        # print id
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        wrap = soup.find('div', {'class': 'detail-wrap box-line'})
        title = wrap.find('div', {'class': 'detail-title'})
        clearfix = title.find('div', {'class': 'clearfix'})
        na = clearfix.find('h1', {'class': 'fl'})
        print na.getText()
        if na.getText() and na.getText() != '' and na.getText() != '[浙江 • 温州 • 鹿城区]':
            # print na.getText()
            i = random.randint(0, len(proxy_list) - 1)
            max_request_nums = 5
            yield Request(url="http://www.yikuaiqu.com/mudidi/photo.php?scenery_id=%s" % id,
                          callback=self.parse_images,
                          meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                'max_request_nums': max_request_nums, 'id': id},
                          errback=self.SetProxy(max_request_nums))
        else:
            print '======wrong page!'

    def parse_images(self, response):
        id = response.meta['id']
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        photo = soup.find('div', {'id': 'photo'})
        lis = photo.findAll('li')
        j = 1
        image_urls = []
        for j in range(1, len(lis)):
            li = lis[j]
            image_urls.append(li.find('a').get('href'))
        # print image_urls
        # print len(image_urls)
        i = random.randint(0, len(proxy_list) - 1)
        max_request_nums = 5
        yield Request(url="http://www.yikuaiqu.com//interface/subject_reason_detail.php?scenery_id=%s" % id,
                      callback=self.parse_part,
                      meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                            'max_request_nums': max_request_nums, 'image_urls': image_urls, 'id': id},
                      errback=self.SetProxy(max_request_nums))

    def parse_part(self, response):
        id = response.meta['id']
        image_urls = response.meta['image_urls']
        print image_urls
        proxy_list = response.meta['proxy_list']
        soup = json.loads(response.body, encoding='utf8')
        # soup=BeautifulSoup(response.body,from_encoding='utf8')
        # print soup
        reasons = ''
        desc = ''
        spots = ''
        food = ''
        culture = ''
        traffic = ''
        try:
            vs = soup['vsource']
            reasons = vs['reasons']
            # print reasons
            # print vs
            # resons=vs['reasons']
            # print resons
            subjects = vs['subjects']
            #print subjects
            i = 0

            #print subjects
            #print len(subjects)
            for i in range(0, len(subjects)):
                #print subjects[i]['sub']
                if subjects[i]['name'] == '景区介绍':
                    desc = subjects[i]['sub']
                elif subjects[i]['name'] == '游玩景点':
                    spots = subjects[i]['sub']
                elif subjects[i]['name'] == '特色美食':
                    food = subjects[i]['sub']
                elif subjects[i]['name'] == '历史文化':
                    culture = subjects[i]['sub']
                elif subjects[i]['name'] == '交通路线':
                    traffic = subjects[i]['sub']
        except:
            pass
        print reasons
        print desc
        print spots
        print food
        print culture
        print traffic
        '''print desc[0]['desc']
        print spots[0]['desc']
        print food[0]['desc']
        print culture[0]['desc']
        print traffic[0]['desc']'''
        max_request_nums = 5
        url = "http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id
        yield Request(url="http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id,
                      callback=self.parse_final,
                      meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                            'max_request_nums': max_request_nums,
                            'reasons': reasons, 'culture': culture, 'desc': desc, 'spots': spots, 'food': food,
                            'traffic': traffic, 'URL': url, 'image_urls': image_urls},
                      errback=self.SetProxy(max_request_nums))


    def parse_final(self, response):
        print 'hello'
        # url=response.meta['URL']
        # print url
        item = YiqiqusightItem()
        reasons = response.meta['reasons']
        sight_url = response.meta['URL']
        image_urls = response.meta['image_urls']
        print len(image_urls)
        desc = response.meta['desc']
        spots = response.meta['spots']
        food = response.meta['food']
        traffic = response.meta['traffic']
        culture = response.meta['culture']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        wrap = soup.find('div', {'class': 'detail-wrap box-line'})
        title = wrap.find('div', {'class': 'detail-title'})
        clearfix = title.find('div', {'class': 'clearfix'})
        na = clearfix.find('h1', {'class': 'fl'}).getText()
        nam = na.split('[')[0]
        name = str(nam)
        ss = clearfix.findAll('a')
        province = ss[0].getText()
        city = ss[1].getText()
        print name
        print province
        print city
        try:
            rate = clearfix.find('span', {'class': 'fl icon-Aqinfei'}).getText()
        except:
            rate = ''
        print rate
        p = title.find('p')
        address = p.find('span', {'class': 'fl'}).getText()
        print address
        dd = title.find('div', {'class': 'detail-money'})
        price = dd.find('span').getText()
        print price
        mm = wrap.find('div', {'class': 'detail-main clearfix'})
        slide = mm.find('div', {'class': 'fl detail-slide'})
        # conbox=slide.find('div',{'class':'conbox'})
        side = mm.find('div', {'class': 'detail-side-wrap fr'})
        theme = side.find('dt').getText()
        pp = side.findAll('dd')
        phone = pp[1].getText()
        opentime = pp[2].getText()
        intro = pp[3].find('div').getText()
        print theme
        print phone
        print opentime
        print intro
        notice = soup.find('table', {'class': 'notice-main'}).getText()
        print notice
        # print item
        print name
        item['web_name'] = 'yiqiqu'
        item['sight_url'] = sight_url
        item['name'] = name
        item['province'] = province
        item['city'] = city
        item['theme'] = theme
        item['rate'] = rate
        item['address'] = address
        item['price'] = price
        item['phone'] = phone
        item['opentime'] = opentime
        item['image_urls'] = image_urls
        item['intro'] = intro
        item['notice'] = notice
        item['desc'] = desc
        item['spots'] = spots
        item['food'] = food
        item['traffic'] = traffic
        item['culture'] = culture
        item['reasons'] = reasons
        yield item


    def SetProxy(self, max_request_nums):
        max_request_nums = max_request_nums['max_request_nums']
        max_request_nums = max_request_nums - 1
        if max_request_nums > 0:
            with open('proxy.json', 'r')as file:
                t = file.readline()
                proxy_list = json.loads(t)
                i = random.randint(0, len(proxy_list) - 1)
            for id in range(12749, 12750):
                url = "http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id
                yield Request(url="http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id,
                              callback=self.parse_judge,
                              meta={'proxy': "http://%s" % proxy_list[i], 'URl': url, 'proxy_list': proxy_list,
                                    'max_request_nums': max_request_nums},
                              errback=self.SetProxy(max_request_nums))


