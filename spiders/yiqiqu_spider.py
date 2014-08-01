from scrapy.contrib.spiders import CrawlSpider
from items import YiqiqusightItem
from bs4 import BeautifulSoup
from scrapy import Request
import json
import random
import re


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
            # for id in range(10000, 99999):
            url = "http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=152038"
            #print url
            yield Request(url="http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=152038",
                          callback=self.parse_part,
                          meta={'URL': url, 'proxy_list': proxy_list, 'max_request_nums': max_request_nums},
                          errback=self.SetProxy(max_request_nums))

    def parse_part(self, response):
        url = response.meta['URL']
        print url
        proxy_list = response.meta['proxy_list']
        soup = json.loads(response.body, encoding='utf8')
        vs = soup['vsource']
        print vs
        # resons=vs['reasons']
        # print resons
        subjects = vs['subjects']
        print subjects
        desc = subjects[0]['sub']
        print desc
        #print desc['desc']
        spots = subjects[1]['sub']
        print spots
        food = subjects[2]['sub']
        print food
        traffic = subjects[3]['sub']
        print traffic
        max_request_nums = 5
        yield Request(url="http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=12749",
                      callback=self.parse_final,
                      meta={'proxy_list': proxy_list, 'max_request_nums': max_request_nums,
                            'desc': desc, 'spots': spots, 'food': food, 'traffic': traffic},
                      errback=self.SetProxy(max_request_nums))


    def parse_final(self, response):
        # url=response.meta['URL']
        # print url
        item = YiqiqusightItem()
        desc = response.meta['desc']
        spots = response.meta['spots']
        food = response.meta['food']
        traffic = response.meta['traffic']
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
        rate = clearfix.find('span', {'class': 'fl icon-Aqinfei'}).getText()
        print rate
        p = title.find('p')
        address = p.find('span', {'class': 'fl'}).getText()
        print address
        dd = title.find('div', {'class': 'detail-money'})
        price = dd.find('span').getText()
        print price
        mm = wrap.find('div', {'class': 'detail-main clearfix'})
        slide = mm.find('div', {'class': 'fl detail-slide'})
        #conbox=slide.find('div',{'class':'conbox'})
        pis = slide.findAll('img')
        img = []
        for it in pis:
            img.append(it.get('src'))
        print img
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
        #print item
        print name
        item['name'] = name
        item['province'] = province
        item['city'] = city
        item['theme'] = theme
        item['rate'] = rate
        item['address'] = address
        item['price'] = price
        item['phone'] = phone
        item['opentime'] = opentime
        item['img'] = img
        item['intro'] = intro
        item['notice'] = notice
        item['desc'] = desc
        item['spots'] = spots
        item['food'] = food
        item['traffic'] = traffic
        yield item


    def SetProxy(self, max_request_nums):
        max_request_nums = max_request_nums['max_request_nums']
        max_request_nums = max_request_nums - 1
        if max_request_nums > 0:
            with open('proxy.json', 'r')as file:
                t = file.readline()
                proxy_list = json.loads(t)
                i = random.randint(0, len(proxy_list) - 1)
            for id in range(10000, 99999):
                url = "http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id
                yield Request(url="http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=%s" % id,
                              callback=self.parse_final,
                              meta={'URl': url, 'proxy_list': proxy_list, 'max_request_nums': max_request_nums},
                              errback=self.SetProxy(max_request_nums))


