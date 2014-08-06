import json
import random

from scrapy.contrib.spiders import CrawlSpider
from bs4 import BeautifulSoup
from scrapy import Request

from items import MafengwoblogItem


class MafengwoSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(MafengwoSpider, self).__init__(*a, **kw)

    # def chooseProxy(self):
    def start_requests(self):
        with open('proxy.json', 'r')as file:
            t = file.readline()
            proxy_list = json.loads(t)
            i = random.randint(0, len(proxy_list) - 1)
            # print len(proxy_list)
        # print i
        # print proxy_list[i]
        Max_Request_Nums = 5
        yield Request(url="http://www.mafengwo.cn/", callback=self.parse_city,
                      meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                            'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))

    def parse_city(self, response):
        # test=response.meta['proxy']
        # print test
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        intenal = soup.find('div', {'class': 'fast-nav-item fast-item-internal'})
        # print intenal
        #clear=[]
        #clear=intenal.findAll('dl',{'class':'clearfix'})
        #print clear
        citys = []
        citys = intenal.findAll('a')
        #print citys
        for city in citys:
            m = {}
            m['url'] = city.get('href')
            m['city'] = city.getText()
            #print m['url']
            i = random.randint(0, len(proxy_list) - 1)
            #print i
            #print proxy_list[i]
            Max_Request_Nums = 5
            yield Request(url="http://www.mafengwo.cn/%s" % m['url'], callback=self.parse_pages,
                          meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))

    def parse_pages(self, response):
        # soup=BeautifulSoup(response.body,from_encoding='utf8')
        # ti=soup.find('a',{'class':'ti'}).get('href')
        # pass
        proxy_list = response.meta['proxy_list']
        #print proxy_list
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        p = soup.find('a', {'class': 'ti last'}).get('href')
        #print p
        pa = p.split('-')
        #print pa[0]
        #print pa[2]
        nums = pa[2].split('.')
        num = int(nums[0])
        #print num
        for i in range(1, num + 1):
            j = random.randint(0, len(proxy_list) - 1)
            Max_Request_Nums = 5
            yield Request(url="http://www.mafengwo.cn%s-0-%s.html" % (pa[0], i), callback=self.parse_blogs,
                          meta={'proxy': "http://%s" % proxy_list[j], 'proxy_list': proxy_list,
                                'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))

    def parse_blogs(self, response):
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        post = soup.find('div', {'class': 'post-list'})
        yahei = post.findAll('h2', {'class': 'post-title yahei'})
        for it in yahei:
            name = it.find('a').get('href')
            url = "http://www.mafengwo.cn%s" % name
            # print url
            i = random.randint(0, len(proxy_list) - 1)
            Max_Request_Nums = 5
            yield Request(url="http://www.mafengwo.cn%s" % name, callback=self.parse_final,
                          meta={'URL': url, 'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))

    def parse_final(self, response):
        blog_url = response.meta['URL']
        print blog_url
        # url="http://www.mafengwo.cn/i/3101108.html"
        item = MafengwoblogItem()
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        # po=soup.findAll('a',{'class':'name'})
        # author_url=po[0].get('href')
        #print author_url
        #print '========soup'
        #print soup
        hd = soup.find('div', {'class': 'post-hd'})
        #print '========hd'
        #print hd
        tit = hd.find('div', {'class': 'post_title clearfix'})
        #print tit
        title = tit.find('h1').getText()
        hot = tit.find('div', {'class': 'num'}).getText()
        print title
        print hot
        info = soup.find('div', {'class': 'post_info'})
        fl = info.find('div', {'class': 'fl'})
        author_url = fl.find('a').get('href')
        print author_url
        date = fl.find('span', {'class': 'date'}).getText()
        content = soup.find('div', {'class': 'a_con_text cont'})
        if content.find('span', {'class': 'digest_icon'}):
            is_recommend = True
        else:
            is_recommend = False
        print is_recommend
        #desc=''
        desc = str(content)
        author_id = content.get('ownerid')
        author_name = content.get('owner')
        print desc
        print author_name
        print author_id
        try:
            basic_info = content.find('div', {'class': 'basic-info'})
            #print basic_info
            clearfix = basic_info.find('ul', {'class': 'clearfix'})
            #print clearfix
            ida = basic_info.find('li', {'class': 'item-date'})
            playtime = ida.find('span').getText()
            print playtime
            ip = basic_info.find('li', {'class': 'item-people'})
            person = ip.find('span').getText()
            print person
            idays = basic_info.find('li', {'class': 'item-days'})
            days = idays.find('span').getText()
            print days
            icos = basic_info.find('li', {'class': 'item-cost'})
            cost = icos.find('span').getText()
            print cost
            itype = basic_info.find('li', {'class': 'item-type'})
            type = itype.find('span').getText()
            print type
        except:
            playtime = ''
            person = ''
            days = ''
            cost = ''
            type = ''
        print playtime
        print person
        print days
        print cost
        print type
        key_info = content.find('div', {'class': 'keyword-info'})
        #print key_info
        try:
            ks = key_info.findAll('a')
            keyword = ''
            for key in ks:
                keyword = keyword + ',' + key.getText()
            print keyword
        except:
            keyword = ''
        ds = content.findAll('p')
        img = []
        imgs = content.findAll('img')
        for it in imgs:
            img.append(it.get('src'))
        print img
        item['web_name'] = 'mafengwo'
        item['author_id'] = author_id
        item['author_name'] = author_name
        item['author_url'] = author_url
        item['blog_url'] = blog_url
        item['date'] = date
        item['title'] = title
        item['tag'] = keyword
        item['hot'] = hot
        item['image_urls'] = img
        item['desc'] = desc
        item['type'] = type
        item['cost'] = cost
        item['person'] = person
        item['play_time'] = playtime
        item['days'] = days
        item['is_recommend'] = is_recommend
        yield item

    def SetProxy(self, Max_Request_Nums):
        Max_Request_Nums = Request.meta['Max_Request_nums']
        Max_Request_Nums = Max_Request_Nums - 1
        if Max_Request_Nums > 0:
            with open('proxy.json', 'r')as file:
                t = file.readline()
                proxy_list = json.loads(t)
                i = random.randint(0, len(proxy_list) - 1)
            yield Request(url="http://www.mafengwo.cn/", callback=self.parse_city,
                          meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list},
                          errback=self.SetProxy(Max_Request_Nums))
        else:
            return








