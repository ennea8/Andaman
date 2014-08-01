# coding=utf-8

from scrapy.contrib.spiders import CrawlSpider
from items import ZailushangItem
from bs4 import BeautifulSoup
from scrapy import Request
import json
import random
from twisted.internet import defer
import re
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class ZailushangSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(ZailushangSpider, self).__init__(*a, **kw)

    def start_requests(self):
        with open('proxy.json', 'r') as file:
            t = file.readline()
            proxy_list = json.loads(t)
            i = random.randint(0, len(proxy_list) - 1)
            max_request_nums = 5
            for i in range(1, 9999999):
                url="http://www.117go.com/tour/%s" % i
                yield Request(url="http://www.117go.com/tour/%s" % i,
                              callback=self.parse_blog,
                              meta={'url':url,'proxy_list': proxy_list, 'max_request_nums': max_request_nums},
                              errback=self.SetProxy(max_request_nums))

    def parse_blog(self, response):
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        if soup.find('div', {'class': 't-header-mask'}):
            item = ZailushangItem()
            blog_url=response.meta['url']
            print blog_url
            header = soup.find('div', {'class': 't-header-mask'})
            author_url = 'http://www.117go' + header.find('a').get('href')
            print author_url
            ddb=author_url.split('/')
            author_id=ddb[-1]
            print author_id
            title = header.find('div', {'class': 't-header-title'}).getText()
            dt = header.find('div', {'class': 't-header-date'}).getText()
            dda=dt.split('@')
            author_name=dda[0]
            date=dda[1]
            try :
                tag = header.find('div', {'class': 't-header-tag'}).getText()
            except:
                tag=''
            print tag
            feeds = soup.find('div', {'class': 't-feeds'})
            content = str(feeds)
            feed=feeds.findAll('div',{'class':'t-feed'})
            image_urls=[]
            for ifd in feed:
                try:
                    img_url=ifd.find('a',{'class':'t-feed-pic'})
                    image_urls.append(img_url.get('href'))
                except:
                    image_urls=[]
                #print image_urls
            print image_urls
            print '====================='
            #print content
            like_num = header.find('div', {'class': 't-header-func-t-like'}).getText()
            cmt_num = header.find('div', {'class': 't-header-func-t-cmt'}).getText()
            print author_name
            print title
            print date
            print tag
            print like_num
            print cmt_num
            preface = feeds.find('div', {'class': 't-foreword paragraph'}).getText()
            print preface
            # re.search(r'(?<=<div class="bm_c">).*?(?=</form>)', content,re.S)
            sidebar = soup.find('div', {'class': 't-sidebar'})
            ss = sidebar.findAll('span', {'class': 't-iti-item-name'})
            #print ss
            sights = []
            for it in ss:
                tt = it.getText()
                sights.append(tt)
            print sights
            item['author_url'] = author_url
            item['author_name'] = author_name
            item['author_id'] = author_id
            item['title'] = title
            item['date'] = date
            item['tag'] = tag
            item['like_num'] = like_num
            item['cmt_num'] = cmt_num
            item['preface'] = preface
            item['sights'] = sights
            item['content'] = content
            item['image_urls']=image_urls
            yield item
        else:
            print '======wrong page'
            pass
            '''for i in range(0,len(sights)):
                print sights[i]'''

    def SetProxy(self, max_request_nums):
        max_request_nums = max_request_nums['max_request_nums']
        max_request_nums = max_request_nums - 1
        if max_request_nums > 0:
            with open('proxy.json', 'r')as file:
                t = file.readline()
                proxy_list = json.loads(t)
                i = random.randint(0, len(proxy_list) - 1)
            for i in range(1, 9999999):
                yield Request(url="http://www.117go.com/tour/%s" % i,
                              callback=self.parse_blog,
                              meta={'proxy_list': proxy_list, 'max_request_nums': max_request_nums},
                              errback=self.SetProxy(max_request_nums))




