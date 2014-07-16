from scrapy.contrib.spiders import CrawlSpider
from items import BlogItem
from bs4 import BeautifulSoup
from scrapy import Request
import json

class blogspider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(blogspider, self).__init__(*a, **kw)
    def start_requests(self):
        yield Request(url="http://www.mafengwo.cn/",callback=self.parse_city)
    def parse_city(self, response):
        soup=BeautifulSoup(response.body,from_encoding='utf8')
        intenal=soup.find('div',{'class':'fast-nav-item fast-item-internal'})
        #print intenal
        #clear=[]
        #clear=intenal.findAll('dl',{'class':'clearfix'})
        #print clear
        citys=[]
        citys=intenal.findAll('a')
        #print citys
        for city in citys:
             m={}
             m['url']=city.get('href')
             m['city']=city.getText()
             #print m['url']
             yield Request(url="http://www.mafengwo.cn/%s" %m['url'],callback=self.parse_pages,meta={'BlogsData':m})
    def parse_pages(self,response):
        #soup=BeautifulSoup(response.body,from_encoding='utf8')
        #ti=soup.find('a',{'class':'ti'}).get('href')
        #pass
            soup=BeautifulSoup(response.body,from_encoding='utf8')
            p=soup.find('a',{'class':'ti last'}).get('href')
            #print p
            pa=p.split('-')
            #print pa[0]
            #print pa[2]
            nums=pa[2].split('.')
            num=int(nums[0])
            #print num
            i=1
            for i in range(1,num+1):
                #url="http://www.mafengwo.cn%s-0-%s" %(pa[0],i)
                #print url
                yield Request(url="http://www.mafengwo.cn%s-0-%s.html"%(pa[0],i),callback=self.parse_blogs)

    def parse_blogs(self, response):
        item=BlogItem()
        soup=BeautifulSoup(response.body,from_encoding='utf8')
        #print soup
        #hd=soup.find('div',{'class':'post-hd-wrap clearfix'})
        #print hd
        title=soup.find('div',{'class':'post_title clearfix'})
        print title
        #tag=title.find('h1').getText()
        #hot=title.find('div',{'class':'num'}).getText()
        #print tag
        #print hot
        info=soup.find('div',{'class':'post_info'})
        fl=info.find('div',{'class':'fl'})
        author=fl.find('a',{'class':'name'})
        author_url=author.get('href')
        author_name=author.getText()
        print author_url
        print author_name
        content=soup.find('div',{'class':'a_con_text cont'})
        key_info=content.find('div',{'class':'keyword_info'})
        ks=key_info.findAll('a')
        key_word=''
        for key in ks:
            keyword=keyword+key.getText()
        print keyword
        ds=content.findAll('p')
        desc=''
        for de in ds:
            desc=desc+de.getText()
        print desc
        img=[]
        imgs=content.findAll('img')
        for it in imgs:
            img.append(it.get('src'))
        print img
        pass


        

