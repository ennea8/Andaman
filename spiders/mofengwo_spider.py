from scrapy.contrib.spiders import CrawlSpider
#from items import BlogItem
from bs4 import BeautifulSoup
from scrapy import Request
import json

class blogspider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(blogspider, self).__init__(*a, **kw)
    def start_requests(self):
        return Request(url="http://www.mafengwo.cn/",callback=self.parse_city)
    def parse_city(self, response):
        soup=BeautifulSoup(response.body,from_encoding='utf8')
        intenal=soup.find('div',{'class':'fast-nav-item fast-item-internal'})
        citys=intenal.findall('a')
        for city in citys:
             m={}
             m['url']=city.get('href')
             m['city']=city.getText()
             yield Request(url="http://www.mafengwo.cn/%s" %(m['url'],m['city']),callback=self.parse_blogs,meta={'BlogsData':m})
    def parse_blogs(self,response):


