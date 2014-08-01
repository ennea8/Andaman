from scrapy.contrib.spiders import CrawlSpider
from items import BlogItem
from bs4 import BeautifulSoup
from scrapy import Request
import json
import MySQLdb


class blogspider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(blogspider, self).__init__(*a, **kw)

    def start_requests(self):
        conn = MySQLdb.connect(host='127.0.0.1',  # @UndefinedVariable
                               user='root',
                               passwd='123',
                               db='plan',
                               charset='utf8')
        cursor = conn.cursor()
        SQL_string = 'select ip from proxy'
        cursor.execute(SQL_string)
        proxys = cursor.fetchall()
        conn.commit()
        proxy_list = []
        for it in proxys:
            proxy_list.append(it[0])
        # print proxy_list
        i = 0
        print proxy_list[0]
        yield Request(url="http://www.mafengwo.cn/", callback=self.parse_city,
                      meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list})
        i = i + 2

    def parse_city(self, response):
        test = response.meta['proxy']
        print test
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
            i = 2
            yield Request(url="http://www.mafengwo.cn/%s" % m['url'], callback=self.parse_pages,
                          meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list})
            i = i + 2

    def parse_pages(self, response):
        # soup=BeautifulSoup(response.body,from_encoding='utf8')
        #ti=soup.find('a',{'class':'ti'}).get('href')
        #pass
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        p = soup.find('a', {'class': 'ti last'}).get('href')
        #print p
        pa = p.split('-')
        #print pa[0]
        #print pa[2]
        nums = pa[2].split('.')
        num = int(nums[0])
        #print num
        j = 4
        for i in range(1, num + 1):
            #url="http://www.mafengwo.cn%s-0-%s" %(pa[0],i)
            #print url
            yield Request(url="http://www.mafengwo.cn%s-0-%s.html" % (pa[0], i), callback=self.parse_blogs,
                          meta={'proxy': "http://%s" % proxy_list[4], 'proxy_list': proxy_list})
            j = j + 2

    def parse_blogs(self, response):
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        post = soup.find('div', {'class': 'post-list'})
        yahei = post.findAll('h2', {'class': 'post-title yahei'})
        for it in yahei:
            name = it.find('a').get('href')
            url = "http://www.mafengwo.cn%s" % name
            # print url
            i = 0
            yield Request(url="http://www.mafengwo.cn%s" % name, callback=self.parse_final,
                          meta={'URL': url, 'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list})
            i = i + 2

    def parse_final(self, response):
        url = response.meta['URL']
        print url
        item = BlogItem()
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        # print soup
        #hd=soup.find('div',{'class':'post-hd-wrap clearfix'})
        #print hd
        title = soup.find('div', {'class': 'post_title clearfix'})
        #print title
        tag = title.find('h1').getText()
        hot = title.find('div', {'class': 'num'}).getText()
        print tag
        print hot
        info = soup.find('div', {'class': 'post_info'})
        fl = info.find('div', {'class': 'fl'})
        date = fl.find('span', {'class': 'date'}).getText()
        content = soup.find('div', {'class': 'a_con_text cont'})
        #desc=''
        desc = str(content)
        author_id = content.get('ownerid')
        author_name = content.get('owner')
        print desc
        print author_name
        print author_id
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
        item['author_id'] = author_id
        item['author_name'] = author_name
        item['date'] = date
        item['tag'] = tag
        item['keyword'] = keyword
        item['desc'] = desc
        item['img'] = img
        item['url'] = url
        yield item




