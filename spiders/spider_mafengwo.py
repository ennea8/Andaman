__author__ = 'lxf'

# coding=utf-8

import pymongo
import re
from scrapy import Request, Selector,Item,Field
from scrapy.contrib.spiders import CrawlSpider


# define field
class MafengwoItem(Item):
   country=Field()  #country
   city=Field()     #city
   spot_url=Field() #url of spot
   spot_id=Field()  #spot id
   spot=Field()     #spot_cname_yname_profile
   tel=Field()      #telphone
   location=Field() #longitude_latitude
   photo=Field()    #photo
   traffic=Field()  #traffic
   price=Field()    #ticket-price
   time=Field()     #open time
   time_cost=Field()   #cost time




class spidermafengwo(CrawlSpider):
    name = 'mafengwo_spider'    #name of spider

    def start_requests(self):       #send request
        first_url = 'http://www.mafengwo.cn'  #draw from the first url
        #m={'url_temp':url_temp}
        yield Request(url=first_url, callback=self.parse_first_url)


    def parse_first_url(self, response):
        sel = Selector(response)       #anlysis the first page,www.mafengwo.cn
        second_url= sel.xpath('//div[contains(@class,"fast-nav-item fast-item-international")]'\
                             '/div[@class="fast-nav-panel"]/div[@class="inner"]/div[@class="panel-content"]'\
                             '/dl[@class="clearfix"]/dd/a/@href').extract()
        if second_url:
            for tmp_url in second_url:
                url='http://www.mafengwo.cn'+tmp_url
                yield Request(url=url,callback=self.parse_second_url)
        else:
            return


    def parse_second_url(self, response):
        sel=Selector(response)                #anlysis the second page http://www.mafengwo.cn/travel-scenic-spot/mafengwo/10579.html
        third_url=sel.xpath('//ul[@class="nav-box"]/li[@class="nav-item"]'\
                            '/a[@class="btn-item"]/@href').extract()        #the third url
        if third_url:
            spot=third_url[0]
            temp=re.search('\d{1,}',spot)     #reglular,example--third_url[0]:/jd/10083/gonglve.html
            if temp:
                url='http://www.mafengwo.cn'+'/jd/'+temp.group()+'/'+'0-0-0-0-0-%d.html'
            lower =1                          #draw the page of spot
            upper=40
            for tem_id in range(lower, upper):
                tmp_url = url % tem_id
                yield Request(url=tmp_url, callback=self.parse_spot_url)
            #tmp_url='http://www.mafengwo.cn/jd/10077/0-0-0-0-0-1.html'
            yield Request(url=tmp_url, callback=self.parse_spot_url)
        else:
            return


    def parse_spot_url(self, response):
        sel=Selector(response)
        match=sel.xpath('//li[@class="item clearfix"]/div[@class="title"]/h3/a/@href').extract()
        if match:
            for tmp in match:
                spot_url='http://www.mafengwo.cn'+tmp
                data={'spot_url':spot_url}
                yield Request(url=spot_url,callback=self.parse_spot,meta={'data':data})
        else:
            return


    def parse_spot(self, response):
        sel = Selector(response)       #anlysis the page
        item = MafengwoItem()
        spot_data= response.meta['data']
        item['spot_url']=spot_data['spot_url']  #draw the url
        match=re.search('\d{1,}',spot_data['spot_url'])
        if match:
            item['spot_id']=match.group()       #draw the id

        #draw the area
        area = sel.xpath('//span[@class="hd"]/a/text()').extract()
        if area:
                item['country']=area[1]         #draw the country
                item['city']=area[2]            #draw the city

        #---------------------------draw the spot------------------------------------------------------
        #exits bug---array access violation
        spot_c_name = sel.xpath('//div[@class="item cur"]/strong/text()').extract()     #draw the chinese name
        info=sel.xpath('//div[@class="bd"]/p/text()').extract()     #draw the infomation of the spot
        if info:
            spot_profile=info[0]
            spot_y_name=info[1]
            spot={'spot_c_name':spot_c_name,'spot_y_name':spot_y_name,'spot_profile':spot_profile}
            item['spot']=spot
            item['tel']=info[3]
            item['traffic']=info[4]
            item['price']=info[5]
            item['time']=info[6]
            item['time_cost']=info[7]
        else:
            item['spot']=None
            item['tel']=None
            item['traffic']=None
            item['price']=None
            item['time']=None
            item['time_cost']=None

        #-----------------------------draw the longitude_latitude------------------------------------------------------
        info = re.findall(r'(lat|lng)\s*:\s*parseFloat[^\d]+([\d\.]+)', response.body)
        if info:
            longitude=info[1][1]
            latitude=info[0][1]
            location={'longitude':longitude,'latitude':latitude}
            item['location']=location
        else:
            item['location']=None

        #-------------------------------------------draw the photo---------------------------------------
        # the request can not jump to the parse_photo
        temp_url=sel.xpath('//div[@class="pic-r"]/a/@href').extract()
        if temp_url:
            url='http://www.mafengwo.cn'+temp_url
            yield Request(url=url, callback=self.parse_photo, meta={'item': item})
        else:
            item['photo']=None
            #return item

    def parse_photo(self, response):
        sel=Selector(response)
        item = response.meta['item']
        photo_url=sel.xpath('//a[@class="cover"]/img/@src').extract()
        if photo_url:
            if len(photo_url)>=2:
                item['photo']=photo_url[0]+photo_url[1]
            elif len(photo_url)==1:
                item['photo']=photo_url[0]
        else:
                item['photo']=None
        return item


    #---------------------------pipline------------------------------------------------------------
class spidermafengwoPipeline(object):

    def process_item(self, item, spider):

        col = pymongo.MongoClient().raw_data.mafengwo

        if not isinstance(item, MafengwoItem):
            return item
        spot_id=item['spot_id']
        data={'spot_id':spot_id}

        if 'country' in item:
            data['country'] = item['country']
        if 'city' in item:
            data['city'] = item['city']
        if 'spot' in item:
            data['spot'] = item['spot']
        if 'tel' in item:
            data['tel'] = item['tel']
        if 'location' in item:
            data['location'] = item['location']
        if 'photo' in item:
            data['photo'] = item['photo']
        if 'traffic' in item:
            data['traffic'] = item['traffic']
        if 'price' in item:
            data['price'] = item['price']
        if 'time' in item:
            data['time'] = item['time']
        if 'time_cost' in item:
            data['time_cost'] = item['time_cost']

        ret = col.find_one({'spot_id': spot_id})
        if ret:
            ret = {}
        else:
            col.insert(data)
        #return item
