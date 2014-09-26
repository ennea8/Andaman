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




class ChanyoujiUserSpider(CrawlSpider):
    name = 'mafengwo_spider_1'    #name of spider

    def start_requests(self):       #send request
        url_template = 'http://www.mafengwo.cn/poi/%d'  #url

        lower =10000        #url_id
        upper = 500000

        #control the lower and upper
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])
        for spot_id in range(lower, upper):
            spot_url = url_template % spot_id
            m = {'spot_id': spot_id,'spot_url':spot_url}
            yield Request(url=spot_url, callback=self.parse, meta={'data': m})

    def parse(self, response):
        sel = Selector(response)       #anlysis the page
        item = MafengwoItem()
        spot_data= response.meta['data']
        item['spot_id'] = spot_data['spot_id']    #get spot id
        item['spot_url']=spot_data['spot_url']

        #draw the location
        location = sel.xpath('//span[@class="hd"]/a/text()').extract()
        if location:
            item['country']=location[1]
            item['city']=location[2]
        else:
            return

        #draw the spot
        spot_c_name = sel.xpath('//div[@class="item cur"]/strong/text()').extract() #chinese name
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

        #draw the longitude_latitude
        info = re.findall(r'(lat|lng)\s*:\s*parseFloat[^\d]+([\d\.]+)', response.body)
        if info:
            longitude=info[1][1]
            latitude=info[0][1]
            location={'longitude':longitude,'latitude':latitude}
            item['location']=location
        else:
            item['location']=None

        #draw the photo
        temp_url=sel.xpath('//div[@class="pic-r"]/a/@href').extract()
        if temp_url:
            url='http://www.mafengwo.cn'+temp_url
            yield Request(url=url, callback=self.parse_photo, meta={'item': item})
        else:
            return

    def parse_photo(self, response):
        sel=Selector(response)
        item = response.meta['item']
        photo_url=sel.xpath('//a[@class="cover"]/@href').extract()
        if photo_url:
            item['photo']=photo_url[0]+photo_url[1]
        else:
            item['photo']=None
        return item


class ChanyoujiUserSpiderPipeline(object):

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

        return item
