# coding=utf-8
__author__ = 'lxf'

import re
import pymongo
from scrapy import Request, Selector, Item, Field
from scrapy.contrib.spiders import CrawlSpider


# define field
class MafengwoItem(Item):
    country = Field()  # country
    city = Field()  # city
    spot_url = Field()  # url of spot
    spot_id = Field()  # spot id
    spot = Field()  # spot_cname_yname_profile
    tel = Field()  # telphone
    lat = Field()
    lng = Field()
    addr = Field()
    photo = Field()  # photo
    traffic = Field()  # traffic
    price = Field()  # ticket-price
    time = Field()  # open time
    time_cost = Field()  # cost time


class spidermafengwo(CrawlSpider):
    name = 'mafengwo_spider'  # name of spider

    def start_requests(self):  # send request
        first_url = 'http://www.mafengwo.cn'  # draw from the first url

        # m={'url_temp':url_temp}
        yield Request(url=first_url, callback=self.parse_first_url)

    def parse_first_url(self, response):
        sel = Selector(response)  # anlysis the first page,www.mafengwo.cn
        second_url = sel.xpath('//div[contains(@class,"fast-nav-item fast-item-international")]' \
                               '/div[@class="fast-nav-panel"]/div[@class="inner"]/div[@class="panel-content"]' \
                               '/dl[@class="clearfix"]/dd/a/@href').extract()
        if second_url:
            for tmp_url in second_url:
                url = 'http://www.mafengwo.cn' + tmp_url
                yield Request(url=url, callback=self.parse_second_url)
        else:
            return

    def parse_second_url(self, response):
        sel = Selector(
            response)  # anlysis the second page http://www.mafengwo.cn/travel-scenic-spot/mafengwo/10579.html
        third_url = sel.xpath('//ul[@class="nav-box"]/li[@class="nav-item"]' \
                              '/a[@class="btn-item"]/@href').extract()  # the third url
        if third_url:
            spot = third_url[0]
            temp = re.search('\d{1,}', spot)  # reglular,example--third_url[0]:/jd/10083/gonglve.html
            if temp:
                url = 'http://www.mafengwo.cn' + '/jd/' + temp.group() + '/' + '0-0-0-0-0-%d.html'
            else:
                return
            lower = 1  # draw the page of spot
            upper = 40
            for tem_id in range(lower, upper):
                tmp_url = url % tem_id
                yield Request(url=tmp_url, callback=self.parse_spot_url)
                # tmp_url='http://www.mafengwo.cn/jd/10077/0-0-0-0-0-1.html'
                # yield Request(url=tmp_url, callback=self.parse_spot_url)
        else:
            return

    def parse_spot_url(self, response):
        sel = Selector(response)
        match = sel.xpath('//li[@class="item clearfix"]/div[@class="title"]/h3/a/@href').extract()
        if match:
            for tmp in match:
                spot_url = 'http://www.mafengwo.cn' + tmp
                data = {'spot_url': spot_url}
                yield Request(url=spot_url, callback=self.parse_spot, meta={'data': data})
        else:
            return

    def parse_spot(self, response):
        sel = Selector(response)  # anlysis the page
        item = MafengwoItem()
        spot_data = response.meta['data']
        item['spot_url'] = spot_data['spot_url']  # draw the url
        match = re.search('\d{1,}', spot_data['spot_url'])
        if match:
            item['spot_id'] = int(match.group())  # draw the id
        else:
            return

        # draw the area
        area = sel.xpath('//span[@class="hd"]/a/text()').extract()
        if area:
            item['country'] = area[1]  # draw the country
            item['city'] = area[2]  # draw the city
        else:
            item['country'] = None
            item['city'] = None

        # ---------------------------draw the spot------------------------------------------------------
        spot = {}
        spot_c_name = sel.xpath('//div[@class="item cur"]/strong/text()').extract()  # draw the chinese name
        if spot_c_name:
            spot['spot_c_name'] = spot_c_name[0]
        else:
            spot['spot_c_name'] = None

        info = sel.xpath(
            '//div[@class="bd"]/p/text() | //div[@class="bd"]/h3/text()').extract()  # draw the infomation of the spot
        if info:
            for i in xrange(len(info) / 2):
                spot_key = info[i * 2]
                spot_value = info[i * 2 + 1]
                if spot_key == u'简介':
                    spot['spot_profile'] = spot_value
                elif spot_key == u'英文名称':
                    spot['spot_y_name'] = spot_value
                elif spot_key == u'电话':
                    item['tel'] = spot_value
                elif spot_key == u'交通':
                    item['traffic'] = spot_value
                elif spot_key == u'开放时间':
                    item['time'] = spot_value
                elif spot_key == u'用时参考':
                    item['time_cost'] = spot_value
                elif spot_key == u'门票':
                    item['price'] = spot_value
                elif spot_key == u'地址':
                    item['addr'] = spot_value
                else:
                    continue

        item['spot'] = spot

        # -----------------------------draw the longitude_latitude------------------------------------------------------
        info = re.findall(r'(lat|lng)\s*:\s*parseFloat[^\d]+([\d\.]+)', response.body)
        if info:
            item['lng'] = info[1][1]
            item['lat'] = info[0][1]

        # -------------------------------------------draw the photo---------------------------------------
        # the request can not jump to the parse_photo(此处为抓取图片处，存在bug)
        ret = sel.xpath('//div[@class="pic-r"]/a[@href]')
        if ret:
            a_node = ret[0]
            ret = a_node.xpath('./span[@class="pic-num"]/text()').extract()
            if not ret:
                return item

            ret = ret[0]
            match = re.search(r'\d+', ret)
            pic_num = 0
            if match:
                pic_num = int(match.group()[0])

            if pic_num == 0:
                return item

            page_idx = 1
            url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=getPoiPhotoList&poiid=%d&page=%d' % (
                item['spot_id'], page_idx)
            return Request(url=url, callback=self.parse_photo, meta={'item': item, 'page_idx': page_idx})
        else:
            item['photo'] = None
            return item

    def parse_photo(self, response):
        sel = Selector(response)
        item = response.meta['item']
        item['photo'] = [re.sub(r'\.rbook_comment\.w\d+\.', '.', tmp) for tmp in
                         sel.xpath('//ul/li/a[@class="cover"]/img/@src').extract()]
        return item


# ---------------------------pipeline------------------------------------------------------------


class spidermafengwoPipeline(object):

    def process_item(self, item, spider):
        if type(item).__name__ != MafengwoItem.__name__:
            return item

        spot_id = item['spot_id']
        data = {'spot_id': spot_id}

        if 'country' in item:
            data['country'] = item['country']
        if 'city' in item:
            data['city'] = item['city']
        if 'spot' in item:
            data['spot'] = item['spot']
        if 'tel' in item:
            data['tel'] = item['tel']
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
        if 'lat' in item:
            data['lat'] = item['lat']
        if 'lng' in item:
            data['lng'] = item['lng']
        if 'addr' in item:
            data['addr'] = item['addr']

        col = pymongo.MongoClient().raw_data.MafengwoSpot
        ret = col.find_one({'spot_id': spot_id})
        if not ret:
            ret = {}
        for k in data:
            ret[k] = data[k]
        col.save(ret)

        return item

