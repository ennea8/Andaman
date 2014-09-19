# coding=utf-8
import re

__author__ = 'zwh'
import copy

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider
import pymongo

from items import YikuaiquSpotItem
from scrapy import log


class YikuaiquSpotSpider(CrawlSpider):
    name = 'yikuaiqu'

    def __init__(self, *a, **kw):
        super(YikuaiquSpotSpider, self).__init__(*a, **kw)

    def start_requests(self):
        lower = 1
        upper = 4000
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])

        for page in xrange(lower, upper):
            url = 'http://www.yikuaiqu.com/mudidi/search.php?province_id=-1&city_id=0&theme_id=0&level_id=0&discount_id=0&human_id=0&mode=0&kw=&page=%d' % page
            yield Request(url=url, callback=self.parse_home)

    def parse_home(self, response):
        match = re.search(r'page=(\d+)', response.url)
        if not match:
            return
        page = int(match.groups()[0])
        self.log('PARSING PAGE: %d' % page, log.INFO)

        sel = Selector(response)
        selitem = sel.xpath('//div[@class="cn_left"]/div[@class="cn_box01"]/div[contains(@class,"cn_shoufu")]')
        for item in selitem:
            m = {}
            ret = item.xpath('./div[@class="cn_sf_img"]/a/img/@data-original').extract()
            if ret:
                m["spot_cover"] = ret[0]
            ret = item.xpath('./div/h2/a/text()').extract()
            if ret:
                m["spot_name"] = ret[0]
            ret = item.xpath('./div[@class="cn_sf_img"]/a/@href').extract()
            if ret:
                m["spot_detail_url"] = ret[0]
            else:
                continue

            # 这里有两种页面：城市页面和景点页面。
            # 前者举例：http://www.yikuaiqu.com/mudidi/city.php?city_id=36
            # 后者举例：http://www.yikuaiqu.com/mudidi/detail.php?scenery_id=12832
            # 我们只处理景点页面。
            url = m["spot_detail_url"]
            match = re.search(r'scenery_id=(\d+)', url)
            if not match:
                continue
            else:
                m['spot_id'] = int(match.groups()[0])

            yield Request(url=url, callback=self.parse_detail, meta={"spot_info": m})

    def parse_detail(self, response):
        sel = Selector(response)
        spot_info = copy.deepcopy(response.meta['spot_info'])
        locality = sel.xpath(
            '//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/div[@class="clearfix"]/h1/span/a/text()').extract()
        county = "".join(sel.xpath(
            '//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/div[@class="clearfix"]/h1/span/text()').extract()).rstrip(
            ']').replace(u'\u2022', '').lstrip('[').strip()
        if county:
            locality.append(county)
        spot_info["spot_locality"] = locality

        ret = sel.xpath('//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/p/span/text()').extract()
        if ret:
            spot_info["spot_address"] = ret[0]

        ret = sel.xpath(
            '//div[contains(@class, "detail-title")]/div[contains(@class, "detail-money")]/h2/span/text()').extract()
        if ret:
            ret = re.search(r'\d+', ret[0])
            if ret:
                spot_info['price'] = int(ret.group())

        ticket_list = []
        for item in sel.xpath(
                '//div[@class="warp"]/div[@id="m_yidin"]/table[@class="reservation"][2]/tbody[@id="ticketdetail"]/tr'):
            ticket = {}
            ticket["Class"] = item.xpath('./th/text()').extract()[0]
            ticket["ticket_name"] = item.xpath('./td/ul/li/a/span/text()').extract()[0]
            ticket["ticket_price"] = item.xpath('./td/ul/li[@class="dele"]/text()').extract()[0]
            ticket["ticket_curmoney"] = item.xpath('./td/ul/li[@class="curmoney"]/text()').extract()[0]
            ticket_info = item.xpath('./td/ul/li[@class="hide-message-wrap"]/div/p/text()')
            if ticket_info:
                ticket["ticket_info"] = ticket_info.extract()[0]
            else:
                ticket["ticket_info"] = None
            ticket_list.append(ticket)
        spot_info["ticket_list"] = ticket_list
        imgpage_url = copy.deepcopy(spot_info["spot_detail_url"]).replace('detail', 'photo')
        spot_info["spot_photopage_url"] = imgpage_url
        yield Request(url=imgpage_url, callback=self.parse_photo, meta={"spot_info": spot_info})

    def parse_photo(self, response):
        sel = Selector(response)
        spot_info = response.meta['spot_info']
        spot_info["img_list"] = sel.xpath('//div[@id="photo"]/ul/li/a[@class="fancybox"]/img/@src').extract()

        item = YikuaiquSpotItem()
        item['spot_id'] = spot_info['spot_id']
        item['name'] = spot_info['spot_name']
        item['locality'] = spot_info['spot_locality']
        item['address'] = spot_info['spot_address']
        item['price'] = spot_info['price']
        item['price_details'] = spot_info['ticket_list']
        item['cover'] = spot_info['spot_cover']
        item['image_list'] = spot_info['img_list']
        item['url'] = spot_info['spot_detail_url']
        yield item


class YikuaiquSpotPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, YikuaiquSpotItem):
            return item

        col = pymongo.Connection().raw_data.YikuaiquViewSpot
        data = col.find_one({'spotId': item['spot_id']})
        if not data:
            data = {}

        data['spotId'] = item['spot_id']

        for key in ('url', 'name', 'locality', 'address', 'price', 'price_details', 'cover', 'image_list'):
            if key not in item:
                continue
            parts = key.split('_')
            tails = [tmp.capitalize() for tmp in parts[1:]]
            head = parts[:1]
            head.extend(tails)
            new_key = ''.join(head)
            data[new_key] = item[key]

        col.save(data)





