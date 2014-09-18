__author__ = 'zwh'
from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider
import copy
from items import YikuaiquSpotItem
import pymongo


class YikuaiquSpotSpider(CrawlSpider):
    name = 'yikuaiqu_spot_spider'

    def __init__(self, *a, **kw):
        super(YikuaiquSpotSpider, self).__init__(*a, **kw)

    def start_requests(self):
        for page in range(3884):
            url = 'http://www.yikuaiqu.com/mudidi/search.php?province_id=-1&city_id=0&theme_id=0&level_id=0&discount_id=0&human_id=0&mode=0&kw=&page=%d' % (
            page + 1)
            yield Request(url=url, callback=self.parse_home)

    def parse_home(self, response):
        sel = Selector(response)
        selitem = sel.xpath('//div[@class="cn_left"]/div[@class="cn_box01"]/div[contains(@class,"cn_shoufu")]')
        for item in selitem:
            m = {}
            m["spot_cover"] = item.xpath('./div[@class="cn_sf_img"]/a/img/@src').extract()[0]
            m["spot_name"] = item.xpath('./div/h2/a/text()').extract()[0]
            m["spot_detail_url"] = item.xpath('./div[@class="cn_sf_img"]/a/@href').extract()[0]
            yield Request(url=m["spot_detail_url"], callback=self.parse_detail, meta={"spot_info": m})

    def parse_detail(self, response):
        sel = Selector(response)
        spot_info = copy.deepcopy(response.meta['spot_info'])
        county ="".join(sel.xpath(
            '//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/div[@class="clearfix"]/h1/span/text()').extract()).rstrip(']')
        province_city = sel.xpath(
            '//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/div[@class="clearfix"]/h1/span/a/text()').extract()
        spot_info["spot_locality"] = {"province_city": province_city, "county": county.replace(u'\u2022', '').lstrip('[').strip()}
        spot_info["spot_address"] = \
        sel.xpath('//div[contains(@class,"detail-wrap")]/div[@class="detail-title"]/p/span/text()').extract()[0]
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
        spot_info = copy.deepcopy(response.meta['spot_info'])
        spot_info["img_list"] = sel.xpath('//div[@id="photo"]/ul/li/a[@class="fancybox"]/img/@src').extract()
        item = YikuaiquSpotItem()
        item["spot_info"] = spot_info
        yield item


class YikuaiquSpotPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, YikuaiquSpotItem):
            return item
        scene = item["spot_info"]
        scene_entry = ({"Scene": scene})
        col = pymongo.MongoClient('localhost', 27017).geo.YikuaiquSpot
        entry_exist = col.find_one({'Scene.spot_name': scene["spot_name"]})
        if entry_exist:
            scene_entry['_id'] = entry_exist['_id']
        col.save(scene_entry)
        return item





