import scrapy


class MafengwoItem(scrapy.Item):
    start_time = scrapy.Field()
    days = scrapy.Field()
    destination = scrapy.Field()
    departure = scrapy.Field()
    people = scrapy.Field()
    description = scrapy.Field()
    author_avatar = scrapy.Field()
    comments = scrapy.Field()
    tid = scrapy.Field()
