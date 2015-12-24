import scrapy


class PintourItem(scrapy.Item):
    title = scrapy.Field()
    type = scrapy.Field()
    start_time = scrapy.Field()
    days = scrapy.Field()
    destination = scrapy.Field()
    departure = scrapy.Field()
    people = scrapy.Field()
    description = scrapy.Field()
    author = scrapy.Field()
    author_avatar = scrapy.Field()
    comments = scrapy.Field()
    tid = scrapy.Field()