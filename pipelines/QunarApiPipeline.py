import pymongo

__author__ = 'zephyre'


class QunarApiPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.col = client.QunarPoiRaw

    def process_item(self, item, spider):
        data = item['data']
        self.col.insert(data)