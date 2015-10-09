# coding=utf-8
from mongoengine import connect, Document, StringField, IntField, DateTimeField, ListField, BooleanField
from datetime import datetime

__author__ = 'zephyre'


class BaseEntry(Document):
    meta = {
        'abstract': True,
    }

    # 问答数据的来源
    source = StringField(choices=('mafengwo', 'baidu', 'qyer', 'qunar', 'ctrip'), required=True)

    # 描述
    contents = StringField()

    # 作者的昵称
    author = StringField()

    # 作者的ID
    author_id = IntField(min_value=0, db_field='authorId')

    # 作者的头像
    avatar = StringField()

    # 发布时间
    timestamp = DateTimeField(required=True)


class Question(BaseEntry):
    """
    提问
    """
    # 问题的编号
    qid = IntField(min_value=0, required=True)

    # 标题
    title = StringField(min_length=1, required=True)

    # 问题的主题（比如：『美国』板块）
    topic = StringField(min_length=1, max_length=20)

    # 标签
    tags = ListField(StringField(min_length=1, max_length=20))

    # 被浏览的次数
    view_cnt = IntField(min_value=0, db_field='viewCnt')

    meta = {'collection': 'Question',
            'indexes': [{'fields': ['qid', 'source'], 'unique': True}]}


class Answer(BaseEntry):
    """
    回答
    """

    # 问题的编号
    qid = IntField(min_value=0, required=True)

    # 回答的编号
    aid = IntField(min_value=0, required=True)

    # 被赞同的次数
    vote_cnt = IntField(min_value=0, db_field='voteCnt')

    # 是否被采纳
    accepted = BooleanField()

    meta = {'collection': 'Answer',
            'indexes': [{'fields': ['qid', 'aid', 'source'], 'unique': True}]}


class QAPipeline(object):
    def __init__(self):
        self._conn = {}

    @staticmethod
    def init_db(settings):
        mongos = settings.getdict('ANDAMAN_SERVICES')['mongo']
        endpoints = ['%s:%d' % (server['host'], server['port']) for server in mongos.values()]
        mongo_conf = settings.getdict('ANDAMAN_CONF')['mongo']
        user = mongo_conf['user']
        password = mongo_conf['password']
        db = mongo_conf['db']
        mongo_uri = 'mongodb://%s:%s@%s/%s' % (user, password, ','.join(endpoints), db)
        return connect(host=mongo_uri)

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('PIPELINE_QA_ENABLED', False):
            from scrapy.exceptions import NotConfigured
            raise NotConfigured
        return cls()

    def process_item(self, item, spider):
        # 惰性初始化数据库
        settings = spider.crawler.settings
        spider_name = spider.name
        if spider_name not in self._conn:
            conn = self.init_db(settings)
            if conn:
                self._conn[spider_name] = conn

        item_type = item['type']
        if item_type == 'question':
            entry = Question.objects(qid=item['qid'], source=item['source']).first() or Question()
            self._process_base(entry, item)
            self._process_question(entry, item)
        elif item_type == 'answer':
            entry = Answer.objects(qid=item['qid'], aid=item['aid'], source=item['source']).first() or Answer()
            self._process_base(entry, item)
            self._process_answer(entry, item)
        else:
            return item

        entry.save()
        return item

    @staticmethod
    def _process_base(entry, item):
        """
        不管是提问还是回答，都需要通过本方法，生成基础部分
        :param entry
        :param item:
        :return:
        """
        entry.source = item['source']
        entry.contents = (item.get('contents') or '').strip()
        entry.author = (item.get('author_nickname') or '').strip()
        entry.author_id = item.get('author_id')
        entry.avatar = (item.get('author_avatar') or '').strip()
        entry.timestamp = datetime.utcfromtimestamp(int(item['timestamp']/1000))

        return entry

    @staticmethod
    def _process_question(entry, item):
        """
        生成问题的数据
        :param entry:
        :param item
        :return:
        """
        entry.qid = item['qid']
        entry.title = item['title']
        entry.topic = (item.get('topic') or '').strip() or None
        entry.tags = filter(lambda v: v.strip(), [tmp.strip() for tmp in item.get('tags', [])])
        return entry

    @staticmethod
    def _process_answer(entry, item):
        """
        生成回答的数据
        :param entry:
        :param item:
        :return:
        """
        entry.qid = item['qid']
        entry.aid = item['aid']
        entry.vote_cnt = item.get('vote_cnt', 0)
        entry.accepted = item.get('accepted', False)
        return entry
