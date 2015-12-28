from pymongo import MongoClient


class GitHubPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('PIPELINE_GITHUB_ENABLED', False):
            from scrapy.exceptions import NotConfigured
            raise NotConfigured
        return cls(crawler.settings)

    def __init__(self, settings):
        mongo_url = settings.get('ANDAMAN_MONGO_URI')
        self.client = MongoClient(mongo_url)
        self.database = self.client['andaman']

    def process_repo(self, item):
        data = item['data']
        _id = data.pop('id')
        self.database['gh_repo'].update({'_id': _id}, {'$set': data}, upsert=True)

    def process_stargazers(self, item):
        data = item['data']
        repo = data['repo']
        stargazers = [t['login'] for t in data['stargazers']]
        self.database['gh_repo'].update({'full_name': repo}, {'$addToSet': {'stargazers': {'$each': stargazers}}},
                                        upsert=True)

    def process_pulls(self, item):
        data = item['data']
        repo = data['repo']
        pull_users = [t['user']['login'] for t in data['pulls']]
        self.database['gh_repo'].update({'full_name': repo}, {'$addToSet': {'pull_users': {'$each': pull_users}}},
                                        upsert=True)

        for pull in data['pulls']:
            _id = pull.pop('id')
            self.database['gh_pull'].update({'_id': _id}, {'$set': pull}, upsert=True)

    def process_forks(self, item):
        data = item['data']
        repo = data['repo']
        fork_users = [t['owner']['login'] for t in data['forks']]
        self.database['gh_repo'].update({'full_name': repo}, {'$addToSet': {'fork_users': {'$each': fork_users}}},
                                        upsert=True)

        for fork in data['forks']:
            _id = fork.pop('id')
            self.database['gh_fork'].update({'_id': _id}, {'$set': fork}, upsert=True)

    def process_issues(self, item):
        data = item['data']
        repo = data['repo']
        issue_users = [t['user']['login'] for t in data['issues']]
        self.database['gh_repo'].update({'full_name': repo}, {'$addToSet': {'issue_users': {'$each': issue_users}}},
                                        upsert=True)

        for issue in data['issues']:
            _id = issue.pop('id')
            self.database['gh_issue'].update({'_id': _id}, {'$set': issue}, upsert=True)

    def process_user(self, item):
        data = item['data']
        _id = data.pop('id')
        self.database['gh_user'].update({'_id': _id}, {'$set': data}, upsert=True)

    def process_item(self, item, spider):
        item_type = item['item_type']
        if item_type == 'repo':
            self.process_repo(item)
        elif item_type == 'stargazers':
            self.process_stargazers(item)
        elif item_type == 'forks':
            self.process_forks(item)
        elif item_type == 'pulls':
            self.process_pulls(item)
        elif item_type == 'issues':
            self.process_issues(item)
        elif item_type == 'user':
            self.process_user(item)
        else:
            assert 'Invalid item_type: %s' % item_type

        return item
