# coding=utf-8
import json
import logging
import threading
import urlparse
from Queue import Queue

import scrapy
import time
from scrapy.http import Request
import re

from andaman.items.github import GitHubItem

__author__ = 'zephyre'


def check_quota():
    def decorator(function):
        # 统计信息日志打印的时间间隔
        STAT_LOG_INTERVAL = 60

        # 统计信息的时间窗口宽度
        STAT_WINDOW_SIZE = 40

        # 统计信息采样的时间间隔
        STAT_SAMPLE_INTERVAL = 2

        # Quota统计
        stat = {'last_sample': 0, 'last_log': 0}
        stat_queue = Queue(maxsize=STAT_WINDOW_SIZE / STAT_SAMPLE_INTERVAL)
        # 锁
        stat_lock = threading.Lock()

        def do_stat(spider):
            stat['last_sample'] = time.time()
            availables = filter(lambda v: v['quota_remaining'] > 0, spider.access_tokens.values())
            if not availables:
                return

            remainings = [t.get('quota_remaining') for t in availables]
            cnt = len(availables)
            total = sum(remainings)

            # 加入采样队列
            head_item = None
            ts = time.time()
            if stat_queue.full():
                head_item = stat_queue.get_nowait()
            stat_queue.put_nowait({'token_cnt': cnt, 'remaining': total, 'sample_time': ts})

            # 是否需要输出到日志?
            if ts - stat['last_log'] > STAT_LOG_INTERVAL and head_item:
                stat['last_log'] = ts
                ts0 = head_item['sample_time']
                if ts != 0:
                    rate = (head_item['remaining'] - total) / (ts - ts0)
                    spider.logger.info('%d available tokens, %d remaining quota in all, cosumption rate is %.2f per sec'
                                       % (cnt, total, rate))

        def wrapper(*args, **kwargs):
            spider = args[0]
            response = args[1]

            token = re.sub(r'^token\s+', '', response.request.headers.get('Authorization', ''))
            if token:
                quota_limit = int(response.headers['X-Ratelimit-Limit'])
                quota_remaining = int(response.headers['X-Ratelimit-Remaining'])
                quota_reset = int(response.headers['X-Ratelimit-Reset'])

                spider.update_quota(token, {'quota_limit': quota_limit, 'quota_remaining': quota_remaining,
                                            'quota_reset': quota_reset})

            # 尝试统计Quota信息
            now = time.time()
            if now - stat['last_sample'] > STAT_SAMPLE_INTERVAL:
                try:
                    stat_lock.acquire()
                    if now - stat['last_sample'] > STAT_SAMPLE_INTERVAL:
                        do_stat(spider)
                finally:
                    stat_lock.release()

            return function(*args, **kwargs)

        return wrapper

    return decorator


class GithubRepo(scrapy.Spider):
    """
    抓取GitHub的repo信息, 以及它们的所有owner, contributor, stargazer, forker, issuer, pull request sender等用户信息
    """
    name = 'github-repo'

    api_host = 'https://api.github.com'

    # GitHub API默认的页数为30, 改为100
    page_size = 100

    def __init__(self, *args, **kwargs):
        super(GithubRepo, self).__init__(*args, **kwargs)
        self.access_tokens = None

    def parse_quota_info(self, response):
        token = response.meta['token']
        quota_limit = int(response.headers['X-Ratelimit-Limit'])
        quota_remaining = int(response.headers['X-Ratelimit-Remaining'])
        quota_reset = int(response.headers['X-Ratelimit-Reset'])

        self.update_quota(token, {'quota_limit': quota_limit, 'quota_remaining': quota_remaining,
                                  'quota_reset': quota_reset})

        # 如果获得了所有的quota, 则进入下一步:
        if len(filter(lambda v: v['quota_limit'] is None, self.access_tokens.values())) == 0:
            availables = filter(lambda v: v['quota_remaining'] > 0, self.access_tokens.values())
            total = sum([v.get('quota_remaining') for v in availables])

            self.logger.info(
                    'Quota information retrieved. %d tokens available, %d remaining quota' % (len(availables), total))

            repos = filter(lambda v: v.strip(), self.settings.get('GITHUB_REPOS', '').split(','))
            if repos:
                # 用户指定需要抓取的对象
                for repo in repos:
                    yield self.build_api_req('/repos/%s' % repo, callback=self.parse_repo)

            if self.settings.getbool('GITHUB_REPOS_SEARCH', False):
                # 抓取repo排行榜
                for req in self.repo_search_req(page_cnt=5):
                    yield req

    def repo_search_req(self, sortby='stars', per_page=100, page_start=1, page_cnt=1):
        """
        搜索repo
        """
        for page in xrange(page_start, page_start + page_cnt):
            yield self.build_api_req(path='/search/repositories?q=stars:>1&sort=%s&order=desc&page=%d&per_page=%d' % (
            sortby, page, per_page), callback=self.parse_repo_search)

    def start_requests(self):
        self.access_tokens = {t: {'quota_limit': None, 'quota_remaining': None, 'quota_reset': None}
                              for t in self.settings.get('GITHUB_ACCESS_TOKENS').split(',')}

        if not self.access_tokens:
            logging.getLogger('scrapy').error('GitHub token is not specified.')
            return

        # 获取各个quota的信息
        self.logger.info('Retrieving quota information...')
        for token, quota in self.access_tokens.items():
            yield Request(url='https://api.github.com/users/scrapy', meta={'token': token}, dont_filter=True,
                          callback=self.parse_quota_info,
                          headers={'Authorization': 'token %s' % token})

    def update_quota(self, token, data):
        """
        更新token的quota信息
        :param token:
        :param data:
        :return:
        """
        quota_item = self.access_tokens[token]
        for k, v in data.items():
            quota_item[k] = v

    def build_api_url(self, path):
        """
        构造API请求的url
        """
        from urlparse import urljoin

        return urljoin(self.api_host, path)

    def build_api_req(self, path, **kwargs):
        """
        构造API请求(自动添加Authorization字段)
        """
        headers = kwargs.get('headers', {})

        import random

        available_tokens = filter(
                lambda v: self.access_tokens[v]['quota_remaining'] > 50 or self.access_tokens[v][
                                                                               'quota_remaining'] is None,
                self.access_tokens.keys())
        token = random.choice(available_tokens)
        headers['Authorization'] = 'token %s' % token
        kwargs['headers'] = headers
        return Request(url=self.build_api_url(path), **kwargs)

    def get_user_request(self, user):
        """
        构造用户详情的请求
        """
        return self.build_api_req('/users/%s' % user, callback=self.parse_user)

    @check_quota()
    def parse_user(self, response):
        """
        解析用户信息
        """
        user_data = json.loads(response.body)

        # self.logger.info(
        #         'user=%s \t email=%s \t blog=%s' % (user_data['login'], user_data['email'], user_data['blog']))

        user_item = GitHubItem()
        user_item['item_type'] = 'user'
        user_item['data'] = user_data
        yield user_item

    @check_quota()
    def parse_repo(self, response):
        """
        解析repo信息
        """
        repo_data = json.loads(response.body)
        repo_name = repo_data['full_name']

        repo_item = GitHubItem()
        repo_item['item_type'] = 'repo'
        repo_item['data'] = repo_data
        yield repo_item

        # Owner
        yield self.get_user_request(repo_data['owner']['login'])

        # 找到stargazers
        yield self.build_api_req('/repos/%s/stargazers?page=1&per_page=%d' % (repo_name, self.page_size),
                                 callback=self.parse_stargazers, meta={'repo_name': repo_name, 'page': 1})

        # 找到pulls
        yield self.build_api_req('/repos/%s/pulls?page=1&per_page=%d' % (repo_name, self.page_size),
                                 callback=self.parse_pulls, meta={'repo_name': repo_name, 'page': 1})

        # fork
        yield self.build_api_req('/repos/%s/forks?page=1&per_page=%d' % (repo_name, self.page_size),
                                 callback=self.parse_forks, meta={'repo_name': repo_name, 'page': 1})

        # issues
        yield self.build_api_req('/repos/%s/issues?page=1&per_page=%d' % (repo_name, self.page_size),
                                 callback=self.parse_issues, meta={'repo_name': repo_name, 'page': 1})

    @check_quota()
    def parse_stargazers(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        page = meta['page']
        data = json.loads(response.body)

        # 加入到repo记录中
        item = GitHubItem()
        item['item_type'] = 'stargazers'
        item['data'] = {'repo': repo_name, 'stargazers': data}
        yield item

        for entry in data:
            yield self.get_user_request(entry['login'])

        # 翻页
        if len(data) == self.page_size:
            page += 1
            m = meta.copy()
            m['page'] = page
            yield self.build_api_req('/repos/%s/stargazers?page=%d&per_page=%d' % (repo_name, page, self.page_size),
                                     callback=self.parse_stargazers, meta=m)

    @check_quota()
    def parse_pulls(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        page = meta['page']
        data = json.loads(response.body)

        # 加入到repo记录中
        item = GitHubItem()
        item['item_type'] = 'pulls'
        item['data'] = {'repo': repo_name, 'pulls': data}
        yield item

        for entry in data:
            yield self.get_user_request(entry['user']['login'])

        # 翻页
        if len(data) == self.page_size:
            page += 1
            m = meta.copy()
            m['page'] = page
            yield self.build_api_req('/repos/%s/pulls?page=%d&per_page=%d' % (repo_name, page, self.page_size),
                                     callback=self.parse_pulls, meta=m)

    @check_quota()
    def parse_forks(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        page = meta['page']
        data = json.loads(response.body)

        # 加入到repo记录中
        item = GitHubItem()
        item['item_type'] = 'forks'
        item['data'] = {'repo': repo_name, 'forks': data}
        yield item

        for entry in data:
            yield self.get_user_request(entry['owner']['login'])

        # 翻页
        if len(data) == self.page_size:
            page += 1
            m = meta.copy()
            m['page'] = page
            yield self.build_api_req('/repos/%s/forks?page=%d&per_page=%d' % (repo_name, page, self.page_size),
                                     callback=self.parse_forks, meta=m)

    @check_quota()
    def parse_issues(self, response):
        meta = response.meta
        repo_name = meta['repo_name']
        page = meta['page']
        data = json.loads(response.body)

        # 加入到repo记录中
        item = GitHubItem()
        item['item_type'] = 'issues'
        item['data'] = {'repo': repo_name, 'issues': data}
        yield item

        for entry in data:
            yield self.get_user_request(entry['user']['login'])

        # 翻页
        if len(data) == self.page_size:
            page += 1
            m = meta.copy()
            m['page'] = page
            yield self.build_api_req('/repos/%s/issues?page=%d&per_page=%d' % (repo_name, page, self.page_size),
                                     callback=self.parse_issues, meta=m)

    @check_quota()
    def parse_repo_search(self, response):
        for item in json.loads(response.body)['items']:
            components = urlparse.urlparse(item['url'])
            yield self.build_api_req(path=components.path, callback=self.parse_repo)
