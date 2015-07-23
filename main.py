# coding=utf-8
import imp

from scrapy.settings import Settings
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy import log
from scrapy import Spider


__author__ = 'zephyre'


def spider_closing(self):
    """Activates on spider closed signal"""
    log.msg("Closing spider", level=log.INFO)
    reactor.stop()


def parse_cmd_args(settings):
    """
    解析命令行参数
    :return: Settings对象
    """
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', type=str, action='append')

    args, leftover = parser.parse_known_args()

    for entry in args.s:
        splits = entry.split('=', 1)
        if len(splits) == 2:
            settings.set(splits[0], splits[1])

    return settings


def register(dir_name, criteria_func, key_func, value_func=lambda v, p: v):
    """
    将spiders路径下的爬虫类进行注册
    将指定路径下的类进行注册（比如Spider，Pipeline等）
    """
    import os
    import imp

    root_dir = os.path.normpath(os.path.split(__file__)[0])
    spider_dir = os.path.normpath(os.path.join(root_dir, dir_name))

    item_map = {}

    def get_relative(path):
        """
        获得path相对于root_dir的路径
        :param path:
        :return:
        """
        if root_dir.endswith('/'):
            head = root_dir
        else:
            head = root_dir + '/'

        if path.startswith(head):
            return path[len(head):].rstrip('/')
        else:
            return None

    for cur, d_list, f_list in os.walk(spider_dir):
        for fname in f_list:
            is_spider = not fname.startswith('.') and fname.endswith('.py')
            if not is_spider:
                continue

            module_name, _ = os.path.splitext(fname)
            package_path = get_relative(cur)
            full_name = '.'.join([package_path.replace('/', '.'), module_name])

            ret = imp.find_module(module_name, [package_path])
            module = imp.load_module(module_name, *ret)

            for attr_name in dir(module):
                c = getattr(module, attr_name)

                if criteria_func(c, full_name):
                    key = key_func(c, full_name)
                    item_map[key] = value_func(c, full_name)

    return item_map


def register_spiders():
    def criteria_func(cls, full_name):
        import inspect

        return inspect.isclass(cls) and issubclass(cls, Spider) and 'name' in dir(cls)

    def key_func(cls, full_name):
        return getattr(cls, 'name')

    return register('andaman/spiders', criteria_func, key_func)


def register_pipelines():
    def criteria_func(cls, full_name):
        import inspect

        return inspect.isclass(cls) and cls.__name__.endswith('Pipeline') and 'process_item' in dir(cls)

    def key_func(cls, full_name):
        return '%s.%s' % (full_name, cls.__name__)

    return register('andaman/pipelines', criteria_func, key_func)


def main():
    spider_map = register_spiders()
    pipelines = register_pipelines()

    from scrapy.utils.log import configure_logging

    settings = Settings()

    # 加载系统中存在的Pipeline
    settings.set('ITEM_PIPELINES', {p: 100 for p in pipelines.keys()})

    ret = imp.find_module('settings', ['andaman'])
    settings_module = imp.load_module('settings', *ret)
    settings.setmodule(settings_module)

    settings = parse_cmd_args(settings)

    configure_logging(settings=settings)

    spider_names = [v for v in settings.get('SPIDERS', '').split(',') if v]
    spiders = [spider_map[name] for name in spider_names if name in spider_map]
    runner = CrawlerRunner(settings=settings)
    for s in spiders:
        runner.crawl(s)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())

    reactor.run()


if __name__ == "__main__":
    main()
