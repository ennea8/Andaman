# coding=utf-8
__author__ = 'zephyre'

global_conf = {}


def load_config():
    """
    加载config文件
    """
    import ConfigParser
    import os

    root_dir = os.path.normpath(os.path.split(__file__)[0])
    cfg_dir = os.path.normpath(os.path.join(root_dir, 'conf'))
    it = os.walk(cfg_dir)
    cf = ConfigParser.ConfigParser()
    for f in it.next()[2]:
        if os.path.splitext(f)[-1] != '.cfg':
            continue
        cf.read(os.path.normpath(os.path.join(cfg_dir, f)))

        for s in cf.sections():
            section = {}
            for opt in cf.options(s):
                section[opt] = cf.get(s, opt)
            global_conf[s] = section

load_config()
