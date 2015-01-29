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


def load_yaml():
    """
    Load YAML-format configuration files
    :return:
    """

    config = getattr(load_yaml, 'config', None)
    if config:
        return config

    from yaml import load
    import os
    from glob import glob

    cfg_dir = os.path.abspath(os.path.join(os.path.split(__file__)[0], 'conf/'))
    cfg_file = os.path.join(cfg_dir, 'andaman.yaml')
    with open(cfg_file) as f:
        config = load(f)

    # Resolve includes
    if 'include' in config:
        for entry in config['include']:
            for fname in glob(os.path.join(cfg_dir, entry)):
                if fname == cfg_file:
                    continue
                try:
                    with open(fname) as f:
                        include_data = load(f)
                        for k, v in include_data.items():
                            config[k] = v
                except IOError:
                    continue

    setattr(load_yaml, 'config', config)
    return config


load_config()
