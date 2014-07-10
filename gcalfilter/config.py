import pprint
import os.path
import yaml

DEFAULT_CONFIG = """
host: localhost
port: 27000
db: gcalfilter
"""

DEFAULT_CONFIGFILE = "~/.gcalfilter.yaml"

def debug(msg):
    print("# " + msg)

def merge(user, default):
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.iteritems():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge(user[k], v)
    return user


def getConfig():
    default_conf = yaml.load(DEFAULT_CONFIG)
    result = default_conf

    configfile = DEFAULT_CONFIGFILE
    try:
        path = os.path.expanduser(configfile)
        with open(path) as f:
            debug("loaded configfile: %s" % path)
            user_conf = yaml.load(f)
            result = merge(user_conf, default_conf)
    except IOError:
        pass

    return result


def test():
    debug("-- config test")
    data = getConfig()
    debug(pprint.pformat(data))


if __name__ == "__main__":
    test()
