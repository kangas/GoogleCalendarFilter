import datetime
import requests
import pprint
import pymongo
import urllib
import urlparse

import config

def getdb():
    cfg = config.get()
    client = pymongo.MongoClient(cfg['host'], cfg['port'])
    return client[ cfg['db'] ]

def normalize_feed_url(url):
    """Nuke query parameters, nothing else"""
    url = url.split('?')[0]
    return url

FEED_PARAMETERS = {
    'alt': 'jsonc',
    'futureevents': 'true',
    'max-results': '100'
    # 'fields':'creator'
}

def fetch_calendar_feed(url):
    norm_url = normalize_feed_url(url)
    r = requests.get(norm_url, params=FEED_PARAMETERS)
    if r.status_code != 200:
        pprint.pprint(r)
        raise NotImplementedError("FEED REQUEST FAILED")
    return r.json()

def refetch_calendars(db):
    for calendar in db.calendars.find():
        url = calendar['feed']
        feed_name = calendar['name']

        print "Fetching: " + str(calendar)
        result = fetch_calendar_feed(url)
        save_feed_result(db, result, feed_name)

def save_feed_result(db, result, feed_name):
    parsed = parse_feed(result)

    for item in parsed:
        pprint.pprint(item)
        print "-------- JUST THE FIRST ------"
        break

    # do a straight bulk insert
    db.cal[feed_name].insert(parsed)

    # do a bulk upsert
    # for item in parsed:
    #     bulk.
    # db.cal[feed_name].upsert(parsed)
    print "Inserted %s entries to cal.%s" % (len(parsed), feed_name)


def parse_feed(feed_data):
    """Returns iterator over items, remapping some elements
    """
    def munge(x):
        x['_id'] = x['id']
        del x['id']
        return x
    return [munge(x) for x in feed_data['data']['items']]

def test():
    db = getdb()
    refetch_calendars(db)

if __name__ == "__main__":
    test()
