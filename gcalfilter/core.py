import datetime
import requests
import pprint
import pymongo
import urllib
import urlparse

import config

_team_cache = {}

def debug(msg):
    print "#DBG "+ str(msg)

def getdb():
    cfg = config.get()
    client = pymongo.MongoClient(cfg['host'], cfg['port'])
    return client[ cfg['db'] ]

def init_caches(db):
    for team in db['teams'].find():
        key = team['_id']
        _team_cache[key] = {m['email']:m['name'] for m in team['members']}

    debug("team_cache: %s entries" % len(_team_cache))

def normalize_feed_url(url):
    """Nuke query parameters, nothing else"""
    url = url.split('?')[0]
    return url

def fetch_calendar_feed(url):
    FEED_PARAMETERS = {
        'alt': 'jsonc',
        'futureevents': 'true',
        'max-results': '100'
        # 'fields':'creator'
    }

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

    # FIXME: do a bulk upsert
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

## ------------------ report generation -------------------

def is_user_in_team(user, team):
    if len(_team_cache) == 0:
        raise RuntimeError("init_caches never happened")
    return user in _team_cache[ team.lower() ]

def entry_creator(e):
    return e['creator']['email']

def entry_title(e):
    return e['title']

def entry_when(e):
    # FIXME normalize
    start = e['when'][0]["start"]
    end = e['when'][0]["end"]
    return (start, end)

def entry_group_by_date(entries):
    raise NotImplementedError()

def filter_calendar_by_team(calendar, team):
    return (entry for entry in calendar
            if is_user_in_team(entry_creator(entry), team))


def run_report_plaintext(db, team, calendar):
    # FIXME: add daterange parameter

    vacations = db.cal[calendar].find()
    report = ""
    
    for entry in filter_calendar_by_team(vacations, team):
        report += plaintext_report_entry(entry, team)
        report += '\n'

    return report

def plaintext_report_entry(e, team):
    email = entry_creator(e)
    name = _team_cache[team][email]
    out  = "%s\n\t%s\n\t%s" % (name, entry_title(e), entry_when(e))
    return out


## --------------------------------------------------------

def test():
    db = getdb()
    refetch_calendars(db)

if __name__ == "__main__":
    test()
