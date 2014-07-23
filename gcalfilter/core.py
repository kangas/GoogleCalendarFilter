import datetime
import itertools
import pprint
import pymongo
import requests
import time
import urllib
import urlparse

import config

_team_cache = {}

def debug(msg):
    if False:
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


## ------------------ feed processing -------------------


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
    now = int(time.time())
    parsed = parse_feed(result, now)

    bulk = db.cal[feed_name].initialize_unordered_bulk_op()

    for item in parsed:
        # pprint.pprint(item['_id'])
        bulk.find({'_id': item['_id']}).upsert().replace_one(item)

    result = bulk.execute()
    pprint.pprint(result)

    print "Inserted %s entries to cal.%s" % (len(parsed), feed_name)
    print "Pruning old records"
    result = db.cal[feed_name].remove({'fetchdate': {'$exists': False}})
    pprint.pprint(result)
    result = db.cal[feed_name].remove({'fetchdate': {'$lt': now}})
    pprint.pprint(result)


def parse_feed(feed_data, fetchdate):
    """Returns iterator over items, remapping some elements
    """
    def munge(x):
        x['_id'] = x['id']
        del x['id']
        x['fetchdate'] = fetchdate
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


def parse_date(datestr):
    datestr = datestr.split("T")[0]
    fmt = r"%Y-%m-%d"
    return datetime.datetime.strptime(datestr, fmt).date()


def entry_add_dateset(entry):
    ds = Dateset()
    for when in entry['when']:
        ds_e = Dateset.from_span(parse_date(when['start']),
                                 parse_date(when['end'])
                    )
        ds.update(ds_e)

    entry['dateset'] = ds
    return entry


def filter_calendar_by_team(calendar, team):
    return (entry for entry in calendar
            if is_user_in_team(entry_creator(entry), team))


def entries_group_by_weeks(raw_entries, num_weeks):
    entries = [entry_add_dateset(e) for e in raw_entries]
    weeks = next_weeks(num_weeks)

    ## Cartesian product (weeks X entries), reduce by week
    week_events = [(w, e)
                   for w in weeks for e in entries
                   if not w.isdisjoint( e['dateset'] )]

    select_0 = lambda x: x[0]
    grouped = itertools.groupby(week_events, select_0)
    
    def _week_sort_by(x):
        (ds, event) = x
        return (min(ds), len(ds))

    for (week, igroup) in grouped:
        events = [g[1] for g in igroup]

        ## Keep only dates in this week
        ## FIXME: dedup multiple events from same user?
        z = [( week.intersection(e['dateset']), e) for e in events]
        z = sorted(z, key=_week_sort_by)

        yield (week, z)


def run_report_plaintext(db, team, calendar, num_weeks=8):

    all_entries = db.cal[calendar].find()
    team_entries = filter_calendar_by_team(all_entries, team)
    grouped = entries_group_by_weeks(team_entries, num_weeks)

    select_0 = lambda x: x[0]

    report = ""
    for n, (week, entries) in enumerate(grouped):
        report += "\n"
        report += "==== Week %d: %s ============================\n" % \
            (n ,week.prettyweek())
        report += "\n"

        for (dates, entry) in entries:
            report += plaintext_report_entry(entry, team, dates)
            report += '\n'

    return report

def plaintext_report_entry(e, team, dates):
    email = entry_creator(e)
    name = _team_cache[team][email]
    out  = "%s\n\t%s\n\t%s" % (name, entry_title(e), dates)
    return out


def next_weeks(num_weeks):
    """Return n Datespans representing the next n weeks"""
    today = datetime.date.today()
    oneweek = datetime.timedelta(7)
    oneday = datetime.timedelta(1)

    monday = today - datetime.timedelta(today.weekday())
    weeks = []
    for n in range(0, num_weeks):
        next_monday = monday + oneweek
        weeks.append( Dateset.from_span(monday, next_monday) )
        monday = next_monday
    return weeks

class Dateset(set):
    @classmethod
    def from_span(cls, datefrom, dateto):
        """Initialize set from date range"""
        assert isinstance(datefrom, datetime.date)
        assert isinstance(dateto, datetime.date)
        oneday = datetime.timedelta(1)

        days = []
        day = datefrom
        while day < dateto:
            days.append(day)
            day += oneday
        return cls(days)

    def __str__(self):
        fmt = r"%a %b %d"
        fmt_y = r"%a %b %d %Y"

        d_min = min(self)
        d_max = max(self)
        delta = d_max - d_min
        contiguous = len(self) == delta.days + 1

        if len(self) == 1:
            return d_min.strftime(fmt_y)
        elif len(self) > 2 and contiguous:
            return '%s thru %s (%d days)' % \
            (d_min.strftime(fmt), d_max.strftime(fmt_y),
             len(self))
        else:
            days = [d.strftime(fmt) for d in sorted(self)]
            return "%s (%d days)" % (", ".join(days), len(self))

    def prettyweek(self):
        """Variant of __str__ for explicit weeks"""
        fmt_y = r"%a %b %d %Y"
        return "%s" % (min(self).strftime(fmt_y))

## --------------------------------------------------------

def test():
    db = getdb()
    refetch_calendars(db)

if __name__ == "__main__":
    test()
