#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re, requests, time, hashlib, HTMLParser, json
from lxml.etree import tostring
from lxml.html.soupparser import fromstring
from lxml.html import HtmlComment
from itertools import izip, cycle
from urlparse import urljoin
from meetingmaps import entmap, uuids
from datetime import date
from lobbyfacts.data import sl, etl_engine
import logging
import sqlalchemy
log = logging.getLogger(__name__)

HEADERS =  { 'User-agent': 'lobbyfacts/1.2' }

mainurl="http://ec.europa.eu/transparencyinitiative/meetings/meeting.do?host=595cf53f-c018-4fc8-afa0-9d66c289795c&d-6679426-p="
trurl="http://ec.europa.eu/transparencyregister/public/consultation/displaylobbyist.do?id="
tregidre=re.compile('[0-9]{9,12}-[0-9]{2}')

h = HTMLParser.HTMLParser()

def unws(obj):
    if isinstance(obj, list):
        obj = u''.join(obj)
    return u' '.join(unicode(obj).split())

def fetch_raw(url, retries=5, ignore=[], params=None):
    try:
        if params:
            r=requests.POST(url, params=params, headers=HEADERS)
        else:
            r=requests.get(url, headers=HEADERS)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout), e:
        if e == requests.exceptions.Timeout:
            retries = min(retries, 1)
        if retries>0:
            time.sleep(4*(6-retries))
            f=fetch_raw(url, retries-1, ignore=ignore, params=params)
        else:
            raise ValueError("failed to fetch %s" % url)
    if r.status_code >= 400 and r.status_code not in [504, 502]+ignore:
        print >>sys.stderr, "[!] %d %s" % (r.status_code, url)
        r.raise_for_status()
    return r.text

def fetch(url, retries=5, ignore=[], params=None):
    f = fetch_raw(url, retries, ignore, params)
    return fromstring(f)

def get_ents(nodes):
    ents = []
    for lm in nodes:
        if not isinstance(lm, HtmlComment) or trurl not in unicode(lm):
            continue
        lm = unws(lm)
        name = lm[:-8]  # '</a> -->'
        start = lm.index(trurl) + len(trurl)
        end = start+lm[start:].index('"')
        tregid = lm[start:end]
        name = h.unescape(name[end+2:].strip())
        if name in entmap:
            name, tregid = entmap[name]
        elif not tregidre.match(tregid):
            tregid = 'unregistered'
            print >>sys.stderr, name.encode('utf8')
        ents.append((name, tregid))
    return ents

def scrape(url, title):
    while True:
        try: root = fetch(url)
        except:
            print url
            raise

        for row in root.xpath('//table[@id="listMeetingsTable"]/tbody/tr'):
            fields = row.xpath('.//td')
            if len(fields) == 5:
                name = unws(fields[0].xpath('.//text()'))
                date = unws(fields[1].xpath('.//text()'))
                location = unws(fields[2].xpath('.//text()'))
                entities = get_ents(fields[3])
                subject = unws(fields[4].xpath('.//text()'))
                org = ''.join(("\t",title)).strip()
            elif len(fields) == 4:
                name = unws(title)
                date = unws(fields[0].xpath('.//text()'))
                location = unws(fields[1].xpath('.//text()'))
                entities = get_ents(fields[2])
                subject = unws(fields[3].xpath('.//text()'))
                org = ''
            else:
                print >>sys.stderr, "fields not 4-5, wtf"
                raise
            # calculate uniq meeting id
            meetid = hashlib.md5('\0' * 16)
            for lm in (name, date, location, '\0'.join('\1'.join(e) for e in entities), subject, org):
                meetid = hashlib.md5(meetid.digest()+lm.encode('utf8'))
            meetid = meetid.hexdigest()
            for entity, entity_id in entities:
                yield {'name': name,
                       'status': 'active',
                       'meetid': meetid,
                       'date': date,
                       'location': location,
                       'subject': subject,
                       'identification_code': entity_id,
                       'representative': entity,
                       'eu_representative': org}

        try: url=urljoin(mainurl, root.xpath('//a/img[@alt="Next"]/..')[0].attrib['href'])
        except: break

def extract(engine):
    table = sl.get_table(engine, 'meeting')
    try:
        sl.update(engine, 'meeting', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'meeting_participants', {}, {'status': 'inactive'}, ensure=False)
    except sqlalchemy.exc.CompileError:
        pass

    i=0
    for title, url in uuids:
        for meeting in scrape(url, title):
            sl.upsert(engine, table, meeting, ['meetid', 'identification_code'])
            i+=1
            if i % 100 == 0:
                log.info("Extracted: %s...", i)


if __name__ == '__main__':
    engine = etl_engine()
    if len(sys.argv)<2:
        # extract current
        extract(engine)
    else:
        # extract from file
        with open(sys.argv[1],'r') as fd:
            extract_data(engine, fd.read().decode('utf-8'))
