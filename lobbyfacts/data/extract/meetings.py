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

host2portfolio = { u"President Jean-Claude Juncker" : "Presidency",
                   u"Commissioner Günther Oettinger":"Digital Economy",
                   u"Commissioner Miguel Arias Cañete":"Climate & Energy",
                   u"Commissioner Cecilia Malmström":"Trade",
                   u"Commissioner Jonathan Hill":"Financial Markets",
                   u"Commissioner Johannes Hahn":"Neighbourhood",
                   u"Vice-President Jyrki Katainen" : "Jobs & Growth",
                   u"Commissioner Christos Stylianides":"Humanitarian Aid",
                   u"Commissioner Corina Crețu":"Regional Policy",
                   u"Commissioner Dimitris Avramopoulos":"Home Affairs",
                   u"First Vice-President Frans Timmermans":"Better Regulation",
                   u"Commissioner Phil Hogan":"Agriculture",
                   u"Commissioner Violeta Bulc":"Transport",
                   u"Commissioner Elżbieta Bieńkowska": "Internal Market",
                   u"Vice-President Kristalina Georgieva": "Budget",
                   u"Vice-President Andrus Ansip":"Digital Single Market",
                   u"Vice-President Maroš Šefčovič":"Energy Union",
                   u"Commissioner Tibor Navracsics":"Education",
                   u"Commissioner Karmenu Vella":"Environment",
                   u"High Representative / Vice-President Federica Mogherini":"External Action",
                   u"Commissioner Vytenis Andriukaitis":"Health",
                   u"Commissioner Neven Mimica":"Development",
                   u"Vice-President Valdis Dombrovskis":"Euro",
                   u"Commissioner Pierre Moscovici":"Economics & Tax",
                   u"Commissioner Věra Jourová":"Justice",
                   u"Commissioner Marianne Thyssen":"Employment",
                   u"Commissioner Carlos Moedas":"Research",
                   u"Commissioner Margrethe Vestager":"Competition",
}

prefix = 'Meetings of '
prefix2 = 'Cabinet members of '
postfix = ' with organisations and self-employed individuals'
urlroot = 'http://ec.europa.eu/transparencyinitiative/meetings/'

def get_urls():
    for _, url in uuids:
        root = fetch(url)

        title = unws(' '.join(root.xpath('//h3//text()')))[len(prefix):-len(postfix)]
        crs = root.xpath('//a[contains(@class,"breadcrumb-segment-last")]//text()')
        if crs[0]=='The Commissioners':
            if title.startswith(prefix2):
                org = host2portfolio[title[len(prefix2):]]
            else:
                org = host2portfolio[title]
        elif crs[0]=='DGs':
            org = crs[1]
            if org == 'Secretariat-General > What we do':
                org = 'Secretariat-General'
        else:
            raise ValueError("uknown crs[0]")
        #print ('\t'.join((url, org, title))).encode('utf8')
        yield (url, org, title)

        predurl = root.xpath('//a[text()="List of predecessors"]')
        if len(predurl)==1:
            # get predecessors urls
            predroot = fetch(urljoin(url, predurl[0].get('href')))
            for pred in predroot.xpath('//div[@id="titlerefpage"]/following-sibling::a'):
                if pred.xpath('./text()')[0].startswith('Back to '):
                    break
                url = "%s%s" % (urlroot, pred.get('href'))
                predroot=fetch(url)
                title = unws(' '.join(predroot.xpath('//h3//text()')))[len(prefix):-len(postfix)]
                crs = predroot.xpath('//a[contains(@class,"breadcrumb-segment-last")]//text()')
                if crs[0]=='The Commissioners':
                    if title.startswith(prefix2):
                        org = host2portfolio[title[len(prefix2):]]
                    else:
                        org = host2portfolio[title]
                elif crs[0]=='DGs':
                    org = crs[1]
                    if org == 'Secretariat-General > What we do':
                        org = 'Secretariat-General'
                else:
                    raise ValueError("uknown crs[0]")
                #print ('\t'.join((url, org, title))).encode('utf8')
                yield (url, org, title)
        elif len(predurl)>1:
            print >>sys.stderr, "wtf", predurl

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
        if not name: continue
        if name in entmap:
            name, tregid = entmap[name]
        elif not tregidre.match(tregid):
            tregid = 'unregistered'
            print >>sys.stderr, '[!] unregistered?', name.encode('utf8')
        ents.append((name, tregid))
    return ents

def scrape(url, title, org):
    while True:
        try: root = fetch(url)
        except:
            print >>sys.stderr, 'failed to fetch url', sys.exc_info(), url
            break

        for row in root.xpath('//table[@id="listMeetingsTable"]/tbody/tr'):
            fields = row.xpath('.//td')
            if len(fields) == 5:
                name = unws(fields[0].xpath('.//text()'))
                date = unws(fields[1].xpath('.//text()'))
                location = unws(fields[2].xpath('.//text()'))
                entities = get_ents(fields[3])
                subject = unws(fields[4].xpath('.//text()'))
            elif len(fields) == 4:
                name = unws(title)
                date = unws(fields[0].xpath('.//text()'))
                location = unws(fields[1].xpath('.//text()'))
                entities = get_ents(fields[2])
                subject = unws(fields[3].xpath('.//text()'))
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
    for url, org, title in get_urls():
        for meeting in scrape(url, title, org):
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
