import logging

from lobbyfacts.data import sl, etl_engine

log = logging.getLogger(__name__)

CATEGORIES = {
    u'I - Professional consultancies/law firms/self-employed consultants': 1,
    u'II - In-house lobbyists and trade/professional associations': 2,
    u'III - Non-governmental organisations': 3,
    u'IV - Think tanks, research and academic institutions': 4,
    u'V - Organisations representing churches and religious communities': 5,
    u'VI - Organisations representing local, regional and municipal authorities, other public or mixed entities, etc.': 6
    }


SUBCATEGORIES = {
    u'Professional consultancies': 11,
    u'Law firms': 12,
    u'Self-employed consultants': 13,
    u'Companies & groups': 21,
    u'Trade, business & professional associations': 22,
    u'Trade unions': 23,
    u'Other similar organisations': 24,
    u'Trade and business organisations': 25,
    u'Trade unions and professional associations': 26,
    u'Other in house lobbyists':27,
    u'Non-governmental organisations, platforms and networks and similar': 31,
    u'Think tanks and research institutions': 41,
    u'Academic institutions': 42,
    u'Organisations representing churches and religious communities': 51,
    u'Local, regional and municipal authorities (at sub-national level)': 61,
    u'Other public or mixed entities, etc.': 62,
    u'Regional structures': 63,
    u'Other sub-national public authorities': 64,
    u'Transnational associations and networks of public regional or other sub-national authorities': 65,
    u'Other public or mixed entities, created by law whose purpose is to act in the public interest': 66}

newcats = {u'II - In-house lobbyists and trade/business/professional associations': u'II - In-house lobbyists and trade/professional associations'}
newsubcats = { u'Other organisations':u'Other in house lobbyists',
               u'Trade and business associations': u'Trade and business organisations',
             }

def remap_categories(engine):
    table = sl.get_table(engine, 'representative')
    for cat in sl.distinct(engine, table, 'main_category'):
        c=newcats.get(cat['main_category'])
        if c:
           sl.update(engine, 'representative', {'main_category': cat['main_category']}, {'main_category': c}, ensure=False)

def remap_subcategories(engine):
    table = sl.get_table(engine, 'representative')
    for cat in sl.distinct(engine, table, 'sub_category'):
        c=newsubcats.get(cat['sub_category'])
        if c:
           sl.update(engine, 'representative', {'sub_category': cat['sub_category']}, {'sub_category': c}, ensure=False)

def code_categories(engine):
    table = sl.get_table(engine, 'representative')
    for cat in sl.distinct(engine, table, 'main_category'):
        if not cat['main_category']: continue
        cat['main_category_id'] = CATEGORIES[cat['main_category']]
        sl.upsert(engine, table, cat, ['main_category'])

def code_subcategories(engine):
    table = sl.get_table(engine, 'representative')
    for cat in sl.distinct(engine, table, 'sub_category'):
        if not cat['sub_category']: continue
        cat['sub_category_id'] = SUBCATEGORIES.get(cat['sub_category'])
        sl.upsert(engine, table, cat, ['sub_category'])

def transform(engine):
    log.info("Performing micro-transforms...")
    remap_categories(engine)
    remap_subcategories(engine)
    code_categories(engine)
    code_subcategories(engine)

if __name__ == '__main__':
    engine = etl_engine()
    transform(engine)

