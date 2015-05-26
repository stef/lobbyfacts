from datetime import datetime
from lxml import etree
from pprint import pprint
import logging

import requests, sys
from lobbyfacts.data import sl, etl_engine
import sqlalchemy

log = logging.getLogger(__name__)

URL = 'http://ec.europa.eu/transparencyregister/public/consultation/statistics.do?action=getLobbyistsXml&fileType=NEW'
NS2 = "{http://www.w3.org/1999/xlink}"
NS = "{http://intragate.ec.europa.eu/transparencyregister/intws/20141104}"
SI = "{http://www.w3.org/2001/XMLSchema-instance}"

def dateconv(ds):
    return datetime.strptime(ds.split("+")[0].strip(), "%Y-%m-%dT%H:%M:%S.%f")

def intconv(val):
    return val

def parse_rep(rep_el):
    rep = {}
    rep['identification_code'] = rep_el.findtext(NS + 'identificationCode')
    rep['status'] = rep_el.findtext(NS + 'status')
    rep['registration_date'] = dateconv(rep_el.findtext(NS + 'registrationDate'))
    rep['last_update_date'] = dateconv(rep_el.findtext(NS + 'lastUpdateDate'))
    rep['legal_status'] = rep_el.findtext(NS + 'legalStatus')
    rep['acronym'] = rep_el.findtext(NS + 'acronym')
    latin_name = rep_el.findtext('.//' + NS + 'nameInLatinAlphabet')
    if latin_name:
        rep['original_name'] = latin_name
        rep['native_name'] = rep_el.findtext('.//' + NS + 'originalName')
    else:
        rep['original_name'] = rep_el.findtext('.//' + NS + 'originalName')

    el = rep_el.find(NS + 'webSiteURL')
    rep['web_site_url'] = el.get(NS2 + 'href') if el is not None else None
    rep['main_category'] = rep_el.findtext('.//' + NS + 'mainCategory')
    rep['sub_category'] = rep_el.findtext('.//' + NS + 'subCategory')

    legal = {}
    legal['title'] = rep_el.findtext(NS + 'legalResp/' + NS + 'title')
    legal['first_name'] = rep_el.findtext(NS + 'legalResp/' + NS +
            'firstName')
    legal['last_name'] = rep_el.findtext(NS + 'legalResp/' + NS +
            'lastName')
    legal['position'] = rep_el.findtext(NS + 'legalResp/' + NS +
            'position')
    rep['legal_person'] = legal

    head = {}
    head['title'] = rep_el.findtext(NS + 'euRelationsResp/' + NS + 'title')
    head['first_name'] = rep_el.findtext(NS + 'euRelationsResp/' + NS +
            'firstName')
    head['last_name'] = rep_el.findtext(NS + 'euRelationsResp/' + NS +
            'lastName')
    head['position'] = rep_el.findtext(NS + 'euRelationsResp/' + NS +
            'position')
    rep['head_person'] = head

    rep['contact_street'] = ' '.join((
        rep_el.findtext(NS + 'contactDetails/' + NS + 'addressline1') or '',
        rep_el.findtext(NS + 'contactDetails/' + NS + 'addressline2') or '',
        rep_el.findtext(NS + 'contactDetails/' + NS + 'addressline2') or ''))
    rep['contact_postbox'] = rep_el.findtext(NS + 'contactDetails/' + NS + 'postBox')
    rep['contact_post_code'] = rep_el.findtext(NS + 'contactDetails/' + NS
            + 'postCode')
    rep['contact_town'] = rep_el.findtext(NS + 'contactDetails/' + NS
            + 'town')
    rep['contact_country'] = rep_el.findtext(NS + 'contactDetails/' + NS
            + 'country')
    rep['contact_indic_phone'] = rep_el.findtext(NS + 'contactDetails//' + NS
            + 'indicPhone')
    rep['contact_indic_fax'] = rep_el.findtext(NS + 'contactDetails//' + NS
            + 'indicFax')
    rep['contact_fax'] = rep_el.findtext(NS + 'contactDetails//' + NS
            + 'fax')
    rep['contact_phone'] = rep_el.findtext(NS + 'contactDetails//' + NS
            + 'phoneNumber')
    rep['goals'] = rep_el.findtext(NS + 'goals')
    act_cats = [
        ('activityConsultCommittees', 'activity_consult_committees'),
        ('activityEuLegislative','activity_eu_legislative'),
        ('activityExpertGroups','activity_expert_groups'),
        ('activityHighLevelGroups','activity_high_level_groups'),
        ('activityIndustryForums','activity_industry_forums'),
        ('activityInterGroups','activity_inter_groups'),
        ('activityOther','activity_other'),
        ('activityRelevantComm','activity_relevant_comm')]
    for lm, k in act_cats:
        rep[k] = rep_el.findtext('.//' + NS + 'activities/' + NS + lm)

    rep['code_of_conduct'] = rep_el.findtext(NS + 'codeOfConduct')

    rep['members_25'] = rep_el.findtext('.//' + NS + 'members25Percent')
    rep['members_50'] = rep_el.findtext('.//' + NS + 'members50Percent')
    rep['members_75'] = rep_el.findtext('.//' + NS + 'members75Percent')
    rep['members_100'] = rep_el.findtext('.//' + NS + 'members100Percent')
    rep['members_fte'] = rep_el.findtext('.//' + NS + 'membersFTE')
    rep['info_members'] = rep_el.findtext('.//' + NS + 'infoMembers')

    rep['action_fields'] = []
    for field in rep_el.findall('.//' + NS + 'actionField/' + NS +
            'actionField'):
        rep['action_fields'].append(field.text)
    rep['interests'] = []
    for interest in rep_el.findall('.//' + NS + 'interest/' + NS +
            'name'):
        rep['interests'].append(interest.text)
    rep['number_of_natural_persons'] = intconv(rep_el.findtext('.//' + NS + 'structure/' + NS
            + 'numberOfNaturalPersons'))
    rep['structure_members'] = rep_el.findtext('.//' + NS + 'structure/' + NS
            + 'structureMembers')
    rep['networking'] = rep_el.findtext('.//' + NS + 'structure/' + NS + 'networking')
    rep['country_of_members'] = []
    el = rep_el.find(NS + 'structure/' + NS + 'countries')
    if el is not None:
        for country in el.findall('.//' + NS + 'country'):
            rep['country_of_members'].append(country.text)
    rep['organisations'] = []
    el = rep_el.find(NS + 'structure/' + NS + 'organisations')
    if el is not None:
        for org_el in el.findall(NS + 'organisation'):
            org = {}
            org['name'] = org_el.findtext(NS + 'name')
            org['number_of_members'] = org_el.findtext(NS + 'numberOfMembers')
            rep['organisations'].append(org)

    fd_el = rep_el.find(NS + 'financialData')
    fd = {}
    rep['new_organisation'] = fd_el.findtext(NS + 'newOrganisation')
    try:
        fd['start_date'] = dateconv(fd_el.findtext(NS + 'startDate'))
    except AttributeError:
        if rep['new_organisation'] != 'true':
            print >>sys.stderr, '[x] missing financial data, check out:', rep['identification_code']
        rep['fd'] = fd
        return rep
    fd['end_date'] = dateconv(fd_el.findtext(NS + 'endDate'))
    fd['eur_sources_procurement'] = intconv(fd_el.findtext(NS + 'eurSourcesProcurement'))
    fd['eur_sources_procurement_src'] = fd_el.findtext(NS + 'eurSourcesProcurementSrc')
    fd['eur_sources_grants'] = intconv(fd_el.findtext(NS + 'eurSourcesGrants'))
    fd['eur_sources_grants_src'] = fd_el.findtext(NS + 'eurSourcesGrantsSrc')
    fi = fd_el.find(NS + 'financialInformation')
    fd['type'] = fi.get(SI + 'type')
    #import ipdb; ipdb.set_trace()
    fd['total_budget'] = intconv(fi.findtext('.//' + NS +
        'totalBudget'))
    fd['public_financing_total'] = intconv(fi.findtext('.//' + NS +
        'totalPublicFinancing'))
    fd['public_financing_national'] = intconv(fi.findtext('.//' + NS +
        'nationalSources'))
    fd['public_financing_infranational'] = intconv(fi.findtext('.//' + NS +
        'infranationalSources'))
    cps = fi.find('.//' + NS + 'customisedPublicSources')
    fd['public_customized'] = []
    if cps is not None:
        for src_el in cps.findall('.//' + NS + 'customisedSource'):
            src = {}
            src['name'] = src_el.findtext(NS + 'name')
            src['amount'] = intconv(src_el.findtext(NS + 'amount'))
            fd['public_customized'].append(src)
    fd['other_sources_total'] = intconv(fi.findtext('.//' + NS +
        'totalOtherSources'))
    fd['other_sources_donation'] = intconv(fi.findtext('.//' + NS +
        'donation'))
    fd['other_sources_contributions'] = intconv(fi.findtext('.//' + NS +
        'contributions'))
    # TODO customisedOther
    cps = fi.find('.//' + NS + 'customisedOther')
    fd['other_customized'] = []
    if cps is not None:
        for src_el in cps.findall('.//' + NS + 'customisedSource'):
            src = {}
            src['name'] = src_el.findtext(NS + 'name')
            src['amount'] = intconv(src_el.findtext(NS + 'amount'))
            fd['other_customized'].append(src)

    fd['direct_rep_costs_min'] = intconv(fi.findtext('.//' + NS +
        'directRepresentationCosts//' + NS + 'min'))
    fd['direct_rep_costs_max'] = intconv(fi.findtext('.//' + NS +
        'directRepresentationCosts//' + NS + 'max'))
    fd['cost_min'] = intconv(fi.findtext('.//' + NS +
        'cost//' + NS + 'min'))
    fd['cost_max'] = intconv(fi.findtext('.//' + NS +
        'cost//' + NS + 'max'))
    fd['cost_absolute'] = intconv(fi.findtext('.//' + NS +
        'cost//' + NS + 'absoluteCost'))
    fd['turnover_min'] = intconv(fi.findtext('.//' + NS +
        'turnover//' + NS + 'min'))
    fd['turnover_max'] = intconv(fi.findtext('.//' + NS +
        'turnover//' + NS + 'max'))
    fd['turnover_absolute'] = intconv(fi.findtext('.//' + NS +
        'turnover//' + NS + 'absoluteAmount'))

    fd['turnover_breakdown'] = []
    for tag in ['turnoverBreakdown', 'newTurnoverBreakdown']:
        tb = fi.find(NS + tag)
        newbd = (tag == 'newTurnoverBreakdown')
        if tb is not None:
            for range_ in tb.findall(NS + 'customersGroupsInAbsoluteRange'):
                max_ = range_.findtext('.//' + NS + 'max')
                min_ = range_.findtext('.//' + NS + 'min')
                for customer in range_.findall('.//' + NS + 'customer'):
                    fd['turnover_breakdown'].append({
                        'name': customer.findtext(NS + 'name'),
                        'new': newbd,
                        'min': intconv(min_),
                        'max': intconv(max_)
                        })
            for range_ in tb.findall(NS + 'customersGroupsInPercentageRange'):
                # FIXME: I hate political compromises going into DB design
                # so directly.
                max_ = range_.findtext('.//' + NS + 'max')
                if max_:
                    max_ = float(max_) / 100.0 * \
                            float(fd['turnover_absolute'] or
                                  fd['turnover_max'] or fd['turnover_min'])
                min_ = range_.findtext('.//' + NS + 'min')
                if min_:
                    min_ = float(min_) / 100.0 * \
                            float(fd['turnover_absolute'] or
                                  fd['turnover_min'] or fd['turnover_max'])
                for customer in range_.findall('.//' + NS + 'customer'):
                    fd['turnover_breakdown'].append({
                        'name': customer.findtext(NS + 'name'),
                        'new': newbd,
                        'min': intconv(min_),
                        'max': intconv(max_)
                        })
    fd['other_financial_information'] = fd_el.findtext(NS + 'otherFinancialInformation')
    rep['fd'] = fd
    return rep

def load_person(person, role, childBase, engine):
    table = sl.get_table(engine, 'person')
    person_ = childBase.copy()
    person_.update(person)
    person_['role'] = role
    person_['name'] = ' '.join((person['title'] or '',
                                person['first_name'] or '',
                                person['last_name'] or ''))
    sl.upsert(engine, table, person_, ['representative_etl_id',
                                       'role',
                                       'name'])


def load_finances(financialData, childBase, engine):
    if financialData == {}: return
    etlId = '%s//%s' % (financialData['start_date'].isoformat(),
                        financialData['end_date'].isoformat())

    financial_sources = \
        [(s, 'other') for s in financialData.pop("other_customized")] + \
        [(s, 'public') for s in financialData.pop("public_customized")]
    for financial_source, type_ in financial_sources:
        financial_source['type'] = type_
        financial_source['financial_data_etl_id'] = etlId
        financial_source.update(childBase)
        sl.upsert(engine, sl.get_table(engine, 'financial_data_custom_source'),
                  financial_source, ['representative_etl_id',
                      'financial_data_etl_id', 'type', 'name'])

    for turnover in financialData.pop("turnover_breakdown"):
        turnover['financial_data_etl_id'] = etlId
        turnover['name'] = turnover['name'].strip()
        turnover.update(childBase)
        sl.upsert(engine, sl.get_table(engine, 'financial_data_turnover'),
                  turnover, ['representative_etl_id', 'financial_data_etl_id',
                             'name'])

    financialData['etl_id'] = etlId
    financialData.update(childBase)
    sl.upsert(engine, sl.get_table(engine, 'financial_data'),
              financialData, ['representative_etl_id', 'etl_id'])
    #pprint(financialData)


def load_rep(rep, engine):
    #etlId = rep['etlId'] = "%s//%s" % (rep['identificationCode'],
    #                                   rep['lastUpdateDate'].isoformat())
    etlId = rep['etl_id'] = "%s//ALL" % rep['identification_code']
    childBase = {'representative_etl_id': etlId,
                 'representative_update_date': rep['last_update_date'],
                 'status': 'active'}
    if not rep['original_name']:
        log.error("Unnamed representative: %r", rep)
        return
    load_person(rep.pop('legal_person'), 'legal', childBase, engine)
    load_person(rep.pop('head_person'), 'head', childBase, engine)
    for actionField in rep.pop('action_fields'):
        rec = childBase.copy()
        rec['action_field'] = actionField
        sl.upsert(engine, sl.get_table(engine, 'action_field'), rec,
                  ['representative_etl_id', 'action_field'])

    for interest in rep.pop('interests'):
        rec = childBase.copy()
        rec['interest'] = interest
        sl.upsert(engine, sl.get_table(engine, 'interest'), rec,
                  ['representative_etl_id', 'interest'])

    for countryOfMember in rep.pop('country_of_members'):
        rec = childBase.copy()
        rec['country'] = countryOfMember
        sl.upsert(engine, sl.get_table(engine, 'country_of_member'), rec,
                  ['representative_etl_id', 'country'])

    for organisation in rep.pop('organisations'):
        rec = childBase.copy()
        rec.update(organisation)
        rec['name'] = organisation['name'].strip()
        sl.upsert(engine, sl.get_table(engine, 'organisation'), rec,
                  ['representative_etl_id', 'name'])

    load_finances(rep.pop('fd'), childBase, engine)
    rep['name'] = rep['original_name'].strip()
    rep['network_extracted'] = False
    sl.upsert(engine, sl.get_table(engine, 'representative'), rep,
              ['etl_id'])

def parse(data):
    doc = etree.fromstring(data.encode('utf-8'))
    for rep_el in doc.findall('.//' + NS + 'interestRepresentative'):
        yield parse_rep(rep_el)

def extract_data(engine, data):
    log.info("Extracting registered interests data...")
    for i, rep in enumerate(parse(data)):
        load_rep(rep, engine)
        if i % 100 == 0:
            log.info("Extracted: %s...", i)

def extract(engine):
    try:
        sl.update(engine, 'representative', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'financial_data', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'financial_data_turnover', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'person', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'organisation', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'accreditation', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'country_of_member', {}, {'status': 'inactive'}, ensure=False)
        sl.update(engine, 'associated_action', {}, {'status': 'inactive'}, ensure=False)
    except sqlalchemy.exc.CompileError:
        pass

    res = requests.get(URL)
    extract_data(engine, res.content.decode('utf-8'))

if __name__ == '__main__':
    engine = etl_engine()
    if len(sys.argv)<2:
        # extract current
        extract(engine)
    else:
        # extract from file
        with open(sys.argv[1],'r') as fd:
            extract_data(engine, fd.read().decode('utf-8'))

