import logging
from pprint import pprint

from lobbyfacts.core import db
from lobbyfacts.data import sl, etl_engine
from lobbyfacts.model import Entity, Representative, Country, Category
from lobbyfacts.model import Organisation, OrganisationMembership, Person
from lobbyfacts.model import Accreditation, FinancialData, FinancialTurnover
from lobbyfacts.model import CountryMembership, ActionField, AssociatedAction
from lobbyfacts.model import Interest, AssociatedInterest, CustomIncome
from lobbyfacts.data.load.util import to_integer, to_float, upsert_person
from lobbyfacts.data.load.util import upsert_person, upsert_organisation, upsert_entity, upsert_tag
from lobbyfacts.core import app
from datetime import datetime

log = logging.getLogger(__name__)

def upsert_category(id, name, parent=None):
    data = {'id': id, 'name': name, 'parent': parent}
    category = Category.by_id(id)
    if category is None:
        category = Category.create(data)
        db.session.commit()
    else:
        category.update(data)
    return category

def load_representative(engine, rep):
    entity = upsert_entity(rep.get('canonical_name'),
                name=rep.get('original_name'),
                suffix=rep.get('name_suffix'),
                acronym=rep.get('acronym'))
    assert entity is not None, entity
    assert entity.id is not None, entity
    rep['entity'] = entity
    rep['members_25'] = to_integer(rep['members_25'])
    rep['members_50'] = to_integer(rep['members_50'])
    rep['members_75'] = to_integer(rep['members_75'])
    rep['members_100'] = to_integer(rep['members_100'])
    rep['members_fte'] = to_integer(rep['members_fte'])
    rep['number_of_natural_persons'] = to_integer(rep['number_of_natural_persons'])

    rep['contact_lat'] = to_float(rep['contact_lat'])
    rep['contact_lon'] = to_float(rep['contact_lon'])

    rep['contact_phone'] = " ".join((rep.get('contact_indic_phone') or '', rep.get('contact_phone') or '')).strip()
    rep['contact_fax'] = " ".join((rep.get('contact_indic_fax') or '', rep.get('contact_fax') or '')).strip()
    rep['contact_country'] = Country.by_code(rep['country_code'])

    if rep.get('main_category'):
        main_category = upsert_category(rep.get('main_category_id'),
                                        rep.get('main_category'))
        rep['main_category'] = main_category
        if rep.get('sub_category'):
            rep['sub_category'] = upsert_category(rep.get('sub_category_id'),
                                                  rep.get('sub_category'),
                                                  main_category)

    accreditations = []
    for person_data in sl.find(engine, sl.get_table(engine, 'person'),
            representative_etl_id=rep['etl_id']):
        person = upsert_person(person_data)
        if person_data.get('role') == 'head':
            rep['head'] = person
        if person_data.get('role') == 'legal':
            rep['legal'] = person
        if person_data.get('role') == 'accredited':
            accreditations.append((person, person_data))

    representative = Representative.by_identification_code(rep['identification_code'])
    if representative is None:
        representative = Representative.create(rep)
    else:
        representative.update(rep)


    for person, data_ in accreditations:
        data_['person'] = person
        data_['representative'] = representative
        accreditation = Accreditation.by_rp(person, representative)
        if accreditation is None:
            accreditation = Accreditation.create(data_)
        else:
            accreditation.update(data_)

    for fd in sl.find(engine, sl.get_table(engine, 'financial_data'),
            representative_etl_id=rep['etl_id']):
        fd['turnover_min'] = to_integer(fd.get('turnover_min'))
        fd['turnover_max'] = to_integer(fd.get('turnover_max'))
        fd['turnover_absolute'] = to_integer(fd.get('turnover_absolute'))
        fd['cost_min'] = to_integer(fd.get('cost_min'))
        fd['cost_max'] = to_integer(fd.get('cost_max'))
        fd['cost_absolute'] = to_integer(fd.get('cost_absolute'))
        fd['direct_rep_costs_min'] = to_integer(fd.get('direct_rep_costs_min'))
        fd['direct_rep_costs_max'] = to_integer(fd.get('direct_rep_costs_max'))
        fd['total_budget'] = to_integer(fd.get('total_budget'))
        fd['public_financing_total'] = to_integer(fd.get('public_financing_total'))
        fd['public_financing_infranational'] = to_integer(fd.get('public_financing_infranational'))
        fd['public_financing_national'] = to_integer(fd.get('public_financing_national'))
        fd['eur_sources_grants'] = to_integer(fd.get('eur_sources_grants'))
        fd['eur_sources_procurement'] = to_integer(fd.get('eur_sources_procurement'))
        fd['other_sources_donation'] = to_integer(fd.get('other_sources_donation'))
        fd['other_sources_contributions'] = to_integer(fd.get('other_sources_donation'))
        fd['other_sources_total'] = to_integer(fd.get('other_sources_total'))
        fd['eur_sources_procurement_src'] = fd.get('eur_sources_procurement_src')
        fd['eur_sources_grants_src'] = fd.get('eur_sources_grants_src')
        fd['other_financial_information'] = fd.get('other_financial_information')
        fd['new_organisation'] = fd.get('new_organisation')
        fd['representative'] = representative
        financial_data = FinancialData.by_rsd(representative, fd.get('start_date'))
        if financial_data is None:
            financial_data = FinancialData.create(fd)
        else:
            financial_data.update(fd)

        for src_ in sl.find(engine, sl.get_table(engine, 'financial_data_custom_source'),
                representative_etl_id=rep['etl_id'], financial_data_etl_id=fd['etl_id']):
            src_['financial_data'] = financial_data
            src_['amount'] = to_integer(src_.get('amount'))
            src = CustomIncome.by_fdn(financial_data, src_['name'])
            if src is None:
                src = CustomIncome.create(src_)
            else:
                src.update(src_)

        for turnover_ in sl.find(engine, sl.get_table(engine, 'financial_data_turnover'),
                representative_etl_id=rep['etl_id'], financial_data_etl_id=fd['etl_id']):
            #if turnover_.get('etl_clean') is False:
            #    continue
            turnover_['entity'] = upsert_entity(turnover_.get('canonical_name'),
                                                turnover_.get('name'))
            assert turnover_['entity'] is not None, turnover_['entity']
            turnover_['financial_data'] = financial_data
            turnover_['min'] = to_integer(turnover_.get('min'))
            turnover_['max'] = to_integer(turnover_.get('max'))
            turnover = FinancialTurnover.by_fde(financial_data, turnover_['entity'])
            if turnover is None:
                turnover = FinancialTurnover.create(turnover_)
            else:
                turnover.update(turnover_)

    for org in sl.find(engine, sl.get_table(engine, 'organisation'),
            representative_etl_id=rep['etl_id']):
        #if org.get('etl_clean') is False:
        #    continue
        org['number_of_members'] = to_integer(org['number_of_members'])
        organisation = upsert_organisation(org)
        omdata = {'representative': representative,
                  'status': org.get('status'),
                  'organisation': organisation}
        om = OrganisationMembership.by_rpo(representative, organisation)
        if om is None:
            om = OrganisationMembership.create(omdata)
        else:
            om.update(omdata)

    for country_ in sl.find(engine, sl.get_table(engine, 'country_of_member'),
            representative_etl_id=rep['etl_id']):
        if not country_.get('country_code'): continue
        #if country_.get('etl_clean') is False:
        #    continue
        cdata = {'representative': representative,
                 'status': country_.get('status'),
                 'country': Country.by_code(country_.get('country_code'))}
        cm = CountryMembership.by_rpc(representative, cdata.get('country'))
        if cm is None:
            cm = CountryMembership.create(cdata)
        else:
            cm.update(cdata)

    for action_ in sl.find(engine, sl.get_table(engine, 'action_field'),
            representative_etl_id=rep['etl_id']):
        if not action_.get('action_field'): continue
        af = ActionField.by_action(action_.get('action_field'))
        if af is None:
            af = ActionField.create({'action': action_.get('action_field')})
            db.session.commit()
        adata = {'representative': representative,
                 'status': action_.get('status'),
                 'action': af}

        am = AssociatedAction.by_rpa(representative, af)
        if am is None:
            am = AssociatedAction.create(adata)
            db.session.commit()
        else:
            am.update(adata)

    for interest_ in sl.find(engine, sl.get_table(engine, 'interest'),
            representative_etl_id=rep['etl_id']):
        if not interest_.get('interest'): continue
        i = Interest.by_interest(interest_.get('interest'))
        if i is None:
            i = Interest.create({'interest': interest_.get('interest')})
            db.session.commit()
        adata = {'representative': representative,
                 'status': action_.get('status'),
                 'interest': i}

        ai = AssociatedInterest.by_rpi(representative, i)
        if ai is None:
            ai = AssociatedInterest.create(adata)
            db.session.commit()
        else:
            ai.update(adata)

    for taglink in sl.find(engine, sl.get_table(engine, 'tags'),
            representative_id=rep['id']):
        etltag=sl.find_one(engine, sl.get_table(engine, 'tag'), id=taglink['tag_id'])
        tag = upsert_tag(etltag['tag'])
        if not tag in representative.tags:
            representative.tags.append(tag)
    db.session.commit()

def external_url_handler(error, endpoint, values):
    return ''

def load(engine):
    for i, rep in enumerate(sl.all(engine, sl.get_table(engine, 'representative'))):
        log.info("Loading(%s): %s", i, rep.get('name'))
        #if rep['etl_clean'] is False:
        #    log.debug("Skipping!")
        #    continue
        load_representative(engine, rep)

if __name__ == '__main__':
    # init flask
    app.url_build_error_handlers.append(external_url_handler)
    ctx = app.test_request_context()
    ctx.push()
    global request
    request = app.preprocess_request()

    engine = etl_engine()
    load(engine)

