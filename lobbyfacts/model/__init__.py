import logging

from lobbyfacts.model.entity import Entity
from lobbyfacts.model.representative import Representative, Tags, Contact
from lobbyfacts.model.country import Country, CountryMembership
from lobbyfacts.model.category import Category
from lobbyfacts.model.person import Person, Accreditation
from lobbyfacts.model.organisation import Organisation, OrganisationMembership
from lobbyfacts.model.financial_data import FinancialData, FinancialTurnover, CustomIncome
from lobbyfacts.model.tag import Tag
from lobbyfacts.model.meeting import Meeting, MeetingParticipants, MeetingDeregistered
from lobbyfacts.model.action import ActionField, AssociatedAction
from lobbyfacts.model.interest import Interest, AssociatedInterest

from lobbyfacts.model.reports import test_report, rep_by_exp, rep_by_country
from lobbyfacts.model.reports import representatives, places, reps_by_accredited
from lobbyfacts.model.reports import rep_by_turnover, rep_by_fte, fte_by_subcategory
from lobbyfacts.model.reports import accredited_by_cat, biggest_reps
from lobbyfacts.model.reports import reps_by_eufunding

log = logging.getLogger(__name__)

REPORTS = {
    'test_report': test_report,
    'places': places,
    'representatives': representatives,
    'rep_by_exp': rep_by_exp,
    'rep_by_country': rep_by_country,
    'rep_by_turnover': rep_by_turnover,
    'rep_by_fte': rep_by_fte,
    'fte_by_subcategory': fte_by_subcategory,
    'by_accredited': reps_by_accredited,
    'accredited_by_cat': accredited_by_cat,
    'reps_by_members': biggest_reps,
    'reps_by_eufunding': reps_by_eufunding,
    }

def update_index():
    from lobbyfacts.core import db
    for entity in db.session.query(Entity).yield_per(1000):
        log.info("Indexing %s...", entity.name)
        entity.update_index()
        #db.session.add(entity)
    db.session.commit()
