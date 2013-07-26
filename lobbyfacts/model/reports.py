from sqlalchemy.sql.expression import nullslast
from sqlalchemy.orm import aliased

from lobbyfacts.core import db

from lobbyfacts.model.entity import Entity
from lobbyfacts.model.country import Country
from lobbyfacts.model.category import Category
from lobbyfacts.model.representative import Representative
from lobbyfacts.model.financial_data import FinancialData
from lobbyfacts.model.person import Accreditation
from lobbyfacts.model.tag import Tag

def _greatest():
    return db.func.greatest

def test_report():
    """ Just a test for debugging reports. """
    return db.session.query(Entity.id.label('id'),
        Entity.name.label('name'))

def representatives():
    """ Full list of representatives and their financials. """
    q = db.session.query(Representative)
    q = q.join(Entity)
    q = q.join(FinancialData)
    MainCategory = aliased(Category, name='MainCategory')
    SubCategory = aliased(Category, name='SubCategory')
    q = q.join(MainCategory, Representative.main_category)
    q = q.join(SubCategory, Representative.sub_category)
    q = q.add_entity(Entity)
    q = q.add_entity(FinancialData)
    q = q.add_entity(MainCategory)
    q = q.add_entity(SubCategory)
    return q

def places():
    """ Abridged version of representatives and locations. """
    q = db.session.query(Representative.id,
            Representative.identification_code,
            Representative.contact_town,
            Representative.contact_street,
            Representative.contact_lon,
            Representative.contact_lat)
    q = q.join(Entity)
    q = q.join(Country)
    q = q.join(FinancialData)
    q = q.add_column(Entity.name)
    q = q.add_column(FinancialData.turnover_min)
    q = q.add_column(FinancialData.turnover_max)
    q = q.add_column(FinancialData.turnover_absolute)
    q = q.add_column(Country.name.label("country"))
    return q

def rep_by_exp(sub_category_id=None):
    """Representatives spending most on lobbying in a subcategory."""
    q = db.session.query(Representative.id,
            Representative.identification_code)
    q = q.join(Country)
    q = q.join(FinancialData)
    q = q.join(Entity)
    q = q.add_column(Country.name.label("contact_country"))
    q = q.add_column(Entity.name)
    cost = _greatest()(FinancialData.cost_absolute,
                       FinancialData.cost_max)
    cost = cost.label("cost")
    if sub_category_id is not None:
        q = q.filter(Representative.sub_category_id==sub_category_id)
    q = q.filter(cost!=None)
    q = q.add_column(cost)
    q = q.order_by(cost.desc())
    return q

def rep_by_country():
    """Group the representatives for each country."""
    q = db.session.query(Country.name)
    q = q.join(Representative)
    q = q.group_by(Country.name)
    count = db.func.count(Representative.id).label("count")
    q = q.add_column(count)
    q = q.order_by(count.desc())
    return q

def rep_by_turnover(sub_category_id=None):
    """Lobbying firms with the highest turnover in a subcategory."""
    q = db.session.query(Representative.id,
            Representative.identification_code)
    q = q.join(Country)
    q = q.join(FinancialData)
    q = q.join(Entity)
    q = q.add_column(Country.name.label("contact_country"))
    q = q.add_column(Entity.name)
    turnover = _greatest()(FinancialData.turnover_absolute,
                       FinancialData.turnover_max)
    turnover = turnover.label("turnover")
    if sub_category_id is not None:
        q = q.filter(Representative.sub_category_id==sub_category_id)
    q = q.filter(turnover!=None)
    q = q.add_column(turnover)
    q = q.order_by(turnover.desc())
    return q

def rep_by_fte(sub_category_id=None):
    """Represenatatives with the most lobbyists employed in a subcategory."""
    q = db.session.query(Representative.id,
            Representative.identification_code,
            Representative.number_of_natural_persons)
    q = q.join(Country)
    q = q.join(Entity)
    q = q.join(Accreditation)
    q = q.group_by(Representative.id,
            Representative.identification_code,
            Representative.number_of_natural_persons,
            Country.name.label("contact_country"),
            Entity.name)
    q = q.add_column(Country.name.label("contact_country"))
    q = q.add_column(Entity.name)
    q = q.add_column(db.func.count(Accreditation.id).label("accreditations"))
    if sub_category_id is not None:
        q = q.filter(Representative.sub_category_id==sub_category_id)
    q = q.order_by(Representative.number_of_natural_persons.desc())
    return q

def fte_by_subcategory():
    """Number of lobbyists and accreditations in each subcategory."""
    q = db.session.query(Category.id, Category.name)
    q = q.filter(Category.parent_id!=None)
    q = q.join(Representative.sub_category)
    #q = q.join(Accreditation)
    q = q.group_by(Category.id, Category.name)
    count = db.func.count(Representative.id).label("representatives")
    q = q.add_column(count)
    #accreditations = db.func.count(Accreditation.id).label("accreditations")
    #q = q.add_column(accreditations)
    ftes = db.func.sum(Representative.members).label("members")
    q = q.order_by(ftes.desc())
    q = q.add_column(ftes)
    return q

def unregistered_representatives():
    """ Full list of unregistered representatives. """
    q = db.session.query(Representative)
    q = q.join(Entity)
    q = q.join(Representative.tags)
    q = q.add_entity(Entity)
    q = q.filter(Tag.tag == 'unregistered')
    return q

def reps_by_accredited():
    """ Top representatives ranked by the number of their accredited persons. """
    count = db.func.count(Accreditation.id).label('count')
    q = db.session.query(count, Entity.name).group_by(Entity.name)
    q = q.join(Representative)
    q = q.join(Entity)
    q = q.order_by(count.desc()).limit(30)
    return q

def accreditted_by_cat():
    """ Top representatives ranked by the number of their accredited persons. """
    count = db.func.count(Accreditation.id).label('count')
    MainCategory = aliased(Category, name='MainCategory')
    SubCategory = aliased(Category, name='SubCategory')
    q = db.session.query(count, MainCategory.name, SubCategory.name).group_by(MainCategory.name, SubCategory.name)
    q = q.join(Representative)
    q = q.join(MainCategory, Representative.main_category)
    q = q.join(SubCategory, Representative.sub_category)
    q = q.order_by(count.desc())
    return q

def biggest_reps():
    """ Full list of representatives and their financials. Sorted by membership size."""
    q = representatives()
    q = q.order_by(Representative.members.desc())
    return q
