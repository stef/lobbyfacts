from lobbyfacts.core import db
from lobbyfacts.model.api import ApiEntityMixIn
from lobbyfacts.model.revision import RevisionedMixIn
from lobbyfacts.model.entity import Entity
from lobbyfacts.model.tag import Tag

class Tags(db.Model, ApiEntityMixIn):
    __tablename__ = 'tags'
    representative_id = db.Column('representative_id', db.String(36), db.ForeignKey('representative.id'), primary_key=True)
    tag_id = db.Column('tag_id', db.BigInteger, db.ForeignKey('tag.id'), primary_key=True)

    def as_shallow(self):
        return { 'representative': Representative.by_id(self.representative_id).entity.name,
                 'tags': Tag.by_id(self.tag_id),
                 'representative_id': self.representative_id }

    def as_dict(self):
        return self.as_shallow()

    @classmethod
    def all(cls):
        return db.session.query(cls)

class Representative(db.Model, RevisionedMixIn, ApiEntityMixIn):
    __tablename__ = 'representative'

    entity_id = db.Column(db.String(36), db.ForeignKey('entity.id'))

    identification_code = db.Column(db.Unicode)
    native_name = db.Column(db.Unicode)
    goals = db.Column(db.Unicode)

    activity_consult_committees = db.Column(db.Unicode)
    activity_eu_legislative = db.Column(db.Unicode)
    activity_expert_groups = db.Column(db.Unicode)
    activity_high_level_groups = db.Column(db.Unicode)
    activity_industry_forums = db.Column(db.Unicode)
    activity_inter_groups = db.Column(db.Unicode)
    activity_other = db.Column(db.Unicode)
    activity_relevant_comm = db.Column(db.Unicode)

    status = db.Column(db.Unicode)
    networking = db.Column(db.Unicode)
    legal_status = db.Column(db.Unicode)
    code_of_conduct = db.Column(db.Unicode)
    web_site_url = db.Column(db.Unicode)
    info_members = db.Column(db.Unicode)
    structure_members = db.Column(db.Unicode)

    members_25 = db.Column(db.BigInteger, nullable=True)
    members_50 = db.Column(db.BigInteger, nullable=True)
    members_75 = db.Column(db.BigInteger, nullable=True)
    members_100 = db.Column(db.BigInteger, nullable=True)
    members_fte = db.Column(db.BigInteger, nullable=True)

    number_of_natural_persons = db.Column(db.BigInteger, nullable=True)

    registration_date = db.Column(db.DateTime)
    last_update_date = db.Column(db.DateTime)

    contact_town = db.Column(db.Unicode)
    contact_street = db.Column(db.Unicode)
    contact_phone = db.Column(db.Unicode)
    contact_post_code = db.Column(db.Unicode)
    contact_postbox = db.Column(db.Unicode)
    contact_fax = db.Column(db.Unicode)
    contact_lat = db.Column(db.Float)
    contact_lon = db.Column(db.Float)
    contact_country_id = db.Column(db.BigInteger, db.ForeignKey('country.id'))

    main_category_id = db.Column(db.BigInteger, db.ForeignKey('category.id'))
    sub_category_id = db.Column(db.BigInteger, db.ForeignKey('category.id'))
    head_id = db.Column(db.Unicode(36), db.ForeignKey('person.id'))
    legal_id = db.Column(db.Unicode(36), db.ForeignKey('person.id'))

    tags = db.relationship("Tag", secondary=Tags.__table__, backref='representatives')

    def update_values(self, data):
        self.entity = data.get('entity')
        assert self.entity is not None, self.entity

        self.identification_code = data.get('identification_code')

        self.native_name = data.get('native_name')
        self.goals = data.get('goals')
        self.status = data.get('status')
        self.activity_consult_committees = data.get('activity_consult_committees')
        self.activity_eu_legislative = data.get('activity_eu_legislative')
        self.activity_expert_groups = data.get('activity_expert_groups')
        self.activity_high_level_groups = data.get('activity_high_level_groups')
        self.activity_industry_forums = data.get('activity_industry_forums')
        self.activity_inter_groups = data.get('activity_inter_groups')
        self.activity_other = data.get('activity_other')
        self.activity_relevant_comm = data.get('activity_relevant_comm')
        self.networking = data.get('networking')
        self.code_of_conduct = data.get('code_of_conduct')
        self.web_site_url = data.get('web_site_url')
        self.legal_status = data.get('legal_status')

        self.members_25 = data.get('members_25')
        self.members_50 = data.get('members_50')
        self.members_75 = data.get('members_75')
        self.members_100 = data.get('members_100')
        self.members_fte = data.get('members_fte')
        self.info_members = data.get('info_members')
        self.structure_members = data.get('structure_members')
        self.number_of_natural_persons = data.get('number_of_natural_persons')

        self.registration_date = data.get('registration_date')
        self.last_update_date = data.get('last_update_date')

        self.contact_town = data.get('contact_town')
        self.contact_street = data.get('contact_street')
        self.contact_phone = data.get('contact_phone')
        self.contact_post_code = data.get('contact_post_code')
        self.contact_postbox = data.get('contact_postbox')
        self.contact_fax = data.get('contact_fax')
        self.contact_lon = data.get('contact_lon')
        self.contact_lat = data.get('contact_lat')
        self.contact_country = data.get('contact_country')

        self.main_category = data.get('main_category')
        self.sub_category = data.get('sub_category')

        self.head = data.get('head')
        self.legal = data.get('legal')

    @classmethod
    def by_identification_code(cls, identification_code):
        return cls.by_attr(cls.identification_code,
                           identification_code)

    @classmethod
    def by_id(cls, id):
        return cls.by_attr(cls.id,
                           id)

    def as_shallow(self):
        d = super(Representative, self).as_dict()
        d.update({
            'uri': self.uri,
            'identification_code': self.identification_code,
            'native_name': self.native_name,
            'goals': self.goals,
            'status': self.status,
            'activity_consult_committees': self.activity_consult_committees,
            'activity_eu_legislative': self.activity_eu_legislative,
            'activity_expert_groups': self.activity_expert_groups,
            'activity_high_level_groups': self.activity_high_level_groups,
            'activity_industry_forums': self.activity_industry_forums,
            'activity_inter_groups': self.activity_inter_groups,
            'activity_other': self.activity_other,
            'activity_relevant_comm': self.activity_relevant_comm,
            'networking': self.networking,
            'code_of_conduct': self.code_of_conduct,
            'web_site_url': self.web_site_url,
            'legal_status': self.legal_status,
            'members_25': self.members_25,
            'members_50': self.members_50,
            'members_75': self.members_75,
            'members_100': self.members_100,
            'members_fte': self.members_fte,
            'structure_members': self.structure_members,
            'info_members': self.info_members,
            'number_of_natural_persons': self.number_of_natural_persons,
            'registration_date': self.registration_date,
            'last_update_date': self.last_update_date,
            'contact_town': self.contact_town,
            'contact_street': self.contact_street,
            'contact_phone': self.contact_phone,
            'contact_lon': self.contact_lon,
            'contact_lat': self.contact_lat,
            'contact_post_code': self.contact_post_code,
            'contact_postbox': self.contact_postbox,
            'contact_fax': self.contact_fax
            })
        if self.entity:
            d['entity']=self.entity_id
            d['name']=self.entity.name
            d['acronym']=self.entity.acronym
        if self.contact_country:
            d['contact_country']=self.contact_country_id
        if self.main_category:
            d['main_category']=self.main_category_id
            d['main_category_title']=self.main_category.name
        if self.sub_category:
            d['sub_category']=self.sub_category_id
            d['sub_category_title']=self.sub_category.name
        if self.head:
            d['head']=self.head_id
        if self.legal:
            d['legal']=self.legal_id
        return d

    def as_dict(self):
        d = self.as_shallow()
        d.update({
            'entity': self.entity.as_shallow() if self.entity else None,
            'contact_country': self.contact_country.as_shallow() if self.contact_country else None,
            'main_category': self.main_category.as_shallow() if self.main_category else None,
            'sub_category': self.sub_category.as_shallow() if self.sub_category else None,
            'head': self.head.as_shallow() if self.head else None,
            'legal': self.legal.as_shallow() if self.legal else None,
            'financial_data': [fd.as_shallow(turnovers=True) for fd in self.financial_datas],
            'organisation_memberships': [om.as_dict(representative=False) for om in self.organisation_memberships],
            'action_fields': [af.action for af in self.action_fields],
            'accreditations': [a.as_dict(representative=False) for a in self.accreditations],
            'tags': [t.tag for t in self.tags]
            })
        return d

    def cascade_delete(self):
        for a in self.accreditations:
            a.delete()
        for om in self.organisation_memberships:
            om.delete()
        for fd in self.financial_datas:
            fd.delete()

    def __repr__(self):
        return "<Representative(%s,%r)>" % (self.id, self.entity)

Entity.representative = db.relationship(Representative,
        uselist=False, backref=db.backref('entity'))


