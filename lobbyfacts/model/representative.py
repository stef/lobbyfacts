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

class Contact(db.Model, ApiEntityMixIn, RevisionedMixIn):
    __tablename__ = 'contact'

    id = db.Column(db.BigInteger, primary_key=True)
    town = db.Column(db.Unicode)
    street = db.Column(db.Unicode)
    phone = db.Column(db.Unicode)
    post_code = db.Column(db.Unicode)
    postbox = db.Column(db.Unicode)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    country_id = db.Column(db.BigInteger, db.ForeignKey('country.id'))
    country = db.relationship('Country', backref=db.backref('contacts'))

    @classmethod
    def create(cls, data):
        cls = cls()
        return cls.update(data)

    def update(self, data):
        self.street = data.get('street')
        self.town = data.get('town')
        self.street = data.get('street')
        self.phone = data.get('phone')
        self.post_code = data.get('post_code')
        self.postbox = data.get('postbox')
        self.lon = data.get('lon')
        self.lat = data.get('lat')
        self.country = data.get('country')
        self.updated_at=data.get('updated_at')
        self.deleted_at=data.get('deleted_at')
        db.session.add(self)
        return self

    @classmethod
    def by_id(cls, id):
        return cls.by_attr(cls.id, id)

    @classmethod
    def all(cls):
        return db.session.query(cls)

    def as_shallow(self):
        d= {
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'town': self.town,
            'street': self.street,
            'phone': self.phone,
            'lon': self.lon,
            'lat': self.lat,
            'post_code': self.post_code,
            'postbox': self.postbox,
            }
        if self.id:
            d['id']=self.id
        return d

    def as_dict(self):
        d = self.as_shallow()
        return d

    def __repr__(self):
        return "<Contact(%s)>" % (' '.join((self.street or '',self.town or '',self.post_code or '',self.country.name)))

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
    other_code_of_conduct = db.Column(db.Unicode)
    web_site_url = db.Column(db.Unicode)
    info_members = db.Column(db.Unicode)
    structure_members = db.Column(db.Unicode)

    members = db.Column(db.BigInteger, nullable=True)
    members_25 = db.Column(db.BigInteger, nullable=True)
    members_50 = db.Column(db.BigInteger, nullable=True)
    members_75 = db.Column(db.BigInteger, nullable=True)
    members_100 = db.Column(db.BigInteger, nullable=True)
    members_fte = db.Column(db.Float, nullable=True)

    number_of_natural_persons = db.Column(db.BigInteger, nullable=True)

    registration_date = db.Column(db.DateTime)
    last_update_date = db.Column(db.DateTime)

    head_office_id = db.Column(db.BigInteger, db.ForeignKey('contact.id'), nullable=True)
    head_office = db.relationship('Contact',
                                  primaryjoin='Representative.head_office_id==Contact.id',
                                  backref=db.backref('head_offices', uselist=False))
    be_office_id = db.Column(db.BigInteger, db.ForeignKey('contact.id'), nullable=True)
    be_office = db.relationship('Contact',
                                primaryjoin='Representative.be_office_id==Contact.id',
                                backref=db.backref('be_offices', uselist=False))
    # for backward compatibility == nationality of rep
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
        self.other_code_of_conduct = data.get('other_code_of_conduct')
        self.web_site_url = data.get('web_site_url')
        self.legal_status = data.get('legal_status')

        self.members = data.get('members')
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
            'other_code_of_conduct': self.other_code_of_conduct,
            'web_site_url': self.web_site_url,
            'legal_status': self.legal_status,
            'members': self.members,
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
            })
        if self.entity:
            d['entity']=self.entity_id
            d['name']=self.entity.name
            d['acronym']=self.entity.acronym
        if self.contact_country:
            d['contact_country']=self.contact_country_id
        if self.head_office:
            d['head_office_phone']=self.head_office.phone
            d['head_office_street']=self.head_office.street
            d['head_office_town']=self.head_office.town
            d['head_office_post_code']=self.head_office.post_code
            d['head_office_postbox']=self.head_office.postbox
            d['head_office_lat']=self.head_office.lat
            d['head_office_lon']=self.head_office.lon
            d['head_office_country']=self.head_office.country.name
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
        if self.be_office:
            d['be_office_phone']=self.be_office.phone
            d['be_office_street']=self.be_office.street
            d['be_office_town']=self.be_office.town
            d['be_office_post_code']=self.be_office.post_code
            d['be_office_postbox']=self.be_office.postbox
            d['be_office_lat']=self.be_office.lat
            d['be_office_lon']=self.be_office.lon
            d['be_office_country']=self.be_office.country.name
        return d

    def as_dict(self):
        # shallow but without flattened office info
        d = {k:v for k,v in self.as_shallow().items() if not k.startswith('head_office') and not k.startswith('be_office')}
        d.update({
            'entity': self.entity.as_shallow() if self.entity else None,
            'contact_country': self.contact_country.as_shallow() if self.contact_country else None,
            'head_office': self.head_office.as_shallow() if self.head_office else None,
            'be_office': self.be_office.as_shallow() if self.be_office else None,
            'main_category': self.main_category.as_shallow() if self.main_category else None,
            'sub_category': self.sub_category.as_shallow() if self.sub_category else None,
            'head': self.head.as_shallow() if self.head else None,
            'legal': self.legal.as_shallow() if self.legal else None,
            'financial_data': [fd.as_shallow(turnovers=True) for fd in self.financial_datas],
            'organisation_memberships': [om.as_dict(representative=False) for om in self.organisation_memberships],
            'action_fields': [af.action for af in self.action_fields],
            'interests': [i.interest for i in self.interests],
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
