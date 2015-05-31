from datetime import datetime

from lobbyfacts.core import db
from lobbyfacts.model.api import ApiEntityMixIn
from lobbyfacts.model.revision import RevisionedMixIn
from lobbyfacts.model import util
from lobbyfacts.model.representative import Representative

class Interest(db.Model, ApiEntityMixIn):
    __tablename__ = 'interest'

    id = db.Column(db.BigInteger, primary_key=True)

    interest = db.Column(db.Unicode)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow,
            onupdate=datetime.utcnow)

    @classmethod
    def create(cls, data):
        cls = cls()
        return cls.update(data)

    def update(self, data):
        self.interest = data.get('interest')
        db.session.add(self)
        return self

    @classmethod
    def by_id(cls, id):
        q = db.session.query(cls)
        q = q.filter_by(id=id)
        return q.first()

    @classmethod
    def by_interest(cls, interest):
        q = db.session.query(cls)
        q = q.filter(cls.interest==interest)
        return q.first()

    @classmethod
    def all(cls):
        return db.session.query(cls)

    def as_shallow(self):
        d= {
            'interest': self.interest,
            'created_at': self.created_at,
            'updated_at': self.updated_at
            }
        if self.id:
            d['id']=self.id
        return d

    def as_dict(self):
        d = self.as_shallow()
        return d

    def __repr__(self):
        return "<Country(%s)>" % (self.code)


class AssociatedInterest(db.Model, RevisionedMixIn, ApiEntityMixIn):
    __tablename__ = 'associated_interest'

    representative_id = db.Column(db.String(36), db.ForeignKey('representative.id'))
    interest_id = db.Column(db.BigInteger(), db.ForeignKey('interest.id'))
    status = db.Column(db.Unicode)

    index = db.Index('idx_rpi', 'representative_id', 'interest_id')

    interest = db.relationship(Interest,
            backref=db.backref('associated_reps',
                lazy='dynamic',
            ))

    representative = db.relationship(Representative,
        uselist=False,
        backref=db.backref('interests',
            lazy='dynamic',
            ))

    @classmethod
    def create(cls, data):
        cls = cls()
        return cls.update(data)

    def update_values(self, data):
        self.representative = data.get('representative')
        self.interest = data.get('interest')
        self.status = data.get('status')

    def as_shallow(self):
        d = super(AssociatedInterest, self).as_dict()
        d.update({
            'representative_id': self.representative_id,
            'interest_id': self.interest_it,
            })
        return d

    @classmethod
    def by_rpi(cls, representative, interest):
        q = db.session.query(cls)
        q = q.filter(cls.interest_id==interest.id)
        q = q.filter(cls.representative_id==representative.id)
        return q.first()

    def __repr__(self):
        return "<AssociatedInterest(%s,%r)>" % (self.id, self.interest)

