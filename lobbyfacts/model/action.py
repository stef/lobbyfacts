from datetime import datetime

from lobbyfacts.core import db
from lobbyfacts.model.api import ApiEntityMixIn
from lobbyfacts.model.revision import RevisionedMixIn
from lobbyfacts.model import util
from lobbyfacts.model.representative import Representative

class ActionField(db.Model, ApiEntityMixIn):
    __tablename__ = 'action_field'

    id = db.Column(db.BigInteger, primary_key=True)

    action = db.Column(db.Unicode)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow,
            onupdate=datetime.utcnow)

    #representatives = db.relationship('Representative',
    #        backref='action_fields')

    @classmethod
    def create(cls, data):
        cls = cls()
        return cls.update(data)

    def update(self, data):
        self.action = data.get('action')
        db.session.add(self)
        return self

    @classmethod
    def by_id(cls, id):
        q = db.session.query(cls)
        q = q.filter_by(id=id)
        return q.first()

    @classmethod
    def by_action(cls, action):
        q = db.session.query(cls)
        q = q.filter(cls.action==action)
        return q.first()

    @classmethod
    def all(cls):
        return db.session.query(cls)

    def as_shallow(self):
        d= {
            'action': self.action,
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


class AssociatedAction(db.Model, RevisionedMixIn, ApiEntityMixIn):
    __tablename__ = 'associated_action'

    representative_id = db.Column(db.String(36), db.ForeignKey('representative.id'))
    action_id = db.Column(db.BigInteger(), db.ForeignKey('action_field.id'))
    status = db.Column(db.Unicode)

    action = db.relationship(ActionField,
            backref=db.backref('associated_reps',
                lazy='dynamic',
            ))

    representative = db.relationship(Representative,
        uselist=False,
        backref=db.backref('action_fields',
            lazy='dynamic',
            ))

    @classmethod
    def create(cls, data):
        cls = cls()
        return cls.update(data)

    def update_values(self, data):
        self.representative = data.get('representative')
        self.action = data.get('action')
        self.status = data.get('status')

    def as_shallow(self):
        d = super(AssociatedAction, self).as_dict()
        d.update({
            'representative_id': self.representative_id,
            'action_field_id': self.action_field_id,
            })
        return d

    @classmethod
    def by_rpa(cls, representative, action):
        q = db.session.query(cls)
        q = q.filter(cls.action_id==action.id)
        q = q.filter(cls.representative_id==representative.id)
        return q.first()

    def __repr__(self):
        return "<AssociatedAction(%s,%r)>" % (self.id, self.action)

