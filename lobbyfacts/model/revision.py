from datetime import datetime

from lobbyfacts.core import db
from lobbyfacts.model.util import make_serial, make_id
from lobbyfacts.model.util import JSONType, JSONEncoder
from lobbyfacts.model.util import MutableDict
from sqlalchemy import inspect

class AuditTrail(db.Model):
    __tablename__ = 'audit_trail'

    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'

    ACTIONS = [CREATE, UPDATE, DELETE]

    id = db.Column(db.String(36), primary_key=True, default=make_id)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    obj = db.Column(MutableDict.as_mutable(JSONType))
    obj_id = db.Column(db.String(36))
    obj_type = db.Column(db.Unicode)
    action = db.Column(db.Unicode)

    @classmethod
    def create(cls, obj, action):
        trail = cls()
        assert action in cls.ACTIONS, action
        trail.action = action
        trail.obj = obj.as_dict()
        trail.obj_id = obj.id
        trail.obj_type = obj.__tablename__
        trail.created_at = obj.updated_at
        db.session.add(trail)
        return trail

    def __repr__(self):
        return "<AuditTrail(%s,%s,%s)>" % (self.obj_type, self.obj_id, self.created_at)

    def as_dict(self):
        return {
                'id': self.id,
                'obj': self.obj,
                'created_at': self.created_at,
                'action': self.action
            }


class RevisionedMixIn(object):
    """ Simple versioning system for the database objects. We are
    creating an audit trail for each object so that we can 
    deserialize its history upon demand. """

    id = db.Column(db.String(36), primary_key=True, default=make_id)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime)
    deleted_at = db.Column(db.DateTime)

    @classmethod
    def create(cls, data):
        """ Create a new, versioned object. """
        obj = cls()
        obj.id = make_id()
        obj.update(data)
        return obj

    def update(self, data):
        self.update_values(data)
        if not self in db.session:
            db.session.add(self)
        for attr in inspect(self).attrs:
            if [x for x in attr.history.added or [] if x] or [x for x in attr.history.deleted or [] if x]:
                self.updated_at = datetime.utcnow()
                action = AuditTrail.UPDATE if self.created_at else AuditTrail.CREATE
                AuditTrail.create(self, action)
                break
        db.session.flush()
        return self

    def update_values(self, data):
        raise TypeError()

    def delete(self):
        if self.deleted_at is not None:
            return
        self.deleted_at = datetime.utcnow()
        self.cascade_delete()

    def cascade_delete(self):
        pass

    def trail(self):
        q = db.session.query(AuditTrail)
        q = q.filter(AuditTrail.obj_id==self.id)
        q = q.filter(AuditTrail.obj_type==self.__tablename__)
        q = q.order_by(AuditTrail.created_at.desc())
        return q

    def as_dict(self):
        return {
            'id': self.id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
            }

    @classmethod
    def by_attr(cls, attr, value):
        q = db.session.query(cls)
        q = q.filter(cls.deleted_at==None)
        q = q.filter(attr==value)
        return q.first()

    @classmethod
    def by_id(cls, id):
        q = db.session.query(cls)
        q = q.filter(cls.deleted_at==None)
        q = q.filter_by(id=id)
        return q.first()

    @classmethod
    def all(cls):
        q = db.session.query(cls)
        q = q.filter(cls.deleted_at==None)
        return q

