from lobbyfacts.core import db
from lobbyfacts.model.revision import RevisionedMixIn
from lobbyfacts.model.representative import Representative

class MeetingParticipants(db.Model, RevisionedMixIn):
    __tablename__ = 'meeting_participants'
    representative_id = db.Column('representative_id', db.String(36), db.ForeignKey('representative.id'), primary_key=True)
    meeting_id = db.Column('meeting_id', db.String(32), db.ForeignKey('meeting.id'), primary_key=True)

    def as_shallow(self):
        return { 'representative': Representative.by_id(self.representative_id).entity.name,
                 'meetings': Meeting.by_id(self.meeting_id),
                 'representative_id': self.representative_id }

    def as_dict(self):
        return self.as_shallow()

    @classmethod
    def all(cls):
        return db.session.query(cls)

class Meeting(db.Model, RevisionedMixIn):
    __tablename__ = 'meeting'

    id = db.Column(db.String(32), primary_key=True)
    ec_representative = db.Column(db.Unicode) # could also be model.person
    ec_org = db.Column(db.Unicode)
    date = db.Column(db.DateTime)
    location = db.Column(db.Unicode)
    subject = db.Column(db.Unicode)
    participants = db.relationship("Representative", secondary=MeetingParticipants.__table__, backref='meetings')

    unregistered = db.Column(db.Unicode)
    cancelled = db.Column(db.Boolean)

    def update_values(self, data):
        self.id = data.get('id')
        self.ec_representative = data.get('ec_representative')
        self.ec_org = data.get('ec_org')
        self.date = data.get('date')
        self.location = data.get('location')
        self.subject = data.get('subject')
        self.unregistered = data.get('unregistered')
        self.cancelled = data.get('cancelled')

    @classmethod
    def by_id(cls, id):
        return cls.by_attr(cls.id, id)

    def as_shallow(self):
        d = super(Meeting, self).as_dict()
        d.update({
            'id': self.id,
            'ec_representative': self.ec_representative,
            'ec_org': self.ec_org,
            'date': self.date,
            'location': self.location,
            'subject': self.subject,
            'participants': [(p.id,p.entity.name if p.entity else 'unknown') for p in self.participants],
            'unregistered': self.unregistered,
            'cancelled': self.cancelled})
        return d

    def as_dict(self):
        d = self.as_shallow()
        d.update({
            'participants': [p.as_shallow() for p in self.participants]
            })
        return d

    def __repr__(self):
        return "<Meeting(%s,%r)>" % (self.ec_org, self.subject)

