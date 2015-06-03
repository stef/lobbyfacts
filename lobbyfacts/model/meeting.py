from lobbyfacts.core import db
from lobbyfacts.model.revision import RevisionedMixIn
from lobbyfacts.model.representative import Representative

class MeetingParticipants(db.Model, RevisionedMixIn):
    __tablename__ = 'meeting_participants'
    representative_id = db.Column('representative_id', db.String(36), db.ForeignKey('representative.id'), primary_key=True)
    meeting_id = db.Column('meeting_id', db.String(32), db.ForeignKey('meeting.id'), primary_key=True)
    status = db.Column(db.Unicode)

    def as_shallow(self):
        return { 'representative': Representative.by_id(self.representative_id).entity.name,
                 'status': self.status,
                 'meetings': Meeting.by_id(self.meeting_id),
                 'representative_id': self.representative_id }

    def as_dict(self):
        return self.as_shallow()

    @classmethod
    def all(cls):
        return db.session.query(cls)

class MeetingDeregistered(db.Model, RevisionedMixIn):
    __tablename__ = 'meeting_deregistered'
    meeting_id = db.Column('meeting_id', db.String(32), db.ForeignKey('meeting.id'), primary_key=True)
    identification_code = db.Column(db.Unicode, primary_key=True)
    name = db.Column(db.Unicode)
    status = db.Column(db.Unicode)
    deregistered = db.relationship('Meeting',
                                   primaryjoin='Meeting.id==MeetingDeregistered.meeting_id',
                                   backref=db.backref('deregistered'))

    def update_values(self, data):
        self.meeting_id = data.get('meeting_id')
        self.name = data.get('name')
        self.identification_code = data.get('identification_code')
        self.status = data.get('status')

    @classmethod
    def by_midc(cls, meeting, identification_code):
        q = db.session.query(cls)
        q = q.filter(cls.meeting_id==meeting.id)
        q = q.filter(cls.identification_code==identification_code)
        return q.first()

    def as_shallow(self):
        return { 'name': self.name,
                 'identification_code': self.identification_code,
                 'status': self.status,
                 'meeting': self.meeting_id}

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
    status = db.Column(db.Unicode)

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
        self.status = data.get('status')

    @classmethod
    def by_id(cls, id):
        return cls.by_attr(cls.id, id)

    def as_shallow(self):
        d = super(Meeting, self).as_dict()
        d.update({
            'id': self.id,
            'ec_representative': self.ec_representative,
            'status': self.status,
            'ec_org': self.ec_org,
            'date': self.date,
            'location': self.location,
            'subject': self.subject,
            'participants': ', '.join(["%s(%s)" % (p.entity.name, p.id) for p in self.participants]),
            'deregistered': ', '.join(["%s(%s)" % (p.name, p.identification_code) for p in self.deregistered]),
            'unregistered': self.unregistered,
            'cancelled': self.cancelled})
        return d

    def as_dict(self):
        d = self.as_shallow()
        d.update({
            'participants': [p.as_shallow() for p in self.participants],
            'deregistered': [p.as_shallow() for p in self.deregistered]
            })
        return d

    def __repr__(self):
        return "<Meeting(%s,%r)>" % (self.ec_org, self.subject)

