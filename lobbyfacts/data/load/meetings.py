import logging

from lobbyfacts.core import db
from lobbyfacts.data import sl, etl_engine
from lobbyfacts.model import Representative, Meeting, MeetingDeregistered
from lobbyfacts.core import app
from datetime import datetime

log = logging.getLogger(__name__)

def load_meeting(engine, meet):
    # rename: id, name, org
    meet['id'] = meet['meetid']
    del meet['meetid']
    meet['ec_representative'] = meet['name']
    del meet['name']
    meet['ec_org'] = meet['eu_representative']
    del meet['eu_representative']

    # handle cancelled
    meet['cancelled']=False
    if 'Cancelled' in meet['date']:
        meet['cancelled']=True
        meet['date']=meet['date'].split()[0]

    # handle date
    meet['date'] = datetime.strptime(meet['date'],"%d/%m/%Y")

    meeting = Meeting.by_id(meet['id'])
    if meeting is None:
        if meet['identification_code'] == 'unregistered':
            meet['unregistered']=meet['representative']
        meeting = Meeting.create(meet)
    else:
        if meet['identification_code'] == 'unregistered':
            if meeting.unregistered:
                meet['unregistered']='; '.join(sorted(set(meeting.unregistered.split('; ')+[meet['representative']])))
            else:
                meet['unregistered']=meet['representative']
        meeting.update(meet)

    if  meet['identification_code'] != 'unregistered':
        rep = Representative.by_identification_code(meet['identification_code'])
        if rep is None:
            #print "could not match", meet['identification_code']
            dereg_ = {'name': meet['representative'],
                      'identification_code': meet['identification_code'],
                      'status': meet['status'],
                      'meeting_id': meeting.id}
            if meeting.deregistered:
                dereg = MeetingDeregistered.by_midc(meeting,meet['identification_code'])
                if not dereg:
                    dereg = MeetingDeregistered.create(dereg_)
                    meeting.deregistered.append(dereg)
                    print 'appended', meeting.unregistered
                else:
                    dereg.update(dereg_)
            else:
                dereg = MeetingDeregistered.create(dereg_)
                meeting.deregistered.append(dereg)

        else:
            #print "\o/ match", meet['representative'], meet['identification_code']
            if rep not in meeting.participants:
                meeting.participants.append(rep)

    db.session.commit()

def external_url_handler(error, endpoint, values):
    return ''

def load(engine):
    for i, meet in enumerate(sl.all(engine, sl.get_table(engine, 'meeting'))):
        log.info("Loading(%s): %s", i, meet.get('name'))
        load_meeting(engine, meet)

if __name__ == '__main__':
    # init flask
    app.url_build_error_handlers.append(external_url_handler)
    ctx = app.test_request_context()
    ctx.push()
    global request
    request = app.preprocess_request()

    engine = etl_engine()
    load(engine)

