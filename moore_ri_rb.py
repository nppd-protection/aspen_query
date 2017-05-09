import aspendb
from aspendb import Location, Relay, Request, Setting, SettingInfo
import re
import sys
import csv

session = aspendb.get_orm_session() # Using SQLAlchemy interface
        
location_id = 'MOORE'
request_list = session.query(Request)\
                .join(Relay)\
                .filter(Relay.locationid == location_id,
                        Relay.relaytype.like('SEL-421%'),
                        Request.status == 'IN SERVICE')\
                .order_by(Relay.protecting)\
                .all()

print('Number returned', len(request_list))


for r in request_list:
    r.RB_setting = session.query(Setting)\
        .join(SettingInfo)\
        .join(Request)\
        .filter(SettingInfo.settingname == 'OUT101',
                Setting.requestid==r.id).first().setting

    r.RI_setting = session.query(Setting)\
        .join(SettingInfo)\
        .join(Request)\
        .filter(SettingInfo.settingname == 'OUT103',
                Setting.requestid==r.id).first().setting

with open('output/moore_ri_rb.csv', 'w') as csvfile:
    csvout = csv.DictWriter(csvfile,
                            ['LOCATIONID', 'DEVICE', 'PROTECTING', 'REQUEST_YEAR', 'RB_RI'],
                            lineterminator = '\n',
                            extrasaction = 'ignore')
    csvout.writeheader()
    csvout.writerows([{'LOCATIONID': r.relay.locationid,
                      'DEVICE': r.relay.device_num,
                      'PROTECTING': r.relay.protecting,
                      'REQUEST_YEAR': r.request_date.year if r.request_date is not None else '',
                      'RB_RI': '\n'.join(['RB = '+r.RB_setting, 'RI = '+r.RI_setting])} for r in request_list])
