import aspendb
from aspendb import Location, Relay, Request, Setting, SettingInfo
import re
import sys
import csv

session = aspendb.get_orm_session() # Using SQLAlchemy interface
        
timer_out = re.compile('(AST0[0-9])Q')
timer_delay = re.compile('AST0[0-9]PT *:= *([.0-9]*)')

set_list = session.query(Setting)\
                .join(SettingInfo)\
                .join(Request).join(Relay)\
                .filter(SettingInfo.settingname.like('OUT%'),
                        (Setting.setting.like('AST%DTT% RX FAIL%') | Setting.setting.like('AST%DTT% RX ALARM%')))\
                .order_by(Relay.locationid, Relay.protecting, Relay.id, Request.request_date)\
                .all()
print('Number returned', len(set_list))


for s in set_list:
    req_timer_match = timer_out.match(s.setting)
    if req_timer_match:
        req_timer = req_timer_match.group(1)
        s.timer = req_timer
        s.timer_RE = req_timer + 'PT%'
        print('Request ID: %s, AST Timer: %s, %s := %s' % (s.requestid, req_timer, s.settinginfo.settingname, s.setting))
    else:
        print('Request ID: %s, No AST match' % (s.request.id,))

for s in set_list:
    auto_setting = session.query(Setting)\
        .join(SettingInfo)\
        .join(Request)\
        .filter(SettingInfo.settingname.like('AUTO%'),
                Setting.setting.like(s.timer_RE),
                Setting.requestid==s.requestid).first()
    #print('%s := %s' % (auto_setting.settinginfo.settingname, auto_setting.setting))

    timer_delay_match = timer_delay.match(auto_setting.setting)
    if timer_delay_match:
        s.delay = float(timer_delay_match.group(1))
    #print(s.request.request_date)

with open('output/dtt_rx_timers2.csv', 'w', newline='') as csvfile:
    csvout = csv.DictWriter(csvfile,
                            ['LOCATIONID', 'DEVICE', 'PROTECTING', 'REQUEST_YEAR', 'Timer', 'Delay'],
                            extrasaction = 'ignore')
    csvout.writeheader()
    csvout.writerows([{'LOCATIONID': s.request.relay.locationid,
                      'DEVICE': s.request.relay.device_num,
                      'PROTECTING': s.request.relay.protecting,
                      'REQUEST_YEAR': s.request.request_date.year if s.request.request_date is not None else '',
                      'Timer': s.timer,
                      'Delay': s.delay} for s in set_list])
