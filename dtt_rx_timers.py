import aspendb
import re
import sys
import csv

with aspendb.connect() as con:
    with con.cursor() as cur:

        query = ''' SELECT TRELAY.LOCATIONID AS LOCATIONID, TRELAY.S07 AS PROTECTING, TRELAY.S03 AS DEVICE,
                        TREQUEST.ID AS TREQUEST_ID, YEAR(TREQUEST.D01) AS REQUEST_YEAR, TSETTYPE1.SETTINGNAME, TSETTING1.SETTING
                    FROM TRELAY, TREQUEST, TSETTING1, TSETTYPE1 
                    WHERE 
                        TRELAY.ID = TREQUEST.RELAYID AND
                        TREQUEST.ID=TSETTING1.REQUESTID  AND
                        TSETTING1.RELAYTYPE=TSETTYPE1.RELAYTYPE AND
                        TSETTING1.GROUPNAME=TSETTYPE1.GROUPNAME AND
                        TSETTING1.ROWNUMBER=TSETTYPE1.ROWNUMBER AND
                        (
                            TSETTYPE1.SETTINGNAME LIKE 'OUT%' AND 
                            (
                                TSETTING1.SETTING LIKE 'AST%DTT% RX FAIL%' OR
                                TSETTING1.SETTING LIKE 'AST%DTT% RX ALARM%'
                            )
                        )
                    ORDER BY LOCATIONID, PROTECTING, TREQUEST.D01
                '''
        timer_out = re.compile('(AST0[0-9])Q')
        timer_delay = re.compile('AST0[0-9]PT *:= *([.0-9]*)')
        cur.execute(query)
        
        req_list = list(cur.fetchall())
        print('Number returned', len(req_list))
        
        for req in req_list:
            req_timer_match = timer_out.match(req['SETTING'])
            if req_timer_match:
                req_timer = req_timer_match.group(1)
                req['Timer'] = req_timer
                req['Timer_RE'] = req_timer + 'PT%'
                print('Request ID: %s, AST Timer: %s, %s := %s' % (req['TREQUEST_ID'], req_timer, req['SETTINGNAME'], req['SETTING']))
            else:
                print('Request ID: %s, No AST match' % (req['TREQUEST_ID'],))
        #print(req_list[:10])
        sys.exit()
        #con._conn.debug_queries = True
                
        for req in req_list:
            cur.execute(''' SELECT 
                                TREQUEST.ID AS TREQUEST_ID, TSETTYPE1.SETTINGNAME, TSETTING1.SETTING
                            FROM TREQUEST, TSETTING1, TSETTYPE1 
                                WHERE 
                                    TREQUEST.ID=%(TREQUEST_ID)d AND
                                    TREQUEST.ID=TSETTING1.REQUESTID  AND
                                    TSETTING1.RELAYTYPE=TSETTYPE1.RELAYTYPE AND
                                    TSETTING1.GROUPNAME=TSETTYPE1.GROUPNAME AND
                                    TSETTING1.ROWNUMBER=TSETTYPE1.ROWNUMBER AND
                                    (
                                        TSETTYPE1.SETTINGNAME LIKE 'AUTO%' AND
                                        TSETTING1.SETTING LIKE %(Timer_RE)s
                                    )
                            ''', req)
            timer_delay_match = timer_delay.match(cur.fetchone()['SETTING'])
            if timer_delay_match:
                req['Delay'] = float(timer_delay_match.group(1))
        with open('output/dtt_rx_timers.csv', 'w', newline='') as csvfile:
            csvout = csv.DictWriter(csvfile,
                                    ['LOCATIONID', 'DEVICE', 'PROTECTING', 'REQUEST_YEAR', 'Timer', 'Delay'],
                                    extrasaction = 'ignore')
            csvout.writeheader()
            csvout.writerows(req_list)
        #for req in req_list:
        #    print('Request ID: %(TREQUEST_ID)s, Location: %(LOCATIONID)s, Device: %(DEVICE)s, Protecting: %(PROTECTING)s, AST Timer: %(Timer)s, Delay: %(Delay)f' % req)