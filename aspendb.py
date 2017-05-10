# Use pymssql to connect to the Microsoft SQL database
# Install using pip install pymssql
# Also need to get FreeTDS and OpenSSL as described at http://pymssql.org/en/latest/freetds.html
#
# These prerequisites are currently installed in an Anaconda environment named 'aspen_query'
#
import pymssql
import sys

# Connection information for MSQL version of Aspen Database
from aspendb_config import server, user, password, database


def connect(server=server, user=user, password=password, database=database, as_dict=True):
    return pymssql.connect(server, user, password, database, as_dict=as_dict)


def simple_connect_test(argv=None):
    if argv is None:
        argv = sys.argv
        
    with connect() as con:
        with con.cursor() as cur:

            query = """SELECT TRELAY.ID FROM TRELAY"""
            
            cur.execute(query)
            
            rtn = list(cur.fetchmany(10))
            print('Number returned', len(rtn))
            print(rtn)
    
# Trying out an SQLAlchemy ORM connection
# See tutorial at http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#building-a-relationship
# Notes on fields in Aspen Database saved at Z:\ASPEN\DB Scripts\Misc database queries
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Date, DateTime, Text, \
        ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.hybrid import hybrid_property
Base = declarative_base()
class Location(Base):
    __tablename__ = 'TLOCATION'
    id = Column(String, primary_key=True)
    name = Column('s01', String)
    area = Column('s02', String)
    region = Column('s03', String)
    voltage = Column('s04', String)
    sap_fl = Column('s05', String)

    @hybrid_property
    def sub_num(self):
        rtn = self.name[-6:]
        if all(c in '0123456789' for c in rtn):
            return rtn
        else:
            return None


    @hybrid_property
    def sap_fl2(self):
        sub_num = self.sub_num
        if sub_num is None:
            return None
        # Check for transmission subs
        if sub_num[0] == '9':
            area_map = {'Northern': 'N',
                        'Eastern': 'E',
                        'Central': 'C',
                        'Western': 'W'}
            try:
                area_code = area_map[self.area]
            except KeyError:
                return None
            return '-'.join(('TS', 'S', area_code, sub_num))
        # Check for retail subs
        #if sub_num[0] == '8':
        #    area_code = '_'
        #    return '-'.join(('R', 'S', area_code, sub_num))
        return None

class Relay(Base):
    __tablename__ = 'TRELAY'
    id = Column(Integer, primary_key=True)
    protecting = Column('s07', String)
    district_num = Column('s01', String)
    relaytype = Column(String)
    memo = Column('m01', Text)
    sap_eq_num = Column('n01', Integer)
    mva_for_oc = Column('n02', Float)
    device_num = Column('s03', String)
    function = Column('s06', String)
    style_num = Column('s02', String)
    serial_num = Column('s08', String)
    inst_book = Column('s04', String)
    ieee_num = Column('s05', String)
    mva_for_dist = Column('s09', String)
    relay_angle = Column('s10', String)
    ct_ratio = Column('s11', String)
    owner = Column('s19', String)
    functional_location = Column('s20', String)
    locationid = Column(String, ForeignKey('TLOCATION.id'))
    location = relationship('Location', back_populates='relays')
    requests = relationship('Request', order_by='Request.request_date',
                            back_populates='relay')

Location.relays = relationship('Relay', order_by=Relay.id,
                               back_populates='location')

class Request(Base):
    __tablename__ = 'TREQUEST'
    id = Column(Integer, primary_key=True)
    requestor = Column('s01', String)
    setting_type = Column('s02', String)
    status = Column('s03', String)
    request_date = Column('d01', Date)
    service_date = Column('d02', Date)
    memo = Column('m01', Text)
    sign = Column(String)
    dlastsigned = Column(DateTime)
    dlastchanged = Column(DateTime)
    relayid = Column(String, ForeignKey('TRELAY.id'))
    relay = relationship('Relay', back_populates='requests')
    settings = relationship('Setting', order_by='(Setting.groupname, '
                                                'Setting.rownumber)',
                            back_populates='request')

class Setting(Base):
    __tablename__ = 'TSETTING1'
    __table_args__ = (ForeignKeyConstraint(['relaytype', 'groupname', 'rownumber'], ['TSETTYPE1.relaytype', 'TSETTYPE1.groupname', 'TSETTYPE1.rownumber']),)
    requestid = Column(Integer, ForeignKey('TREQUEST.id'), primary_key=True)
    relaytype = Column(String, primary_key=True)
    groupname = Column(String, primary_key=True)
    rownumber = Column(Float, primary_key=True)
    setting = Column(String)
    range = Column(String)
    comments = Column(String)
    settinginfo = relationship('SettingInfo',
                    back_populates='settings',
                    uselist=False,
                    lazy='joined')
    request = relationship('Request', back_populates='settings')


class SettingInfo(Base):
    __tablename__ = 'TSETTYPE1'
    relaytype = Column(String, primary_key=True)
    groupname = Column(String, primary_key=True)
    rownumber = Column(Float, primary_key=True)
    settingname = Column(String)
    range = Column(String)
    defaultvalue = Column(String)
    comments = Column(String)
    settings = relationship('Setting', back_populates='settinginfo')


class RTU_Equipment(Base):
    __tablename__ = 'TUSERDEF2'
    id = Column(Integer, primary_key=True)
    protecting = Column('s07', String)
    district_num = Column('s01', String)
    relaytype = Column('s09', String)
    memo = Column('m01', Text)
    sap_eq_num = Column('n01', Integer)
    device_num = Column('s03', String)
    function = Column('s06', String)
    style_num = Column('s02', String)
    serial_num = Column('s08', String)
    inst_book = Column('s04', String)
    ieee_num = Column('s05', String)
    owner = Column('s19', String)
    functional_location = Column('s20', String)
    locationid = Column(String, ForeignKey('TLOCATION.id'))
    location = relationship('Location', back_populates='rtu_equipment')
    requests = relationship('RTURequest', order_by='RTURequest.request_date',
                            back_populates='rtu_equipment')


Location.rtu_equipment = relationship('RTU_Equipment',
                                      order_by=RTU_Equipment.id,
                                      back_populates='location')


class RTURequest(Base):
    __tablename__ = 'TUSERDEF2REQUEST'
    id = Column(Integer, primary_key=True)
    requestor = Column('s01', String)
    setting_type = Column('s02', String)
    status = Column('s03', String)
    request_date = Column('d01', Date)
    service_date = Column('d02', Date)
    memo = Column('m01', Text)
    sign = Column(String)
    dlastsigned = Column(DateTime)
    dlastchanged = Column(DateTime)
    deviceid = Column(String, ForeignKey('TUSERDEF2.id'))
    rtu_equipment = relationship('RTU_Equipment', back_populates='requests')
    settings = relationship('RTUSetting',
                            order_by='(RTUSetting.groupname, '
                                     'RTUSetting.rownumber)',
                            back_populates='request')


class RTUSetting(Base):
    __tablename__ = 'TDEVSETTING1'
    __table_args__ = (
    ForeignKeyConstraint(['template', 'groupname', 'rownumber'],
                         ['TDEVSETTYPE1.template', 'TDEVSETTYPE1.groupname',
                          'TDEVSETTYPE1.rownumber']),)
    __mapper_args__ = {
        'polymorphic_on': 'userdef_table',
        'polymorphic_identity': 'TUSERDEF2'
    }
    requestid = Column(Integer, ForeignKey('TUSERDEF2REQUEST.id'),
                       primary_key=True)
    devicetype = Column('template', String, primary_key=True)
    userdef_table = Column('device', String, primary_key=True)
    groupname = Column(String, primary_key=True)
    rownumber = Column(Float, primary_key=True)
    setting = Column(String)
    range = Column(String)
    comments = Column(String)
    settinginfo = relationship('RTUSettingInfo',
                               back_populates='settings',
                               uselist=False,
                               lazy='joined')

    request = relationship('RTURequest', back_populates='settings')


class RTUSettingInfo(Base):
    __tablename__ = 'TDEVSETTYPE1'
    __mapper_args__ = {
        'polymorphic_on': 'userdef_table',
        'polymorphic_identity': 'TUSERDEF2'
    }
    devicetype = Column('template', String, primary_key=True)
    userdef_table = Column('device', String, primary_key=True)
    groupname = Column(String, primary_key=True)
    rownumber = Column(Float, primary_key=True)
    settingname = Column(String)
    range = Column(String)
    defaultvalue = Column(String)
    comments = Column(String)
    settings = relationship('RTUSetting',
                            back_populates='settinginfo')
    

def get_orm_sessionmaker(server=server, user=user, password=password, database=database):
    engine = sqlalchemy.create_engine('mssql+pymssql://%(user)s:%(password)s@%(server)s/%(database)s?charset=utf8' %
                                            {'user': user, 'password': password, 'server': server, 'database': database},
                                       echo=False)
    return sessionmaker(bind=engine)


def _abort_ro(*args,**kwargs):
    ''' Monkey patch function for session flush to make it read-only.
        Based on code at https://writeonly.wordpress.com/2009/07/16/simple-read-only-sqlalchemy-sessions/
    '''
    #print('Error writing. Database opened in read-only mode.')
    return 
    
def get_orm_session(server=server, user=user, password=password, database=database, readonly=True):
    Session = get_orm_sessionmaker(server=server, user=user, password=password, database=database)
    session = Session()
    if readonly:
        session.flush = _abort_ro
    return session


def get_all_subs():
    session = get_orm_session()
    return list(session.query(Location) \
                .filter(Location.sap_fl !=None) \
                .order_by(Location.id))

def orm_connect_test(argv=None):
    if argv is None:
        argv = sys.argv
        
    
    session = get_orm_session()
    for location in get_all_subs():
        print(location.id, location.name, location.sub_num, location.sap_fl,
              location.sap_fl2)
        if location.sap_fl != None and location.sap_fl2 != None and \
                location.sap_fl != location.sap_fl2:
            print('Functional Location Mismatch')
        
    for relay in session.query(Relay) \
            .join(Location) \
            .filter(Location.id=='ANTELOPE') \
            .filter(Relay.id==15535):
        print(relay.id, relay.protecting, relay.locationid, relay.location.name)
        for req in relay.requests:
            print(req.request_date, req.service_date, req.status, req.setting_type)
            for s in filter(lambda s: s.groupname=='HRDWR', req.settings):
                print(s.settinginfo.settingname, s.setting)
            print('Other method')
            for s in session.query(Setting)\
                    .join(Request)\
                    .join(Relay)\
                    .join(SettingInfo)\
                    .filter(Request.id==req.id, SettingInfo.settingname.like('OUT%')):
                print(s.settinginfo.settingname, s.setting)

    for rtu_device in session.query(RTU_Equipment) \
            .join(Location) \
            .filter(Location.id=='ANTELOPE') \
            .filter(RTU_Equipment.district_num=='077984'):
        print(rtu_device.id, rtu_device.locationid, rtu_device.location.name)
        for req in rtu_device.requests:
            print(req.request_date, req.service_date, req.status, req.setting_type)
            for s in req.settings:
                print(s.settinginfo.settingname, s.setting)
            print('Other method')
            for s in session.query(Setting)\
                    .join(Request)\
                    .join(Relay)\
                    .join(SettingInfo)\
                    .filter(Request.id==req.id, SettingInfo.settingname.like('OUT%')):
                print(s.settinginfo.settingname, s.setting)
    return
            
if __name__ =='__main__':
    sys.exit(orm_connect_test())

