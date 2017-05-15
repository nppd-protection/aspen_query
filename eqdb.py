# Connection information for SAP Equipment database
# Need cx_Oracle module. Download from
# https://pypi.python.org/pypi/cx_Oracle/
# The version (32-bit or 64-bit, 11g or 12c) must match the installed Oracle
# client dll.
import sys

import cx_Oracle

# Connection information for SAP equipment database
from eqdb_config import user, password, tns, schema


def connect():
    con = cx_Oracle.Connection(user, password, tns)
    if schema is not None:
        con.current_schema = schema
    return con


def simple_connect_test(argv=None):
    if argv is None:
        argv = sys.argv

    with connect() as con:
        cur = con.cursor()
        query = """SELECT * FROM equipment_prot_relay eq"""

        cur.execute(query)

        rtn = list(cur.fetchmany(10))
        print('Number returned', len(rtn))
        print(rtn)
    return

# Trying out an SQLAlchemy ORM connection
# See tutorial at
# http://docs.sqlalchemy.org/en/rel_1_0/orm/
# tutorial.html#building-a-relationship
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Unicode, Float
from sqlalchemy.ext.hybrid import hybrid_property

Base = declarative_base()


class SAPEquipment(Base):
    __abstract__ = True

    @classmethod
    def all_subclasses(cls):
        rtn = cls.__subclasses__() + [g for s in cls.__subclasses__()
                                       for g in s.all_subclasses()]
        return filter(lambda s: hasattr(s, '__tablename__'), rtn)

    """ Common columns for all equipment tables. """
    sap_eq_num = Column('equipment_number', Integer, primary_key=True)
    functional_location = Column(Unicode)
    functional_location_description = Column(
        'functional_location_descriptin', Unicode)
    manufacturer = Column(Unicode)
    model_number = Column(Unicode)
    construction_year = Column(Integer)
    serial_num = Column('manufacturer_serial_number', Unicode)
    district_num = Column('inventory_no', Unicode)
    owner_identification = Column(Unicode)

    # pseudo columns to be over-ridden in child classes where available
    #@hybrid_property
    #def NPPD_device(self):
    #    return ''


    #@hybrid_property
    #def protecting(self):
    #    return ''

    #@hybrid_property
    #def owner(self):
    #    return ''


class Relay(SAPEquipment):
    __tablename__ = "equipment_prot_relay"
    relay_type = Column('type', Unicode)
    NPPD_device = Column('nppd_ieee_device_number', Unicode)
    relay_function = Column(Unicode)
    protecting = Column(Unicode)
    nerc_critical = Column(Unicode)
    firmware = Column(Unicode)
    boot_firmware = Column(Unicode)
    panel = Column(Unicode)


class HMI_generic(SAPEquipment):
    __abstract__ = True
    power_supply_voltage = Column(Unicode)
    hard_disk_1_type = Column(Unicode)
    hard_disk_1_brand = Column(Unicode)
    hard_disk_1_size = Column(Unicode)
    hard_disk_2_type = Column(Unicode)
    hard_disk_2_brand = Column(Unicode)
    hard_disk_2_size = Column(Unicode)
    system_memory_size = Column(Unicode)
    baseline = Column(Unicode)
    panel = Column(Unicode)


class HMI(HMI_generic):
    __tablename__ = "equipment_hmi"


class Annunciator(HMI_generic):
    __tablename__ = "equipment_annunciator"


class Generic_CIP(SAPEquipment):
    __abstract__ = True
    sap_eq_num = Column('equipment', Integer, primary_key=True)
    panel = Column(Unicode)
    firmware = Column(Unicode)
    patch_date = Column(Unicode)


class GPS_Clock(Generic_CIP):
    __tablename__ = "equipment_gps_clock"


class Fault_Recorder(Generic_CIP):
    __tablename__ = "equipment_fault_recorder"


class Ethernet_Switch(Generic_CIP):
    __tablename__ = "equipment_ethernet_switch"


class Ethernet_Fiber_Converter(Generic_CIP):
    __tablename__ = "equipment_ethernet_fbr_conv"
    __mapper_args__ = {
        'polymorphic_identity': 'equipment_fbr_conv',
        'concrete': True}


class IO_Terminal_Blocks(SAPEquipment):
    __tablename__ = "equipment_io_term_blks"
    io_term_block_type = Column(Unicode)
    io_term_block_quantity = Column(Float)
    panel = Column(Unicode)
    firmware = Column(Unicode)
    boot_firmware = Column(Unicode)


class Comm_Interface(SAPEquipment):
    __tablename__ = "equipment_comm_interface"
    rev_number = Column(Unicode)
    panel = Column(Unicode)
    firmware_1 = Column(Unicode)
    firmware_2 = Column(Unicode)
    firmware_3 = Column(Unicode)
    firmware_4 = Column(Unicode)
    firmware_5 = Column(Unicode)
    baseline = Column(Unicode)


def get_orm_sessionmaker():
    engine = sqlalchemy.create_engine(
        'oracle+cx_oracle://',
        creator=connect,
        echo=False)
    return sessionmaker(bind=engine)


def _abort_ro(*args, **kwargs):
    """ Monkey patch function for session flush to make it read-only.
        Based on code at https://writeonly.wordpress.com/2009/07/16/
        simple-read-only-sqlalchemy-sessions/
    """
    # print('Error writing. Database opened in read-only mode.')
    return


def get_orm_session(readonly=True):
    Session = get_orm_sessionmaker()
    session = Session()
    if readonly:
        session.flush = _abort_ro
    return session


def orm_connect_test(argv=None):
    if argv is None:
        argv = sys.argv

    session = get_orm_session()

    for relay in session.query(Relay).order_by(Relay.sap_eq_num):
        print(relay.functional_location)

    return


if __name__ == '__main__':
    simple_connect_test()
    orm_connect_test()
    print(SAPEquipment.__subclasses__())
    print(SAPEquipment.all_subclasses())
    sys.exit()
