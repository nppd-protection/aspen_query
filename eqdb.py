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
from sqlalchemy import Column, Integer, String, Float
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
    functional_location = Column(String)
    functional_location_description = Column(
        'functional_location_descriptin', String)
    manufacturer = Column(String)
    model_number = Column(String)
    construction_year = Column(Integer)
    manufacturer_serial_number = Column(String)
    district_num = Column('inventory_no', String)
    owner_identification = Column(String)

    # pseudo columns to be over-ridden in child classes where available
    @hybrid_property
    def NPPD_device(self):
        return ''


    @hybrid_property
    def protecting(self):
        return ''

    @hybrid_property
    def owner(self):
        return ''


class Relay(SAPEquipment):
    __tablename__ = "equipment_prot_relay"
    relay_type = Column('type', String)
    NPPD_device = Column('nppd_ieee_device_number', String)
    relay_function = Column(String)
    protecting = Column(String)
    nerc_critical = Column(String)
    firmware = Column(String)
    boot_firmware = Column(String)
    panel = Column(String)


class HMI_generic(SAPEquipment):
    __abstract__ = True
    power_supply_voltage = Column(String)
    hard_disk_1_type = Column(String)
    hard_disk_1_brand = Column(String)
    hard_disk_1_size = Column(String)
    hard_disk_2_type = Column(String)
    hard_disk_2_brand = Column(String)
    hard_disk_2_size = Column(String)
    system_memory_size = Column(String)
    baseline = Column(String)
    panel = Column(String)


class HMI(HMI_generic):
    __tablename__ = "equipment_hmi"


class Annunciator(HMI_generic):
    __tablename__ = "equipment_annunciator"


class Generic_CIP(SAPEquipment):
    __abstract__ = True
    sap_eq_num = Column('equipment', Integer, primary_key=True)
    panel = Column(String)
    firmware = Column(String)
    patch_date = Column(String)


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
    io_term_block_type = Column(String)
    io_term_block_quantity = Column(Float)
    panel = Column(String)
    firmware = Column(String)
    boot_firmware = Column(String)


class Comm_Interface(SAPEquipment):
    __tablename__ = "equipment_comm_interface"
    rev_number = Column(String)
    panel = Column(String)
    firmware_1 = Column(String)
    firmware_2 = Column(String)
    firmware_3 = Column(String)
    firmware_4 = Column(String)
    firmware_5 = Column(String)
    baseline = Column(String)


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
