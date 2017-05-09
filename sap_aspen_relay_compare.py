from __future__ import print_function

import eqdb
import aspendb
import csv
import xlsxwriter #https://xlsxwriter.readthedocs.io/
import sys

wb = xlsxwriter.Workbook('SAP-Aspen Relay Compare.xlsx')
header_format = wb.add_format({'bold': True,
                                        'text_wrap': True,
                                        'align': 'center',
                                        'valign': 'bottom',
                                        'bottom': 1})
# Set up database connections
aspen_sess = aspendb.get_orm_session()
sap_sess = eqdb.get_orm_session()

# Locations to check
# aspen_location first, then sap_fl
locations = [
    ('AINSWT',      'TS-S-N-910021'),
    ('AINSWT WIND', 'TS-S-N-910240'),
    ('ALBION',      'TS-S-N-912011'),
    ('ALDA',        'TS-S-C-910033'),
    ('ANTELOPE',    'TS-S-N-930272'),
    ('AURORA',      'TS-S-E-910085'),
    ('AXTELL',      'TS-S-C-930198'),
    ('BANCROFT',    'TS-S-N-910093'),
    ('BAT CRK',     'TS-S-N-910172')
]
locations = list((l.id, l.sap_fl) for l in aspendb.get_all_subs())

check_field = 'sap_eq_num'
#check_field = 'district_num'
print_full_list = True

def skip_none(field):
    return field if field is not None else ''


def aspen_header(flag=None):
    if flag is not None:
        data = [flag]
    else:
        data = []
    data.extend(('SAP Equipment Number',
                 'District Number',
                 'Functional Location',
                 'Device Number',
                 'Relay Type',
                 'Model Number',
                 'Serial Number',
                 'Protecting',
                 'Owner'))
    return data


def aspen_data(eq, flag=None):
    if flag is not None:
        data = [flag]
    else:
        data = []
    data.extend((eq.sap_eq_num,
                 eq.district_num,
                 eq.sap_fl,
                 eq.device_num,
                 eq.relaytype,
                 eq.style_num,
                 eq.serial_num,
                 eq.protecting,
                 eq.owner))
    return data


def sap_header(flag=None):
    if flag is not None:
        data = [flag]
    else:
        data = []
    data.extend(('SAP Equipment Number',
                 'District Number',
                 'Functional Location',
                 'Device Number',
                 'Manufacturer',
                 'Model Number',
                 'Serial Number',
                 'Protecting',
                 'Owner'))
    return data


def sap_data(eq, flag=None):
    if flag is not None:
        data = [flag]
    else:
        data = []
    data.extend((eq.sap_eq_num,
                 eq.district_num,
                 eq.functional_location,
                 eq.NPPD_device,
                 eq.manufacturer,
                 eq.model_number,
                 eq.manufacturer_serial_number,
                 eq.protecting,
                 eq.owner))
    return data


def print_aspen_eq(eq, flag=None):
    data = aspen_data(eq, flag)
    csv.writer(sys.stdout).writerow(data)


def xl_write(sheet, data, format=None):
    sheet.write(sheet.cur_row, 0, data, format)
    sheet.cur_row += 1


def xl_write_header(sheet, fmt, flag=None):
    """
    Write out header above Excel rows. Format selects whether rows will be 
    Aspen or SAP equipment information. fmt should be either 'Aspen' or 'SAP'.
    Returns starting row and number of columns, which can be used for 
    setting an autofilter.
    """
    data = aspen_header(flag) if fmt == 'Aspen' else sap_header(flag)
    sheet.write_row(row=sheet.cur_row, col=0, data=data,
                    cell_format=header_format)
    sheet.cur_row += 1
    return (sheet.cur_row - 1, len(data))


def xl_write_eq(sheet, eq, fmt, flag=None):
    """
    Write out Excel rows. Format selects whether rows will be 
    Aspen or SAP equipment information. fmt should be either 'Aspen' or 'SAP'.
    """
    data = aspen_data(eq, flag) if fmt == 'Aspen' else sap_data(eq, flag)
    sheet.write_row(row=sheet.cur_row, col=0, data=data)
    if flag is not None and flag != 'X':
        pass #sheet.set_row(sheet.cur_row, options={'hidden': True})
    sheet.cur_row += 1

def xl_set_formatting(sheet):
    #sheet.freeze_panes(2, 0)
    for n, w in enumerate((10, 13, 14.5, 33, 14, 28, 24, 15, 35, 10)):
        sheet.set_column(n, n, w)


def print_sap_eq(eq, flag=None):
    data = sap_data(eq, flag)
    csv.writer(sys.stdout).writerow(data)

for aspen_location, sap_fl in locations:
    print('='*80)
    print('Checking location %s / %s' % (aspen_location, sap_fl))
    sheet = wb.add_worksheet(aspen_location.replace('*', '_'))
    sheet.cur_row = 0
    xl_set_formatting(sheet)

    all_aspen = []
    all_sap = []
    aspen_devices = set()
    sap_devices = set()
    no_aspen_data = []
    no_sap_data = []

    # Aspen Database query list
    for device_type in (aspendb.Relay, aspendb.RTU_Equipment):
        for eq in aspen_sess.query(device_type) \
                .filter(device_type.locationid == aspen_location):
            data = getattr(eq, check_field)
            all_aspen.append(eq)
            if data is None or data is '':
                no_aspen_data.append(eq)
            else:
                aspen_devices.add(data)

    # SAP Database query list
    for device_type in eqdb.SAPEquipment.all_subclasses():
        for eq in sap_sess.query(device_type) \
                .filter(device_type.functional_location.like(sap_fl + '%')):
            data = getattr(eq, check_field)
            all_sap.append(eq)
            if data is None or data is '':
                no_sap_data.append(eq)
            else:
                sap_devices.add(data)


    if print_full_list:
        # Print all rows from Aspen
        print('-' * 80)
        print('Devices found in Aspen')
        print('-' * 80)
        xl_write(sheet, 'Devices found in Aspen')
        table_start = xl_write_header(sheet, fmt='Aspen', flag='Missing in SAP')

        all_aspen.sort(key=lambda eq: getattr(eq, check_field))
        for eq in all_aspen:
            flag = 'X' if getattr(eq, check_field) not in sap_devices else ''
            print_aspen_eq(eq, flag)
            xl_write_eq(sheet, eq, fmt='Aspen', flag=flag)
        if sheet.cur_row - table_start[0] > 1:
            sheet.add_table(table_start[0], 0, sheet.cur_row - 1,
                            table_start[1] - 1,
                            {'columns': [{'header': s,
                                          'header_format': header_format}
                                         for s in aspen_header(flag='Missing in SAP')],
                             'style': 'Table Style Medium 2',
                             'name': aspen_location.replace(' ', '_').replace(
                                 '-', '_').replace('*', '_') + '_Aspen'})
        #sheet.filter_column(0, 'Missing == "X"')

        # Print all rows from SAP
        print('-' * 80)
        print('Devices found in SAP')
        print('-' * 80)
        sheet.cur_row += 1
        xl_write(sheet, 'Devices found in SAP')
        table_start = xl_write_header(sheet, fmt='SAP', flag='Missing in Aspen')

        all_sap.sort(key=lambda eq: getattr(eq, check_field))
        for eq in all_sap:
            flag = 'X' if getattr(eq, check_field) not in aspen_devices else ''
            print_sap_eq(eq, flag)
            xl_write_eq(sheet, eq, fmt='SAP', flag=flag)
        if sheet.cur_row - table_start[0] > 1:
            sheet.add_table(table_start[0], 0, sheet.cur_row - 1,
                            table_start[1] - 1,
                            {'columns': [{'header': s,
                                          'header_format': header_format}
                                         for s in aspen_header(flag='Missing in Aspen')],
                             'style': 'Table Style Medium 2',
                             'name': aspen_location.replace(' ', '_').replace(
                                 '-', '_').replace('*', '_') + '_SAP'})
        #sheet.filter_column(0, 'Missing == "X"')

    else:
        # List equipment in Aspen without compare field
        if len(no_aspen_data) > 0:
            print('-' * 80)
            print('Devices in Aspen without the compare field (%s):' % check_field)
            print('-' * 80)
            for eq in no_aspen_data:
                print_aspen_eq(eq)

        # List equipment in SAP without compare field
        if len(no_sap_data) > 0:
            print('-' * 80)
            print('Devices in SAP without the compare field (%s):' % check_field)
            print('-' * 80)
            for eq in no_sap_data:
                print_sap_eq(eq)



        sap_only = sap_devices - aspen_devices

        if len(sap_only) > 0:
            print('-' * 80)
            print('Relays/RTU Equipment in SAP not in Aspen:')
            print('-' * 80)
            for device_type in eqdb.SAPEquipment.all_subclasses():
                for eq in sap_sess.query(device_type) \
                        .filter(device_type.functional_location.like(sap_fl + '%')) \
                        .filter(getattr(device_type, check_field).in_(sap_only)):
                    print_sap_eq(eq)

        aspen_only = aspen_devices - sap_devices

        if len(aspen_only) > 0:
            print('-' * 80)
            print('Relays/RTU Equipment in Aspen not in SAP:')
            print('-' * 80)
            for device_type in (aspendb.Relay, aspendb.RTU_Equipment):
                for eq in aspen_sess.query(device_type) \
                        .filter(device8_type.locationid == aspen_location) \
                        .filter(getattr(device_type, check_field).in_(aspen_only)):
                    print_aspen_eq(eq)

    print('')

wb.close()