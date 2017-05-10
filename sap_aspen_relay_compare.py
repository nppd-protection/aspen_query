from __future__ import print_function

import eqdb
import aspendb
import csv
import xlsxwriter  # Documentation at https://xlsxwriter.readthedocs.io/
import sys

wb = xlsxwriter.Workbook('output/SAP-Aspen Relay Compare.xlsx')
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
locations = list((l.id, l.sap_fl) for l in aspendb.get_all_subs())

# match_field can be set to 'sap_eq_num' or 'district_num'
# 'sap_eq_num' seems to provide better matching results.
match_field = 'sap_eq_num'


def skip_none(field):
    return field if field is not None else ''


def aspen_header(flag='Missing in SAP'):
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
                 eq.functional_location,
                 eq.device_num,
                 eq.relaytype,
                 eq.style_num,
                 eq.serial_num,
                 eq.protecting,
                 eq.owner))
    return data


def sap_header(flag='Missing in Aspen'):
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


def xl_write(sheet, data, fmt=None):
    sheet.write(sheet.cur_row, 0, data, fmt)
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
    return sheet.cur_row - 1, len(data)


def xl_write_eq(sheet, eq, fmt, flag=None):
    """
    Write out Excel rows. Format selects whether rows will be 
    Aspen or SAP equipment information. fmt should be either 'Aspen' or 'SAP'.
    """
    data = aspen_data(eq, flag) if fmt == 'Aspen' else sap_data(eq, flag)
    sheet.write_row(row=sheet.cur_row, col=0, data=data)
    if flag is not None and flag != 'X':
        # To hide row, use following code instead of pass
        # sheet.set_row(sheet.cur_row, options={'hidden': True})
        pass
    sheet.cur_row += 1


def xl_set_formatting(sheet):
    # Set column widths based on "typical" expected need. Cannot auto-fit
    # width outside of Excel itself.
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

    # Dict of lists. Key is match field value, set to None if not valid.
    # Each value is a list of devices with that key.
    all_devices = {}
    all_aspen = {}
    all_sap = {}
    aspen_devices = set()
    sap_devices = set()
    no_aspen_data = []
    no_sap_data = []

    # Aspen Database query list
    for device_type in (aspendb.Relay, aspendb.RTU_Equipment):
        for eq in aspen_sess.query(device_type) \
                .filter(device_type.locationid == aspen_location):
            data = getattr(eq, match_field)
            if data is None or data is '':
                data = None
            try:
                all_aspen[data].append(eq)
            except KeyError:
                all_aspen[data] = []
                all_aspen[data].append(eq)

    # SAP Database query list
    for device_type in eqdb.SAPEquipment.all_subclasses():
        for eq in sap_sess.query(device_type) \
                .filter(device_type.functional_location.like(sap_fl + '%')):
            data = getattr(eq, match_field)
            if data is None or data is '':
                data = None
            try:
                all_sap[data].append(eq)
            except KeyError:
                all_sap[data] = []
                all_sap[data].append(eq)

    # Print all rows from Aspen
    print('-' * 80)
    print('Devices found in Aspen')
    print('-' * 80)
    xl_write(sheet, 'Devices found in Aspen')
    table_start = xl_write_header(sheet, fmt='Aspen', flag='Missing in SAP')

    for k in sorted(all_aspen.keys()):
        for eq in all_aspen[k]:
            flag = 'X' if k is None or k not in all_sap else ''
            xl_write_eq(sheet, eq, fmt='Aspen', flag=flag)
    if sheet.cur_row - table_start[0] > 1:
        sheet.add_table(table_start[0], 0, sheet.cur_row - 1,
                        table_start[1] - 1,
                        {'columns': [{'header': s,
                                      'header_format': header_format}
                                     for s in aspen_header()],
                         'style': 'Table Style Medium 2',
                         'name': aspen_location.replace(' ', '_').replace(
                             '-', '_').replace('*', '_').replace('&','_') +
                                 '_Aspen'})

    # Print all rows from SAP
    print('-' * 80)
    print('Devices found in SAP')
    print('-' * 80)
    sheet.cur_row += 1
    xl_write(sheet, 'Devices found in SAP')
    table_start = xl_write_header(sheet, fmt='SAP', flag='Missing in Aspen')

    for k in sorted(all_sap.keys()):
        for eq in all_sap[k]:
            flag = 'X' if k is None or k not in all_aspen else ''
            print_sap_eq(eq, flag)
            xl_write_eq(sheet, eq, fmt='SAP', flag=flag)
    if sheet.cur_row - table_start[0] > 1:
        sheet.add_table(table_start[0], 0, sheet.cur_row - 1,
                        table_start[1] - 1,
                        {'columns': [{'header': s,
                                      'header_format': header_format}
                                     for s in sap_header()],
                         'style': 'Table Style Medium 2',
                         'name': aspen_location.replace(' ', '_').replace(
                             '-', '_').replace('*', '_').replace('&','_') + '_SAP'})

    print('')

wb.close()
