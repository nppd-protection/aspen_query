from __future__ import print_function, unicode_literals

import eqdb
import aspendb
import csv, codecs, cStringIO  # Use UnicodeWriter code from https://docs.python.org/2/library/csv.html
import xlsxwriter  # Documentation at https://xlsxwriter.readthedocs.io/
import io  # for using csv.writer to write to a string

wb = xlsxwriter.Workbook('output/SAP-Aspen Relay Compare.xlsx')

# Set up database connections
aspen_sess = aspendb.get_orm_session()
sap_sess = eqdb.get_orm_session()

# Locations to check
# aspen_location first, then sap_fl
locations = list((l.id, l.sap_fl) for l in aspendb.get_all_subs())

# match_field can be set to 'sap_eq_num' or 'district_num'
# 'sap_eq_num' seems to provide better matching results.
match_field = 'sap_eq_num'


# List of fields to display and fields for each database
# One and only one field should have a 'match' parameter.
# Fields with 'check' set to True will be compared using equality.
# Fields can also be checked with a callable function taking two parameters,
# the Aspen field value and the SAP field value, and will return True if they
#  match and False otherwise.
# Fields with different headers or fields for Aspen vs SAP will have
# parameters named 'aspen_header' / 'sap_header' or 'aspen_field' /
# 'sap_field' respectively.
fields = [{'aspen_header': 'Missing from SAP',
           'sap_header': 'Missing from Aspen',
           'field': 'flag',
           'check': False},
          {'header': 'SAP Equipment Number',
           'field': 'sap_eq_num',
           'match': True},
          {'header': 'District Number',
           'field': 'district_num',
           'check': True},
          {'header': 'Functional Location',
           'field': 'functional_location',
           'check': True},
          {'header': 'Device Number',
           'aspen_field': 'device_num',
           'sap_field': 'NPPD_device',
           'check': True},
          {'aspen_header': 'Relay Type',
           'sap_header': 'Manufacturer',
           'aspen_field': 'relaytype',
           'sap_field': 'manufacturer',
           'check': False},
          {'header': 'Model Number',
           'aspen_field': 'style_num',
           'sap_field': 'model_number',
           'check': True},
          {'header': 'Serial Number',
           'field': 'serial_num',
           'check': True},
          {'header': 'Protecting',
           'field': 'protecting',
           'check': False},
          {'header': 'Owner',
           'field': 'owner',
           'check': False}]


def skip_none(field):
    return field if field is not None else ''


def xl_write(sheet, data, fmt=None):
    """ Writes a single cell to a line of an Excel sheet and moves the cursor 
        down a row.
    """
    sheet.write(sheet.cur_row, 0, data, fmt)
    sheet.cur_row += 1


def xl_write_row(sheet, data, fmt=None):
    """
    Write out a row of data with the specified format in each cell.
    """
    sheet.write_row(row=sheet.cur_row, col=0, data=data,
                    cell_format=fmt)
    sheet.cur_row += 1


def xl_safe_tablename(s):
    """ Make a safe Excel table name from a possibly unsafe string. For now 
        this is "just enough" to pass based on what is in our Aspen location
        IDs.
    """
    # Could be implemented using a translate table, but this "just works"
    unsafe = ' -*&'
    for c in unsafe:
        s = s.replace(c, '_')
    return s


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([str(s).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Table:
    def __init__(self, title, fields, fmt):
        self.title = title
        self.fields = fields
        self.fmt = fmt
        self.headers = self.row_info('header')
        self.obj_list = []  # List of objects represented in table
        self.data = []  # List of rows, starts empty
        self.highlights = []  # List of rows, starts empty
        self.comments = []  # List of rows, starts empty
        # Header row of Excel table. Only saved when written to Excel.
        self.header_row = None

        # Some defaults saved in the class
        self.header_format = wb.add_format({'bold': True,
                                            'text_wrap': True,
                                            'align': 'center',
                                            'valign': 'bottom',
                                            'bottom': 1})

    def row_info(self, kind):
        data = []
        for f in self.fields:
            try:
                data.append(f[kind])
            except KeyError:
                data.append(f[self.fmt + '_' + kind])
        return data

    def mk_row(self, eq):
        fields = self.row_info('field')
        data = []
        for f in fields:
            data.append(getattr(eq, f))
        return data

    def mk_data_rows(self, eq_list):
        self.obj_list = []
        self.data = []
        for eq in eq_list:
            self.obj_list.append(eq)
            self.data.append(self.mk_row(eq))

    def __str__(self):
            with io.StringIO() as out:
                out.write('-' * 80 + '\n')
                out.write(self.title + '\n')
                out.write('-' * 80 + '\n')
                if len(self.data) > 0:
                    writer = UnicodeWriter(out)
                    writer.writerow(self.headers)
                    writer.writerows(self.data)
                else:
                    out.write('Empty table\n')
                rtn = out.getvalue()
            return rtn

    def xl_write(self, sheet, style='Table Style Medium 2'):
        """ Writes table out to Excel sheet starting at current cursor row 
            position. Adds Excel table formatting if possible.
        """
        xl_write(sheet, self.title)
        self.header_row = sheet.cur_row  # Keep track or header row number
        xl_write_row(sheet, self.headers, self.header_format)
        for row_data in self.data:
            # To hide row, use following code
            # sheet.set_row(sheet.cur_row, options={'hidden': True})
            xl_write_row(sheet, row_data)

        # Add Excel table if at least one data row is present
        if self.data:
            sheet.add_table(self.header_row, 0, sheet.cur_row - 1,
                            len(self.headers) - 1,
                            {'columns': [{'header': s,
                                          'header_format': self.header_format}
                                         for s in self.headers],
                             'style': style,
                             'name': xl_safe_tablename(aspen_location +
                                                       '_' + self.fmt)})


def xl_set_formatting(sheet):
    # Set column widths based on "typical" expected need. Cannot auto-fit
    # width outside of Excel itself.
    for n, w in enumerate((10, 13, 14.5, 33, 14, 28, 24, 15, 35, 10)):
        sheet.set_column(n, n, w)

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

    # Set flags on Aspen equipment list for what is missing in SAP and make
    # master list.
    table_eq_list = []
    for k in sorted(all_aspen.keys()):
        for eq in all_aspen[k]:
            eq.flag = 'X' if k is None or k not in all_sap else ''
            table_eq_list.append(eq)

    # Print all rows from Aspen
    aspen_table = Table('Devices found in Aspen', fields, 'aspen')
    aspen_table.mk_data_rows(table_eq_list)
    print(aspen_table)
    aspen_table.xl_write(sheet)

    # Leave a blank row in the worksheet
    sheet.cur_row += 1

    # Set flags on SAP equipment list for what is missing in Aspen
    table_eq_list = []
    for k in sorted(all_sap.keys()):
        for eq in all_sap[k]:
            eq.flag = 'X' if k is None or k not in all_aspen else ''
            table_eq_list.append(eq)

    # Print all rows from SAP
    sap_table = Table('Devices found in SAP', fields, 'sap')
    sap_table.mk_data_rows(table_eq_list)
    print(sap_table)
    sap_table.xl_write(sheet)

    print('')

wb.close()
