#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           gstat.py
#   DESCRIPTION:    Python driver for Firebird - Firebird gstat output processing
#   CREATED:        8.11.2017
#
#  Software distributed under the License is distributed AS IS,
#  WITHOUT WARRANTY OF ANY KIND, either express or implied.
#  See the License for the specific language governing rights
#  and limitations under the License.
#
#  The Original Code was created by Pavel Cisar
#
#  Copyright (c) Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): ______________________________________.
#
# See LICENSE.TXT for details.

from fdb import ParseError
from fdb.utils import ObjectList
from itertools import imap
from string import strip
import datetime
import weakref
from collections import namedtuple

GSTAT_25 = 2
GSTAT_30 = 3

ATTRIBUTES = ['force write', 'no reserve', 'shared cache disabled',
              'active shadow', 'multi-user maintenance',
              'single-user maintenance', 'full shutdown', 'read only',
              'backup lock', 'backup merge', 'wrong backup state']

ATTR_FORCE_WRITE = 0  #'force write'
ATTR_NO_RESERVE = 1  #'no reserve'
ATTR_NO_SHARED_CACHE = 2  #'shared cache disabled'
ATTR_ACTIVE_SHADOW = 3  #'active shadow'
ATTR_SHUTDOWN_MULTI = 4  #'multi-user maintenance'
ATTR_SHUTDOWN_SINGLE = 5  #'single-user maintenance'
ATTR_SHUTDOWN_FULL = 6  #'full shutdown'
ATTR_READ_ONLY = 7  #'read only'
ATTR_BACKUP_LOCK = 8  #'backup lock'
ATTR_BACKUP_MERGE = 9  #'backup merge'
ATTR_BACKUP_WRONG = 10  #'wrong backup state %d'


FillDistribution = namedtuple('FillDistribution', 'd20,d40,d50,d80,d100')
Encryption = namedtuple('Encryption', 'pages,encrypted,unencrypted')

def empty_str(str_):
    return true if str_ is None else str_.strip() == ''


class StatTable(object):
    "Statisctics for single database table."
    def __init__(self):
        self.name = None
        self.table_id = None
        self.primary_pointer_page = None
        self.index_root_page = None
        self.avg_record_length = None
        self.total_records = None
        self.avg_version_length = None
        self.total_versions = None
        self.max_versions = None
        self.data_pages = None
        self.data_page_slots = None
        self.avg_fill = None
        self.distribution = None
        self.indices = []

class StatTable3(StatTable):
    "Statisctics for single database table (Firebird 3 and above)."
    def __init__(self):
        super(StatTable3,self).__init__()
        #
        self.pointer_pages = None
        self.total_formats = None
        self.used_formats = None
        self.avg_fragment_length = None
        self.total_fragments = None
        self.max_fragments = None
        self.avg_unpacked_length = None
        self.compression_ratio = None
        self.primary_pages = None
        self.secondary_pages = None
        self.swept_pages = None
        self.empty_pages = None
        self.full_pages = None
        self.blobs = None
        self.blobs_total_length = None
        self.blob_pages = None
        self.level_0 = None
        self.level_1 = None
        self.level_2 = None

class StatIndex(object) :
    "Statisctics for single database index."
    def __init__(self, table):
        self.table = weakref.proxy(table)
        table.indices.append(weakref.proxy(self))
        self.name = None
        self.index_id = None
        #
        self.depth = None
        self.leaf_buckets = None
        self.nodes = None
        self.avg_data_length = None
        self.total_dup = None
        self.max_dup = None
        self.distribution = None

class StatIndex3(StatIndex) :
    "Statisctics for single database index (Firebird 3 and above)."
    def __init__(self, table):
        super(StatIndex3,self).__init__(table)
        #
        self.root_page = None
        self.avg_node_length = None
        self.avg_key_length = None
        self.compression_ratio = None
        self.avg_prefix_length = None
        self.clustering_factor = None
        self.ratio = None

class StatDatabase(object):
    """Firebird database statistics (produced by gstat).
"""
    def __init__(self):
        self.gstat_version = None
        self.system_change_number = None # ver3
        self.executed = None
        self.completed = None # ver3
        self.filename = None
        self.flags = 0
        self.checksum = 12345 # ver2
        self.generation = 0
        self.page_size = 0
        self.ods_version = None
        self.oit = 0
        self.oat = 0
        self.ost = 0
        self.next_transaction = 0
        self.bumped_transaction = None # ver2
        self.sequence_number = 0
        self.next_attachment_id = 0
        self.implementation_id = 0 # ver2
        self.implementation = None # ver3
        self.shadow_count = 0
        self.page_buffers = 0
        self.next_header_page = 0
        self.database_dialect = 0
        self.creation_date = None
        self.attributes = []
        # Variable data
        self.sweep_interval = None  # Sweep interval:
        self.continuation_file = None  # Continuation file:
        self.last_logical_page = None  # Last logical page:
        self.backup_guid = None  # Database backup GUID:
        self.root_filename = None  # Root file name:
        self.replay_logging_file = None  # Replay logging file:
        self.backup_diff_file = None  # Backup difference file:
        # Unrecognized option %d, length %d
        # Encoded option %d, length %d
        # Encryption
        self.encrypted_data_pages = None
        self.encrypted_index_pages = None
        self.encrypted_blob_pages = None
        #
        self.continuation_files = []
        #
        self.tables = None
        self.indices = None
    def has_table_stats(self):
        "Return True if instance contains information about tables."
        return self.tables[0].primary_pointer_page is not None if len(self.tables) > 0 else False
    def has_row_stats(self):
        "Return True if instance contains information about table rows."
        return self.has_table_stats() and self.tables[0].avg_version_length is not None
    def has_index_stats(self):
        "Return True if instance contains information about indices."
        return self.indices[0].depth is not None if len(self.indices) > 0 else False
    def has_encryption_stats(self):
        "Return True if instance contains information about database encryption."
        return self.encrypted_data_pages is not None
    def has_system(self):
        "Return True if instance contains information about system tables."
        return self.tables.contains('RDB$DATABASE', 'item.name')

class StatParser(object):
    def parse(self, lines):
        """Parse output from Firebird gstat utility.

            :param lines: Iterable of lines produced by Firebird gstat utility.
            :returns: :class:`~fdb.gstat.StatDatabase` instance with parsed results.

            :raises fdb.ParseError: When any problem is found in input stream.
    """
        def parse_hdr(line):
            for key, valtype, name in ITEMS_HDR:
                if line.startswith(key):
                    # Check for GSTAT_VERSION
                    if db.gstat_version is None:
                        if key == 'Checksum':
                            db.gstat_version = GSTAT_25
                            db.tables = ObjectList(_cls=StatTable, key_expr= 'item.name')
                            db.indices = ObjectList(_cls=StatIndex, key_expr= 'item.name')
                        elif key == 'System Change Number':
                            db.gstat_version = GSTAT_30
                            db.tables = ObjectList(_cls=StatTable3, key_expr= 'item.name')
                            db.indices = ObjectList(_cls=StatIndex3, key_expr= 'item.name')
                    #
                    value = line[len(key):].strip()
                    if valtype == 'i':  # integer
                        value = int(value)
                    elif valtype == 's':  # string
                        pass
                    elif valtype == 'd':  # date time
                        value = datetime.datetime.strptime(value, '%b %d, %Y %H:%M:%S')
                    elif valtype == 'l':  # list
                        if value == '':
                            value = []
                        else:
                            value = [x.strip() for x in value.split(',')]
                            value = tuple([ATTRIBUTES.index(x) for x in value])
                    else:
                        raise ParseError("Unknown value type %s" % valtype)
                    if name is None:
                        name = key.lower().replace(' ','_')
                    setattr(db, name, value)
                    return
            raise ParseError('Unknown information (line %i)' % line_no)
        def parse_var(line):
            if line == '*END*':
                return
            for key, valtype, name in ITEMS_VAR:
                if line.startswith(key):
                    value = line[len(key):].strip()
                    if valtype == 'i':  # integer
                        value = int(value)
                    elif valtype == 's':  # string
                        pass
                    elif valtype == 'd':  # date time
                        value = datetime.datetime.strptime(value, '%b %d, %Y %H:%M:%S')
                    else:
                        raise ParseError("Unknown value type %s" % valtype)
                    if name is None:
                        name = key.lower().strip(':').replace(' ','_')
                    setattr(db, name, value)
                    return
            raise ParseError('Unknown information (line %i)' % line_no)
        def parse_fseq(line):
            if not line.startswith('File '):
                raise ParseError("Bad file specification (line %i)" % line_no)
            if 'is the only file' in line:
                return
            if ' is the ' in line:
                db.continuation_files.append(line[5:line.index(' is the ')])
            elif ' continues as' in line:
                db.continuation_files.append(line[5:line.index(' continues as')])
            else:
                raise ParseError("Bad file specification (line %i)" % line_no)
        def parse_table(line, table):
            if table.name is None:
                # We should parse header
                tname, tid = line.split(' (')
                table.name = tname.strip(' "')
                table.table_id = int(tid.strip('()'))
            else:
                if ',' in line:  # Data values
                    for item in line.split(','):
                        item = item.strip()
                        found = False
                        items = ITEMS_TBL2 if db.gstat_version == GSTAT_25 else ITEMS_TBL3
                        for key, valtype, name in items:
                            if item.startswith(key):
                                value = item[len(key):].strip()
                                if valtype == 'i':  # integer
                                    value = int(value)
                                elif valtype == 'f':  # float
                                    value = float(value)
                                elif valtype == 'p':  # %
                                    value = int(value.strip('%'))
                                else:
                                    raise ParseError("Unknown value type %s" % valtype)
                                if name is None:
                                    name = key.lower().strip(':').replace(' ','_')
                                setattr(table, name, value)
                                found = True
                                break
                        if not found:
                            raise ParseError('Unknown information (line %i)' % line_no)
                else:  # Fill distribution
                    if '=' in line:
                        fill_range, fill_value = line.split('=')
                        i = ITEMS_FILL.index(fill_range.strip())
                        if table.distribution is None:
                            table.distribution = [0, 0, 0, 0, 0]
                        table.distribution[i] = int(fill_value.strip())
                    elif line.startswith('Fill distribution:'):
                        pass
                    else:
                        raise ParseError('Unknown information (line %i)' % line_no)
        def parse_index(line, index):
            if index.name is None:
                # We should parse header
                iname, iid = line[6:].split(' (')
                index.name = iname.strip(' "')
                index.index_id = int(iid.strip('()'))
            else:
                if ',' in line:  # Data values
                    for item in line.split(','):
                        item = item.strip()
                        found = False
                        items = ITEMS_IDX2 if db.gstat_version == GSTAT_25 else ITEMS_IDX3
                        for key, valtype, name in items:
                            if item.startswith(key):
                                value = item[len(key):].strip()
                                if valtype == 'i':  # integer
                                    value = int(value)
                                elif valtype == 'f':  # float
                                    value = float(value)
                                elif valtype == 'p':  # %
                                    value = int(value.strip('%'))
                                else:
                                    raise ParseError("Unknown value type %s" % valtype)
                                if name is None:
                                    name = key.lower().strip(':').replace(' ','_')
                                setattr(index, name, value)
                                found = True
                                break
                        if not found:
                            raise ParseError('Unknown information (line %i)' % line_no)
                else:  # Fill distribution
                    if '=' in line:
                        fill_range, fill_value = line.split('=')
                        i = ITEMS_FILL.index(fill_range.strip())
                        if index.distribution is None:
                            index.distribution = [0, 0, 0, 0, 0]
                        index.distribution[i] = int(fill_value.strip())
                    elif line.startswith('Fill distribution:'):
                        pass
                    else:
                        raise ParseError('Unknown information (line %i)' % line_no)
        def parse_encryption(line):
            try:
                total, encrypted, unencrypted = line.split(',')
                pad, total = total.rsplit(' ',1)
                total = int(total)
                pad, encrypted = encrypted.rsplit(' ',1)
                encrypted = int(encrypted)
                pad, unencrypted = unencrypted.rsplit(' ',1)
                unencrypted = int(unencrypted)
                data = Encryption(total, encrypted, unencrypted)
            except:
                raise ParseError('Malformed encryption information (line %i)' % line_no)
            if 'Data pages:' in line:
                db.encrypted_data_pages = data
            elif 'Index pages:' in line:
                db.encrypted_index_pages= data
            elif 'Blob pages: in line':
                db.encrypted_blob_pages = data
            else:
                raise ParseError('Unknown encryption information (line %i)' % line_no)

        #
        ITEMS_HDR = [('Flags', 'i', None),
                     ('Checksum', 'i', None),
                     ('Generation', 'i', None),
                     ('System Change Number', 'i', 'system_change_number'),
                     ('Page size', 'i', None),
                     ('ODS version', 's', None),
                     ('Oldest transaction', 'i', 'oit'),
                     ('Oldest active', 'i', 'oat'),
                     ('Oldest snapshot', 'i', 'ost'),
                     ('Next transaction', 'i', None),
                     ('Bumped transaction', 'i', None),
                     ('Sequence number', 'i', None),
                     ('Next attachment ID', 'i', None),
                     ('Implementation ID', 'i', None),
                     ('Implementation', 's', None),
                     ('Shadow count', 'i', None),
                     ('Page buffers', 'i', None),
                     ('Next header page', 'i', None),
                     ('Database dialect', 'i', None),
                     ('Creation date', 'd', None),
                     ('Attributes', 'l', None)]

        ITEMS_VAR = [('Sweep interval:', 'i', None),
                     ('Continuation file:', 's', None),
                     ('Last logical page:', 'i', None),
                     ('Database backup GUID:', 's', 'backup_guid'),
                     ('Root file name:', 's', 'root_filename'),
                     ('Replay logging file:', 's', None),
                     ('Backup difference file:', 's', 'backup_diff_file')]

        ITEMS_TBL2 = [('Primary pointer page:', 'i', None),
                      ('Index root page:', 'i', None),
                      ('Pointer pages:', 'i', 'pointer_pages'),
                      ('Average record length:', 'f', 'avg_record_length'),
                      ('total records:', 'i', None),
                      ('Average version length:', 'f', 'avg_version_length'),
                      ('total versions:', 'i', None),
                      ('max versions:', 'i', None),
                      ('Data pages:', 'i', None),
                      ('data page slots:', 'i', None),
                      ('average fill:', 'p', 'avg_fill'),
                      ('Primary pages:', 'i', None),
                      ('secondary pages:', 'i', None),
                      ('swept pages:', 'i', None),
                      ('Empty pages:', 'i', None),
                      ('full pages:', 'i', None)
                      ]

        ITEMS_TBL3 = [('Primary pointer page:', 'i', None),
                      ('Index root page:', 'i', None),
                      ('Total formats:', 'i', None),
                      ('used formats:', 'i', None),
                      ('Average record length:', 'f', 'avg_record_length'),
                      ('total records:', 'i', None),
                      ('Average version length:', 'f', 'avg_version_length'),
                      ('total versions:', 'i', None),
                      ('max versions:', 'i', None),
                      ('Average fragment length:', 'f', 'avg_fragment_length'),
                      ('total fragments:', 'i', None),
                      ('max fragments:', 'i', None),
                      ('Average unpacked length:', 'f', 'avg_unpacked_length'),
                      ('compression ratio:', 'f', None),
                      ('Pointer pages:', 'i', 'pointer_pages'),
                      ('data page slots:', 'i', None),
                      ('Data pages:', 'i', None),
                      ('average fill:', 'p', 'avg_fill'),
                      ('Primary pages:', 'i', None),
                      ('secondary pages:', 'i', None),
                      ('swept pages:', 'i', None),
                      ('Empty pages:', 'i', None),
                      ('full pages:', 'i', None),
                      ('Blobs:', 'i', None),
                      ('total length:', 'i', 'blobs_total_length'),
                      ('blob pages:', 'i', None),
                      ('Level 0:', 'i', None),
                      ('Level 1:', 'i', None),
                      ('Level 2:', 'i', None)
                      ]

        ITEMS_IDX2 = [('Depth:', 'i', None),
                      ('leaf buckets:', 'i', None),
                      ('nodes:', 'i', None),
                      ('Average data length:', 'f', 'avg_data_length'),
                      ('total dup:', 'i', None),
                      ('max dup:', 'i', None)]
        ITEMS_IDX3 = [('Root page:', 'i', None),
                      ('depth:', 'i', None),
                      ('leaf buckets:', 'i', None),
                      ('nodes:', 'i', None),
                      ('Average node length:', 'f', 'avg_node_length'),
                      ('total dup:', 'i', None),
                      ('max dup:', 'i', None) ,
                      ('Average key length:', 'f', 'avg_key_length'),
                      ('compression ratio:', 'f', None),
                      ('Average prefix length:', 'f', 'avg_prefix_length'),
                      ('average data length:', 'f', 'avg_data_length'),
                      ('Clustering factor:', 'f', None),
                      ('ratio:', 'f', None)
                      ]

        ITEMS_FILL = ['0 - 19%', '20 - 39%', '40 - 59%', '60 - 79%', '80 - 99%']
        #
        db = StatDatabase()
        line_no = 0
        table = None
        index = None
        new_block = True
        in_table = False
        #
        line_no = 0
        step = 0  # Look for sections and skip empty lines
        # Skip empty lines at start
        for line in imap(strip, lines):
            line_no += 1
            if line.startswith('Gstat completion time'):
                db.completed = datetime.datetime.strptime(line[22:], '%a %b %d %H:%M:%S %Y')
            elif step == 0:  # Looking for section or db name
                if line.startswith('Gstat execution time'):
                    db.executed = datetime.datetime.strptime(line[21:], '%a %b %d %H:%M:%S %Y')
                elif line.startswith('Database header page information:'):
                    step = 1
                elif line.startswith('Variable header data:'):
                    step = 2
                elif line.startswith('Database file sequence:'):
                    step = 3
                elif 'encrypted' in line and 'non-crypted' in line:
                    parse_encryption(line)
                elif line.startswith('Analyzing database pages ...'):
                    step = 4
                elif empty_str(line):
                    pass
                elif line.startswith('Database "'):
                    x, s = line.split(' ')
                    db.filename = s.strip('"')
                    step = 0
                else:
                    raise ParseError("Unrecognized data (line %i)" % line_no)
            elif step == 1:  # Header
                if empty_str(line):  # section ends with empty line
                    step = 0
                else:
                    parse_hdr(line)
            elif step == 2:  # Variable data
                if empty_str(line):  # section ends with empty line
                    step = 0
                else:
                    parse_var(line)
            elif step == 3:  # File sequence
                if empty_str(line):  # section ends with empty line
                    step = 0
                else:
                    parse_fseq(line)
            elif step == 4:  # Tables and indices
                if empty_str(line):  # section ends with empty line
                    new_block = True
                else:
                    if new_block:
                        new_block = False
                        if not line.startswith('Index '):
                            # Should be table
                            table = StatTable() if db.gstat_version == GSTAT_25 else StatTable3()
                            db.tables.append(table)
                            in_table = True
                            parse_table(line, table)
                        else:  # It's index
                            index = StatIndex(table) if db.gstat_version == GSTAT_25 else StatIndex3(table)
                            db.indices.append(index)
                            in_table = False
                            parse_index(line, index)
                    else:
                        if in_table:
                            parse_table(line, table)
                        else:
                            parse_index(line, index)
        # Final touch
        if db.has_table_stats():
            for table in db.tables:
                table.distribution = FillDistribution(*table.distribution)
        if db.has_index_stats():
            for index in db.indices:
                index.distribution = FillDistribution(*index.distribution)
        db.tables.freeze()
        db.indices.freeze()
        return db
