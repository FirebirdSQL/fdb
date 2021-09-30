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
import datetime
import weakref
from collections import namedtuple
from locale import LC_ALL, LC_CTYPE, getlocale, setlocale, resetlocale
import sys

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

_LOCALE_ = LC_CTYPE if sys.version_info[0] == 3 else LC_ALL

def empty_str(str_):
    "Return True if string is empty (whitespace don't count) or None"
    return true if str_ is None else str_.strip() == ''


class StatTable(object):
    "Statisctics for single database table."
    def __init__(self):
        #: str: Table name
        self.name = None
        #: int: Table ID
        self.table_id = None
        #: int: Primary Pointer Page for table
        self.primary_pointer_page = None
        #: int: Index Root Page for table
        self.index_root_page = None
        #: float: Average record length
        self.avg_record_length = None
        #: int: Total number of record in table
        self.total_records = None
        #: float: Average record version length
        self.avg_version_length = None
        #: int: Total number of record versions
        self.total_versions = None
        #: int: Max number of versions for single record
        self.max_versions = None
        #: int: Number of data pages for table
        self.data_pages = None
        #: int: Number of data page slots for table
        self.data_page_slots = None
        #: float: Average data page fill ratio
        self.avg_fill = None
        #: :class:`FillDistribution`: Data page fill distribution statistics
        self.distribution = None
        #: :class:`~fdb.utils.ObjectList`: Indices belonging to table
        self.indices = []

class StatTable3(StatTable):
    "Statisctics for single database table (Firebird 3 and above)."
    def __init__(self):
        super(StatTable3, self).__init__()
        #: int: Number of Pointer Pages
        self.pointer_pages = None
        #: int: Number of record formats
        self.total_formats = None
        #: int: Number of actually used record formats
        self.used_formats = None
        #: float: Average length of record fragments
        self.avg_fragment_length = None
        #: int: Total number of record fragments
        self.total_fragments = None
        #: int: Max number of fragments for single record
        self.max_fragments = None
        #: float: Average length of unpacked record
        self.avg_unpacked_length = None
        #: float: Record compression ratio
        self.compression_ratio = None
        #: int: Number of Primary Data Pages
        self.primary_pages = None
        #: int: Number of Secondary Data Pages
        self.secondary_pages = None
        #: int: Number of swept data pages
        self.swept_pages = None
        #: int: Number of empty data pages
        self.empty_pages = None
        #: int: Number of full data pages
        self.full_pages = None
        #: int: Number of BLOB values
        self.blobs = None
        #: int: Total length of BLOB values (bytes)
        self.blobs_total_length = None
        #: int: Number of BLOB pages
        self.blob_pages = None
        #: int: Number of Level 0 BLOB values
        self.level_0 = None
        #: int: Number of Level 1 BLOB values
        self.level_1 = None
        #: int: Number of Level 2 BLOB values
        self.level_2 = None

class StatIndex(object):
    "Statisctics for single database index."
    def __init__(self, table):
        #: wekref.proxy: Proxy to parent :class:`TableStats`
        self.table = weakref.proxy(table)
        table.indices.append(weakref.proxy(self))
        #: str: Index name
        self.name = None
        #: int: Index ID
        self.index_id = None
        #: int: Depth of index tree
        self.depth = None
        #: int: Number of leaft index tree buckets
        self.leaf_buckets = None
        #: int: Number of index tree nodes
        self.nodes = None
        #: float: Average data length
        self.avg_data_length = None
        #: int: Total number of duplicate keys
        self.total_dup = None
        #: int: Max number of occurences for single duplicate key
        self.max_dup = None
        #: :class:`FillDistribution`: Index page fill distribution statistics
        self.distribution = None

class StatIndex3(StatIndex):
    "Statisctics for single database index (Firebird 3 and above)."
    def __init__(self, table):
        super(StatIndex3, self).__init__(table)
        #: int: Index Root page
        self.root_page = None
        #: float: Average node length
        self.avg_node_length = None
        #: float: Average key length
        self.avg_key_length = None
        #: float: Index key compression ratio
        self.compression_ratio = None
        #: float: Average key prefix length
        self.avg_prefix_length = None
        #: float: Index clustering factor
        self.clustering_factor = None
        #: float: Ration
        self.ratio = None

class StatDatabase(object):
    """Firebird database statistics (produced by gstat).
"""
    def __init__(self):
        #: int: GSTAT version
        self.gstat_version = None
        #: int: System change number (v3 only)
        self.system_change_number = None # ver3
        #: datetime.datetime: gstat execution timestamp
        self.executed = None
        #: datetime.datetime; gstat completion timestamp
        self.completed = None # ver3
        #: str: Database filename
        self.filename = None
        #: int: Database flags
        self.flags = 0
        #: int: Checksum (v2 only)
        self.checksum = 12345 # ver2
        #: int: Database header generation
        self.generation = 0
        #: int: Database page size
        self.page_size = 0
        #self.ods_version = None
        #: int: Oldest Interesting Transaction
        self.oit = 0
        #: int: Oldest Active Transaction
        self.oat = 0
        #: int: Oldest Snapshot Transaction
        self.ost = 0
        #: int: Next Transaction
        self.next_transaction = 0
        #: int: Bumped Transaction (v2 only)
        self.bumped_transaction = None # ver2
        #self.sequence_number = 0
        #: int: Next attachment ID
        self.next_attachment_id = 0
        #: int: Implementation ID (v2 only)
        self.implementation_id = 0 # ver2
        #: str: Implementation (v3 only)
        self.implementation = None # ver3
        #: int: Number of shadows
        self.shadow_count = 0
        #: int: Number of page buffers
        self.page_buffers = 0
        #: int: Next header page
        self.next_header_page = 0
        #: int: SQL Dialect
        self.database_dialect = 0
        #: datetime.datetime: Database creation timestamp
        self.creation_date = None
        #: list: Database attributes
        self.attributes = []
        # Variable data
        #: int: Sweep interval (variable hdr item)
        self.sweep_interval = None
        #: str: Continuation file (variable hdr item)
        self.continuation_file = None
        #: int: Last logical page (variable hdr item)
        self.last_logical_page = None
        #: str: Backup GUID (variable hdr item)
        self.backup_guid = None
        #: str: Root file name (variable hdr item)
        self.root_filename = None
        #: str: replay logging file (variable hdr item)
        self.replay_logging_file = None
        #: str: Backup difference file (variable hdr item)
        self.backup_diff_file = None
        #: Encryption: Stats for enxrypted data page
        self.encrypted_data_pages = None
        #: Encryption: Stats for enxrypted index page
        self.encrypted_index_pages = None
        #: Encryption: Stats for enxrypted blob page
        self.encrypted_blob_pages = None
        #: list: database file names
        self.continuation_files = []
        #: :class:`~fdb.utils.ObjectList`: :class:`StatTable` or :class:`StatTable3` instances
        self.tables = None
        #: :class:`~fdb.utils.ObjectList`: :class:`StatIndex` or :class:`StatIndex3` instances
        self.indices = None
    def has_table_stats(self):
        """Return True if instance contains information about tables.

        .. important::

           This is not the same as check for empty :data:`tables` list. When gstat is run with `-i` without
           `-d` option, :data:`tables` list contains instances that does not have any other information about table
           but table name and its indices.
"""
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


def parse(lines):
    """Parse output from Firebird gstat utility.

    Args:
        lines (iterable): Lines produced by Firebird gstat utility.

    Returns:
        :class:`~fdb.gstat.StatDatabase` instance with parsed results.

    Raises:
        fdb.ParseError: When any problem is found in input stream.
"""
    def parse_hdr(line):
        "Parse line from header"
        for key, valtype, name in items_hdr:
            if line.startswith(key):
                # Check for GSTAT_VERSION
                if db.gstat_version is None:
                    if key == 'Checksum':
                        db.gstat_version = GSTAT_25
                        db.tables = ObjectList(_cls=StatTable, key_expr='item.name')
                        db.indices = ObjectList(_cls=StatIndex, key_expr='item.name')
                    elif key == 'System Change Number':
                        db.gstat_version = GSTAT_30
                        db.tables = ObjectList(_cls=StatTable3, key_expr='item.name')
                        db.indices = ObjectList(_cls=StatIndex3, key_expr='item.name')
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
                    name = key.lower().replace(' ', '_')
                setattr(db, name, value)
                return
        raise ParseError('Unknown information (line %i)' % line_no)
    def parse_var(line):
        "Parse line from variable header data"
        if line == '*END*':
            return
        for key, valtype, name in items_var:
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
                    name = key.lower().strip(':').replace(' ', '_')
                setattr(db, name, value)
                return
        raise ParseError('Unknown information (line %i)' % line_no)
    def parse_fseq(line):
        "Parse line from file sequence"
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
        "Parse line from table data"
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
                    items = items_tbl2 if db.gstat_version == GSTAT_25 else items_tbl3
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
                                name = key.lower().strip(':').replace(' ', '_')
                            setattr(table, name, value)
                            found = True
                            break
                    if not found:
                        raise ParseError('Unknown information (line %i)' % line_no)
            else:  # Fill distribution
                if '=' in line:
                    fill_range, fill_value = line.split('=')
                    i = items_fill.index(fill_range.strip())
                    if table.distribution is None:
                        table.distribution = [0, 0, 0, 0, 0]
                    table.distribution[i] = int(fill_value.strip())
                elif line.startswith('Fill distribution:'):
                    pass
                else:
                    raise ParseError('Unknown information (line %i)' % line_no)
    def parse_index(line, index):
        "Parse line from index data"
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
                    items = items_idx2 if db.gstat_version == GSTAT_25 else items_idx3
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
                                name = key.lower().strip(':').replace(' ', '_')
                            setattr(index, name, value)
                            found = True
                            break
                    if not found:
                        raise ParseError('Unknown information (line %i)' % line_no)
            else:  # Fill distribution
                if '=' in line:
                    fill_range, fill_value = line.split('=')
                    i = items_fill.index(fill_range.strip())
                    if index.distribution is None:
                        index.distribution = [0, 0, 0, 0, 0]
                    index.distribution[i] = int(fill_value.strip())
                elif line.startswith('Fill distribution:'):
                    pass
                else:
                    raise ParseError('Unknown information (line %i)' % line_no)
    def parse_encryption(line):
        "Parse line from encryption data"
        try:
            total, encrypted, unencrypted = line.split(',')
            pad, total = total.rsplit(' ', 1)
            total = int(total)
            pad, encrypted = encrypted.rsplit(' ', 1)
            encrypted = int(encrypted)
            pad, unencrypted = unencrypted.rsplit(' ', 1)
            unencrypted = int(unencrypted)
            data = Encryption(total, encrypted, unencrypted)
        except:
            raise ParseError('Malformed encryption information (line %i)' % line_no)
        if 'Data pages:' in line:
            db.encrypted_data_pages = data
        elif 'Index pages:' in line:
            db.encrypted_index_pages = data
        elif 'Blob pages:' in line:
            db.encrypted_blob_pages = data
        else:
            raise ParseError('Unknown encryption information (line %i)' % line_no)

    #
    items_hdr = [('Flags', 'i', None),
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

    items_var = [('Sweep interval:', 'i', None),
                 ('Continuation file:', 's', None),
                 ('Last logical page:', 'i', None),
                 ('Database backup GUID:', 's', 'backup_guid'),
                 ('Root file name:', 's', 'root_filename'),
                 ('Replay logging file:', 's', None),
                 ('Backup difference file:', 's', 'backup_diff_file')]

    items_tbl2 = [('Primary pointer page:', 'i', None),
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
                  ('full pages:', 'i', None)]

    items_tbl3 = [('Primary pointer page:', 'i', None),
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
                  ('Level 2:', 'i', None)]

    items_idx2 = [('Depth:', 'i', None),
                  ('leaf buckets:', 'i', None),
                  ('nodes:', 'i', None),
                  ('Average data length:', 'f', 'avg_data_length'),
                  ('total dup:', 'i', None),
                  ('max dup:', 'i', None)]
    items_idx3 = [('Root page:', 'i', None),
                  ('depth:', 'i', None),
                  ('leaf buckets:', 'i', None),
                  ('nodes:', 'i', None),
                  ('Average node length:', 'f', 'avg_node_length'),
                  ('total dup:', 'i', None),
                  ('max dup:', 'i', None),
                  ('Average key length:', 'f', 'avg_key_length'),
                  ('compression ratio:', 'f', None),
                  ('Average prefix length:', 'f', 'avg_prefix_length'),
                  ('average data length:', 'f', 'avg_data_length'),
                  ('Clustering factor:', 'f', None),
                  ('ratio:', 'f', None)]

    items_fill = ['0 - 19%', '20 - 39%', '40 - 59%', '60 - 79%', '80 - 99%']
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
    locale = getlocale(_LOCALE_)
    try:
        if sys.platform == 'win32':
            setlocale(_LOCALE_, 'English_United States')
        else:
            setlocale(_LOCALE_, 'en_US')
        # Skip empty lines at start
        for line in (x.strip() for x in lines):
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
    finally:
        if locale[0] is None:
            if sys.platform == 'win32':
                setlocale(_LOCALE_, '')
            else:
                resetlocale(_LOCALE_)
        else:
            setlocale(_LOCALE_, locale)
    return db
