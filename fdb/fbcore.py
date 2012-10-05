#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           fbcore.py
#   DESCRIPTION:    Python driver for Firebird
#   CREATED:        8.10.2011
#
#  Software distributed under the License is distributed AS IS,
#  WITHOUT WARRANTY OF ANY KIND, either express or implied.
#  See the License for the specific language governing rights
#  and limitations under the License.
#
#  The Original Code was created by Pavel Cisar
#
#  Copyright (c) 2011 Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): Philippe Makowski <pmakowski@ibphoenix.fr>
#                  ______________________________________.
#
# See LICENSE.TXT for details.

import sys
import os
from . import ibase
import ctypes
import struct
import time
import datetime
import decimal
import weakref
import threading
try:
    # Python 2
    from itertools import izip_longest
except ImportError:
    # Python 3
    from itertools import zip_longest as izip_longest
from fdb.ibase import (frb_info_att_charset, isc_dpb_activate_shadow,
    isc_dpb_address_path, isc_dpb_allocation, isc_dpb_begin_log,
    isc_dpb_buffer_length, isc_dpb_cache_manager, isc_dpb_cdd_pathname,
    isc_dpb_connect_timeout, isc_dpb_damaged, isc_dpb_dbkey_scope,
    isc_dpb_debug, isc_dpb_delete_shadow, isc_dpb_disable_journal,
    isc_dpb_disable_wal, isc_dpb_drop_walfile,
    isc_dpb_dummy_packet_interval, isc_dpb_enable_journal,
    isc_dpb_encrypt_key, isc_dpb_force_write, isc_dpb_garbage_collect,
    isc_dpb_gbak_attach, isc_dpb_gfix_attach, isc_dpb_gsec_attach,
    isc_dpb_gstat_attach, isc_dpb_interp, isc_dpb_journal,
    isc_dpb_lc_ctype, isc_dpb_lc_messages, isc_dpb_license,
    isc_dpb_no_garbage_collect, isc_dpb_no_reserve,
    isc_dpb_num_buffers, isc_dpb_number_of_users, isc_dpb_old_dump_id,
    isc_dpb_old_file, isc_dpb_old_file_size, isc_dpb_old_num_files,
    isc_dpb_old_start_file, isc_dpb_old_start_page,
    isc_dpb_old_start_seqno, isc_dpb_online, isc_dpb_online_dump,
    isc_dpb_overwrite, isc_dpb_page_size, isc_dpb_password,
    isc_dpb_password_enc, isc_dpb_quit_log, isc_dpb_reserved,
    isc_dpb_sec_attach, isc_dpb_set_db_charset,
    isc_dpb_set_db_readonly, isc_dpb_set_db_sql_dialect,
    isc_dpb_set_page_buffers, isc_dpb_shutdown, isc_dpb_shutdown_delay,
    isc_dpb_sql_dialect, isc_dpb_sql_role_name, isc_dpb_sweep,
    isc_dpb_sweep_interval, isc_dpb_sys_user_name,
    isc_dpb_sys_user_name_enc, isc_dpb_trace, isc_dpb_user_name,
    isc_dpb_verify, isc_dpb_version1, isc_dpb_wal_backup_dir,
    isc_dpb_wal_bufsize, isc_dpb_wal_chkptlen,
    isc_dpb_wal_grp_cmt_wait, isc_dpb_wal_numbufs,
    isc_dpb_working_directory, isc_info_active_tran_count,
    isc_info_end, 
    isc_info_active_transactions, isc_info_allocation,
    isc_info_attachment_id, isc_info_backout_count,
    isc_info_base_level, isc_info_bpage_errors, isc_info_creation_date,
    isc_info_cur_log_part_offset, isc_info_cur_logfile_name,
    isc_info_current_memory, isc_info_db_SQL_dialect,
    isc_info_db_class, isc_info_db_id, isc_info_db_provider,
    isc_info_db_read_only, isc_info_db_size_in_pages,
    isc_info_db_sql_dialect, isc_info_delete_count,
    isc_info_dpage_errors, isc_info_expunge_count, isc_info_fetches,
    isc_info_firebird_version, isc_info_forced_writes,
    isc_info_implementation, isc_info_insert_count,
    isc_info_ipage_errors, isc_info_isc_version, isc_info_license,
    isc_info_limbo, isc_info_logfile, isc_info_marks,
    isc_info_max_memory, isc_info_next_transaction,
    isc_info_no_reserve, isc_info_num_buffers,
    isc_info_num_wal_buffers, isc_info_ods_minor_version,
    isc_info_ods_version, isc_info_oldest_active,
    isc_info_oldest_snapshot, isc_info_oldest_transaction,
    isc_info_page_errors, isc_info_page_size, isc_info_ppage_errors,
    isc_info_purge_count, isc_info_read_idx_count,
    isc_info_read_seq_count, isc_info_reads, isc_info_record_errors,
    isc_info_set_page_buffers, isc_info_sql_stmt_commit,
    isc_info_sql_stmt_ddl, isc_info_sql_stmt_delete,
    isc_info_sql_stmt_exec_procedure, isc_info_sql_stmt_get_segment,
    isc_info_sql_stmt_insert, isc_info_sql_stmt_put_segment,
    isc_info_sql_stmt_rollback, isc_info_sql_stmt_savepoint,
    isc_info_sql_stmt_select, isc_info_sql_stmt_select_for_upd,
    isc_info_sql_stmt_set_generator, isc_info_sql_stmt_start_trans,
    isc_info_sql_stmt_update, isc_info_sweep_interval,
    isc_info_tpage_errors, isc_info_tra_access,
    isc_info_tra_concurrency, isc_info_tra_consistency,
    isc_info_tra_id, isc_info_tra_isolation, isc_info_tra_lock_timeout,
    isc_info_tra_no_rec_version, isc_info_tra_oldest_active,
    isc_info_tra_oldest_interesting, isc_info_tra_oldest_snapshot,
    isc_info_tra_read_committed, isc_info_tra_readonly,
    isc_info_tra_readwrite, isc_info_tra_rec_version,
    isc_info_update_count, isc_info_user_names, isc_info_version,
    isc_info_wal_avg_grpc_size, isc_info_wal_avg_io_size,
    isc_info_wal_buffer_size, isc_info_wal_ckpt_length,
    isc_info_wal_cur_ckpt_interval, isc_info_wal_grpc_wait_usecs,
    isc_info_wal_num_commits, isc_info_wal_num_io,
    isc_info_wal_prv_ckpt_fname, isc_info_wal_prv_ckpt_poffset,
    isc_info_wal_recv_ckpt_fname, isc_info_wal_recv_ckpt_poffset,
    isc_info_window_turns, isc_info_writes, isc_tpb_autocommit,
    isc_tpb_commit_time, isc_tpb_concurrency, isc_tpb_consistency,
    isc_tpb_exclusive, isc_tpb_ignore_limbo, isc_tpb_lock_read,
    isc_tpb_lock_timeout, isc_tpb_lock_write, isc_tpb_no_auto_undo,
    isc_tpb_no_rec_version, isc_tpb_nowait, isc_tpb_protected,
    isc_tpb_read, isc_tpb_read_committed, isc_tpb_rec_version,
    isc_tpb_restart_requests, isc_tpb_shared, isc_tpb_verb_time,
    isc_tpb_version3, isc_tpb_wait, isc_tpb_write,
    
    b, s, ord2, int2byte, mychr, mybytes, myunicode, mylong, StringType, 
    IntType, LongType, FloatType, ListeType, UnicodeType, TupleType, xrange,
    charset_map,
    
    isc_sqlcode, isc_sql_interprete, fb_interpret, isc_dsql_execute_immediate,
    ISC_STATUS_PTR, XSQLDA_PTR, ISC_SHORT, ISC_LONG, ISC_SCHAR, 
    SHRT_MIN, SHRT_MAX, USHRT_MAX, INT_MIN, INT_MAX, LONG_MIN, LONG_MAX,
    ISC_STATUS_ARRAY,

    SQL_TEXT, SQL_VARYING, SQL_SHORT, SQL_LONG, SQL_FLOAT, SQL_DOUBLE,
    SQL_D_FLOAT, SQL_TIMESTAMP, SQL_BLOB, SQL_ARRAY, SQL_QUAD, SQL_TYPE_TIME,
    SQL_TYPE_DATE, SQL_INT64, SUBTYPE_NUMERIC, SUBTYPE_DECIMAL,
    MAX_BLOB_SEGMENT_SIZE

    )

PYTHON_MAJOR_VER = sys.version_info[0]

if PYTHON_MAJOR_VER != 3:
    from exceptions import NotImplementedError


__version__ = '0.9'

apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

# Exceptions required by Python Database API


class Warning(Exception):
    """Exception raised for important warnings like data
    truncations while inserting, etc."""
    pass

class Error(Exception):
    """Exception that is the base class of all other error
    exceptions. You can use this to catch all errors with one
    single 'except' statement. Warnings are not considered
    errors and thus should not use this class as base."""
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the
    database interface rather than the database itself."""
    pass

class DatabaseError(Error):
    "Exception raised for errors that are related to the database."
    pass
    #def __init__(self,msg, sqlcode=0,error_code=0):
        #self.sqlcode = sqlcode
        #self.error_code = error_code
        #self.msg = msg

class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with
    the processed data like division by zero, numeric value
    out of range, etc."""
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the
    database's operation and not necessarily under the control
    of the programmer, e.g. an unexpected disconnect occurs,
    the data source name is not found, a transaction could not
    be processed, a memory allocation error occurred during
    processing, etc."""
    pass

class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the
    database is affected, e.g. a foreign key check fails."""
    pass

class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the
    transaction is out of sync, etc."""
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not
    found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc."""
    pass

class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used
    which is not supported by the database"""
    pass

class TransactionConflict(DatabaseError):
    pass

# Named positional constants to be used as indices into the description
# attribute of a cursor (these positions are defined by the DB API spec).
# For example:
#   nameOfFirstField = cursor.description[0][kinterbasdb.DESCRIPTION_NAME]

DESCRIPTION_NAME = 0
DESCRIPTION_TYPE_CODE = 1
DESCRIPTION_DISPLAY_SIZE = 2
DESCRIPTION_INTERNAL_SIZE = 3
DESCRIPTION_PRECISION = 4
DESCRIPTION_SCALE = 5
DESCRIPTION_NULL_OK = 6

# Types Required by Python DB-API 2.0

def Date(year, month, day):
    return datetime.date(year, month, day)

def Time(hour, minite, second):
    return datetime.time(hour, minite, second)

def DateFromTicks(ticks):
    return apply(Date, time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return apply(Time, time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return apply(Timestamp, time.localtime(ticks)[:6])

def Binary(b):
    return b

class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

STRING = DBAPITypeObject(str)

if PYTHON_MAJOR_VER == 3:
    BINARY = DBAPITypeObject(bytes)
else:
    BINARY = DBAPITypeObject(str)

NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID = DBAPITypeObject()

_FS_ENCODING = sys.getfilesystemencoding()
DIST_TRANS_MAX_DATABASES = 16

def bs(byte_array):
    return bytes(byte_array) if PYTHON_MAJOR_VER == 3 else ''.join((chr(c) for c in byte_array))

ISOLATION_LEVEL_READ_COMMITED_LEGACY = bs([isc_tpb_version3, 
                                           isc_tpb_write, 
                                           isc_tpb_wait,
                                           isc_tpb_read_committed, 
                                           isc_tpb_no_rec_version])
ISOLATION_LEVEL_READ_COMMITED = bs([isc_tpb_version3, 
                                    isc_tpb_write, 
                                    isc_tpb_wait,
                                    isc_tpb_read_committed, 
                                    isc_tpb_rec_version])
ISOLATION_LEVEL_REPEATABLE_READ = bs([isc_tpb_version3, 
                                      isc_tpb_write, 
                                      isc_tpb_wait,
                                      isc_tpb_concurrency])
ISOLATION_LEVEL_SNAPSHOT = ISOLATION_LEVEL_REPEATABLE_READ
ISOLATION_LEVEL_SERIALIZABLE = bs([isc_tpb_version3, 
                                   isc_tpb_write, 
                                   isc_tpb_wait,
                                   isc_tpb_consistency])
ISOLATION_LEVEL_SNAPSHOT_TABLE_STABILITY = ISOLATION_LEVEL_SERIALIZABLE
ISOLATION_LEVEL_READ_COMMITED_RO = bs([isc_tpb_version3, 
                                       isc_tpb_read, 
                                       isc_tpb_wait,
                                       isc_tpb_read_committed, 
                                       isc_tpb_rec_version])

_SIZE_OF_SHORT = ctypes.sizeof(ctypes.c_short)

_tenTo = [10 ** x for x in range(20)]

if PYTHON_MAJOR_VER != 3:
    del x

__xsqlda_cache = {}
__tebarray_cache = {}

_DATABASE_INFO_CODES_WITH_INT_RESULT = (
    isc_info_allocation, isc_info_no_reserve, isc_info_db_sql_dialect,
    isc_info_ods_minor_version, isc_info_ods_version, isc_info_page_size,
    isc_info_current_memory, isc_info_forced_writes, isc_info_max_memory,
    isc_info_num_buffers, isc_info_sweep_interval, isc_info_limbo,
    isc_info_attachment_id, isc_info_fetches, isc_info_marks, isc_info_reads,
    isc_info_writes, isc_info_set_page_buffers, isc_info_db_read_only,
    isc_info_db_size_in_pages, isc_info_page_errors, isc_info_record_errors,
    isc_info_bpage_errors, isc_info_dpage_errors, isc_info_ipage_errors,
    isc_info_ppage_errors, isc_info_tpage_errors,
    isc_info_oldest_transaction, isc_info_oldest_active,
    isc_info_oldest_snapshot, isc_info_next_transaction,
    isc_info_active_tran_count,
)
_DATABASE_INFO_CODES_WITH_COUNT_RESULTS = (
    isc_info_backout_count, isc_info_delete_count, isc_info_expunge_count,
    isc_info_insert_count, isc_info_purge_count, isc_info_read_idx_count,
    isc_info_read_seq_count, isc_info_update_count
)
_DATABASE_INFO_CODES_WITH_TIMESTAMP_RESULT = (isc_info_creation_date,)

_DATABASE_INFO__KNOWN_LOW_LEVEL_EXCEPTIONS = (isc_info_user_names,)

def xsqlda_factory(size):
    if size in __xsqlda_cache:
        cls = __xsqlda_cache[size]
    else:
        class XSQLDA(ctypes.Structure):
            pass
        XSQLDA._fields_ = [
            ('version', ISC_SHORT),
            ('sqldaid', ISC_SCHAR * 8),
            ('sqldabc', ISC_LONG),
            ('sqln', ISC_SHORT),
            ('sqld', ISC_SHORT),
            ('sqlvar', ibase.XSQLVAR * size),
        ]
        __xsqlda_cache[size] = XSQLDA
        cls = XSQLDA
    xsqlda = cls()
    xsqlda.version = ibase.SQLDA_version1
    xsqlda.sqln = size
    return xsqlda

def tebarray_factory(size):
    if size in __tebarray_cache:
        cls = __tebarray_cache[size]
    else:
        cls = ibase.ISC_TEB * size
        __xsqlda_cache[size] = cls
    teb_array = cls()
    return teb_array

buf_pointer = ctypes.POINTER(ctypes.c_char)

def b2u(st, charset):
    "Decode to unicode if charset is defined. For conversion of result set data."
    if charset:
        return st.decode(charset)
    else:
        return st

def p3fix(st, charset):
    """For P3 convert bytes to string using connection charset, P2 as is. 
    For conversion of info results to native strings."""
    if PYTHON_MAJOR_VER == 3:
        return st.decode(charset)
    else:
        return st
    
def inc_pointer(pointer):
    t = type(pointer)
    p = ctypes.cast(pointer,ctypes.c_void_p)
    p.value += 1
    return ctypes.cast(p,t)
    
def bytes_to_bint(b):           # Read as big endian
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b == 2:
        fmt = '>h'
    elif len_b == 4:
        fmt = '>l'
    elif len_b == 8:
        fmt = '>q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bytes_to_int(b):            # Read as little endian.
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b == 2:
        fmt = '<h'
    elif len_b == 4:
        fmt = '<l'
    elif len_b == 8:
        fmt = '<q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def bint_to_bytes(val, nbytes):  # Convert int value to big endian bytes.
    if nbytes == 1:
        fmt = 'b'
    elif nbytes == 2:
        fmt = '>h'
    elif nbytes == 4:
        fmt = '>l'
    elif nbytes == 8:
        fmt = '>q'
    else:
        raise InternalError
    return struct.pack(fmt, val)

def int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    if nbytes == 1:
        fmt = 'b'
    elif nbytes == 2:
        fmt = '<h'
    elif nbytes == 4:
        fmt = '<l'
    elif nbytes == 8:
        fmt = '<q'
    else:
        raise InternalError
    return struct.pack(fmt, val)

def db_api_error(status_vector):
    return status_vector[0] == 1 and status_vector[1] > 0

def exception_from_status(error, status, preamble=None):
    msglist = []
    msg = ctypes.create_string_buffer(512)

    if preamble:
        msglist.append(preamble)
    sqlcode = isc_sqlcode(status)
    error_code = status[1]
    msglist.append('- SQLCODE: %i' % sqlcode)

    #isc_sql_interprete(sqlcode, msg, 512)
    #if PYTHON_MAJOR_VER == 3:
        #### Todo: trouble? decode from connection charset?
        #msglist.append('- ' + (msg.value).decode('utf_8'))
    #else:
        #msglist.append('- ' + msg.value)

    pvector = ctypes.cast(ctypes.addressof(status), ISC_STATUS_PTR)

    while True:
        result = fb_interpret(msg, 512, pvector)
        if result != 0:
            if PYTHON_MAJOR_VER == 3:
                ### Todo: trouble? decode from connection charset?
                msglist.append('- ' + (msg.value).decode('utf_8'))
            else:
                msglist.append('- ' + msg.value)
        else:
            break
    return error('\n'.join(msglist), sqlcode, error_code)

def build_dpb(user, password, sql_dialect, role, charset, buffers, force_write,
              no_reserve, db_key_scope):
    params = [int2byte(isc_dpb_version1)]

    def addString(codeAsByte, s):
        if PYTHON_MAJOR_VER == 3 or isinstance(s,UnicodeType):
            s = s.encode(charset_map.get(charset, charset))
        sLen = len(s)
        if sLen >= 256:
            # Because the length is denoted in the DPB by a single byte.
            raise ProgrammingError("Individual component of database"
                                   " parameter buffer is too large.  Components must be less"
                                   " than 256 bytes."
                                   )
        myformat = 'cc%ds' % sLen  # like 'cc50s' for a 50-byte string
        newEntry = struct.pack(myformat, int2byte(codeAsByte),
                               int2byte(sLen), s)
        params.append(newEntry)

    def addInt(codeAsByte, value):
        if (not isinstance(value, (int, mylong))
            or value < 0 or value > 255):
            raise ProgrammingError("The value for an integer DPB code must be"
                                   " an int or long with a value between 0 and 255."
                                   )
        newEntry = struct.pack('ccc', int2byte(codeAsByte),
                               b('\x01'), int2byte(value))
        params.append(newEntry)

    if user:
        addString(isc_dpb_user_name, user)
    if password:
        addString(isc_dpb_password, password)
    if role:
        addString(isc_dpb_sql_role_name, role)
    if sql_dialect:
        addInt(isc_dpb_sql_dialect, sql_dialect)
    if charset:
        addString(isc_dpb_lc_ctype, charset)
    if buffers:
        addInt(isc_dpb_num_buffers, buffers)
    if force_write:
        addInt(isc_dpb_force_write, force_write)
    if no_reserve:
        addInt(isc_dpb_no_reserve, no_reserve)
    if db_key_scope:
        addInt(isc_dpb_dbkey_scope, db_key_scope)
    return b('').join(params)

def connect(dsn='', user=None, password=None, host=None, port=3050, database=None,
            sql_dialect=3, role=None, charset=None, buffers=None,
            force_write=None, no_reserve=None, db_key_scope=None,
            isolation_level=ISOLATION_LEVEL_READ_COMMITED):
    """
    Establish a connection to database. 
    
    :param dsn: Connection string in format [host[/port]]:database
    :param string user: User name. If not specified, fdb attempts to use ISC_USER envar.
    :param string password: User password. If not specified, fdb attempts to use ISC_PASSWORD envar.
    :param string host: Server host machine specification.
    :param integer port: Port used by Firebird server **(not used)**.
    :param string database: Database specification (file spec. or alias)
    :param sql_dialect: SQL Dialect for connection.
    :type sql_dialect): 1, 2 or 3
    :param string role: User role.
    :param string charset: Character set for connection.
    :param integer buffers: Page case size override for connection.
    :param integer force_writes: Forced writes override for connection.
    :param integer no_reserve: Page space reservation override for connection.
    :param integer db_key_scope: DBKEY scope override for connection.
    :param isolation_level: Default transaction isolation level for connection **(not used)**.
    :type isolation_level: 0, 1, 2 or 3
    :returns: Connection to database.
    :rtype: :class:`Connection` instance.
    
    :raises ProgrammingError: For bad parameter values.
    :raises DatabaseError: When connection cannot be established.
    
    .. important::  
    
       You may specify the database using either `dns` or `database` (with optional `host`), 
       but not both.
    
    Examples:
    
    .. code-block:: python
    
       con = fdb.connect(dsn='host:/path/database.fdb', user='sysdba', password='pass', charset='UTF8')
       con = fdb.connect(host='myhost', database='/path/database.fdb', user='sysdba', password='pass', charset='UTF8')
    """
    if not user:
        user = os.environ.get('ISC_USER', None)
    if not password:
        password = os.environ.get('ISC_PASSWORD', None)

    if sql_dialect not in [1, 2, 3]:
        raise ProgrammingError("SQl Dialect must be either 1, 2 or 3")

    if ((not dsn and not host and not database)
        or (dsn and (host or database))
        or (host and not database)
        ):
        raise ProgrammingError(
            "Must supply one of:\n"
            " 1. keyword argument dsn='host:/path/to/database'\n"
            " 2. both keyword arguments host='host' and"
            " database='/path/to/database'\n"
            " 3. only keyword argument database='/path/to/database'"
        )
    if not dsn:
        if host and host.endswith(':'):
            raise ProgrammingError("Host must not end with a colon."
                " You should specify host='%s' rather than host='%s'."
                % (host[:-1], host)
                )
        elif host:
            dsn = '%s:%s' % (host, database)
        else:
            dsn = database

    dsn = b(dsn,_FS_ENCODING)
    
    dpb = build_dpb(user, password, sql_dialect, role, charset, buffers,
                    force_write, no_reserve, db_key_scope)

    _isc_status = ISC_STATUS_ARRAY()
    _db_handle = ibase.isc_db_handle(0)

    ibase.isc_attach_database(_isc_status, len(dsn), dsn, _db_handle, len(dpb),
                              dpb)
    if db_api_error(_isc_status):
        raise exception_from_status(DatabaseError, _isc_status,
                                    "Error while connecting to database:")

    return Connection(_db_handle, dpb, sql_dialect, charset)

def create_database(sql, sql_dialect=3):
    """
    Creates a new database with the supplied "CREATE DATABASE" statement.
    
    :param sql: "CREATE DATABASE" statement.
    :param sql_dialect: SQL Dialect for newly created database.
    :type sql_dialect: 1 or 3
    :returns: Connection to the newly created database.
    :rtype: :class:`Connection` instance.
    
    :raises ProgrammingError: When database creation fails.

    Example:
    
    .. code-block:: python
    
       con = fdb.create_database("create database '/temp/db.db' user 'sysdba' password 'pass'")
    """
    if isinstance(sql, myunicode):
        sql = sql.encode(_FS_ENCODING)
        
    isc_status = ISC_STATUS_ARRAY(0)
    trans_handle = ibase.isc_tr_handle(0)
    db_handle = ibase.isc_db_handle(0)
    xsqlda = xsqlda_factory(1)

    # For yet unknown reason, the isc_dsql_execute_immediate segfaults when
    # NULL (None) is passed as XSQLDA, so we provide one here
    isc_dsql_execute_immediate(isc_status, db_handle, trans_handle,
            ctypes.c_ushort(len(sql)), sql, sql_dialect,
            ctypes.cast(ctypes.pointer(xsqlda),XSQLDA_PTR))
    if db_api_error(isc_status):
        raise exception_from_status(ProgrammingError, isc_status,
                                    "Error while creating database:")

    return Connection(db_handle)

class Connection(object):
    """
    Represents a connection between the database client (the Python process)
    and the database server.

    .. important::
    
       DO NOT create instances of this class directly! Use only 
       :func:`connect` or :func:`create_database` to get Connection instances.
    """

    def __init__(self, db_handle, dpb=None, sql_dialect=3,
                 charset=None):
        """
        :param db_handle: Database handle provided by factory function.
        :param dpb: Database Parameter Block associated with database handle.
        :param integer sql_dialect: SQL Dialect associated with database handle.
        :param string charset: Character set associated with database handle.
        """
        if charset:
            self._charset = charset.upper()
        else:
            self._charset = None
        self._python_charset = charset_map.get(self.charset, self.charset)

        self._default_tpb = ISOLATION_LEVEL_READ_COMMITED
        self._main_transaction = Transaction([self], 
                                             default_tpb=self._default_tpb)
        self._transactions = [self._main_transaction]
        self.__precision_cache = {}
        self.__conduits = []
        self.__group = None

        #: (integer) sql_dialect for this connection, do not change.
        self.sql_dialect = sql_dialect
        self._dpb = dpb
        self._isc_status = ISC_STATUS_ARRAY()
        self._db_handle = db_handle
        # Cursor for internal use
        self.__ic = Cursor(self, self.main_transaction)
        self.__ic._set_as_internal()
    
    def __remove_cursor(self, cursor_ref):
        self._cursors.remove(cursor_ref)
    def __remove_group(self, group_ref):
        self.__group = None
    def __ensure_group_membership(self, must_be_member, err_msg):
        if must_be_member:
            if self.group is None:
                raise ProgrammingError(err_msg)
        else:
            if self.group is not None:
                raise ProgrammingError(err_msg)
    def __check_attached(self):
        if self._db_handle == None:
            raise ProgrammingError("Connection object is detached from database")
    def __close(self, detach=True):
        if self._db_handle != None:
            self.__ic.close()
            for conduit in self.__conduits:
                conduit.close()
            for transaction in self._transactions:
                transaction.default_action = 'rollback' # Required by Python DB API 2.0
                transaction.close()
            if detach:
                ibase.isc_detach_database(self._isc_status, self._db_handle)
            self._db_handle = None
    def __get_main_transaction(self):
        return self._main_transaction
    def __get_transactions(self):
        return tuple(self._transactions)
    def __get_closed(self):
        return self._db_handle == None
    def __get_server_version(self):
        return self.db_info([ibase.isc_info_version])[ibase.isc_info_version]
        #return self.db_info([ibase.isc_info_firebird_version])[ibase.isc_info_firebird_version]
    def __get_default_tpb(self):
        return self._default_tpb
    def __set_default_tpb(self, value):
        self._default_tpb = _validateTPB(value)
    def __parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_int(raw_value) + 678882
        century = (4 * nday - 1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day - 3) // 153
        day = 5 * day - 3 - 153 * month
        day = (day + 5) // 5
        year = 100 * century + nday
        if month < 10:
            month += 3
        else:
            month -= 9
            year += 1
        return year, month, day
    def __parse_time(self, raw_value):
        "Convert raw data to datetime.time"
        n = bytes_to_int(raw_value)
        s = n // 10000
        m = s // 60
        h = m // 60
        m = m % 60
        s = s % 60
        return (h, m, s, (n % 10000) * 100)
    def _determine_field_precision(self, sqlvar):
        if sqlvar.relname_length == 0 or sqlvar.sqlname_length == 0:
            # Either or both field name and relation name are not provided,
            # so we cannot determine field precision. It's normal situation
            # for example for queries with dynamically computed fields
            return 0
        # Special case for automatic RDB$DB_KEY fields.
        if ((sqlvar.sqlname_length == 6 and sqlvar.sqlname == 'DB_KEY') or
            (sqlvar.sqlname_length == 10 and sqlvar.sqlname == 'RDB$DB_KEY')):
            return 0
        precision = self.__precision_cache.get((sqlvar.relname,
                                                sqlvar.sqlname))
        # First, try table
        self.__ic.execute("SELECT FIELD_SPEC.RDB$FIELD_PRECISION"
                          " FROM RDB$FIELDS FIELD_SPEC,"
                          " RDB$RELATION_FIELDS REL_FIELDS"
                          " WHERE"
                          " FIELD_SPEC.RDB$FIELD_NAME ="
                          " REL_FIELDS.RDB$FIELD_SOURCE"
                          " AND REL_FIELDS.RDB$RELATION_NAME = ?"
                          " AND REL_FIELDS.RDB$FIELD_NAME = ?",
                          (p3fix(sqlvar.relname,self._python_charset),
                           p3fix(sqlvar.sqlname,self._python_charset)))
        result = self.__ic.fetchone()
        if result:
            self.__ic.close()
            return result[0]
        # Next, try stored procedure output parameter
        self.__ic.execute("SELECT FIELD_SPEC.RDB$FIELD_PRECISION"
                          " FROM RDB$FIELDS FIELD_SPEC,"
                          " RDB$PROCEDURE_PARAMETERS REL_FIELDS"
                          " WHERE"
                          " FIELD_SPEC.RDB$FIELD_NAME ="
                          " REL_FIELDS.RDB$FIELD_SOURCE"
                          " AND RDB$PROCEDURE_NAME = ?"
                          " AND RDB$PARAMETER_NAME = ?"
                          " AND RDB$PARAMETER_TYPE = 1",
                          (p3fix(sqlvar.relname,self._python_charset),
                           p3fix(sqlvar.sqlname,self._python_charset)))
        result = self.__ic.fetchone()
        if result:
            self.__ic.close()
            return result[0]
        # We ran out of options
        return 0
    def drop_database(self):
        """Drops the database to which this connection is attached.

        Unlike plain file deletion, this method behaves responsibly, in that
        it removes shadow files and other ancillary files for this database.
        
        :raises ProgrammingError: When connection is a member of a :class:`ConnectionGroup`.
        :raises DatabaseError: When error is returned from server.
        """
        self.__ensure_group_membership(False, "Cannot drop database via"
                    " connection that is part of a ConnectionGroup.")
        saved_handle = ibase.isc_db_handle(self._db_handle.value)
        self.__close(detach=False)
        ibase.isc_drop_database(self._isc_status, saved_handle)
        if db_api_error(self._isc_status):
            self._db_handle = saved_handle
            raise exception_from_status(DatabaseError, self._isc_status,
                                        "Error while dropping database:")
    def execute_immediate(self, sql):
        """Executes a statement in context of :attr:`main_transaction` without 
        caching its prepared form. 
        
        Automatically starts transaction if it's not already started.
        
        :param string sql: SQL statement to execute.
        
        .. important::
        
           **The statement must not be of a type that returns a result set.**
           In most cases (especially cases in which the same statement – perhaps 
           a parameterized statement – is executed repeatedly), it is better to 
           create a cursor using the connection’s cursor method, then execute 
           the statement using one of the cursor’s execute methods.
        
        :param string sql: SQL statement to execute.
        
        :raises ProgrammingError: When connection is closed.
        :raises DatabaseError: When error is returned from server.
        """
        self.__check_attached()
        self.main_transaction.execute_immediate(sql)
    def database_info(self, info_code, result_type):
        """Wraps the Firebird C API function `isc_database_info`.

        For documentation, see the IB 6 API Guide section entitled
        "Requesting information about an attachment" (p. 51).

        Note that this method is a VERY THIN wrapper around the FB C API
        function `isc_database_info`. This method does NOT attempt to interpret
        its results except with regard to whether they are a string or an
        integer.

        For example, requesting `isc_info_user_names` will return a string
        containing a raw succession of length-name pairs.  A thicker wrapper
        might interpret those raw results and return a Python tuple, but it
        would need to handle a multitude of special cases in order to cover
        all possible isc_info_* items.

        :param integer info_code: One of the `isc_info_*` constants.
        :param string result_type: Must be either ‘s’ if you expect a string result, 
           or ‘i’ if you expect an integer result.
        
        :raises DatabaseError: When error is returned from server.
        :raises OperationalError: When returned information is bigger than SHRT_MAX.
        :raises InternalError: On unexpected processing condition.
        :raises ValueError: On illegal `result_type` value.
        
        .. seealso:: Extracting data with the database_info function is rather 
           clumsy. See :meth:`db_info` for higher-level means of accessing the 
           same information.

        .. note::  Some of the information available through this method would be
           more easily retrieved with the Services API (see submodule
           :mod:`fdb.services`).
        """
        self.__check_attached()
        request_buffer = bs([info_code])
        buf_size = 256
        while True:
            res_buf = int2byte(0) * buf_size
            ibase.isc_database_info(self._isc_status, self._db_handle,
                                    len(request_buffer), request_buffer,
                                    len(res_buf), res_buf)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                        "Error while requesting database information:")
            i = buf_size - 1
            while i >= 0:
                if res_buf[i] != mychr(0):
                    break
                else:
                    i -= 1
            if ord2(res_buf[i]) == ibase.isc_info_truncated:
                if buf_size < SHRT_MAX:
                    buf_size *= 2
                    if buf_size > SHRT_MAX:
                        buf_size = SHRT_MAX
                    continue
                else:
                    raise OperationalError("Result is too large to fit into"
                        " buffer of size SHRT_MAX, yet underlying info "
                        " function only accepts buffers with size <= SHRT_MAX.")
            else:
                break
        if ord2(res_buf[i]) != isc_info_end:
            raise InternalError("Exited request loop sucessfuly, but"
                                " res_buf[i] != sc_info_end.")
        if request_buffer[0] != res_buf[0]:
            raise InternalError("Result code does not match request code.")
        if result_type.upper() == 'I':
            return bytes_to_int(res_buf[3:3 + bytes_to_int(res_buf[1:3])])
        elif (result_type.upper() == 'S'
              and info_code not in _DATABASE_INFO__KNOWN_LOW_LEVEL_EXCEPTIONS):
            # The result buffers for a few request codes don't follow the generic
            # conventions, so we need to return their full contents rather than
            # omitting the initial infrastructural bytes.
            return ctypes.string_at(res_buf[3:], i - 3)
        elif result_type.upper() == 'S':
            return ctypes.string_at(res_buf, i)
        else:
            raise ValueError("Unknown result type requested "
                                 "(must be 'i' or 's').")
    def db_info(self, request):
        """
        Higher-level convenience wrapper around the :meth:`database_info` method 
        that parses the output of `database_info` into Python-friendly objects 
        instead of returning raw binary buffers in the case of complex result types.

        :param request: Single `fdb.isc_info_*` info request code or a sequence 
                        of such codes.
        :returns: Mapping of (info request code -> result).
        :raises ValueError: When requested code is not recognized.
        :raises OperationalError: On unexpected processing condition.
        """
        def _extractDatabaseInfoCounts(buf):
            # Extract a raw binary sequence
            # of (unsigned short, signed int) pairs into
            # a corresponding Python dictionary.
            uShortSize = struct.calcsize('<H')
            intSize = struct.calcsize('<i')
            pairSize = uShortSize + intSize
            pairCount = len(buf) / pairSize

            counts = {}
            for i in range(pairCount):
                bufForThisPair = buf[i * pairSize:(i + 1) * pairSize]
                relationId = struct.unpack('<H',
                                           bufForThisPair[:uShortSize])[0]
                count = struct.unpack('<i', bufForThisPair[uShortSize:])[0]
                counts[relationId] = count
            return counts
        # Notes:
        #
        # - IB 6 API Guide page 391:  "In InterBase, integer values...
        #   are returned in result buffers in a generic format where
        #   the least significant byte is first, and the most
        #   significant byte last."

        # We process request as a sequence of info codes, even if only one code
        # was supplied by the caller.
        requestIsSingleton = isinstance(request, int)
        if requestIsSingleton:
            request = (request,)

        results = {}
        for infoCode in request:
            if infoCode == isc_info_base_level:
                # (IB 6 API Guide page 52)
                buf = self.database_info(infoCode, 's')
                # Ignore the first byte.
                if PYTHON_MAJOR_VER == 3:
                    baseLevel = struct.unpack('B', int2byte(buf[1]))[0]
                else:
                    baseLevel = struct.unpack('B', buf[1])[0]
                results[infoCode] = baseLevel
            elif infoCode == isc_info_db_id:
                # (IB 6 API Guide page 52)
                buf = self.database_info(infoCode, 's')
                pos = 0

                if PYTHON_MAJOR_VER == 3:
                    conLocalityCode = struct.unpack('B',
                                                int2byte(buf[pos]))[0]
                else:
                    conLocalityCode = struct.unpack('B', buf[pos])[0]
                pos += 1

                if PYTHON_MAJOR_VER == 3:
                    dbFilenameLen = struct.unpack('B', int2byte(buf[1]))[0]
                else:
                    dbFilenameLen = struct.unpack('B', buf[1])[0]
                pos += 1

                dbFilename = buf[pos:pos + dbFilenameLen]
                pos += dbFilenameLen

                if PYTHON_MAJOR_VER == 3:
                    siteNameLen = struct.unpack('B', int2byte(buf[pos]))[0]
                else:
                    siteNameLen = struct.unpack('B', buf[pos])[0]
                pos += 1

                siteName = buf[pos:pos + siteNameLen]
                pos += siteNameLen

                results[infoCode] = (conLocalityCode,
                                     p3fix(dbFilename,self._python_charset),
                                     p3fix(siteName,self._python_charset))
            elif infoCode == isc_info_implementation:
                # (IB 6 API Guide page 52)
                buf = self.database_info(infoCode, 's')
                # Skip the first four bytes.
                pos = 1

                if PYTHON_MAJOR_VER == 3:
                    implNumber = struct.unpack('B', int2byte(buf[pos]))[0]
                else:
                    implNumber = struct.unpack('B', buf[pos])[0]
                pos += 1

                if PYTHON_MAJOR_VER == 3:
                    classNumber = struct.unpack('B', int2byte(buf[pos]))[0]
                else:
                    classNumber = struct.unpack('B', buf[pos])[0]
                pos += 1

                results[infoCode] = (implNumber, classNumber)
            elif infoCode in (isc_info_version, isc_info_firebird_version):
                # (IB 6 API Guide page 53)
                buf = self.database_info(infoCode, 's')
                # Skip the first byte.
                pos = 1

                if PYTHON_MAJOR_VER == 3:
                    versionStringLen = (struct.unpack('B',
                                                  int2byte(buf[pos]))[0])
                else:
                    versionStringLen = (struct.unpack('B', buf[pos])[0])
                pos += 1

                versionString = buf[pos:pos + versionStringLen]

                results[infoCode] = p3fix(versionString,self._python_charset)
            elif infoCode == isc_info_user_names:
                # (IB 6 API Guide page 54)
                #
                # The isc_info_user_names results buffer does not exactly match
                # the format declared on page 54 of the IB 6 API Guide.
                #   The buffer is formatted as a sequence of clusters, each of
                # which begins with the byte isc_info_user_names, followed by a
                # two-byte cluster length, followed by a one-byte username
                # length, followed by a single username.
                #   I don't understand why the lengths are represented
                # redundantly (the two-byte cluster length is always one
                # greater than the one-byte username length), but perhaps it's
                # an attempt to adhere to the general format of an information
                # cluster declared on page 51 while also [trying, but failing
                # to] adhere to the isc_info_user_names-specific format
                # declared on page 54.
                buf = self.database_info(infoCode, 's')

                usernames = []
                pos = 0
                while pos < len(buf):
                    ### Todo: check Python version differences, merge as possible
                    if PYTHON_MAJOR_VER == 3:
                        if buf[pos] != isc_info_user_names:
                            raise OperationalError('While trying to service'
                                ' isc_info_user_names request, found unexpected'
                                ' results buffer contents at position %d of [%s]'
                                % (pos, buf)
                                )
                        pos += 1

                        # The two-byte cluster length:
                        nameClusterLen = (struct.unpack('<H',buf[pos:pos + 2])[0])
                        pos += 2

                        # The one-byte username length:
                        nameLen = buf[pos]
                        assert nameLen == nameClusterLen - 1
                        pos += 1

                        usernames.append(p3fix(buf[pos:pos + nameLen],
                                               self._python_charset))
                        pos += nameLen
                    else:
                        if (struct.unpack('B', buf[pos])[0]
                            != isc_info_user_names):
                            raise OperationalError('While trying to service'
                                ' isc_info_user_names request, found unexpected'
                                ' results buffer contents at position %d of [%s]'
                                % (pos, buf)
                                )
                        pos += 1

                        # The two-byte cluster length:
                        nameClusterLen = (struct.unpack('<H',buf[pos:pos + 2])[0])
                        pos += 2

                        # The one-byte username length:
                        nameLen = struct.unpack('B', buf[pos])[0]
                        assert nameLen == nameClusterLen - 1
                        pos += 1

                        usernames.append(p3fix(buf[pos:pos + nameLen],
                                               self._python_charset))
                        pos += nameLen

                # The client-exposed return value is a dictionary mapping
                # username -> number of connections by that user.
                res = {}
                for un in usernames:
                    res[un] = res.get(un, 0) + 1

                results[infoCode] = res
            elif infoCode in _DATABASE_INFO_CODES_WITH_INT_RESULT:
                results[infoCode] = self.database_info(infoCode, 'i')
            elif infoCode in _DATABASE_INFO_CODES_WITH_COUNT_RESULTS:
                buf = self.database_info(infoCode, 's')
                countsByRelId = _extractDatabaseInfoCounts(buf)
                # Decided not to convert the relation IDs to relation names
                # for two reasons:
                #  1) Performance + Principle of Least Surprise
                #     If the client program is trying to do some delicate
                #     performance measurements, it's not helpful for
                #     kinterbasdb to be issuing unexpected queries behind the
                #     scenes.
                #  2) Field RDB$RELATIONS.RDB$RELATION_NAME is a CHAR field,
                #     which means its values emerge from the database with
                #     trailing whitespace, yet it's not safe in general to
                #     strip that whitespace because actual relation names can
                #     have trailing whitespace (think
                #     'create table "table1 " (f1 int)').
                results[infoCode] = countsByRelId
            elif infoCode in _DATABASE_INFO_CODES_WITH_TIMESTAMP_RESULT:
                buf = self.database_info(infoCode, 's')
                yyyy, mm, dd = self.__parse_date(buf[:4])
                h, m, s, ms = self.__parse_time(buf[4:])
                results[infoCode] = datetime.datetime(yyyy, mm, dd, h, m, s,
                                                      ms)
            else:
                raise ValueError('Unrecognized database info code %s' % str(infoCode))

        if requestIsSingleton:
            return results[request[0]]
        else:
            return results
    def transaction_info(self, info_code, result_type):
        """Returns information about active transaction.
        Thin wrapper around Firebird API `isc_transaction_info` call. 
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.transaction_info` for details.
        
        :param integer info_code: One from next constants:
        
                                  * isc_info_tra_id
                                  * isc_info_tra_oldest_interesting
                                  * isc_info_tra_oldest_snapshot
                                  * isc_info_tra_oldest_active
                                  * isc_info_tra_isolation
                                  * isc_info_tra_access
                                  * isc_info_tra_lock_timeout

                                  See Firebird API Guide for details.
        :param string result_type: String code for result type:

                                   * ‘i’ for Integer
                                   * ‘s’ fro String

        :returns: Decoded response(s) for specified request code(s). When multiple 
                  requests are passed, returns a dictionary where key is the 
                  request code and value is the response from server.
        :raises ProgrammingError: When transaction is not active.
        :raises OperationalError: When result is too large to fit into buffer of 
                                  size SHRT_MAX.
        :raises InternalError: On unexpected processing condition.
        :raises ValueError: On illegal result_type value.
        """
        return self._main_transaction.transaction_info(info_code, result_type)
    def trans_info(self, request):
        """Pythonic wrapper around :meth:`transaction_info()` call.
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.trans_info` for details.
        
        :param request: One or more information request codes (see 
                        :meth:`transaction_info` for details). Multiple codes 
                        must be passed as tuple.
        :returns: Decoded response(s) for specified request code(s). When multiple 
                  requests are passed, returns a dictionary where key is the 
                  request code and value is the response from server.
        :raises ProgrammingError: When transaction is not active.
        :raises OperationalError: When result is too large to fit into buffer of 
                                  size SHRT_MAX.
        :raises InternalError: On unexpected processing condition.
        """
        return self._main_transaction.trans_info(request)
    def trans(self, default_tpb=None):
        """Creates a new :class:`Transaction` that operates within the context 
        of this connection. Cursors can be created within that Transaction via 
        its :meth:`~Transaction.cursor()` method.
        
        :param default_tpb: (optional) Transaction Parameter Block for newly created 
                    Transaction. If not specified, :attr:`default_tpb` is used.
        :type default_tpb: :class:`TPB` instance, list/tuple of `isc_tpb_*` constants
                   or `bytestring`
        :raises ProgrammingError: If Connection is :attr:`closed`.
        """
        self.__check_attached()
        if default_tpb:
            _tpb = default_tpb
        else:
            _tpb = self._default_tpb
        transaction = Transaction([self], default_tpb =_tpb)
        self._transactions.append(transaction)
        return transaction
    def close(self):
        """Close the connection now (rather than whenever `__del__` is called). 
        The connection will be unusable from this point forward; an :exc:`Error` 
        (or subclass) exception will be raised if any operation is attempted 
        with the connection. The same applies to all cursor and transaction 
        objects trying to use the connection.
        
        Also closes all :class:`EventConduit`, :class:`Cursor` and :class:`Transaction`
        instances associated with this connection.
        
        :raises ProgrammingError: When connection is a member of a :class:`ConnectionGroup`.
        """
        self.__ensure_group_membership(False, "Cannot close a connection that"
                                       " is a member of a ConnectionGroup.")
        self.__close()
    def begin(self, tpb=None):
        """Starts a transaction explicitly. 
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.begin` for details.
        
        :param tpb: (Optional) Transaction Parameter Buffer for newly started 
                    transaction. If not specified, :attr:`default_tpb` is used.
        :type tpb: :class:`TPB` instance, list/tuple of `isc_tpb_*` constants
                   or `bytestring`
        """
        self.__check_attached()
        self._main_transaction.begin(tpb)
    def savepoint(self, name):
        """Establishes a named SAVEPOINT for current transaction.
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.savepoint` for details.

        :param string name: Name for savepoint.
        :raises ProgrammingError: If Connection is :attr:`closed`.
        
        Example:

        .. code-block:: python
        
            con.savepoint('BEGINNING_OF_SOME_SUBTASK')
            ...
            con.rollback(savepoint='BEGINNING_OF_SOME_SUBTASK')
        """
        self.__check_attached()
        return self._main_transaction.savepoint(name)
    def commit(self, retaining=False):
        """Commit pending transaction to the database.
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.commit` for details.

        :param boolean retaining:  (Optional) Indicates whether the transactional 
                                   context of the transaction being resolved 
                                   should be recycled.
        :raises ProgrammingError: If Connection is :attr:`closed`.
        """
        self.__check_attached()
        self._main_transaction.commit(retaining)
    def rollback(self, retaining=False, savepoint=None):
        """Causes the the database to roll back to the start of pending transaction.
        Operates on :attr:`main_transaction`. 
        See :meth:`Transaction.rollback` for details.

        :param boolean retaining: (Optional) Indicates whether the transactional 
                                  context of the transaction being resolved 
                                  should be recycled.
        :param string savepoint: (Optional) Causes the transaction to roll back 
                                 only as far as the designated savepoint, rather 
                                 than rolling back entirely.
        :raises ProgrammingError: If Connection is :attr:`closed`.
        """
        self.__check_attached()
        self._main_transaction.rollback(retaining, savepoint)
    def cursor(self):
        """Return a new :class:`Cursor` instance using the connection
        associated with :attr:`main_transaction`.
        See :meth:`Transaction.cursor` for details.

        :raises ProgrammingError: If Connection is :attr:`closed`.
        """
        self.__check_attached()
        return self.main_transaction.cursor()
    def event_conduit(self,event_names):
        """Creates a conduit through which database event notifications will 
        flow into the Python program.
        
        :param event_names: A sequence of string event names.
        :returns: An :class:`EventConduit` instance.
        """
        conduit = EventConduit(self._db_handle,event_names)
        self.__conduits.append(conduit)
        return conduit
    def __del__(self):
        if self._db_handle != None:
            self.__close()
    def _get_charset(self):
        return self._charset
    def _set_charset(self, value):
        # More informative error message:
        raise AttributeError("A connection's 'charset' property can be"
            " specified upon Connection creation as a keyword argument to"
            " kinterbasdb.connect, but it cannot be modified thereafter."
            )
    def _set_group(self, group):
        # This package-private method allows ConnectionGroup's membership
        # management functionality to bypass the conceptually read-only nature
        # of the Connection.group property.
        if group:
            self.__group = weakref.ref(group,self.__remove_group)
        else:
            self.__group = None
    def _group_get(self):
        if self.__group:
            return self.__group()
        else:
            return None
    #: (Read Only) :class:`ConnectionGroup` this Connection belongs to, or None.
    group = property(_group_get)
    #: (Read Only) (string) Connection Character set name.
    charset = property(_get_charset, _set_charset)
    #: (Read Only) (tuple) :class:`Transaction` instances associated with this connection.
    transactions = property(__get_transactions)
    #: (Read Only) Main :class:`Transaction` instance for this connection
    #: Connection methods :meth:`begin`, :meth:`savepoint`, :meth:`commit` and
    #: :meth:`rollback` are delegated to this transaction object.
    main_transaction = property(__get_main_transaction)
    #: (Read/Write) Deafult Transaction Parameter Block used for all newly started transactions.
    default_tpb = property(__get_default_tpb, __set_default_tpb)
    #: (Read Only) True if connection is closed.
    closed = property(__get_closed)
    #: (Read Only) (string) Version string returned by server for this connection.
    server_version = property(__get_server_version)

class EventBlock(object):
    """
    """
    def __init__(self,queue,db_handle,event_names):
        self.__first = True
        def callback(result, length, updated):
            ctypes.memmove(result,updated,length)
            self.__queue.put((ibase.OP_RECORD_AND_REREGISTER,self))
            return 0
        
        self.__queue = queue
        self._db_handle = db_handle
        self._isc_status = ISC_STATUS_ARRAY(0)
        self.event_names = list(event_names)

        self.__results = ibase.RESULT_VECTOR(0)
        self.__closed = False
        self.__callback = ibase.ISC_EVENT_CALLBACK(callback)
        
        self.event_buf = ctypes.pointer(ibase.ISC_UCHAR(0))
        self.result_buf = ctypes.pointer(ibase.ISC_UCHAR(0))
        self.buf_length = 0
        self.event_id = ibase.ISC_LONG(0)

        self.buf_length = ibase.isc_event_block(ctypes.pointer(self.event_buf),
                                                  ctypes.pointer(self.result_buf),
                                                  *[b(x) for x in event_names])
        self.__wait_for_events()
    def __lt__(self,other):
        return self.event_id < other.event_id
    def __wait_for_events(self):
        ibase.isc_que_events(self._isc_status,self._db_handle,self.event_id,
                             self.buf_length,self.event_buf,
                             self.__callback,self.result_buf)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                                        "Error while waiting for events:")
    def count_and_reregister(self):
        result = {}
        ibase.isc_event_counts(self.__results, self.buf_length,
                               self.event_buf, self.result_buf)
        if self.__first:
            # Ignore the first call, it's for setting up the table
            self.__first = False
            self.__wait_for_events()
            return None
        
        for i in xrange(len(self.event_names)):
            result[self.event_names[i]] = int(self.__results[i])
        self.__wait_for_events()
        return result
    def close(self):
        if not self.closed:
            ibase.isc_cancel_events(self._isc_status,self._db_handle,self.event_id)
            self.__closed = True
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while canceling events:")
    def __get_closed(self):
        return self.__closed
    def __del__(self):
        self.close()
    closed = property(__get_closed)


class EventConduit(object):
    """Represents a conduit through which database event notifications will flow 
    into the Python program.
    
    .. important::
    
       DO NOT create instances of this class directly! Use only 
       :meth:`Connection.event_conduit` to get EventConduit instances.
    
    From the moment the conduit is created by the :meth:`Connection.event_conduit` 
    method, notifications of any events that occur will accumulate asynchronously 
    within the conduit’s internal queue until the conduit is closed either 
    explicitly (via the :meth:`close` method) or implicitly (via garbage collection).
    """
    def __init__(self,db_handle,event_names):
        """
        :param db_handle: Database handle.
        :param event_names: List of strings that represent event names.
        """
        def event_process(queue):
            while True:
                operation, data = queue.get()
                if operation == ibase.OP_RECORD_AND_REREGISTER:
                    events = data.count_and_reregister()
                    if events:
                        for key,value in events.items():
                            self.__events[key] += value
                        self.__events_ready.set()
                elif operation == ibase.OP_DIE:
                    return

        self._db_handle = db_handle
        self._isc_status = ISC_STATUS_ARRAY(0)
        self.__event_names = list(event_names)
        self.__events = {}.fromkeys(self.__event_names,0)
        self.__event_blocks = []
        self.__closed = False
        self.__queue = ibase.PriorityQueue()
        self.__events_ready = threading.Event()
        self.__process_thread = threading.Thread(target=event_process,args=(self.__queue,))
        self.__process_thread.start()

        blocks = [[x for x in y if x] for y in izip_longest(*[iter(event_names)]*15)]
        for block_events in blocks:
            self.__event_blocks.append(EventBlock(self.__queue,db_handle,block_events))

    def wait(self,timeout=None):
        """Wait for events.
        
        Blocks the calling thread until at least one of the events occurs, or 
        the specified timeout (if any) expires.
        
        :param timeout: Number of seconds (use a float to indicate fractions of 
                        seconds). If not even one of the relevant events has 
                        occurred after timeout seconds, this method will unblock 
                        and return None. The default timeout is infinite.
        :type timeout: integer or float
        :returns: `None` if the wait timed out, otherwise a dictionary that maps 
                  `event_name -> event_occurrence_count`.
        
        Example:
        
        .. code-block:: python
        
           >>>conduit = connection.event_conduit( ('event_a', 'event_b') )
           >>>conduit.wait()
           {
            'event_a': 1,
            'event_b': 0
           }
        
        In the example above `event_a` occurred once and `event_b` did not occur 
        at all.
        """
        if not self.closed:
            self.__events_ready.wait(timeout)
            return self.__events.copy()
    def flush(self):
        """Clear any event notifications that have accumulated in the conduit’s 
        internal queue.
        """
        if not self.closed:
            self.__events_ready.clear()
            self.__events = {}.fromkeys(self.__event_names,0)
    def close(self):
        """Cancels the standing request for this conduit to be notified of events.
        
        After this method has been called, this EventConduit object is useless, 
        and should be discarded.
        """
        if not self.closed:
            self.__queue.put((ibase.OP_DIE,self))
            self.__process_thread.join()
            for block in self.__event_blocks:
                block.close()
            self.__closed = True
    def __get_closed(self):
        return self.__closed
    def __del__(self):
        self.close()
    #: (Read Only) (boolean) True if conduit is closed.
    closed = property(__get_closed)

class PreparedStatement(object):
    """Represents a prepared statement, an "inner" database cursor, which is used
    to manage the SQL statement execution and context of a fetch operation.

    .. important::
    
       DO NOT create instances of this class directly! Use only :meth:`Cursor.prep` 
       to get PreparedStatement instances.
       
    .. note::
    
       PreparedStatements are bound to :class:`Cursor` instance that created them,
       and using them with other Cursor would report an error.
    """
    #: Constant for internal use by this class.
    RESULT_SET_EXHAUSTED = 100
    #: Constant for internal use by this class.
    NO_FETCH_ATTEMPTED_YET = -1

    def __init__(self, operation, cursor, internal=True):
        self.__sql = operation
        self.__internal = internal
        if internal:
            self.cursor = weakref.ref(cursor)
        else:
            #: :class:`Cursor` instance that manages this PreparedStatement.
            self.cursor = cursor
        self._stmt_handle = None  # isc_stmt_handle(0)
        self._isc_status = ISC_STATUS_ARRAY()
        #: Internal XSQLDA structure for output values.
        self.out_sqlda = xsqlda_factory(10)
        #: Internal XSQLDA structure for input values.
        self.in_sqlda = xsqlda_factory(10)
        #: Internal list to save original input SQLDA structures when they has 
        #: to temporarily augmented.
        self.in_sqlda_save = []
        #: (integer) An integer code that can be matched against the statement 
        #: type constants in the isc_info_sql_stmt_* series.
        self.statement_type = None
        self.__streamed_blobs = []
        self.__blob_readers = []
        self.__executed = False
        self.__prepared = False
        self.__closed = False
        self.__description = None
        self.__output_cache = None
        #self.out_buffer = None
        self._last_fetch_status = ibase.ISC_STATUS(self.NO_FETCH_ATTEMPTED_YET)
        connection = self.__get_connection()
        self.__charset = connection.charset
        self.__python_charset = connection._python_charset
        self.__sql_dialect = connection.sql_dialect

        # allocate statement handle
        self._stmt_handle = ibase.isc_stmt_handle(0)
        ibase.isc_dsql_allocate_statement(self._isc_status,
                                          connection._db_handle,
                                          self._stmt_handle)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                                        "Error while allocating SQL statement:")
        # prepare statement
        op = b(operation,self.__python_charset)
        ibase.isc_dsql_prepare(self._isc_status,
                               self.__get_transaction()._tr_handle,
                               self._stmt_handle,
                               len(op),op,
                               self.__sql_dialect,
                               ctypes.cast(ctypes.pointer(self.out_sqlda),
                                           XSQLDA_PTR))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                "Error while preparing SQL statement:")
        # Determine statement type
        info = b(' ') * 20
        ibase.isc_dsql_sql_info(self._isc_status, self._stmt_handle, 1,
                                bs([ibase.isc_info_sql_stmt_type]),
                                len(info), info)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                "Error while determining SQL statement type:")
        if ord2(info[0]) != ibase.isc_info_sql_stmt_type:
            raise InternalError("Cursor.execute, determine statement type:\n"
                "first byte must be 'isc_info_sql_stmt_type'")
        self.statement_type = bytes_to_int(info[3:3 + bytes_to_int(info[1:3])])
        # Init XSQLDA for input parameters
        ibase.isc_dsql_describe_bind(self._isc_status, self._stmt_handle,
                                     self.__sql_dialect,
                                     ctypes.cast(ctypes.pointer(self.in_sqlda),
                                                 XSQLDA_PTR))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                "Error while determining SQL statement parameters:")
        if self.in_sqlda.sqld > self.in_sqlda.sqln:
            self.in_sqlda = xsqlda_factory(self.in_sqlda.sqld)
            ibase.isc_dsql_describe_bind(self._isc_status, self._stmt_handle,
                                         self.__sql_dialect,
                                         ctypes.cast(ctypes.pointer(self.in_sqlda),
                                                     XSQLDA_PTR))
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                    "Error while determining SQL statement parameters:")
        #: The number of input parameters the statement requires.
        self.n_input_params = self.in_sqlda.sqld
        # record original type and size information so it can be restored for
        # subsequent executions (mind the implicit string conversions!)
        for sqlvar in self.in_sqlda.sqlvar[:self.n_input_params]:
            self.in_sqlda_save.append((sqlvar.sqltype, sqlvar.sqllen))
        # Init output XSQLDA
        ibase.isc_dsql_describe(self._isc_status, self._stmt_handle,
                                self.__sql_dialect,
                                ctypes.cast(ctypes.pointer(self.out_sqlda),
                                            XSQLDA_PTR))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                "Error while determining SQL statement output:")
        if self.out_sqlda.sqld > self.out_sqlda.sqln:
            self.out_sqlda = xsqlda_factory(self.out_sqlda.sqld)
            ibase.isc_dsql_describe(self._isc_status, self._stmt_handle,
                                    self.__sql_dialect,
                                    ctypes.cast(ctypes.pointer(self.out_sqlda),
                                                XSQLDA_PTR))
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                    "Error while determining SQL statement output:")
        #: The number of output fields the statement produces.
        self.n_output_params = self.out_sqlda.sqld
        self.__coerce_XSQLDA(self.out_sqlda)
        self.__prepared = True
        self._name = None
    def __get_name(self):
        return self._name
    def __set_name(self,name):
        if self._name:
            raise ProgrammingError("Cursor's name has already been declared")
        self._set_cursor_name(name)
    def __get_connection(self):
        if self.__internal:
            cur = self.cursor()
        else:
            cur = self.cursor
        if cur:
            return cur._connection if isinstance(cur._connection,Connection) else cur._connection()
        else:
            return None
    def __get_transaction(self):
        if self.__internal:
            return self.cursor()._transaction
        else:
            return self.cursor._transaction
    def __get_closed(self):
        return self.__closed
    def __get_plan(self):
        buf_size = 256
        while True:
            info = b(' ') * buf_size
            ibase.isc_dsql_sql_info(self._isc_status, self._stmt_handle, 2,
                                    bs([ibase.isc_info_sql_get_plan,isc_info_end]),
                                    len(info), info)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while determining rowcount:")
            if ord2(info[0]) == ibase.isc_info_truncated:
                if buf_size < SHRT_MAX:
                    buf_size *= 2
                    if buf_size > SHRT_MAX:
                        buf_size = SHRT_MAX
                    continue
                else:
                    return "Plan is too big"
            else:
                break
        if ord2(info[0]) == isc_info_end:
            return None
        if ord2(info[0]) != ibase.isc_info_sql_get_plan:
            raise IndentationError("Unexpected code in result buffer while"
                                   " querying SQL plan.")
        size = bytes_to_int(info[1:_SIZE_OF_SHORT + 1])
        # Skip first byte: a new line
        ### Todo: Better handling of P version specifics
        result = ctypes.string_at(info[_SIZE_OF_SHORT + 2:], size - 1)
        if PYTHON_MAJOR_VER == 3:
            return b2u(result,self.__python_charset)
            #return result.decode(charset_map.get(self.__charset,self.__charset))
        else:
            return result
    def __get_sql(self):
        return self.__sql
    def __is_fixed_point(self, dialect, data_type, subtype, scale):
        return ((data_type in [SQL_SHORT, SQL_LONG,
                               SQL_INT64]
                 and (subtype or scale)
                 )
                or ((dialect < 3) and scale and
                    (data_type in [SQL_DOUBLE, SQL_D_FLOAT]))
                )
    def __get_external_data_type_name(self, dialect, data_type, subtype,
                                      scale):
        if data_type == SQL_TEXT:
            return 'CHAR'
        elif data_type == SQL_VARYING:
            return 'VARCHAR'
        elif self.__is_fixed_point(dialect, data_type, subtype, scale):
            if subtype == SUBTYPE_NUMERIC:
                return 'NUMERIC'
            elif subtype == SUBTYPE_DECIMAL:
                return 'DECIMAL'
            else:
                return 'NUMERIC/DECIMAL'
        elif data_type == SQL_SHORT:
            return 'SMALLINT'
        elif data_type == SQL_LONG:
            return 'INTEGER'
        elif data_type == SQL_INT64:
            return 'BIGINT'
        elif data_type == SQL_FLOAT:
            return 'FLOAT'
        elif data_type in [SQL_DOUBLE, SQL_D_FLOAT]:
            return 'DOUBLE'
        elif data_type == SQL_TIMESTAMP:
            return 'TIMESTAMP'
        elif data_type == SQL_TYPE_DATE:
            return 'DATE'
        elif data_type == SQL_TYPE_TIME:
            return 'TIME'
        elif data_type == SQL_BLOB:
            return 'BLOB'
        else:
            return 'UNKNOWN'
    def __get_internal_data_type_name(self, data_type):
        if data_type == SQL_TEXT:
            return 'SQL_TEXT'
        elif data_type == SQL_VARYING:
            return 'SQL_VARYING'
        elif data_type == SQL_SHORT:
            return 'SQL_SHORT'
        elif data_type == SQL_LONG:
            return 'SQL_LONG'
        elif data_type == SQL_INT64:
            return 'SQL_INT64'
        elif data_type == SQL_FLOAT:
            return 'SQL_FLOAT'
        elif data_type in [SQL_DOUBLE, SQL_D_FLOAT]:
            return 'SQL_DOUBLE'
        elif data_type == SQL_TIMESTAMP:
            return 'SQL_TIMESTAMP'
        elif data_type == SQL_TYPE_DATE:
            return 'SQL_TYPE_DATE'
        elif data_type == SQL_TYPE_TIME:
            return 'SQL_TYPE_TIME'
        elif data_type == SQL_BLOB:
            return 'SQL_BLOB'
        else:
            return 'UNKNOWN'
    def __get_description(self):
        if not self.__description:
            desc = []
            if self.__prepared and (self.out_sqlda.sqld > 0):
                for sqlvar in self.out_sqlda.sqlvar[:self.out_sqlda.sqld]:
                    # Field name (or alias)
                    sqlname = p3fix(sqlvar.sqlname[:sqlvar.sqlname_length],
                                    self.__python_charset)
                    alias = p3fix(sqlvar.aliasname[:sqlvar.aliasname_length],
                                  self.__python_charset)
                    if alias != sqlname:
                        sqlname = alias
                    # Type information
                    intsize = sqlvar.sqllen
                    vartype = sqlvar.sqltype & ~1
                    scale = sqlvar.sqlscale
                    precision = 0
                    if vartype in [SQL_TEXT, SQL_VARYING]:
                        vtype = StringType
                        dispsize = sqlvar.sqllen
                    elif (vartype in [SQL_SHORT, SQL_LONG,
                                      SQL_INT64]
                          and (sqlvar.sqlsubtype or scale)):
                        vtype = decimal.Decimal
                        precision = (self.__get_connection()._determine_field_precision(sqlvar))
                        dispsize = 20
                    elif vartype == SQL_SHORT:
                        vtype = IntType
                        dispsize = 6
                    elif vartype == SQL_LONG:
                        vtype = IntType
                        dispsize = 11
                    elif vartype == SQL_INT64:
                        vtype = LongType
                        dispsize = 20
                    elif vartype in [SQL_FLOAT, SQL_DOUBLE,
                                     SQL_D_FLOAT]:
                        # Special case, dialect 1 DOUBLE/FLOAT
                        # could be Fixed point
                        if (self.__sql_dialect < 3) and scale:
                            vtype = decimal.Decimal
                            precision = (self.__get_connection()._determine_field_precision(sqlvar))
                        else:
                            vtype = FloatType
                        dispsize = 17
                    elif vartype == SQL_BLOB:
                        scale = sqlvar.sqlsubtype
                        vtype = StringType
                        dispsize = 0
                    elif vartype == SQL_TIMESTAMP:
                        vtype = datetime.datetime
                        dispsize = 22
                    elif vartype == SQL_TYPE_DATE:
                        vtype = datetime.date
                        dispsize = 10
                    elif vartype == SQL_TYPE_TIME:
                        vtype = datetime.time
                        dispsize = 11
                    elif vartype == SQL_ARRAY:
                        vtype = ListeType
                        dispsize = -1
                    else:
                        vtype = None
                        dispsize = -1
                    desc.append(tuple([sqlname, vtype, dispsize, intsize,
                                       precision, scale,
                                       bool(sqlvar.sqltype & 1)]))
                self.__description = tuple(desc)
        return self.__description
    def __get_rowcount(self):
        result = -1
        if (self.__executed and
            self.statement_type in [isc_info_sql_stmt_select,
                                    isc_info_sql_stmt_insert,
                                    isc_info_sql_stmt_update,
                                    isc_info_sql_stmt_delete]):
            info = b(' ') * 64
            ibase.isc_dsql_sql_info(self._isc_status, self._stmt_handle, 2,
                                    bs([ibase.isc_info_sql_records,
                                        isc_info_end]),
                                    len(info), info)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while determining rowcount:")
            if ord2(info[0]) != ibase.isc_info_sql_records:
                raise InternalError("Cursor.get_rowcount:\n"
                                    "first byte must be 'isc_info_sql_records'")
            res_walk = 3
            short_size = ctypes.sizeof(ctypes.c_short)
            while ord2(info[res_walk]) != isc_info_end:
                cur_count_type = ord2(info[res_walk])
                res_walk += 1
                size = bytes_to_int(info[res_walk:res_walk + short_size])
                res_walk += short_size
                count = bytes_to_int(info[res_walk:res_walk + size])
                if ((cur_count_type == ibase.isc_info_req_select_count
                     and self.statement_type == isc_info_sql_stmt_select)
                    or (cur_count_type == ibase.isc_info_req_insert_count
                        and self.statement_type == isc_info_sql_stmt_insert)
                    or (cur_count_type == ibase.isc_info_req_update_count
                        and self.statement_type == isc_info_sql_stmt_update)
                    or (cur_count_type == ibase.isc_info_req_delete_count
                        and self.statement_type == isc_info_sql_stmt_delete)
                    ):
                    result = count
                res_walk += size
        return result
    def _parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_int(raw_value) + 678882
        century = (4 * nday - 1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day - 3) // 153
        day = 5 * day - 3 - 153 * month
        day = (day + 5) // 5
        year = 100 * century + nday
        if month < 10:
            month += 3
        else:
            month -= 9
            year += 1
        return year, month, day
    def _parse_time(self, raw_value):
        "Convert raw data to datetime.time"
        n = bytes_to_int(raw_value)
        s = n // 10000
        m = s // 60
        h = m // 60
        m = m % 60
        s = s % 60
        return (h, m, s, (n % 10000) * 100)
    def _convert_date(self, v):  # Convert datetime.date to BLR format data
        i = v.month + 9
        jy = v.year + (i // 12) - 1
        jm = i % 12
        c = jy // 100
        jy -= 100 * c
        j = ((146097 * c) // 4 + (1461 * jy) // 4
             + (153 * jm + 2) // 5 + v.day - 678882)
        return int_to_bytes(j, 4)
    def _convert_time(self, v):  # Convert datetime.time to BLR format time
        t = ((v.hour * 3600 + v.minute * 60 + v.second) * 10000
             + v.microsecond // 100)
        return int_to_bytes(t, 4)
    def _convert_timestamp(self, v):   # Convert datetime.datetime
                                        # to BLR format timestamp
        return self._convert_date(v.date()) + self._convert_time(v.time())
    def _check_integer_rage(self, value, dialect, data_type, subtype, scale):
        if data_type == SQL_SHORT:
            vmin = SHRT_MIN
            vmax = SHRT_MAX
        elif data_type == SQL_LONG:
            vmin = INT_MIN
            vmax = INT_MAX
        elif data_type == SQL_INT64:
            vmin = LONG_MIN
            vmax = LONG_MAX
        if (value < vmin) or (value > vmax):
            msg = """numeric overflow: value %s
 (%s scaled for %d decimal places) is of
 too great a magnitude to fit into its internal storage type %s,
 which has range [%s,%s].""" % (str(value),
                                self.__get_external_data_type_name(dialect,
                                                                   data_type,
                                                                   subtype,
                                                                   scale),
                                scale,
                                self.__get_internal_data_type_name(data_type),
                                str(vmin), str(vmax))
            raise ProgrammingError(msg, -802)
    def __coerce_XSQLDA(self, xsqlda):
        """Allocate space for SQLVAR data.
        """
        for sqlvar in xsqlda.sqlvar[:self.out_sqlda.sqld]:
            if sqlvar.sqltype & 1:
                sqlvar.sqlind = ctypes.pointer(ISC_SHORT(0))
            vartype = sqlvar.sqltype & ~1
            if vartype in [SQL_TEXT, SQL_VARYING]:
                sqlvar.sqldata = ctypes.create_string_buffer(sqlvar.sqllen + 2)
            elif vartype == SQL_SHORT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_LONG:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_INT64:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_FLOAT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_DOUBLE:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_D_FLOAT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_BLOB:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_TIMESTAMP:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_TYPE_DATE:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_TYPE_TIME:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            elif vartype == SQL_ARRAY:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(
                    sqlvar.sqllen),buf_pointer)
            else:
                pass
    def __XSQLDA2Tuple(self, xsqlda):
        """Move data from output XSQLDA to result tuple.
        """
        values = []
        for sqlvar in xsqlda.sqlvar[:xsqlda.sqld]:
            value = '<NOT_IMPLEMENTED>'
            vartype = sqlvar.sqltype & ~1
            scale = sqlvar.sqlscale
            # NULL handling
            if ((sqlvar.sqltype & 1) != 0) and (bool(sqlvar.sqlind)
                                                and sqlvar.sqlind.contents.value == -1):
                value = None
            elif vartype == SQL_TEXT:
                value = ctypes.string_at(sqlvar.sqldata,sqlvar.sqllen)
                #value = sqlvar.sqldata[:sqlvar.sqllen]
                ### Todo: verify handling of P version differences
                if ((self.__charset or PYTHON_MAJOR_VER == 3) 
                    and sqlvar.sqlsubtype != 1):   # non OCTETS
                    value = b2u(value,self.__python_charset)
                # CHAR with multibyte encoding requires special handling
                if sqlvar.sqlsubtype in (4, 69):  # UTF8 and GB18030
                    reallength = sqlvar.sqllen // 4
                elif sqlvar.sqlsubtype == 3:  # UNICODE_FSS
                    reallength = sqlvar.sqllen // 3
                else:
                    reallength = sqlvar.sqllen
                value = value[:reallength]
            elif vartype == SQL_VARYING:
                size = bytes_to_int(sqlvar.sqldata[:1])
                #value = ctypes.string_at(sqlvar.sqldata[2],2+size)
                ### Todo: verify handling of P version differences
                if PYTHON_MAJOR_VER == 3:
                    value = bytes(sqlvar.sqldata[2:2 + size])
                else:
                    value = str(sqlvar.sqldata[2:2 + size])
                if ((self.__charset or PYTHON_MAJOR_VER == 3) 
                    and sqlvar.sqlsubtype != 1):   # non OCTETS
                    value = b2u(value,self.__python_charset)
            elif vartype in [SQL_SHORT, SQL_LONG, SQL_INT64]:
                value = bytes_to_int(sqlvar.sqldata[:sqlvar.sqllen])
                # It's scalled integer?
                if (sqlvar.sqlsubtype or scale):
                    value = decimal.Decimal(value) / _tenTo[abs(scale)]
            elif vartype == SQL_TYPE_DATE:
                yyyy, mm, dd = self._parse_date(sqlvar.sqldata[:sqlvar.sqllen])
                value = datetime.date(yyyy, mm, dd)
            elif vartype == SQL_TYPE_TIME:
                h, m, s, ms = self._parse_time(sqlvar.sqldata[:sqlvar.sqllen])
                value = datetime.time(h, m, s, ms)
            elif vartype == SQL_TIMESTAMP:
                yyyy, mm, dd = self._parse_date(sqlvar.sqldata[:4])
                h, m, s, ms = self._parse_time(sqlvar.sqldata[4:sqlvar.sqllen])
                value = datetime.datetime(yyyy, mm, dd, h, m, s, ms)
            elif vartype == SQL_FLOAT:
                value = struct.unpack('f', sqlvar.sqldata[:sqlvar.sqllen])[0]
            elif vartype == SQL_DOUBLE:
                value = struct.unpack('d', sqlvar.sqldata[:sqlvar.sqllen])[0]
            elif vartype == SQL_BLOB:
                val = sqlvar.sqldata[:sqlvar.sqllen]
                blobid = ibase.ISC_QUAD(bytes_to_int(val[:4]),
                                        bytes_to_int(val[4:sqlvar.sqllen]))
                # Check if stream BLOB is requested instead materialized one
                use_stream = False
                if self.__streamed_blobs:
                    # Get the BLOB name
                    sqlname = p3fix(sqlvar.sqlname[:sqlvar.sqlname_length],
                                    self.__python_charset)
                    alias = p3fix(sqlvar.aliasname[:sqlvar.aliasname_length],
                                  self.__python_charset)
                    if alias != sqlname:
                        sqlname = alias
                    if sqlname in self.__streamed_blobs:
                        use_stream = True
                if use_stream:
                    # Stream BLOB
                    value = BlobReader(blobid,self.__get_connection()._db_handle,
                                       self.__get_transaction()._tr_handle,
                                       sqlvar.sqlsubtype == 1,
                                       self.__charset)
                    self.__blob_readers.append(value)
                else:
                    # Materialized BLOB
                    blob_handle = ibase.isc_blob_handle()
                    ibase.isc_open_blob2(self._isc_status,
                                         self.__get_connection()._db_handle,
                                         self.__get_transaction()._tr_handle,
                                         blob_handle, blobid, 0, None)
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,
                                                    self._isc_status,
                                                    "Cursor.read_output_blob/isc_open_blob2:")
                    # Get BLOB total length and max. size of segment
                    result = ctypes.cast(ctypes.create_string_buffer(20),
                                         buf_pointer)
                    ibase.isc_blob_info(self._isc_status, blob_handle, 2,
                                        bs([ibase.isc_info_blob_total_length,
                                            ibase.isc_info_blob_max_segment]),
                                        20, result)
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,
                                                    self._isc_status,
                                                    "Cursor.read_output_blob/isc_blob_info:")
                    offset = 0
                    while bytes_to_int(result[offset]) != isc_info_end:
                        code = bytes_to_int(result[offset])
                        offset += 1
                        if code == ibase.isc_info_blob_total_length:
                            length = bytes_to_int(result[offset:offset + 2])
                            blob_length = bytes_to_int(result[
                                offset + 2:offset + 2 + length])
                            offset += length + 2
                        elif code == ibase.isc_info_blob_max_segment:
                            length = bytes_to_int(result[offset:offset + 2])
                            segment_size = bytes_to_int(result[
                                offset + 2:offset + 2 + length])
                            offset += length + 2
                    # Load BLOB
                    allow_incomplete_segment_read = False
                    status = ibase.ISC_STATUS(0)
                    blob = ctypes.create_string_buffer(blob_length)
                    bytes_read = 0
                    bytes_actually_read = ctypes.c_ushort(0)
                    while bytes_read < blob_length:
                        status = ibase.isc_get_segment(self._isc_status,
                                                       blob_handle,
                                                       bytes_actually_read,
                                                       min(segment_size,
                                                           blob_length - bytes_read),
                                                       ctypes.byref(
                                                           blob, bytes_read))
                        if status != 0:
                            if ((status == ibase.isc_segment)
                                and allow_incomplete_segment_read):
                                bytes_read += bytes_actually_read.value
                            else:
                                raise exception_from_status(DatabaseError,
                                                            self._isc_status,
                                                            "Cursor.read_output_blob/isc_get_segment:")
                        else:
                            bytes_read += bytes_actually_read.value
                    # Finish
                    ibase.isc_close_blob(self._isc_status, blob_handle)
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,
                                                    self._isc_status,
                                                    "Cursor.read_otput_blob/isc_close_blob:")
                    value = blob.value
                    if ((self.__charset or PYTHON_MAJOR_VER == 3) 
                        and sqlvar.sqlsubtype == 1):
                        value = b2u(value,self.__python_charset)
            elif vartype == SQL_ARRAY:
                value = []
            values.append(value)

        return tuple(values)
    def __Tuple2XSQLDA(self, xsqlda, parameters):
        """Move data from parameters to input XSQLDA.
        """
        for i in xrange(xsqlda.sqld):
            sqlvar = xsqlda.sqlvar[i]
            value = parameters[i]
            vartype = sqlvar.sqltype & ~1
            scale = sqlvar.sqlscale
            # NULL handling
            if value == None:
                # Set the null flag whether sqlvar definition allows it or not,
                # to give BEFORE triggers to act on value without
                # our interference.
                if (sqlvar.sqltype & 1) == 0:
                    # NULLs were not allowed, so set it allowed or FB will complain
                    sqlvar.sqltype += 1
                sqlvar.sqlind = ctypes.pointer(ISC_SHORT(-1))
                sqlvar.sqldata = None
            else:
                # if sqlvar allows null, allocate the null flag
                # I don't know whether it's necessary,
                # but we'll do it anyway for safety
                if ((sqlvar.sqltype & 1) != 0):
                    sqlvar.sqlind = ctypes.pointer(ISC_SHORT(0))
                # Fill in value by type
                if ((vartype != SQL_BLOB and
                     isinstance(value, (StringType, UnicodeType)))
                    or vartype in [SQL_TEXT, SQL_VARYING]):
                    ### Todo: verify handling of P version differences
                    if PYTHON_MAJOR_VER == 3:
                        if not isinstance(value, StringType):
                            value = str(value)
                    # Place for Implicit Conversion of Input Parameters
                    # from Strings
                    if isinstance(value, UnicodeType):
                        value = value.encode(self.__python_charset)
                    ### Todo: verify handling of P version differences
                    if PYTHON_MAJOR_VER != 3:
                        if not isinstance(value, StringType):
                            value = str(value)
                    if vartype in [SQL_TEXT, SQL_VARYING] and len(value) > sqlvar.sqllen:
                        raise ValueError("Value of parameter (%i) is too long,"
                                         " expected %i, found %i" % (i, sqlvar.sqllen,
                                                                     len(value)))
                    sqlvar.sqltype = SQL_TEXT | (sqlvar.sqltype & 1)
                    sqlvar.sqllen = ctypes.c_short(len(value))
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(
                        ctypes.create_string_buffer(value)), buf_pointer)
                elif vartype in [SQL_SHORT, SQL_LONG,
                                 SQL_INT64]:
                    # It's scalled integer?
                    if (sqlvar.sqlsubtype or scale):
                        if isinstance(value, decimal.Decimal):
                            value = int(
                                (value * _tenTo[abs(scale)]).to_integral())
                        elif isinstance(value, (int, mylong, float,)):
                            value = int(value * _tenTo[abs(scale)])
                        else:
                            raise TypeError('Objects of type %s are not '
                                            ' acceptable input for'
                                            ' a fixed-point column.' % str(type(value)))
                    self._check_integer_rage(value,
                                             self.__sql_dialect,
                                             vartype, sqlvar.sqlsubtype,
                                             sqlvar.sqlscale)
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(
                        ctypes.create_string_buffer(
                            int_to_bytes(value, sqlvar.sqllen))), buf_pointer)
                elif vartype == SQL_TYPE_DATE:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(
                        ctypes.create_string_buffer(
                            self._convert_date(value))), buf_pointer)
                elif vartype == SQL_TYPE_TIME:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(
                        ctypes.create_string_buffer(
                            self._convert_time(value))), buf_pointer)
                elif vartype == SQL_TIMESTAMP:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(
                        ctypes.create_string_buffer(
                            self._convert_timestamp(value))), buf_pointer)
                elif vartype == SQL_FLOAT:
                    sqlvar.sqldata = ctypes.cast(
                        ctypes.pointer(ctypes.create_string_buffer(
                            struct.pack('f', value))), buf_pointer)
                elif vartype == SQL_DOUBLE:
                    sqlvar.sqldata = ctypes.cast(
                        ctypes.pointer(ctypes.create_string_buffer(
                            struct.pack('d', value))), buf_pointer)
                elif vartype == SQL_BLOB:
                    blobid = ibase.ISC_QUAD(0, 0)
                    blob_handle = ibase.isc_blob_handle()
                    ### Todo: verify handling of P version differences
                    if ((self.__charset or 
                         (PYTHON_MAJOR_VER == 3 and isinstance(value, str))) 
                        and sqlvar.sqlsubtype == 1):
                        value = value.encode(self.__python_charset)
                    if hasattr(value,'read'):
                        # It seems we've got file-like object, use stream BLOB
                        ibase.isc_create_blob2(self._isc_status,
                                               self.__get_connection()._db_handle,
                                               self.__get_transaction()._tr_handle,
                                               blob_handle, blobid, 4, 
                                               bs([ibase.isc_bpb_version1,
                                                   ibase.isc_bpb_type,1,
                                                   ibase.isc_bpb_type_stream]))
                        if db_api_error(self._isc_status):
                            raise exception_from_status(DatabaseError,
                                                        self._isc_status,
                                                        "Cursor.write_input_blob/isc_create_blob2:")
                        sqlvar.sqldata = ctypes.cast(ctypes.pointer(blobid),
                                                     buf_pointer)
                        blob = ctypes.create_string_buffer(MAX_BLOB_SEGMENT_SIZE)
                        blob.raw = ibase.b(value.read(MAX_BLOB_SEGMENT_SIZE))
                        while len(blob.value) > 0:
                            ibase.isc_put_segment(self._isc_status, blob_handle,
                                                  len(blob.value),
                                                  ctypes.byref(blob)
                                                  )
                            if db_api_error(self._isc_status):
                                raise exception_from_status(DatabaseError,
                                                            self._isc_status,
                                                            "Cursor.write_input_blob/isc_put_segment:")
                            ctypes.memset(blob,0,MAX_BLOB_SEGMENT_SIZE)
                            blob.raw = ibase.b(value.read(MAX_BLOB_SEGMENT_SIZE))
                        ibase.isc_close_blob(self._isc_status, blob_handle)
                        if db_api_error(self._isc_status):
                            raise exception_from_status(DatabaseError,
                                                        self._isc_status,
                                                        "Cursor.write_input_blob/isc_close_blob:")
                    else:
                        # Non-stream BLOB
                        if isinstance(value, str if PYTHON_MAJOR_VER == 3 else UnicodeType):
                            raise TypeError('Unicode strings are not'
                                            ' acceptable input for'
                                            ' a non-textual BLOB column.')
                        blob = ctypes.create_string_buffer(value)
                        ibase.isc_create_blob2(self._isc_status,
                                               self.__get_connection()._db_handle,
                                               self.__get_transaction()._tr_handle,
                                               blob_handle, blobid, 0, None)
                        if db_api_error(self._isc_status):
                            raise exception_from_status(DatabaseError,
                                                        self._isc_status,
                                                        "Cursor.write_input_blob/isc_create_blob2:")
                        sqlvar.sqldata = ctypes.cast(ctypes.pointer(blobid),
                                                     buf_pointer)
                        total_size = len(value)
                        bytes_written_so_far = 0
                        bytes_to_write_this_time = MAX_BLOB_SEGMENT_SIZE
                        while (bytes_written_so_far < total_size):
                            if (
                                (total_size - bytes_written_so_far) <
                                MAX_BLOB_SEGMENT_SIZE
                                ):
                                bytes_to_write_this_time = (total_size -
                                                            bytes_written_so_far)
                            ibase.isc_put_segment(self._isc_status, blob_handle,
                                                  bytes_to_write_this_time,
                                                  ctypes.byref(blob,
                                                               bytes_written_so_far
                                                               )
                                                  )
                            if db_api_error(self._isc_status):
                                raise exception_from_status(DatabaseError,
                                                            self._isc_status,
                                                            "Cursor.write_input_blob/isc_put_segment:")
                            bytes_written_so_far += bytes_to_write_this_time
                        ibase.isc_close_blob(self._isc_status, blob_handle)
                        if db_api_error(self._isc_status):
                            raise exception_from_status(DatabaseError,
                                                        self._isc_status,
                                                        "Cursor.write_input_blob/isc_close_blob:")
                elif vartype == SQL_ARRAY:
                    raise NotImplementedError('array')
    def _free_handle(self):
        if self._stmt_handle != None and not self.__closed:
            self.__executed = False
            self.__closed = True
            self.__output_cache = None
            self._name = None
            while len(self.__blob_readers) > 0:
                self.__blob_readers.pop().close()
            if self.statement_type == isc_info_sql_stmt_select:
                ibase.isc_dsql_free_statement(self._isc_status,
                                              self._stmt_handle,
                                              ibase.DSQL_close)
                if db_api_error(self._isc_status):
                    raise exception_from_status(DatabaseError, self._isc_status,
                                  "Error while releasing SQL statement handle:")
    def _callproc(self, procname, parameters=None):
        raise NotImplementedError('PreparedStatement.callproc')
    def _close(self):
        if self._stmt_handle != None:
            while len(self.__blob_readers) > 0:
                self.__blob_readers.pop().close()
            stmt_handle = self._stmt_handle
            self._stmt_handle = None
            self.__executed = False
            self.__prepared = False
            self.__closed = True
            self.__description = None
            self.__output_cache = None
            self._name = None
            #self.out_buffer = None
            connection = self.__get_connection()
            if connection and not connection.closed:
                ibase.isc_dsql_free_statement(self._isc_status, stmt_handle,
                                              ibase.DSQL_drop)
                if db_api_error(self._isc_status):
                    raise exception_from_status(DatabaseError, self._isc_status,
                                                "Error while closing SQL statement:")
    def _execute(self, parameters=None):
        # Bind parameters
        if parameters:
            if not isinstance(parameters, (ListeType, TupleType)):
                raise TypeError("parameters must be list or tuple")
            if len(parameters) > self.in_sqlda.sqln:
                raise ProgrammingError("Statement parameter sequence contains"
                                       " %d parameters, but only %d are allowed" %
                                       (len(parameters), self.in_sqlda.sqln))
            # Restore original type and size information for input parameters
            i = 0
            for sqlvar in self.in_sqlda.sqlvar[:self.n_input_params]:
                sqlvar.sqltype, sqlvar.sqllen = self.in_sqlda_save[i]
                i += 1
            self.__Tuple2XSQLDA(self.in_sqlda, parameters)
            xsqlda_in = ctypes.cast(ctypes.pointer(self.in_sqlda), XSQLDA_PTR)
        else:
            xsqlda_in = None
        # Execute the statement
        if ((self.statement_type == isc_info_sql_stmt_exec_procedure)
            and (self.out_sqlda.sqld > 0)):
            # NOTE: We have to pass xsqlda_out only for statements that return
            # single row
            xsqlda_out = ctypes.cast(ctypes.pointer(self.out_sqlda), XSQLDA_PTR)
            ibase.isc_dsql_execute2(self._isc_status,
                                    self.__get_transaction()._tr_handle,
                                    self._stmt_handle,
                                    self.__sql_dialect,
                                    xsqlda_in,
                                    xsqlda_out)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while executing Stored Procedure:")
            # The result was returned immediately, but we have to provide it
            # via fetch*() calls as Python DB API requires. However, it's not
            # possible to call fetch on open such statement, so we'll cache
            # the result and return it in fetchone instead calling fetch.
            self.__output_cache = self.__XSQLDA2Tuple(self.out_sqlda)
        else:
            ibase.isc_dsql_execute2(self._isc_status,
                                    self.__get_transaction()._tr_handle,
                                    self._stmt_handle,
                                    self.__sql_dialect,
                                    xsqlda_in,
                                    None)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while executing SQL statement:")
            self.__output_cache = None
        self.__executed = True
        self.__closed = False
        self._last_fetch_status = ibase.ISC_STATUS(self.NO_FETCH_ATTEMPTED_YET)
    def _fetchone(self):
        if (self._last_fetch_status == self.RESULT_SET_EXHAUSTED 
            and not self.__output_cache):
            return None
        if self.__executed:
            if self.__output_cache:
                if self._last_fetch_status == self.RESULT_SET_EXHAUSTED:
                    self._free_handle()
                    return None
                else:
                    self._last_fetch_status = self.RESULT_SET_EXHAUSTED
                    return self.__output_cache
            else:
                self._last_fetch_status = ibase.isc_dsql_fetch(
                    self._isc_status,
                    self._stmt_handle,
                    self.__sql_dialect,
                    ctypes.cast(ctypes.pointer(self.out_sqlda), XSQLDA_PTR))
                if self._last_fetch_status == 0:
                    return self.__XSQLDA2Tuple(self.out_sqlda)
                elif self._last_fetch_status == self.RESULT_SET_EXHAUSTED:
                    self._free_handle()
                    return None
                else:
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,
                            self._isc_status,
                            "Cursor.fetchone:")
        elif self.__closed:
            raise ProgrammingError("Cannot fetch from closed cursor.")
        else:
            raise ProgrammingError("Cannot fetch from this cursor because"
                                   " it has not executed a statement.")
    def _set_cursor_name(self, name):
        ibase.isc_dsql_set_cursor_name(self._isc_status,
                                       self._stmt_handle, b(name), 0)
        if db_api_error(self._isc_status):
            raise exception_from_status(OperationalError, self._isc_status,
                                        "Could not set cursor name:")
        self._name = name
    def set_stream_blob(self,blob_name):
        """Specify a BLOB column(s) to work in `stream` mode instead classic, 
        materialized mode.
        
        :param blob_name: Single name or sequence of column names. Name must 
                          be in format as it's stored in database (refer
                          to :attr:`description` for real value).
        :type blob_name: string or sequence
        
        .. important::
        
           BLOB name is **permanently** added to the list of BLOBs handled
           as `stream` BLOBs by this instance.
        
        :param string blob_name: Name of BLOB column.
        """
        if hasattr(blob_name,'__iter__'):
            self.__streamed_blobs.extend(blob_name)
        else:
            self.__streamed_blobs.append(blob_name)
    def __del__(self):
        if self._stmt_handle != None:
            self._close()
    def close(self):
        """Drops the resources associated with executed prepared statement, but
        keeps it prepared for another execution.
        """
        self._free_handle()

    #: (Read Only) (string) SQL command this PreparedStatement executes.
    sql = property(__get_sql)
    #: (Read Only) Sequence of 7-item sequences.
    #: Each of these sequences contains information describing one result column:
    #: (name, type_code, display_size,internal_size, precision, scale, null_ok)
    description = property(__get_description)
    #: (Read Only) (integer) Specifies the number of rows that the last execution
    #: produced (for DQL statements like select) or affected (for DML statements 
    #: like update or insert ). 
    #:
    #: The attribute is -1 in case the statement was not yet executed
    #: or the rowcount of the operation is not determinable by the interface.
    rowcount = property(__get_rowcount)
    #: (Read Only) (string) A string representation of the execution plan generated 
    #: for this statement by the database engine’s optimizer.
    plan = property(__get_plan)
    #: (Read/Write) (string) Name for the SQL cursor. This property can be 
    #: ignored entirely if you don’t need to use it.
    name = property(__get_name, __set_name)
    #: (Read Only) (boolean) True if closed. Note that closed means that PS
    #: statement handle was closed for further fetching, releasing server resources,
    #: but wasn't dropped, and couldbe still used for another execution.
    closed = property(__get_closed)


class Cursor(object):
    """Represents a database cursor, which is used to execute SQL statement and 
    manage the context of a fetch operation.

    .. important::
    
       DO NOT create instances of this class directly! Use only 
       :meth:`Connection.cursor`, :meth:`Transaction.cursor` and 
       :meth:`ConnectionGroup.cursor` to get Cursor instances that operate in
       desired context.
       
    .. note::
    
       Cursor is actually a high-level wrapper around :class:`PreparedStatement` 
       instance(s) that handle the actual SQL statement execution and result 
       management. Cursor has an internal cache of PreparedStatements, so when
       the same SQL statement is executed repeatedly, related PreparedStatement
       is re-used, which enhances performance. This convenient enhancement has 
       a side effect: Cursor that was used to perform many different SQL statements 
       may hold very large cache of PerapedStatements. To clear the cache, you'll 
       need to call :meth:`clear_cache`.
    
    .. tip::
    
       Cursor supports the iterator protocol, yielding tuples of values like
       :meth:`fetchone`.
    """
    #: (Read/Write) As required by the Python DB API 2.0 spec, the value of this 
    #: attribute is observed with respect to the :meth:`fetchmany` method. However, 
    #: changing the value of this attribute does not make any difference in fetch 
    #: efficiency because the database engine only supports fetching a single row 
    #: at a time.
    arraysize = 1

    def __init__(self, connection, transaction):
        """
        .. important::
        
           The association between a Cursor and its :class:`Transaction` and 
           :class:`Connection` is set when the Cursor is created, and cannot be 
           changed during the lifetime of that Cursor.
        
        :param connection: :class:`Connection` instance this cursor should be bound to.
        :param transaction: :class:`Transaction` instance this cursor should be bound to.
        """
        self._prepared_statements = {}
        self._connection = connection
        self._transaction = transaction
        self._ps = None  # current prepared statement
    def next(self):
        """Return the next item from the container. Part of *iterator protocol*.
        
        :raises StopIteration: If there are no further items. 
        """
        row = self.fetchone()
        if row:
            return row
        else:
            raise StopIteration
    __next__ = next
    def __iter__(self):
        return self
    def __get_description(self):
        return self._ps.description
    def __get_rowcount(self):
        if self._ps:
            return self._ps.rowcount
        else:
            return -1
    def __get_name(self):
        if self._ps:
            return self._ps._name
        else:
            return None
    def __set_name(self, name):
        if name == None or not isinstance(name, StringType):
            raise ProgrammingError("The name attribute can only be set to a"
                                   " string, and cannot be deleted")
        if not self._ps:
            raise ProgrammingError("This cursor has not yet executed a"
                                   " statement, so setting its name attribute"
                                   " would be meaningless")
        if self._ps._name:
            raise ProgrammingError("Cursor's name has already been declared in"
                                   " context of currently executed statement")
        self._ps._set_cursor_name(name)
    def __get_plan(self):
        if self._ps != None:
            return self._ps.plan
        else:
            return None
    def _set_as_internal(self):
        self._connection = weakref.ref(self._connection)
    def callproc(self, procname, parameters=None):
        """Call a stored database procedure with the given name.
        
        The result of the call is available through the standard fetchXXX() methods.
        
        :param string procname: Stored procedure name.
        :param parameters: (Optional) Sequence of parameters. Must contain one 
                           entry for each argument that the procedure expects.
        :type parameters: List or Tuple
        :returns: parameters, as required by Python DB API 2.0 Spec.
        :raises TypeError: When parameters is not List or Tuple.
        :raises ProgrammingError: When more parameters than expected are suplied.
        :raises DatabaseError: When error is returned by server.
        """
        if not parameters:
            params = []
        else:
            if isinstance(parameters, (ListeType, TupleType)):
                params = parameters
            else:
                raise TypeError("callproc paremeters must be List or Tuple")
        sql = ('EXECUTE PROCEDURE ' + procname + ' '
               + ','.join('?' * len(params)))
        self.execute(sql, params)
        return parameters
    def clear_cache(self):
        """Clear the internal cache with :class:`PreparedStatement` instances.
        
        All prepared statements except current one are **permanently** closed,
        and the cache is cleared.
        
        .. note:: 
        
           Current prepared statement is still in its actual state, but  it's 
           removed from cache, so when next statement (even the same SQL command) 
           is executed, this current :class:`PreparedStatement` instance is disposed.
        """
        for ps in self._prepared_statements.values():
            if ps != self._ps:
                ps._close()
        self._prepared_statements.clear()
    def close(self):
        """Close the cursor now (rather than whenever `__del__` is called).
        
        Closes any currently open :class:`PreparedStatement`. However, the cursor 
        is still bound to :class:`Connection` and :class:`Transaction`, so it 
        could be still used to execute SQL statements. Also the cache with 
        prepared statements is left intact.
        
        .. warning::
        
           FDB's implementation of Cursor somewhat violates the Python DB API 2.0,
           which requires that cursor will be unusable after call to `close`; and 
           an Error (or subclass) exception should be raised if any operation is 
           attempted with the cursor.
           
           If you’ll take advantage of this anomaly, your code would be less 
           portable to other Python DB API 2.0 compliant drivers.
        """
        if self._ps != None:
            self._ps.close()
            self._ps = None
    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).
        
        .. note::
        
           Execution is handled by :class:`PreparedStatement` that is either
           supplied as `operation` parameter, or created internally when
           `operation` is a string. Internally created PreparedStatements are
           stored in cache for later reuse, when the same `operation` string is
           used again.
        
        :param operation: SQL command specification.
        :type operation: string or :class:`PreparedStatement` instance
        :param parameters: (Optional) Sequence of parameters. Must contain one 
                           entry for each argument that the operation expects.
        :type parameters: List or Tuple
        :raises ValueError: When operation PreparedStatement belongs to different 
                            Cursor instance.
        :raises TypeError: When parameters is not List or Tuple.
        :raises ProgrammingError: When more parameters than expected are suplied.
        :raises DatabaseError: When error is returned by server.
        """
        if self._ps != None:
            self._ps.close()
        if not self._transaction.active:
            self._transaction.begin()
        if isinstance(operation, PreparedStatement):
            if operation.cursor is not self:
                raise ValueError("PreparedStatement was created by different Cursor.")
            self._ps = operation
        else:
            self._ps = self._prepared_statements.get(operation)
            if not self._ps:
                self._ps = self._prepared_statements.setdefault(operation,
                                    PreparedStatement(operation, self, True))
        self._ps._execute(parameters)
    def prep(self, operation):
        """Create prepared statement for repeated execution.
        
        .. note::
        
           Returned :class:`PreparedStatement` instance is bound to its Cursor
           instance via strong reference, and is not stored in Cursor's
           internal cache of prepared statements.
        
        :param string operation: SQL command
        :returns: :class:`PreparedStatement` instance.
        :raises DatabaseError: When error is returned by server.
        :raises InternalError: On unexpected processing condition.
        """
        if not self._transaction.active:
            self._transaction.begin()
        return PreparedStatement(operation, self, False)
    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it 
        against all parameter sequences or mappings found in the sequence 
        `seq_of_parameters`.
        
        .. note::
        
           This function simply calls :meth:`execute` in a loop, feeding it with
           parameters from `seq_of_parameters`. Because `execute` caches 
           `PreparedStatements`, calling `executemany` is equally efective as
           direct use of prepared statement and calling `execute` in a loop 
           directly in application.
        
        :param operation: SQL command specification.
        :type operation: string or :class:`PreparedStatement` instance
        :param seq_of_parameters: Sequence of sequences of parameters. Must contain 
                                  one sequence of parameters for each execution
                                  that has one entry for each argument that the 
                                  operation expects.
        :type seq_of_parameters: List or Tuple
        :raises ValueError: When operation PreparedStatement belongs to different 
                            Cursor instance.
        :raises TypeError: When seq_of_parameters is not List or Tuple.
        :raises ProgrammingError: When there are more parameters in any sequence 
                                  than expected.
        :raises DatabaseError: When error is returned by server.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
    def fetchone(self):
        """Fetch the next row of a query result set.
        
        :returns: tuple of returned values, or None when no more data is available.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        if self._ps:
            return self._ps._fetchone()
        else:
            raise ProgrammingError("Cannot fetch from this cursor because"
                                   " it has not executed a statement.")
    def fetchmany(self, size=arraysize):
        """Fetch the next set of rows of a query result, returning a sequence of 
        sequences (e.g. a list of tuples). An empty sequence is returned when no 
        more rows are available. The number of rows to fetch per call is specified 
        by the parameter. If it is not given, the cursor’s arraysize determines 
        the number of rows to be fetched. The method does try to fetch as many 
        rows as indicated by the size parameter. If this is not possible due to 
        the specified number of rows not being available, fewer rows may be 
        returned.
        
        :param integer size: Max. number of rows to fetch.
        :returns: List of tuples, where each tuple is one row of returned values.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        i = 0
        result = []
        while i < size:
            row = self.fetchone()
            if row:
                result.append(row)
                i += 1
            else:
                return result
        return result
    def fetchall(self):
        """Fetch all (remaining) rows of a query result.
        
        :returns: List of tuples, where each tuple is one row of returned values.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        return [row for row in self]
    def fetchonemap(self):
        """Fetch the next row of a query result set like :meth:`fetchone`, 
        except that it returns a mapping of field name to field  value, rather 
        than a tuple.
        
        :returns: :class:`fbcore._RowMapping` of returned values, or None when 
                  no more data is available.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        row = self.fetchone()
        if row:
            row = _RowMapping(self.description, row)
        return row
    def fetchmanymap(self, size=arraysize):
        """Fetch the next set of rows of a query result, like :meth:`fetchmany`, 
        except that it returns a list of mappings of field name to field 
        value, rather than a list of tuples.
        
        :param integer size: Max. number of rows to fetch.
        :returns: List of :class:`fbcore._RowMapping` instances, one such instance for
                  each row.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        i = 0
        result = []
        while i < size:
            row = self.fetchonemap()
            if row:
                result.append(row)
                i += 1
            else:
                return result
        return result
    def fetchallmap(self):
        """Fetch all (remaining) rows of a query result like :meth:`fetchall`, 
        except that it returns a list of mappings of field name to field 
        value, rather than a list of tuples.

        :returns: List of :class:`fbcore._RowMapping` instances, one such instance for
                  each row.
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When underlying :class:`PreparedStatement` is
                                  closed, statement was not yet executed, or
                                  unknown status is returned by fetch operation.
        """
        return [row for row in self.itermap()]
    def iter(self):
        """Equivalent to the :meth:`fetchall`, except that it returns iterator 
        rather than materialized list.
        
        :returns: Iterator that yields tuple of values like :meth:`fetchone`.
        """
        return self
    def itermap(self):
        """Equivalent to the :meth:`fetchallmap`, except that it returns iterator 
        rather than materialized list.
        
        :returns: Iterator that yields :class:`fbcore._RowMapping` instance 
                  like :meth:`fetchonemap`.
        """
        return Iterator(self.fetchonemap, None)
    def setinputsizes(self, sizes):
        """Required by Python DB API 2.0, but pointless for Firebird, so it
        does nothing."""
        pass
    def setoutputsize(self, size, column=None):
        """Required by Python DB API 2.0, but pointless for Firebird, so it
        does nothing."""
        pass
    def set_stream_blob(self,blob_name):
        """Specify a BLOB column(s) to work in `stream` mode instead classic, 
        materialized mode for already executed statement.
        
        :param blob_name: Single name or sequence of column names. Name must 
                          be in format as it's stored in database (refer
                          to :attr:`description` for real value).
        :type blob_name: string or sequence
        
        .. important::
        
           BLOB name is **permanently** added to the list of BLOBs handled
           as `stream` BLOBs by current :class:`PreparedStatement` instance.
           If instance is stored in internal cache of prepared statements,
           the same command executed repeatedly will retain this setting.
        
        :param string blob_name: Name of BLOB column.
        :raises ProgrammingError: 
        """
        if self._ps:
            self._ps.set_stream_blob(blob_name)
        else:
            raise ProgrammingError
    def __del__(self):
        self.clear_cache()
        self.close()
    #: (Read Only) Sequence of 7-item sequences.
    #: Each of these sequences contains information describing one result column:
    #: (name, type_code, display_size,internal_size, precision, scale, null_ok)
    #:
    #: If cursor doesn't have a prepared statement, the value is None.
    description = property(__get_description)
    #: (Read Only) (integer) Specifies the number of rows that the last executeXXX() 
    #: produced (for DQL statements like select) or affected (for DML statements 
    #: like update or insert ). 
    #:
    #: The attribute is -1 in case no executeXXX() has been performed on the cursor 
    #: or the rowcount of the last operation is not determinable by the interface.
    #:
    #: .. note::
    #:
    #:    The database engine's own support for the determination of 
    #:    “rows affected”/”rows selected” is quirky. The database engine only 
    #:    supports the determination of rowcount for INSERT, UPDATE, DELETE, 
    #:    and SELECT statements. When stored procedures become involved, row 
    #:    count figures are usually not available to the client. Determining 
    #:    rowcount for SELECT statements is problematic: the rowcount is reported 
    #:    as zero until at least one row has been fetched from the result set, 
    #:    and the rowcount is misreported if the result set is larger than 1302 
    #:    rows. The server apparently marshals result sets internally in batches 
    #:    of 1302, and will misreport the rowcount for result sets larger than 
    #:    1302 rows until the 1303rd row is fetched, result sets larger than 2604 
    #:    rows until the 2605th row is fetched, and so on, in increments of 1302. 
    rowcount = property(__get_rowcount)
    #: (Read/Write) (string) Name for the SQL cursor. This property can be 
    #: ignored entirely if you don’t need to use it.
    name = property(__get_name, __set_name)
    #: (Read Only) (string) A string representation of the execution plan 
    #: for last executed statement generated by the database engine’s optimizer.
    #: `None` if no statement was executed.
    plan = property(__get_plan)


class Transaction(object):
    """Represents a transaction context, which is used to execute SQL statement.
    
    .. important::
    
       DO NOT create instances of this class directly! :class:`Connection` and
       :class:`ConnectionGroup` manage Transaction internally, surfacing all
       important methods directly in their interfaces. If you want additional
       transactions independent from :attr:`Connection.main_transaction`,
       use :meth:`Connection.trans` method to obtain such `Transaction` instance.
    
    """
    def __init__(self, connections, default_tpb=None, default_action='commit'):
        """
        :param iterable connections: Sequence of (up to 16) :class:`Connection` instances.
        :param default_tpb: Transaction Parameter Block for this transaction. 
                    If `None` is specified, uses `ISOLATION_LEVEL_READ_COMMITED`.
        :type default_tpb: :class:`TPB` instance, list/tuple of `isc_tpb_*` constants
                   or `bytestring`
        :param default_action: Action taken when active transaction is ended
                               automatically (during :meth:`close` or :meth:`begin`).
        :type default_action: string 'commit' or 'rollback'
        :raises ProgrammingError: When zero or more than 16 connections are given.
        """
        if len(connections) > 16:
            raise ProgrammingError("Transaction can't accept more than 16 Connections")
        elif len(connections) == 0:
            raise ProgrammingError("Transaction requires at least one Connection")
        self._connections = [weakref.ref(c) for c in connections]
        self.__python_charset = connections[0]._python_charset
        if default_tpb == None:
            self.default_tpb = ISOLATION_LEVEL_READ_COMMITED
        else:
            #: (Read/Write) Transaction Parameter Block.
            #: **Default is ISOLATION_LEVEL_READ_COMMITED**.
            self.default_tpb = default_tpb
        self.default_action = default_action
        self._cursors = []  # Weak references to cursors
        self._isc_status = ISC_STATUS_ARRAY()
        self._tr_handle = None
        self.__closed = False
    def __remove_cursor(self, cursor_ref):
        self._cursors.remove(cursor_ref)
    def __get_closed(self):
        return self.__closed
        #return self._tr_handle == None
    def __get_active(self):
        return self._tr_handle != None
    def __get_cursors(self):
        return [x() for x in self._cursors]
    def __check_active(self):
        if not self.active:
            raise ProgrammingError("Transaction object is not active")
    def __close_cursors(self):
        for cursor in self._cursors:
            cursor().close()
    def __con_in_list(self,connection):
        for con in self._connections:
            if con() == connection:
                return True
        return False
    def __get_default_action(self):
        return self.__default_action
    def __set_default_action(self,action):
        action = action.lower()
        if not action in ('commit','rollback'):
            raise ProgrammingError("Transaction's default action must be either"
                                   "'commit' or 'rollback'.")
        else:
            self.__default_action = action
    def execute_immediate(self, sql):
        """Executes a statement without caching its prepared form on 
           **all connections** this transaction is bound to. 
        
        Automatically starts transaction if it's not already started.
        
        :param string sql: SQL statement to execute.
        
        .. important::
        
           **The statement must not be of a type that returns a result set.**
           In most cases (especially cases in which the same statement – perhaps 
           a parameterized statement – is executed repeatedly), it is better to 
           create a cursor using the connection’s cursor method, then execute 
           the statement using one of the cursor’s execute methods.
        
        :param string sql: SQL statement to execute.
        
        :raises DatabaseError: When error is returned from server.
        """
        if not self.active:
            self.begin()
        for connection in self._connections:
            con = connection()
            sql = b(sql, con._python_charset)
            ibase.isc_execute_immediate(self._isc_status,
                                        con._db_handle,
                                        self._tr_handle,
                                        ctypes.c_short(len(sql)), sql)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while executing SQL statement:")
    def _finish(self):
        if self._tr_handle != None:
            try:
                if self.default_action == 'commit':
                    self.commit()
                else:
                    self.rollback()
            except Exception as e:
                self._tr_handle = None
                raise e
    def begin(self, tpb=None):
        """Starts a transaction explicitly.
        
        :param tpb: (optional) Transaction Parameter Block for newly created 
                    Transaction. If not specified, :attr:`default_tpb` is used.
        :type tpb: :class:`TPB` instance, list/tuple of `isc_tpb_*` constants
                   or `bytestring`
        
        .. note::
        
           Calling this method directly is never required; a transaction will be 
           started implicitly if necessary.

        .. important::
        
           If the physical transaction is unresolved when this method is called, 
           a :meth:`commit` or :meth:`rollback` will be performed first, accordingly
           to :attr:`default_action` value.
           
        :raises DatabaseError: When error is returned by server.
        :raises ProgrammingError: When TPB is in usupported format, or transaction
                                  is permanently :attr:`closed`.
        """
        if self.__closed:
            raise ProgrammingError("Transaction is permanently closed.")
        self._finish()  # Make sure that previous transaction (if any) is ended
        self._tr_handle = ibase.isc_tr_handle(0)
        _tpb = tpb if tpb else self.default_tpb
        if isinstance(_tpb, TPB):
            _tpb = _tpb.render()
        elif isinstance(_tpb, (ListeType, TupleType)):
            _tpb = bs(_tpb)
        elif not isinstance(_tpb, mybytes):
            raise ProgrammingError("TPB must be either string, list/tuple of"
                                   " numeric constants or TPB instance.")
        ### Todo: verify handling of P version differences
        if PYTHON_MAJOR_VER == 3:
            if int2byte(_tpb[0]) != bs([isc_tpb_version3]):
                _tpb = bs([isc_tpb_version3]) + _tpb
        else:
            if _tpb[0] != bs([isc_tpb_version3]):
                _tpb = bs([isc_tpb_version3]) + _tpb
        if len(self._connections) == 1:
            ibase.isc_start_transaction(self._isc_status, self._tr_handle, 1,
                                        self._connections[0]()._db_handle,
                                        len(_tpb), _tpb)
            if db_api_error(self._isc_status):
                self._tr_handle = None
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while starting transaction:")
        elif len(self._connections) > 1:
            cnum = len(self._connections)
            teb_array = tebarray_factory(cnum)
            for i in xrange(cnum):
                teb_array[i].db_ptr = ctypes.pointer(self._connections[i]()._db_handle)
                teb_array[i].tpb_len = len(_tpb)
                teb_array[i].tpb_ptr = _tpb
            ibase.isc_start_multiple(self._isc_status, self._tr_handle,
                                     cnum,
                                     teb_array
                                     )
            if db_api_error(self._isc_status):
                self._tr_handle = None
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while starting transaction:")
    def commit(self, retaining=False):
        """Commit any pending transaction to the database.
        
        .. note::
           If transaction is not active, this method does nothing, because
           the consensus among Python DB API experts is that transactions should
           always be started implicitly, even if that means allowing a `commit()`
           or `rollback()` without an actual transaction.
        
        :param boolean retaining: Indicates whether the transactional context of 
                                  the transaction being resolved should be recycled.
        :raises DatabaseError: When error is returned by server as response to commit.
        """
        if not self.active:
            return
        if retaining:
            ibase.isc_commit_retaining(self._isc_status, self._tr_handle)
        else:
            self.__close_cursors()
            ibase.isc_commit_transaction(self._isc_status, self._tr_handle)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError, self._isc_status,
                                        "Error while commiting transaction:")
        if not retaining:
            self._tr_handle = None
    def rollback(self, retaining=False, savepoint=None):
        """Rollback any pending transaction to the database.
        
        .. note::
           If transaction is not active, this method does nothing, because
           the consensus among Python DB API experts is that transactions should
           always be started implicitly, even if that means allowing a `commit()`
           or `rollback()` without an actual transaction.
        
        :param boolean retaining: Indicates whether the transactional context of 
                                  the transaction being resolved should be recycled.
                                  Mutually exclusive with 'savepoint`.
        :param string savepoint: Savepoint name. Causes the transaction to roll 
                                 back only as far as the designated savepoint, 
                                 rather than rolling back entirely. Mutually 
                                 exclusive with 'retaining`.
        :raises ProgrammingError: If both `savepoint` and `retaining` are specified.
        :raises DatabaseError: When error is returned by server as response to rollback.
        """
        if not self.active:
            return
        if retaining and savepoint:
            raise ProgrammingError("Can't rollback to savepoint while"
                                   " retaining context")
        if savepoint:
            self.execute_immediate('rollback to %s' % savepoint)
        else:
            if retaining:
                ibase.isc_rollback_retaining(self._isc_status, self._tr_handle)
            else:
                self.__close_cursors()
                ibase.isc_rollback_transaction(self._isc_status,
                                               self._tr_handle)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                                            "Error while rolling back transaction:")
            if not retaining:
                self._tr_handle = None
    def close(self):
        """Permanently closes the Transaction object and severs its associations 
        with other objects (:class:`Cursor` and :class:`Connection` instances).

        .. important::
        
           If the physical transaction is unresolved when this method is called, 
           a :meth:`commit` or :meth:`rollback` will be performed first, accordingly
           to :attr:`default_action` value.
           
        """
        exc = None
        try:
            self._finish()
        except Exception as e:
            exc = e
        del self._cursors[:]
        del self._connections[:]
        self.__closed = True
        if exc:
            raise exc
    def savepoint(self, name):
        """Establishes a savepoint with the specified name.
        
        .. note::
        
           If transaction is bound to multiple connections, savepoint is
           created on all of them.
           
        .. important::
        
           Because savepoint is created not through Firebird API (there is no
           such API call), but by executing `SAVEPOINT <name>` SQL statement,
           calling this method starts the transaction if it was not yet started.
        
        :param string name: Savepoint name.
        """
        self.execute_immediate('SAVEPOINT %s' % name)
    def cursor(self,connection = None):
        """Creates a new :class:`Cursor` that will operate in the context of this 
        Transaction.
        
        :param connection: **Required only when** Transaction is bound to multiple 
                           `Connections`, to specify to which `Connection` the 
                           returned Cursor should be bound.
        :type connection: :class:`Connection` instance
        
        :raises ProgrammingError: When transaction operates on multiple `Connections`
                                  and: `connection` parameter is not specified, or
                                  specified `connection` is not among `Connections`
                                  this Transaction is bound to.
        """
        if len(self._connections) > 1:
            if not connection:
                raise ProgrammingError("Transaction.cursor on multiple connections"
                                       " requires 'connection' parameter")
            if not self.__con_in_list(connection):
                raise ProgrammingError("Transaction.cursor connection not in"
                                       " list of connections for this transaction")
            con = connection
        else:
            con = self._connections[0]()
        c = Cursor(con, self)
        self._cursors.append(weakref.ref(c, self.__remove_cursor))
        return c
    def trans_info(self, request):
        """Pythonic wrapper around :meth:`transaction_info` call.
        
        :param request: One or more information request codes (see 
                        :meth:`transaction_info` for details). Multiple codes 
                        must be passed as tuple.
        :returns: Decoded response(s) for specified request code(s). When 
                  multiple requests are passed, returns a dictionary where key 
                  is the request code and value is the response from server.
        """
        # We process request as a sequence of info codes, even if only one code
        # was supplied by the caller.
        requestIsSingleton = isinstance(request, int)
        if requestIsSingleton:
            request = (request,)

        results = {}
        for infoCode in request:
            # The global().get(...) workaround is here because only recent
            # versions of FB expose constant isc_info_tra_isolation:
            if infoCode == globals().get('isc_info_tra_isolation', -1):
                buf = self.transaction_info(infoCode, 's')
                buf = buf[1 + struct.calcsize('h'):]
                if len(buf) == 1:
                    results[infoCode] = bytes_to_int(buf)
                else:
                    # For isolation level isc_info_tra_read_committed, the
                    # first byte indicates the isolation level
                    # (isc_info_tra_read_committed), while the second indicates
                    # the record version flag (isc_info_tra_rec_version or
                    # isc_info_tra_no_rec_version).
                    isolationLevelByte, recordVersionByte = struct.unpack('cc',
                                                                          buf)
                    isolationLevel = bytes_to_int(isolationLevelByte)
                    recordVersion = bytes_to_int(recordVersionByte)
                    results[infoCode] = (isolationLevel, recordVersion)
            else:
                # At the time of this writing (2006.02.09),
                # isc_info_tra_isolation is the only known return value of
                # isc_transaction_info that's not a simple integer.
                results[infoCode] = self.transaction_info(infoCode, 'i')

        if requestIsSingleton:
            return results[request[0]]
        else:
            return results
    def transaction_info(self, info_code, result_type):
        """Return information about active transaction. 
        
        This is very thin wrapper around Firebird API `isc_transaction_info` call. 
        
        :param integer info_code: One from the `isc_info_tra_*` constants.
        :param result_type: Code for result type.
        :type result_type: string 's' or 'i'
        :raises ProgrammingError: If transaction is not active.
        :raises OperationalError: When result is too large to fit into buffer of 
                                  size SHRT_MAX.
        :raises InternalError: On unexpected processing condition.
        :raises ValueError: When illegal result type code is specified.
        """
        self.__check_active()
        request_buffer = bs([info_code])
        buf_size = 256
        while True:
            res_buf = int2byte(0) * buf_size
            ibase.isc_transaction_info(self._isc_status, self._tr_handle,
                                       len(request_buffer), request_buffer,
                                       len(res_buf), res_buf)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError, self._isc_status,
                    "Error while requesting transaction information:")
            i = buf_size - 1
            while i >= 0:
                if res_buf[i] != mychr(0):
                    break
                else:
                    i -= 1
            if ord2(res_buf[i]) == ibase.isc_info_truncated:
                if buf_size < SHRT_MAX:
                    buf_size *= 2
                    if buf_size > SHRT_MAX:
                        buf_size = SHRT_MAX
                    continue
                else:
                    raise OperationalError("Result is too large to fit into"
                        " buffer of size SHRT_MAX, yet underlying info"
                        " function only accepts buffers with size <= SHRT_MAX.")
            else:
                break
        if ord2(res_buf[i]) != isc_info_end:
            raise InternalError("Exited request loop sucessfuly, but"
                                " res_buf[i] != sc_info_end.")
        if request_buffer[0] != res_buf[0]:
            raise InternalError("Result code does not match request code.")
        if result_type.upper() == 'I':
            return bytes_to_int(res_buf[3:3 + bytes_to_int(res_buf[1:3])])
        elif result_type.upper() == 'S':
            return p3fix(ctypes.string_at(res_buf, i),self.__python_charset)
        else:
            raise ValueError("Unknown result type requested (must be 'i'"
                                 "or 's').")
    def prepare(self):
        """Manually triggers the first phase of a two-phase commit (2PC). 
        
        .. note::
        
           Direct use of this method is optional; if preparation is not triggered 
           manually, it will be performed implicitly by `commit()` in a 2PC.
        """
        self.__check_active()
        ibase.isc_prepare_transaction(self._isc_status, self._tr_handle)
        if db_api_error(self._isc_status):
            self.rollback()
            raise exception_from_status(DatabaseError, self._isc_status,
                                        "Error while preparing transaction:")        
    def __del__(self):
        if self._tr_handle != None:
            self.close()
    
    #: (Read Only) True if transaction is closed.
    closed = property(__get_closed)
    #: (Read Only) True if transaction is active.
    active = property(__get_active)
    #: (Read Only) List of :class:`Cursor` objects associated with this Transaction.
    cursors = property(__get_cursors)
    #: (Read/Write) (string) 'commit' or 'rollback', action to be 
    #: taken when physical transaction has to be ended automatically.
    #: **Default is 'commit'**.
    default_action = property(__get_default_action,__set_default_action)

class ConnectionGroup(object):
    """Manager for distributed transactions, i.e. transactions that span multiple 
    databases.
    
    .. tip::
    
       ConnectionGroup supports `in` operator to check membership of connections.
    """
    # XXX: ConnectionGroup objects currently are not thread-safe.  Since
    # separate Connections can be manipulated simultaneously by different
    # threads in kinterbasdb, it would make sense for a container of multiple
    # connections to be safely manipulable simultaneously by multiple threads.

    # XXX: Adding two connections to the same database freezes the DB client
    # library.  However, I've no way to detect with certainty whether any given
    # con1 and con2 are connected to the same database, what with database
    # aliases, IP host name aliases, remote-vs-local protocols, etc.
    # Therefore, a warning must be added to the docs.

    def __init__(self, connections=()):
        """
        :param iterable connections: Sequence of :class:`Connection` instances.
        
        .. seealso:: See :meth:`add` for list of exceptions the constructor may throw.
        """
        self._cons = []
        self._transaction = None
        self._default_tpb = ISOLATION_LEVEL_READ_COMMITED
        for con in connections:
            self.add(con)
    def __del__(self):
        self.disband()
    def __get_default_tpb(self):
        return self._default_tpb
    def __set_default_tpb(self, value):
        self._default_tpb = _validateTPB(value)
    def disband(self):
        """Forcefully deletes all connections from connection group.
        
        .. note:: If transaction is active, it’s canceled (**rollback**).
        
        .. note::
        
           Any error during transaction finalization doesn't stop the disband
           process, however the exception catched is eventually reported.
        """
        exc = None
        if self._transaction: 
            try:
                self._transaction.default_action = 'rollback'
                self._transaction.close()
            except Exception as e:
                exc = e
        self._transaction = None
        self.clear()
        if exc:
            raise exc
    # Membership methods:
    def add(self, con):
        """Adds active connection to the group.
        
        :param con: A :class:`Connection` instance to add to this group.
        :raises TypeError: When `con` is not :class:`Connection` instance.
        :raises ProgrammingError: When `con` is already member of this or another
                                  group, or :attr:`~Connection.closed`.
                                  When this group has unresolved transaction or
                                  contains 16 connections.
        """
        ### CONTRAINTS ON $con: ###
        # con must be an instance of kinterbasdb.Connection:
        if not isinstance(con, Connection):
            raise TypeError("con must be an instance of fdb.Connection")
        # con cannot already be a member of this group:
        if con in self:
            raise ProgrammingError("con is already a member of this group.")
        # con cannot belong to more than one group at a time:
        if con.group:
            raise ProgrammingError("con is already a member of another group;"
                " it cannot belong to more than one group at once.")
        # con must be connected to a database; it must not have been closed.
        if con.closed:
            raise ProgrammingError("con has been closed; it cannot join a group.")
        #if con._timeout_enabled:
            #raise ProgrammingError('Connections with timeout enabled cannot'
                #' participate in distributed transactions.')

        ### CONTRAINTS ON $self: ###
        # self cannot accept new members while self has an unresolved
        # transaction:
        self.__require_transaction_state(False,
            "Cannot add connection to group that has an unresolved transaction.")
        self.__drop_transaction()
        # self cannot have more than DIST_TRANS_MAX_DATABASES members:
        if self.count() >= DIST_TRANS_MAX_DATABASES:
            raise ProgrammingError("The database engine limits the number of"
                " database handles that can participate in a single"
                " distributed transaction to %d or fewer; this group already"
                " has %d members."
                % (DIST_TRANS_MAX_DATABASES, self.count())
                )

        ### CONTRAINTS FINISHED ###
        # Can't set con.group directly (read-only); must use package-private
        # method.
        con._set_group(self)
        self._cons.append(con)
    def remove(self, con):
        """Removes specified connection from group. 

        :param con: A :class:`Connection` instance to remove.
        :raises ProgrammingError: When `con` doesn't belong to this group or 
                                  transaction is active.
        """
        if con not in self:
            raise ProgrammingError("con is not a member of this group.")
        assert con.group is self
        self.__require_transaction_state(False,
            "Cannot remove connection from group that has an unresolved transaction.")
        self.__drop_transaction()
        con._set_group(None)
        self._cons.remove(con)
    def clear(self):
        """Removes all connections from group.
        
        :raises ProgrammingError: When transaction is active.
        """
        self.__require_transaction_state(False,
            "Cannot clear group that has an unresolved transaction.")
        self.__drop_transaction()
        for con in self.members():
            self.remove(con)
        assert self.count() == 0
    def cursor(self, connection):
        """Creates a new :class:`Cursor` that will operate in the context of 
        distributed transaction and specific :class:`Connection` that belongs
        to this group.
        
        .. note:: Automatically starts transaction if it's not already started.
        
        :param connection: :class:`Connection` instance.
        :raises ProgrammingError: When group is empty or specified `connection` 
                                  doesn't belong to this group.
        """
        if not self._transaction:
            self.__require_non_empty_group('start')
            self._transaction = Transaction(self._cons)
        return self._transaction.cursor(connection)
    def members(self):
        "Returns list of connection objects that belong to this group."
        return self._cons[:] # return a *copy* of the internal list
    def count(self):
        "Returns number of :class:`Connection` objects that belong to this group."
        return len(self._cons)
    def contains(self, con):
        """Returns True if specified connection belong to this group.
        
        :param con: :class:`Connection` instance.
        """
        return con in self._cons
    __contains__ = contains # alias to support the 'in' operator
    def __iter__(self):
        return iter(self._cons)
    def __drop_transaction(self):
        if self._transaction:
            self._transaction.close()
            self._transaction = None
    def __require_transaction_state(self, must_be_active, err_msg=''):
        transaction = self._transaction
        if (
            (must_be_active and transaction is None)
            or (not must_be_active and (transaction is not None and transaction.active))
            ):
            raise ProgrammingError(err_msg)
    def __require_non_empty_group(self, operation_name):
        if self.count() == 0:
            raise ProgrammingError("Cannot %s distributed transaction with"
                " an empty ConnectionGroup." % operation_name)
    def __ensure_transaction(self):
        if not self._transaction:
            self.__require_non_empty_group('start')
            self._transaction = Transaction(self._cons,
                                            default_tpb=self.default_tpb)
    # Transactional methods:
    def execute_immediate(self, sql):
        """Executes a statement on all member connections without caching its 
        prepared form. 
        
        Automatically starts transaction if it's not already started.
        
        :param string sql: SQL statement to execute.
        
        .. important::
        
           **The statement must not be of a type that returns a result set.**
           In most cases (especially cases in which the same statement – perhaps 
           a parameterized statement – is executed repeatedly), it is better to 
           create a cursor using the connection’s cursor method, then execute 
           the statement using one of the cursor’s execute methods.
        
        :param string sql: SQL statement to execute.
        
        :raises DatabaseError: When error is returned from server.
        """
        self.__ensure_transaction()
        self._transaction.execute_immediate(sql)
    def begin(self, tpb=None):
        """Starts distributed transaction over member connections.
        
        :param tpb: (Optional) Transaction Parameter Buffer for newly started 
                    transaction. If not specified, :attr:`default_tpb` is used.
        :type tpb: :class:`TPB` instance, list/tuple of `isc_tpb_*` constants
                   or `bytestring`
        :raises ProgrammingError: When group is empty or has active transaction.
        """
        self.__require_transaction_state(False,
            "Must resolve current transaction before starting another.")
        self.__ensure_transaction()
        self._transaction.begin(tpb)
    def savepoint(self, name):
        """Establishes a named SAVEPOINT on all member connections. 
        See :meth:`Transaction.savepoint` for details.

        :param string name: Name for savepoint.
        :raises ProgrammingError: When group is empty.
        """
        self.__require_non_empty_group('savepoint')
        return self._transaction.savepoint(name)
    def prepare(self):
        """
          Manually triggers the first phase of a two-phase commit (2PC).  Use
        of this method is optional; if preparation is not triggered manually,
        it will be performed implicitly by commit() in a 2PC.
        """
        self.__require_non_empty_group('prepare')
        self.__require_transaction_state(True,"This group has no transaction to prepare.")
        self._transaction.prepare()
    def commit(self, retaining=False):
        """Commits distributed transaction over member connections using 2PC.
        
        .. note::
           If transaction is not active, this method does nothing, because
           the consensus among Python DB API experts is that transactions should
           always be started implicitly, even if that means allowing a `commit()`
           or `rollback()` without an actual transaction.
        
        :param boolean retaining: Indicates whether the transactional context of 
                                  the transaction being resolved should be recycled.
        :raises ProgrammingError: When group is empty.
        """
        self.__require_non_empty_group('commit')
        # The consensus among Python DB API experts is that transactions should
        # always be started implicitly, even if that means allowing a commit()
        # or rollback() without an actual transaction.
        if self._transaction is None:
            return
        self._transaction.commit(retaining)
    def rollback(self, retaining=False, savepoint=None):
        """Rollbacks distributed transaction over member connections.
        
        .. note::
           If transaction is not active, this method does nothing, because
           the consensus among Python DB API experts is that transactions should
           always be started implicitly, even if that means allowing a `commit()`
           or `rollback()` without an actual transaction.
        
        :param boolean retaining: Indicates whether the transactional context of 
                                  the transaction being resolved should be recycled.
        :raises ProgrammingError: When group is empty.
        """
        self.__require_non_empty_group('rollback')
        # The consensus among Python DB API experts is that transactions should
        # always be started implicitly, even if that means allowing a commit()
        # or rollback() without an actual transaction.
        if self._transaction is None:
            return
        self._transaction.rollback(retaining, savepoint)

    #: (Read/Write) Deafult Transaction Parameter Block used for transactions.
    default_tpb = property(__get_default_tpb, __set_default_tpb)


class BlobReader(object):
    """BlobReader is a “file-like” class, so it acts much like a file instance 
    opened in `rb` mode.
    
    .. important::
    
       DO NOT create instances of this class directly! BlobReader instances are 
       returned automatically in place of output BLOB values when `stream`
       BLOB access is requested via :meth:`PreparedStatement.set_stream_blob`.
    
    .. tip::
    
       BlobReader supports iterator protocol, yielding lines like :meth:`readline`.
    """
    def __init__(self, blobid, db_handle, tr_handle, is_text, charset):
        self.__closed = False
        self.__mode = 'rb'
        self.__bytes_read = 0
        self.__pos = 0
        self.__index = 0
        #self.__bstream = ibase.Bopen(blobid, db_handle, tr_handle, self.__mode)
        self.__db_handle = db_handle
        self.__tr_handle = tr_handle
        self.__is_text = is_text
        self.__charset = charset
        self.__python_charset = charset_map.get(charset,charset)
        self.__blobid = blobid
        self.__opened = False
        self._blob_handle = ibase.isc_blob_handle()
        self._isc_status = ISC_STATUS_ARRAY()
    def __ensure_open(self):
        if self.closed:
            raise ProgrammingError("BlobReader is closed.")
        if not self.__opened:
            self.__open()
    def __open(self):
        ibase.isc_open_blob2(self._isc_status,
                             self.__db_handle,
                             self.__tr_handle,
                             self._blob_handle, self.__blobid, 4, 
                             bs([ibase.isc_bpb_version1,
                                 ibase.isc_bpb_type,1,
                                 ibase.isc_bpb_type_stream]))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,
                                        self._isc_status,
                                        "Cursor.read_output_blob/isc_open_blob2:")
        # Get BLOB total length and max. size of segment
        result = ctypes.cast(ctypes.create_string_buffer(20),
                             buf_pointer)
        ibase.isc_blob_info(self._isc_status, self._blob_handle, 2,
                            bs([ibase.isc_info_blob_total_length,
                                ibase.isc_info_blob_max_segment]),
                            20, result)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,
                                        self._isc_status,
                                        "Cursor.read_output_blob/isc_blob_info:")
        offset = 0
        while bytes_to_int(result[offset]) != isc_info_end:
            code = bytes_to_int(result[offset])
            offset += 1
            if code == ibase.isc_info_blob_total_length:
                length = bytes_to_int(result[offset:offset + 2])
                self._blob_length = bytes_to_int(result[
                    offset + 2:offset + 2 + length])
                offset += length + 2
            elif code == ibase.isc_info_blob_max_segment:
                length = bytes_to_int(result[offset:offset + 2])
                self._segment_size = bytes_to_int(result[
                    offset + 2:offset + 2 + length])
                offset += length + 2
        # Create internal buffer
        self.__buf = ctypes.create_string_buffer(self._segment_size)
        self.__buf_pos = 0
        self.__buf_data = 0
        self.__opened = True
    def __reset_buffer(self):
        ctypes.memset(self.__buf,0,self._segment_size)
        self.__buf_pos = 0
        self.__buf_data = 0
    def __BLOB_get(self):
        self.__reset_buffer()
        # Load BLOB
        allow_incomplete_segment_read = False
        status = ibase.ISC_STATUS(0)
        bytes_read = 0
        bytes_actually_read = ctypes.c_ushort(0)
        status = ibase.isc_get_segment(self._isc_status,
                                       self._blob_handle,
                                       bytes_actually_read,
                                       self._segment_size,
                                       ctypes.byref(self.__buf))
        if status != 0:
            if status == ibase.isc_segstr_eof:
                self.__buf_data = 0
            elif ((status == ibase.isc_segment)
                and allow_incomplete_segment_read):
                self.__buf_data = bytes_actually_read.value
            else:
                raise exception_from_status(DatabaseError,
                                            self._isc_status,
                                            "BlobReader.__BLOB_get/isc_get_segment:")
        else:
            self.__buf_data = bytes_actually_read.value
    def close(self):
        """Closes the Reader. Like :meth:`file.close`.
        
        :raises DatabaseError: When error is returned by server.
        """
        if self.__opened and not self.closed:
            self.__closed = True
            ibase.isc_close_blob(self._isc_status, self._blob_handle)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,
                                            self._isc_status,
                                            "BlobReader.close/isc_close_blob:")
    def flush(self):
        """Flush the internal buffer. Like :meth:`file.flush`. Does nothing as
        it's pointless for reader."""
        pass
    def next(self):
        """Return the next line from the BLOB. Part of *iterator protocol*.
        
        :raises StopIteration: If there are no further lines. 
        """
        line = self.readline()
        if line:
            return line
        else:
            raise StopIteration
    __next__ = next
    def __iter__(self):
        return self
    def read(self, size = -1):
        """Read at most size bytes from the file (less if the read hits EOF 
        before obtaining size bytes). If the size argument is negative or omitted, 
        read all data until EOF is reached. The bytes are returned as a string 
        object. An empty string is returned when EOF is encountered immediately.
        Like :meth:`file.read`.
        
        :raises ProgrammingError: When reader is closed.
        
        .. note::
        
           Performs automatic conversion to `unicode` for TEXT BLOBs, if used 
           Python is v3 or `connection charset` is defined.
        """
        self.__ensure_open()
        if size >= 0:
            to_read = size
        else:
            to_read = self._blob_length - self.__pos
        result = ctypes.create_string_buffer(to_read)
        pos = 0
        while to_read > 0:
            to_copy = min(to_read, self.__buf_data - self.__buf_pos)
            if to_copy == 0:
                self.__BLOB_get()
                to_copy = min(to_read, self.__buf_data - self.__buf_pos)
                if to_copy == 0:
                    # BLOB EOF
                    break
            ctypes.memmove(ctypes.byref(result,pos),
                           ctypes.byref(self.__buf,self.__buf_pos),
                           to_copy)
            pos += to_copy
            self.__pos += to_copy
            self.__buf_pos += to_copy
            to_read -= to_copy
        result = result.value
        if (self.__charset or PYTHON_MAJOR_VER == 3) and self.__is_text:
            result = b2u(result,self.__python_charset)
        return result
    def readline(self):
        """Read one entire line from the file. A trailing newline character is 
        kept in the string (but may be absent when a file ends with an incomplete 
        line). An empty string is returned when EOF is encountered immediately.
        Like :meth:`file.readline`.

        :raises ProgrammingError: When reader is closed.
        
        .. note::
        
           Performs automatic conversion to `unicode` for TEXT BLOBs, if used 
           Python is v3 or `connection charset` is defined.
        """
        self.__ensure_open()
        line = []
        to_read = self._blob_length - self.__pos
        to_copy = 0
        found = False
        while to_read > 0 and not found:
            to_scan = min(to_read, self.__buf_data - self.__buf_pos)
            if to_scan == 0:
                self.__BLOB_get()
                to_scan = min(to_read, self.__buf_data - self.__buf_pos)
                if to_scan == 0:
                    # BLOB EOF
                    break
            pos = 0
            result = ''
            while pos < to_scan:
                if self.__buf[self.__buf_pos+pos] == ibase.b('\n'):
                    found = True
                    pos += 1
                    break
                pos += 1
            result = ctypes.string_at(ctypes.byref(self.__buf,self.__buf_pos), pos)
            if (self.__charset or PYTHON_MAJOR_VER == 3)  and self.__is_text:
                result = b2u(result,self.__python_charset)
            line.append(result)
            self.__buf_pos += pos
            self.__pos += pos
            to_read -= pos
        return ''.join(line)
    def readlines(self, sizehint = None):
        """Read until EOF using :meth:`readline` and return a list containing 
        the lines thus read. The optional sizehint argument (if present) is ignored.
        Like :meth:`file.readlines`.

        .. note::
        
           Performs automatic conversion to `unicode` for TEXT BLOBs, if used 
           Python is v3 or `connection charset` is defined.
        """
        result = []
        line = self.readline()
        while line:
            result.append(line)
            line = self.readline()
        return result
    def seek(self, offset, whence = os.SEEK_SET):
        """Set the file’s current position, like stdio‘s `fseek()`. 
        See :meth:`file.seek` details.
        
        :param integer offset: Offset from specified position.
        :param whence: (Optional) Context for offset.
        :type whence: os.SEEK_SET, os.SEEK_CUR or os.SEEK_END
        
        :raises ProgrammingError: When reader is closed.
        
        .. warning::
        
           If BLOB was NOT CREATED as `stream` BLOB, this method raises
           :exc:`DatabaseError` exception. This constraint is set by Firebird.
        """
        self.__ensure_open()
        pos = ibase.ISC_LONG(0)
        ibase.isc_seek_blob (self._isc_status,
                             self._blob_handle,
                             whence, ibase.ISC_LONG(offset), ctypes.byref(pos))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,
                                        self._isc_status,
                                        "BlobReader.seek/isc_blob_info:")
        self.__pos = pos.value
        self.__reset_buffer()
    def tell(self):
        """Return current position in BLOB, like stdio‘s `ftell()` 
        and :meth:`file.tell`."""
        return self.__pos
    def __get_closed(self):
        return self.__closed
    def __get_mode(self):
        return self.__mode
    def __del__(self):
        self.close()
    #: (Read Only) (boolean) True is BlobReader is closed.
    closed = property(__get_closed)
    #: (Read Only) (string) File mode - always "rb"
    mode = property(__get_mode)

class Iterator(object):
    def __init__(self, method, sentinel):
        self.getnext = method
        self.sentinel = sentinel
        self.exhausted = False
    def __iter__(self):
        return self
    def next(self):
        if self.exhausted:
            raise StopIteration
        else:
            result = self.getnext()
            self.exhausted = (result == self.sentinel)
            if self.exhausted:
                raise StopIteration
            else:
                return result
    __next__ = next


class _RowMapping(object):
    """An internal dictionary-like class that wraps a row of results in order to 
    map field name to field value.

    .. warning::
    
       We make ABSOLUTELY NO GUARANTEES about the return value of the
       `fetch(one|many|all)` methods except that it is a sequence indexed by field
       position, and no guarantees about the return value of the
       `fetch(one|many|all)map` methods except that it is a mapping of field name
       to field value.
    
       Therefore, client programmers should NOT rely on the return value being
       an instance of a particular class or type.
    """
    def __init__(self, description, row):
        self._description = description
        fields = self._fields = {}
        pos = 0
        for fieldSpec in description:
            # It's possible for a result set from the database engine to return
            # multiple fields with the same name, but kinterbasdb's key-based
            # row interface only honors the first (thus setdefault, which won't
            # store the position if it's already present in self._fields).
            fields.setdefault(fieldSpec[DESCRIPTION_NAME], row[pos])
            pos += 1
    def __len__(self):
        return len(self._fields)
    def __getitem__(self, fieldName):
        fields = self._fields
        # Straightforward, unnormalized lookup will work if the fieldName is
        # already uppercase and/or if it refers to a database field whose
        # name is case-sensitive.
        if fieldName in fields:
            return fields[fieldName]
        else:
            fieldNameNormalized = _normalizeDatabaseIdentifier(fieldName)
            try:
                return fields[fieldNameNormalized]
            except KeyError:
                raise KeyError('Result set has no field named "%s".  The field'
                               ' name must be one of: (%s)'
                               % (fieldName, ', '.join(fields.keys()))
                               )
    def get(self, fieldName, defaultValue=None):
        try:
            return self[fieldName]
        except KeyError:
            return defaultValue
    def __contains__(self, fieldName):
        try:
            self[fieldName]
        except KeyError:
            return False
        else:
            return True
    def __str__(self):
        # Return an easily readable dump of this row's field names and their
        # corresponding values.
        return '<result set row with %s>' % ', '.join([
            '%s = %s' % (fieldName, self[fieldName])
            for fieldName in self._fields.keys()
        ])
    def keys(self):
        # Note that this is an *ordered* list of keys.
        return [fieldSpec[DESCRIPTION_NAME] for fieldSpec in self._description]
    def values(self):
        # Note that this is an *ordered* list of values.
        return [self[fieldName] for fieldName in self.keys()]
    def items(self):
        return [(fieldName, self[fieldName]) for fieldName in self.keys()]
    def iterkeys(self):
        for fieldDesc in self._description:
            yield fieldDesc[DESCRIPTION_NAME]
    __iter__ = iterkeys
    def itervalues(self):
        for fieldName in self:
            yield self[fieldName]
    def iteritems(self):
        for fieldName in self:
            yield fieldName, self[fieldName]


class _RequestBufferBuilder(object):
    def __init__(self, clusterIdentifier=None):
        self.clear()
        if clusterIdentifier:
            self._addCode(clusterIdentifier)
    def render(self):
        # Convert the RequestBufferBuilder's components to a binary Python str.
        return b('').join(self._buffer)
    def clear(self):
        self._buffer = []
    def _extend(self, otherRequestBuilder):
        self._buffer.append(otherRequestBuilder.render())
    def _addRaw(self, rawBuf):
        assert isinstance(rawBuf, mybytes)
        self._buffer.append(rawBuf)
    def _addCode(self, code):
        self._code2reqbuf(self._buffer, code)
    def _code2reqbuf(self, reqBuf, code):
        if isinstance(code, str):
            assert len(code) == 1
            code = ord(code)

        # The database engine considers little-endian integers "portable"; they
        # need to have been converted to little-endianness before being sent
        # across the network.
        reqBuf.append(struct.pack('<b', code))
    def _addString(self, code, s):
        _string2reqbuf(self._buffer, code, s)
    def _addNumeric(self, code, n, numCType='I'):
        _numeric2reqbuf(self._buffer, code, n, num_ctype=numCType)


class TPB(_RequestBufferBuilder):
    """Helper class for convenient and safe construction of custom Transaction
    Parameter Blocks.
    """
    def __init__(self):
        _RequestBufferBuilder.__init__(self)
        self._access_mode = isc_tpb_write
        self._isolation_level = isc_tpb_concurrency
        self._lock_resolution = isc_tpb_wait
        self._lock_timeout = None
        self._table_reservation = None
    def copy(self):
        "Returns a copy of self."
        # A shallow copy of self would be entirely safe except that
        # .table_reservation is a complex object that needs to be copied
        # separately.
        import copy
        other = copy.copy(self)
        if self._table_reservation is not None:
            other._table_reservation = copy.copy(self._table_reservation)
        return other
    def render(self):
        """Create valid `transaction parameter block` according to current 
        values of member attributes.
        
        :returns: (string) TPB block.
        """
        # YYY: Optimization:  Could memoize the rendered TPB str.
        self.clear()
        self._addCode(isc_tpb_version3)
        self._addCode(self._access_mode)
        il = self._isolation_level
        if not isinstance(il, tuple):
            il = (il,)
        for code in il:
            self._addCode(code)
        self._addCode(self._lock_resolution)
        if self._lock_timeout is not None:
            self._addCode(isc_tpb_lock_timeout)
            self._addRaw(struct.pack(
                # One bytes tells the size of the following value; an unsigned
                # int tells the number of seconds to wait before timing out.
                '<bI', struct.calcsize('I'), self._lock_timeout
            ))
        if self._table_reservation is not None:
            self._addRaw(self._table_reservation.render())
        return _RequestBufferBuilder.render(self)
    # access_mode property:
    def _get_access_mode(self):
        return self._access_mode
    def _set_access_mode(self, access_mode):
        if access_mode not in (isc_tpb_read, isc_tpb_write):
            raise ValueError('Access mode must be one of'
                                   ' (isc_tpb_read, isc_tpb_write).'
                                   )
        self._access_mode = access_mode
    
    #: (integer) Required access mode (`isc_tpb_read` or `isc_tpb_write`). 
    #: **Default:** `isc_tpb_write`
    access_mode = property(_get_access_mode, _set_access_mode)
    # isolation_level property:
    def _get_isolation_level(self):
        return self._isolation_level
    def _set_isolation_level(self, isolation_level):
        if isinstance(isolation_level, tuple):
            if len(isolation_level) != 2:
                raise ValueError('The tuple variant of isolation level'
                    ' must have two elements:  isc_tpb_read_committed in the'
                    ' first element and one of (isc_tpb_rec_version,'
                    ' isc_tpb_no_rec_version) in the second.')
            isolation_level, suboption = isolation_level
        elif isolation_level == isc_tpb_read_committed:
            suboption = isc_tpb_rec_version

        if isolation_level not in (isc_tpb_concurrency,
                                   isc_tpb_consistency,
                                   isc_tpb_read_committed):
            raise ValueError('Isolation level must be one of'
                                   ' (isc_tpb_concurrency, isc_tpb_consistency,'
                                   ' isc_tpb_read_committed).')

        if isolation_level == isc_tpb_read_committed:
            if suboption not in (isc_tpb_rec_version,
                                 isc_tpb_no_rec_version):
                raise ValueError('With isolation level'
                    ' isc_tpb_read_committed, suboption must be one of'
                    ' (isc_tpb_rec_version, isc_tpb_no_rec_version).')
            isolation_level = isolation_level, suboption
        self._isolation_level = isolation_level
    
    #: (integer or tuple) Required Transaction Isolation Level. 
    #: Single integer value equivalent to `isc_tpb_concurrency` or
    #: `isc_tpb_consistency`, or tuple of exactly two integer values, where
    #: the first one is `isc_tpb_read_committed` and second either 
    #: `isc_tpb_rec_version` or `isc_tpb_no_rec_version`. 
    #:
    #: When value `isc_tpb_read_committed` is assigned without suboption,
    #: the `isc_tpb_rec_version` is assigned as default suboption.
    #:
    #: **Default:** `isc_tpb_concurrency`
    isolation_level = property(_get_isolation_level, _set_isolation_level)

    # lock_resolution property:
    def _get_lock_resolution(self):
        return self._lock_resolution
    def _set_lock_resolution(self, lock_resolution):
        if lock_resolution not in (isc_tpb_wait, isc_tpb_nowait):
            raise ValueError('Lock resolution must be one of'
                                   ' (isc_tpb_wait, isc_tpb_nowait).')
        self._lock_resolution = lock_resolution
    
    #: (integer) Required lock resolution method. Either `isc_tpb_wait` or
    #: `isc_tpb_nowait`.
    #:
    #: **Default:** `isc_tpb_wait`
    lock_resolution = property(_get_lock_resolution, _set_lock_resolution)

    # lock_timeout property:
    def _get_lock_timeout(self):
        return self._lock_timeout
    def _set_lock_timeout(self, lock_timeout):
        if lock_timeout is not None:
            UINT_MAX = 2 ** (struct.calcsize('I') * 8) - 1
            if (not isinstance(lock_timeout, (int, mylong))) or (
                lock_timeout < 0 or lock_timeout > UINT_MAX):
                raise ValueError('Lock resolution must be either None'
                    ' or a non-negative int number of seconds between 0 and'
                    ' %d.' % UINT_MAX)
        self._lock_timeout = lock_timeout
    
    #: (integer) Required lock timeout or None. 
    #:
    #: **Default:** `None`
    lock_timeout = property(_get_lock_timeout, _set_lock_timeout)

    # table_reservation property (an instance of TableReservation):
    def _get_table_reservation(self):
        if self._table_reservation is None:
            self._table_reservation = TableReservation()
        return self._table_reservation
    def _set_table_reservation_access(self, _):
        raise ProgrammingError('Instead of changing the value of the'
            ' .table_reservation object itself, you must change its *elements*'
            ' by manipulating it as though it were a dictionary that mapped'
            '\n  "TABLE_NAME": (sharingMode, accessMode)'
            '\nFor example:'
            '\n  tpbBuilder.table_reservation["MY_TABLE"] ='
            ' (kinterbasdb.isc_tpb_protected, kinterbasdb.isc_tpb_lock_write)'
            )
    
    #: (:class:`TableReservation`) Table reservation specification. 
    #:
    #: **Default:** `None`.
    #:
    #: Instead of changing the value of the TableReservation object itself, you 
    #: must change its elements by manipulating it as though it were a dictionary 
    #: that mapped “TABLE_NAME”: (sharingMode, accessMode) For example:
    #:
    #: .. code-block:: python
    #:
    #:   tpb.table_reservation["MY_TABLE"] = (fdb.isc_tpb_protected, fdb.isc_tpb_lock_write)
    table_reservation = property(_get_table_reservation,
                                 _set_table_reservation_access)


class TableReservation(object):
    """A dictionary-like helper class that maps 
    “TABLE_NAME”: (sharingMode, accessMode). It performs validation of values
    assigned to keys.
    """
    _MISSING = object()
    _SHARING_MODE_STRS = {
        isc_tpb_shared:    'isc_tpb_shared',
        isc_tpb_protected: 'isc_tpb_protected',
        isc_tpb_exclusive: 'isc_tpb_exclusive',
    }
    _ACCESS_MODE_STRS = {
        isc_tpb_lock_read:  'isc_tpb_lock_read',
        isc_tpb_lock_write: 'isc_tpb_lock_write',
    }

    def __init__(self):
        self._res = {}
    def copy(self):
        # A shallow copy is fine.
        import copy
        return copy.copy(self)
    def render(self):
        """Create valid `table access parameter block` according to current 
        key/value pairs.
        
        :returns: (string) Table access definition block.
        """
        if not self:
            return b('')
        frags = []
        _ = frags.append
        for tableName, resDefs in self.iteritems():
            tableNameLenWithTerm = len(b(tableName)) + 1
            for (sharingMode, accessMode) in resDefs:
                _(int2byte(accessMode))
                _(struct.pack('<b%ds' % tableNameLenWithTerm,
                              tableNameLenWithTerm, b(tableName)
                              ))
                _(int2byte(sharingMode))
        return b('').join(frags)
    def __len__(self):
        return sum([len(item) for item in self._res.items()])
    def __nonzero__(self):
        return len(self) != 0
    def __getitem__(self, key):
        key = self._validateKey(key)
        if key in self._res:
            return self._res[key]
        else:
            nonNormalizedKey = key
            key = _normalizeDatabaseIdentifier(key)
            try:
                return self._res[key]
            except KeyError:
                raise KeyError('No table named "%s" is present.' %
                               nonNormalizedKey)
    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, TypeError):
            return default
    def __contains__(self, key):
        return (
            self.get(key, TableReservation._MISSING)
            is not TableReservation._MISSING
        )
    def __str__(self):
        if not self:
            return '<TableReservation with no entries>'
        frags = ['<TableReservation with entries:\n']
        _ = frags.append
        for tableName, resDefs in self.iteritems():
            _('  "%s":\n' % tableName)
            for rd in resDefs:
                sharingModeStr = TableReservation._SHARING_MODE_STRS[rd[0]]
                accessModeStr = TableReservation._ACCESS_MODE_STRS[rd[1]]
                _('    (%s, %s)\n' % (sharingModeStr, accessModeStr))
        _('>')
        return ''.join(frags)
    def keys(self):
        return self._res.keys()
    def values(self):
        return self._res.values()
    def items(self):
        return self._res.items()
    def iterkeys(self):
        return self._res.iterkeys()
    def itervalues(self):
        return self._res.itervalues()
    def iteritems(self):
        ### Todo: verify handling of P version differences
        if PYTHON_MAJOR_VER == 3:
            return self._res.items()
        else:
            return self._res.iteritems()
    def __setitem__(self, key, value):
        key = self._validateKey(key)
        key = _normalizeDatabaseIdentifier(key)
        # If the += operator is being applied, the form of value will be like:
        #   [(sharingMode0, accessMode0), ..., newSharingMode, newAccessMode]
        # For the sake of convenience, we detect this situation and handle it
        # "naturally".
        if isinstance(value, list) and len(value) >= 3:
            otherValues = value[:-2]
            value = tuple(value[-2:])
        else:
            otherValues = None
        if (
            (not isinstance(value, tuple))
            or len(value) != 2
            or value[0] not in
            (isc_tpb_shared, isc_tpb_protected, isc_tpb_exclusive)
            or value[1] not in (isc_tpb_lock_read, isc_tpb_lock_write)
            ):
            raise ValueError('Table reservation entry must be a 2-tuple of'
                             ' the following form:\n'
                             'element 0: sharing mode (one of (isc_tpb_shared,'
                             ' isc_tpb_protected, isc_tpb_exclusive))\n'
                             'element 1: access mode (one of (isc_tpb_lock_read,'
                             ' isc_tpb_lock_write))\n'
                             '%s is not acceptable.' % str(value)
                             )
        if otherValues is None:
            value = [value]
        else:
            otherValues.append(value)
            value = otherValues
        self._res[key] = value
    def _validateKey(self, key):
        ### Todo: verify handling of P version differences, refactor
        if PYTHON_MAJOR_VER == 3:
            keyMightBeAcceptable = isinstance(key, str)
            if keyMightBeAcceptable and isinstance(key, str):
                try:
                    key.encode('ASCII')
                except UnicodeEncodeError:
                    keyMightBeAcceptable = False
            if not keyMightBeAcceptable:
                raise TypeError('Only str keys are allowed.')
        else:
            keyMightBeAcceptable = isinstance(key, basestring)
            if keyMightBeAcceptable and isinstance(key, unicode):
                try:
                    key = key.encode('ASCII')
                except UnicodeEncodeError:
                    keyMightBeAcceptable = False
            if not keyMightBeAcceptable:
                raise TypeError('Only str keys are allowed.')
        return key


def _validateTPB(tpb):
    if isinstance(tpb, TPB):
        # TPB's accessor methods perform their own validation, and its
        # render method takes care of infrastructural trivia.
        return tpb
    elif isinstance(tpb, (ListeType, TupleType)):
        return tpb
    elif not (isinstance(tpb, mybytes) and len(tpb) > 0):
        raise ProgrammingError('TPB must be non-unicode string of length > 0')
    # The kinterbasdb documentation promises (or at least strongly implies)
    # that if the user tries to set a TPB that does not begin with
    # isc_tpb_version3, kinterbasdb will automatically supply that
    # infrastructural value.  This promise might cause problems in the future,
    # when isc_tpb_version3 is superseded.  A possible solution would be to
    # check the first byte against all known isc_tpb_versionX version flags,
    # like this:
    #   if tpb[0] not in (isc_tpb_version3, ..., isc_tpb_versionN):
    #      tpb = isc_tpb_version3 + tpb
    # That way, compatibility with old versions of the DB server would be
    # maintained, but client code could optionally specify a newer TPB version.
    if tpb[0] != isc_tpb_version3:
        tpb = isc_tpb_version3 + tpb
    return tpb

def _normalizeDatabaseIdentifier(ident):
    if ident.startswith('"') and ident.endswith('"'):
        # Quoted name; leave the case of the field name untouched, but
        # strip the quotes.
        return ident[1:-1]
    else:
        # Everything else is normalized to uppercase to support case-
        # insensitive lookup.
        return ident.upper()
