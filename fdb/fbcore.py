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
#  Contributor(s): ______________________________________.
#
# See LICENSE.TXT for details.

import sys
import os
import types
import unittest
import ibase
import ctypes, struct, time, datetime, decimal, weakref
from fdb.ibase import (frb_info_att_charset, isc_dpb_activate_shadow, 
    isc_dpb_address_path, isc_dpb_allocation, isc_dpb_begin_log, 
    isc_dpb_buffer_length, isc_dpb_cache_manager, isc_dpb_cdd_pathname, 
    isc_dpb_connect_timeout, isc_dpb_damaged, isc_dpb_dbkey_scope, isc_dpb_debug, 
    isc_dpb_delete_shadow, isc_dpb_disable_journal, isc_dpb_disable_wal, 
    isc_dpb_drop_walfile, isc_dpb_dummy_packet_interval, isc_dpb_enable_journal, 
    isc_dpb_encrypt_key, isc_dpb_force_write, isc_dpb_garbage_collect, 
    isc_dpb_gbak_attach, isc_dpb_gfix_attach, isc_dpb_gsec_attach, 
    isc_dpb_gstat_attach, isc_dpb_interp, isc_dpb_journal, isc_dpb_lc_ctype, 
    isc_dpb_lc_messages, isc_dpb_license, isc_dpb_no_garbage_collect, 
    isc_dpb_no_reserve, isc_dpb_num_buffers, isc_dpb_number_of_users, 
    isc_dpb_old_dump_id, isc_dpb_old_file, isc_dpb_old_file_size, 
    isc_dpb_old_num_files, isc_dpb_old_start_file, isc_dpb_old_start_page, 
    isc_dpb_old_start_seqno, isc_dpb_online, isc_dpb_online_dump, 
    isc_dpb_overwrite, isc_dpb_page_size, isc_dpb_password, isc_dpb_password_enc, 
    isc_dpb_quit_log, isc_dpb_reserved, isc_dpb_sec_attach, 
    isc_dpb_set_db_charset, isc_dpb_set_db_readonly, isc_dpb_set_db_sql_dialect, 
    isc_dpb_set_page_buffers, isc_dpb_shutdown, isc_dpb_shutdown_delay, 
    isc_dpb_sql_dialect, isc_dpb_sql_role_name, isc_dpb_sweep, 
    isc_dpb_sweep_interval, isc_dpb_sys_user_name, isc_dpb_sys_user_name_enc, 
    isc_dpb_trace, isc_dpb_user_name, isc_dpb_verify, isc_dpb_version1, 
    isc_dpb_wal_backup_dir, isc_dpb_wal_bufsize, isc_dpb_wal_chkptlen, 
    isc_dpb_wal_grp_cmt_wait, isc_dpb_wal_numbufs, isc_dpb_working_directory, 
    isc_info_active_tran_count, isc_info_active_transactions, isc_info_allocation, 
    isc_info_attachment_id, isc_info_backout_count, isc_info_base_level, 
    isc_info_bpage_errors, isc_info_creation_date, isc_info_cur_log_part_offset, 
    isc_info_cur_logfile_name, isc_info_current_memory, isc_info_db_SQL_dialect, 
    isc_info_db_class, isc_info_db_id, isc_info_db_provider, isc_info_db_read_only, 
    isc_info_db_size_in_pages, isc_info_db_sql_dialect, isc_info_delete_count, 
    isc_info_dpage_errors, isc_info_expunge_count, isc_info_fetches, 
    isc_info_firebird_version, isc_info_forced_writes, isc_info_implementation, 
    isc_info_insert_count, isc_info_ipage_errors, isc_info_isc_version, 
    isc_info_license, isc_info_limbo, isc_info_logfile, isc_info_marks, 
    isc_info_max_memory, isc_info_next_transaction, isc_info_no_reserve, 
    isc_info_num_buffers, isc_info_num_wal_buffers, isc_info_ods_minor_version, 
    isc_info_ods_version, isc_info_oldest_active, isc_info_oldest_snapshot, 
    isc_info_oldest_transaction, isc_info_page_errors, isc_info_page_size, 
    isc_info_ppage_errors, isc_info_purge_count, isc_info_read_idx_count, 
    isc_info_read_seq_count, isc_info_reads, isc_info_record_errors, 
    isc_info_set_page_buffers, isc_info_sql_stmt_commit, isc_info_sql_stmt_ddl, 
    isc_info_sql_stmt_delete, isc_info_sql_stmt_exec_procedure, 
    isc_info_sql_stmt_get_segment, isc_info_sql_stmt_insert, 
    isc_info_sql_stmt_put_segment, isc_info_sql_stmt_rollback, 
    isc_info_sql_stmt_savepoint, isc_info_sql_stmt_select, 
    isc_info_sql_stmt_select_for_upd, isc_info_sql_stmt_set_generator, 
    isc_info_sql_stmt_start_trans, isc_info_sql_stmt_update, isc_info_sweep_interval, 
    isc_info_tpage_errors, isc_info_tra_access, isc_info_tra_concurrency, 
    isc_info_tra_consistency, isc_info_tra_id, isc_info_tra_isolation, 
    isc_info_tra_lock_timeout, isc_info_tra_no_rec_version, isc_info_tra_oldest_active, 
    isc_info_tra_oldest_interesting, isc_info_tra_oldest_snapshot, 
    isc_info_tra_read_committed, isc_info_tra_readonly, isc_info_tra_readwrite, 
    isc_info_tra_rec_version, isc_info_update_count, isc_info_user_names, 
    isc_info_version, isc_info_wal_avg_grpc_size, isc_info_wal_avg_io_size, 
    isc_info_wal_buffer_size, isc_info_wal_ckpt_length, 
    isc_info_wal_cur_ckpt_interval, isc_info_wal_grpc_wait_usecs, 
    isc_info_wal_num_commits, isc_info_wal_num_io, isc_info_wal_prv_ckpt_fname, 
    isc_info_wal_prv_ckpt_poffset, isc_info_wal_recv_ckpt_fname, 
    isc_info_wal_recv_ckpt_poffset, isc_info_window_turns, isc_info_writes, 
    isc_tpb_autocommit, isc_tpb_commit_time, isc_tpb_concurrency, 
    isc_tpb_consistency, isc_tpb_exclusive, isc_tpb_ignore_limbo, isc_tpb_lock_read, 
    isc_tpb_lock_timeout, isc_tpb_lock_write, isc_tpb_no_auto_undo, 
    isc_tpb_no_rec_version, isc_tpb_nowait, isc_tpb_protected, isc_tpb_read, 
    isc_tpb_read_committed, isc_tpb_rec_version, isc_tpb_restart_requests, 
    isc_tpb_shared, isc_tpb_verb_time, isc_tpb_version3, isc_tpb_wait, isc_tpb_write
    )

from exceptions import NotImplementedError

PYTHON_MAJOR_VER = sys.version_info[0]

__version__ = '0.7.1'
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

# Exceptions required by Python Database API

class Warning(Exception):
    pass
class Error(Exception):
    pass
class InterfaceError(Error):
    pass
class DatabaseError(Error):
    pass
    #def __init__(self,msg, sqlcode=0,error_code=0):
        #self.sqlcode = sqlcode
        #self.error_code = error_code
        #self.msg = msg
class DataError(DatabaseError):
    pass
class OperationalError(DatabaseError):
    pass
class IntegrityError(DatabaseError):
    pass
class InternalError(DatabaseError):
    pass
class ProgrammingError(DatabaseError):
    pass
class NotSupportedError(DatabaseError):
    pass

class TransactionConflict(DatabaseError):
    pass

# Named positional constants to be used as indices into the description
# attribute of a cursor (these positions are defined by the DB API spec).
# For example:
#   nameOfFirstField = cursor.description[0][kinterbasdb.DESCRIPTION_NAME]

DESCRIPTION_NAME            = 0
DESCRIPTION_TYPE_CODE       = 1
DESCRIPTION_DISPLAY_SIZE    = 2
DESCRIPTION_INTERNAL_SIZE   = 3
DESCRIPTION_PRECISION       = 4
DESCRIPTION_SCALE           = 5
DESCRIPTION_NULL_OK         = 6

# Types Required by Python DB-API 2.0

def Date(year, month, day):
    return datetime.date(year, month, day)
def Time(hour, minite, second):
    return datetime.time(hour, minite, second)
def DateFromTicks(ticks):
    return apply(Date,time.localtime(ticks)[:3])
def TimeFromTicks(ticks):
    return apply(Time,time.localtime(ticks)[3:6])
def TimestampFromTicks(ticks):
    return apply(Timestamp,time.localtime(ticks)[:6])
def Binary(b):
    return b

class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1
STRING = DBAPITypeObject(str)
if PYTHON_MAJOR_VER==3:
    BINARY = DBAPITypeObject(bytes)
else:
    BINARY = DBAPITypeObject(str)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID = DBAPITypeObject()

_FS_ENCODING = sys.getfilesystemencoding()

def bs(byte_array):
    if PYTHON_MAJOR_VER==3:
        return bytes(byte_array)
    return ''.join([chr(c) for c in byte_array])

ISOLATION_LEVEL_READ_UNCOMMITTED = 0
ISOLATION_LEVEL_READ_COMMITED = 1
ISOLATION_LEVEL_REPEATABLE_READ = 2
ISOLATION_LEVEL_SERIALIZABLE = 3

transaction_parameter_block = {
    ISOLATION_LEVEL_READ_UNCOMMITTED:
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, 
        isc_tpb_read_committed, isc_tpb_no_rec_version]),
    ISOLATION_LEVEL_READ_COMMITED:
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, 
        isc_tpb_read_committed, isc_tpb_rec_version]),
    ISOLATION_LEVEL_REPEATABLE_READ:
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, 
        isc_tpb_concurrency]),
    ISOLATION_LEVEL_SERIALIZABLE:
    bs([isc_tpb_version3, isc_tpb_write, isc_tpb_wait, 
        isc_tpb_consistency]),
}

_SIZE_OF_SHORT = ctypes.sizeof(ctypes.c_short)

_tenTo = [10**x for x in range(20)]
del x

__xsqlda_cache = {}

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
    isc_info_oldest_transaction, isc_info_oldest_active, isc_info_oldest_snapshot,
    isc_info_next_transaction, isc_info_active_tran_count,
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
            ('version', ibase.ISC_SHORT),
            ('sqldaid', ibase.ISC_SCHAR * 8),
            ('sqldabc', ibase.ISC_LONG),
            ('sqln', ibase.ISC_SHORT),
            ('sqld', ibase.ISC_SHORT),
            ('sqlvar', ibase.XSQLVAR * size),
        ]
        __xsqlda_cache[size] = XSQLDA
        cls = XSQLDA
    xsqlda = cls()
    xsqlda.version = ibase.SQLDA_version1
    xsqlda.sqln = size
    return xsqlda


buf_pointer = ctypes.POINTER(ctypes.c_char)
def bytes_to_bint(b):           # Read as big endian
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b ==2:
        fmt = '>h'
    elif len_b ==4:
        fmt = '>l'
    elif len_b ==8:
        fmt = '>q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]
def bytes_to_int(b):            # Read as little endian.
    len_b = len(b)
    if len_b == 1:
        fmt = 'b'
    elif len_b ==2:
        fmt = '<h'
    elif len_b ==4:
        fmt = '<l'
    elif len_b ==8:
        fmt = '<q'
    else:
        raise InternalError
    return struct.unpack(fmt, b)[0]
def bint_to_bytes(val, nbytes): # Convert int value to big endian bytes.
    if nbytes == 1:
        fmt = 'b'
    elif nbytes ==2:
        fmt = '>h'
    elif nbytes ==4:
        fmt = '>l'
    elif nbytes ==8:
        fmt = '>q'
    else:
        raise InternalError
    return struct.pack(fmt, val)
def int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    if nbytes == 1:
        fmt = 'b'
    elif nbytes ==2:
        fmt = '<h'
    elif nbytes ==4:
        fmt = '<l'
    elif nbytes ==8:
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
    sqlcode = ibase.isc_sqlcode(status)
    error_code = status[1]
    msglist.append('- SQLCODE: %i' % sqlcode)
    
    ibase.isc_sql_interprete(sqlcode,msg,512)
    msglist.append('- '+msg.value)
    
    pvector = ctypes.cast(ctypes.addressof(status),ibase.ISC_STATUS_PTR)
    result = 0

    while result == 0:
        result = ibase.fb_interpret(msg,512,pvector)
        msglist.append('- '+msg.value)
    return error('\n'.join(msglist),sqlcode,error_code)

def build_dpb(user,password,sql_dialect,role,charset,buffers,force_write,no_reserve,db_key_scope):
    params = [chr(isc_dpb_version1)]

    def addString(codeAsByte, s):
        #assert isinstance(codeAsByte,types.IntType) and codeAsByte >= 0 and codeAsByte <= 255
        sLen = len(s)
        if sLen >= 256:
            # Because the length is denoted in the DPB by a single byte.
            raise ProgrammingError("Individual component of database"
                " parameter buffer is too large.  Components must be less"
                " than 256 bytes."
              )
        format = 'cc%ds' % sLen # like 'cc50s' for a 50-byte string
        newEntry = struct.pack(format, chr(codeAsByte), chr(sLen), s)
        params.append(newEntry)
    def addInt(codeAsByte, value):
        #assert isinstance(codeAsByte,types.IntType) and codeAsByte >= 0 and codeAsByte <= 255
        if not isinstance(value, (int, long)) or value < 0 or value > 255:
            raise ProgrammingError("The value for an integer DPB code must be"
                " an int or long with a value between 0 and 255."
              )
        newEntry = struct.pack('ccc', chr(codeAsByte), '\x01', chr(value))
        params.append(newEntry)

    if user:
        addString(isc_dpb_user_name,user)
    if password:
        addString(isc_dpb_password,password)
    if role:
        addString(isc_dpb_sql_role_name,role)
    if sql_dialect:
        addInt(isc_dpb_sql_dialect,sql_dialect)
    if charset:
        addString(isc_dpb_lc_ctype, charset)
    if buffers:
        addInt(isc_dpb_num_buffers,buffers)
    if force_write:
        addInt(isc_dpb_force_write,force_write)
    if no_reserve:
        addInt(isc_dpb_no_reserve,no_reserve)
    if db_key_scope:
        addInt(isc_dpb_dbkey_scope,db_key_scope)
    return ''.join(params)

def connect(*args, **kwargs):
    """
    Establishes a firedb.Connection to a database.
    """
    dsn = kwargs.get('dsn','')
    user = kwargs.get('user',os.environ.get('ISC_USER', None))
    password = kwargs.get('password',os.environ.get('ISC_PASSWORD', None))
    host = kwargs.get('host',None)
    database = kwargs.get('database',None)
    sql_dialect = kwargs.get('sql_dialect',3)
    role = kwargs.get('role',None)
    charset = kwargs.get('charset',None)
    buffers = kwargs.get('buffers',None)
    force_write = kwargs.get('force_write',None)
    no_reserve = kwargs.get('no_reserve',None)
    db_key_scope = kwargs.get('db_key_scope',None)
    isolation_level = kwargs.get('isolation_level',ISOLATION_LEVEL_READ_COMMITED)
    #port = kwargs.get('port',3050)
    
    if sql_dialect not in [1,2,3]:
        raise ProgrammingError("SQl Dialect must be either 1, 2 or 3")
        
    if (   (not dsn and not host and not database)
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

    if _FS_ENCODING:
        dsn = dsn.encode(_FS_ENCODING)
    
    dpb = build_dpb(user,password,sql_dialect,role,charset,buffers,force_write,no_reserve,db_key_scope)
    
    _isc_status = ibase.ISC_STATUS_ARRAY()
    _db_handle = ibase.isc_db_handle(0)
    
    ibase.isc_attach_database(_isc_status,len(dsn),dsn,_db_handle,len(dpb),dpb)
    if db_api_error(_isc_status):
        raise exception_from_status(DatabaseError,_isc_status,
                              "Error while connecting to database:")
    
    return Connection(_db_handle,dpb,sql_dialect,charset)

def create_database(*args):
    """
      Creates a new database with the supplied "CREATE DATABASE" statement.
      Returns an active kinterbasdb.Connection to the newly created database.

    Parameters:
    $sql: string containing the CREATE DATABASE statement.  Note that you may
       need to specify a username and password as part of this statement (see
       the Firebird SQL Reference for syntax).
    $dialect: (optional) the SQL dialect under which to execute the statement
    """
    if len(args) >= 1 and isinstance(args[0], unicode):
        args = (args[0].encode(_FS_ENCODING),) + args[1:]

    if len(args) > 1:
        dialect = args[1]
    else:
        dialect = 3
    sql = args[0]
    
    isc_status = ibase.ISC_STATUS_ARRAY(0)
    trans_handle = ibase.isc_tr_handle(0)
    db_handle = ibase.isc_db_handle(0)
    xsqlda = xsqlda_factory(1)
    
    # For yet unknown reason, the isc_dsql_execute_immediate segfaults when
    # NULL (None) is passed as XSQLDA, so we provide one here
    ibase.isc_dsql_execute_immediate(isc_status,db_handle,trans_handle,
                                     ctypes.c_ushort(len(sql)),sql,
                                     dialect,ctypes.cast(ctypes.pointer(xsqlda),ctypes.POINTER(ibase.XSQLDA)))
    if db_api_error(isc_status):
        raise exception_from_status(ProgrammingError,isc_status,
                              "Error while creating database:")
    
    return Connection(db_handle)

class Connection(object):
    """
    Represents a connection between the database client (the Python process)
    and the database server.

    The basic functionality of this class is documented by the Python DB API
    Specification 2.0, while the large amount of additional functionality is
    documented by the FireDB Usage Guide.
    """

    def __init__(self, db_handle, dpb = None, sql_dialect = 3, charset = None):
        
        self._main_transaction = Transaction([self])
        self._transactions = [self._main_transaction]
        self._cursors = []  # Weak references to cursors
        self.__precision_cache = {}
        self._default_tpb = transaction_parameter_block[ISOLATION_LEVEL_READ_COMMITED]
        
        self.sql_dialect = sql_dialect
        self._dpb = dpb
        self._charset = charset

        self._isc_status = ibase.ISC_STATUS_ARRAY()
        self._db_handle = db_handle
        self.out_sqlda = xsqlda_factory(10)
        self.__ic = Cursor(self,self.main_transaction) # Cursor for internal use
    def __remove_cursor(self,cursor_ref):
        self._cursors.remove(cursor_ref)
    def _determine_field_precision(self,sqlvar):
        if sqlvar.relname_length == 0 or sqlvar.sqlname_length == 0:
            # Either or both field name and relation name are not provided,
            # so we cannot determine field precision. It's normal situation
            # for example for queries with dynamically computed fields
            return 0
        # Special case for automatic RDB$DB_KEY fields.
        if ((sqlvar.sqlname_length == 6 and sqlvar.sqlname == 'DB_KEY') or
            (sqlvar.sqlname_length == 10 and sqlvar.sqlname == 'RDB$DB_KEY')):
            return 0
        precision = self.__precision_cache.get((sqlvar.relname,sqlvar.sqlname))
        # First, try table
        self.__ic.execute("SELECT FIELD_SPEC.RDB$FIELD_PRECISION"
                          " FROM RDB$FIELDS FIELD_SPEC, RDB$PROCEDURE_PARAMETERS REL_FIELDS"
                          " WHERE"
                          " FIELD_SPEC.RDB$FIELD_NAME = REL_FIELDS.RDB$FIELD_SOURCE"
                          " AND RDB$PROCEDURE_NAME = ?"
                          " AND RDB$PARAMETER_NAME = ?"
                          " AND RDB$PARAMETER_TYPE = 1",(sqlvar.relname,sqlvar.sqlname))
        result = self.__ic.fetchone()
        if result:
            return result[0]
        # Next, try stored procedure output parameter
        self.__ic.execute("SELECT FIELD_SPEC.RDB$FIELD_PRECISION"
                          " FROM RDB$FIELDS FIELD_SPEC, RDB$RELATION_FIELDS REL_FIELDS"
                          " WHERE"
                          " FIELD_SPEC.RDB$FIELD_NAME = REL_FIELDS.RDB$FIELD_SOURCE"
                          " AND REL_FIELDS.RDB$RELATION_NAME = ?"
                          " AND REL_FIELDS.RDB$FIELD_NAME = ?",(sqlvar.relname,sqlvar.sqlname))
        result = self.__ic.fetchone()
        if result:
            return result[0]
        # We ran out of options
        return 0
    def __check_attached(self):
        if self._db_handle == None:
            raise ProgrammingError("Connection object is detached from database")
    def __get_main_transaction(self):
        return self._main_transaction
    def __get_transactions(self):
        return tuple(self._transactions)
    def __get_closed(self):
        return self._db_handle == None
    def __default_tpb_get(self):
        return self._default_tpb
    def __default_tpb_set(self, value):
        self._default_tpb = _validateTPB(value)
    def __parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_int(raw_value) + 678882
        century = (4 * nday -1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day -3) // 153
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
    def drop_database(self):
        """
          Drops the database to which this connection is attached.

          Unlike plain file deletion, this method behaves responsibly, in that
        it removes shadow files and other ancillary files for this database.
        """
        saved_handle = ibase.isc_db_handle(self._db_handle.value)
        self.close(detach=False)
        ibase.isc_drop_database(self._isc_status,saved_handle)
        if db_api_error(self._isc_status):
            self._db_handle = saved_handle
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while dropping database:")
    def execute_immediate(self,sql):
        self.__check_attached()
        if self.main_transaction.closed:
            self.begin()
        self.main_transaction._execute_immediate(sql)
    def database_info(self, info_code, result_type):
        """
          Wraps the Interbase C API function isc_database_info.

          For documentation, see the IB 6 API Guide section entitled
        "Requesting information about an attachment" (p. 51).

          Note that this method is a VERY THIN wrapper around the IB C API
        function isc_database_info.  This method does NOT attempt to interpret
        its results except with regard to whether they are a string or an
        integer.

          For example, requesting isc_info_user_names will return a string
        containing a raw succession of length-name pairs.  A thicker wrapper
        might interpret those raw results and return a Python tuple, but it
        would need to handle a multitude of special cases in order to cover
        all possible isc_info_* items.

          Note:  Some of the information available through this method would be
        more easily retrieved with the Services API (see submodule
        kinterbasdb.services).

        Parameters:
        $result_type must be either:
           's' if you expect a string result, or
           'i' if you expect an integer result
        """
        self.__check_attached()
        request_buffer = bs([info_code])
        buf_size = 256
        while True:
            res_buf = chr(0) * buf_size
            ibase.isc_database_info(self._isc_status,self._db_handle,
                                       len(request_buffer),request_buffer,
                                       len(res_buf),res_buf)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while requesting database information:")
            i = buf_size - 1
            while i >= 0:
                if res_buf[i] != chr(0):
                    break
                else:
                    i -= 1
            if ord(res_buf[i]) == ibase.isc_info_truncated:
                if buf_size < ibase.SHRT_MAX:
                    buf_size *= 2
                    if buf_size > ibase.SHRT_MAX:
                        buf_size = ibase.SHRT_MAX
                    continue
                else:
                    raise OperationalError("Result is too large to fit into buffer"
                            " of size SHRT_MAX, yet underlying info function only"
                            " accepts buffers with size <= SHRT_MAX.")
            else:
                break
        if ord(res_buf[i]) != ibase.isc_info_end:
            raise InternalError("Exited request loop sucessfuly, but"
                                " res_buf[i] != sc_info_end.")
        if request_buffer[0] != res_buf[0]:
            raise InternalError("Result code does not match request code.")
        if result_type.upper() == 'I':
            return bytes_to_int(res_buf[3:3+bytes_to_int(res_buf[1:3])])
        elif (result_type.upper() == 'S' 
              and info_code not in _DATABASE_INFO__KNOWN_LOW_LEVEL_EXCEPTIONS):
            # The result buffers for a few request codes don't follow the generic
            # conventions, so we need to return their full contents rather than
            # omitting the initial infrastructural bytes.
            return ctypes.string_at(res_buf[3:],i-3)
        elif result_type.upper() == 'S':
            return ctypes.string_at(res_buf,i)
        else:
            raise InterfaceError("Unknown result type requested (must be 'i' or 's').")

    def db_info(self, request):
        """
          Higher-level convenience wrapper around the database_info method that
        parses the output of database_info into Python-friendly objects instead
        of returning raw binary buffers in the case of complex result types.
        If an unrecognized code is requested, ValueError is raised.

        Parameters:
        $request must be either:
          - A single kinterbasdb.isc_info_* info request code.
            In this case, a single result is returned.
          - A sequence of such codes.
            In this case, a mapping of (info request code -> result) is
            returned.
        """
        def _extractDatabaseInfoCounts(buf):
            # Extract a raw binary sequence of (unsigned short, signed int) pairs into
            # a corresponding Python dictionary.
            uShortSize = struct.calcsize('<H')
            intSize = struct.calcsize('<i')
            pairSize = uShortSize + intSize
            pairCount = len(buf) / pairSize
        
            counts = {}
            for i in range(pairCount):
                bufForThisPair = buf[i*pairSize:(i+1)*pairSize]
                relationId = struct.unpack('<H', bufForThisPair[:uShortSize])[0]
                count      = struct.unpack('<i', bufForThisPair[uShortSize:])[0]
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
                baseLevel = struct.unpack('B', buf[1])[0]
                results[infoCode] = baseLevel
            elif infoCode == isc_info_db_id:
                # (IB 6 API Guide page 52)
                buf = self.database_info(infoCode, 's')
                pos = 0

                conLocalityCode = struct.unpack('B', buf[pos])[0]
                pos += 1

                dbFilenameLen = struct.unpack('B', buf[1])[0]
                pos += 1

                dbFilename = buf[pos:pos+dbFilenameLen]
                pos += dbFilenameLen

                siteNameLen = struct.unpack('B', buf[pos])[0]
                pos += 1

                siteName = buf[pos:pos+siteNameLen]
                pos += siteNameLen

                results[infoCode] = (conLocalityCode, dbFilename, siteName)
            elif infoCode == isc_info_implementation:
                # (IB 6 API Guide page 52)
                buf = self.database_info(infoCode, 's')
                # Skip the first four bytes.
                pos = 1

                implNumber = struct.unpack('B', buf[pos])[0]
                pos += 1

                classNumber = struct.unpack('B', buf[pos])[0]
                pos += 1

                results[infoCode] = (implNumber, classNumber)
            elif infoCode in (isc_info_version, isc_info_firebird_version):
                # (IB 6 API Guide page 53)
                buf = self.database_info(infoCode, 's')
                # Skip the first byte.
                pos = 1

                versionStringLen = struct.unpack('B', buf[pos])[0]
                pos += 1

                versionString = buf[pos:pos+versionStringLen]

                results[infoCode] = versionString
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
                    if struct.unpack('B', buf[pos])[0] != isc_info_user_names:
                        raise OperationalError('While trying to service'
                            ' isc_info_user_names request, found unexpected'
                            ' results buffer contents at position %d of [%s]'
                            % (pos, buf)
                          )
                    pos += 1

                    # The two-byte cluster length:
                    nameClusterLen = struct.unpack('<H', buf[pos:pos+2])[0]
                    pos += 2

                    # The one-byte username length:
                    nameLen = struct.unpack('B', buf[pos])[0]
                    assert nameLen == nameClusterLen - 1
                    pos += 1

                    usernames.append(buf[pos:pos+nameLen])
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
                results[infoCode] = datetime.datetime(yyyy, mm, dd, h, m, s, ms)
            else:
                raise ValueError('Unrecognized database info code %s' % str(infoCode))

        if requestIsSingleton:
            return results[request[0]]
        else:
            return results
    def transaction_info(self, info_code, result_type):
        return self._main_transaction.transaction_info(info_code, result_type)
    def trans_info(self, request):
        return self._main_transaction.trans_info(request)
    def trans(self,tpb=None):
        self.__check_attached()
        if tpb:
            _tpb = tpb
        else:
            _tpb = self._default_tpb
        transaction = Transaction([self],_tpb)
        self._transactions.append(transaction)
        return transaction
    def close(self,detach=True):
        if self._db_handle != None:
            self.__ic.close()
            for cursor in self._cursors:
                cursor().close()
            for transaction in self._transactions:
                transaction.close()
            if detach:
                ibase.isc_detach_database(self._isc_status,self._db_handle)
            self._db_handle = None
    def begin(self,tpb=None):
        self.__check_attached()
        if tpb:
            self._main_transaction.tpb = tpb
        self._main_transaction.begin()
    def savepoint(self, name):
        """
          Establishes a SAVEPOINT named $name.
          To rollback to this SAVEPOINT, use rollback(savepoint=name).

          Example:
            con.savepoint('BEGINNING_OF_SOME_SUBTASK')
            ...
            con.rollback(savepoint='BEGINNING_OF_SOME_SUBTASK')
        """
        return self._main_transaction.savepoint(name)
    def commit(self,retaining=False):
        self.__check_attached()
        self._main_transaction.commit(retaining)
    def rollback(self,retaining=False,savepoint=None):
        self.__check_attached()
        self._main_transaction.rollback(retaining,savepoint)
    def cursor(self):
        self.__check_attached()
        c = Cursor(self,self.main_transaction)
        self._cursors.append(weakref.ref(c,self.__remove_cursor))
        return c
    def prepare(self):
        self.__check_attached()
        raise NotImplementedError('Connection.prepare')
    def __del__(self):
        if self._db_handle != None:
            self.close()
    def _get_charset(self):
        return self._charset
    def _set_charset(self, value):
        # More informative error message:
        raise AttributeError("A connection's 'charset' property can be"
            " specified upon Connection creation as a keyword argument to"
            " kinterbasdb.connect, but it cannot be modified thereafter."
          )
    charset = property(_get_charset, _set_charset)
    transactions = property(__get_transactions)
    main_transaction = property(__get_main_transaction)
    default_tpb = property(__default_tpb_get,__default_tpb_set)
    closed = property(__get_closed)

class PreparedStatement(object):
    """
    Represents a prepared statement, an "inner" database cursor, which is used 
    to manage the context of a fetch operation. 
    """
    RESULT_SET_EXHAUSTED = 100
    NO_FETCH_ATTEMPTED_YET = -1
    
    def __init__(self,operation,cursor,internal=True):
        self.sql = operation
        self.__internal = internal
        if internal:
            self.cursor = weakref.ref(cursor)
        else:
            self.cursor = cursor
        self._stmt_handle = None # isc_stmt_handle(0)
        self._isc_status = ibase.ISC_STATUS_ARRAY()
        self.out_sqlda = xsqlda_factory(10)
        self.in_sqlda = xsqlda_factory(10)
        self.in_sqlda_save = []
        self.statement_type = None
        self.__executed = False
        self.__prepared = False
        self.__closed = False
        self.__description = None
        self.__output_cache = None
        #self.out_buffer = None
        self._last_fetch_status = ibase.ISC_STATUS(self.NO_FETCH_ATTEMPTED_YET)

        # allocate statement handle
        self._stmt_handle = ibase.isc_stmt_handle(0)
        ibase.isc_dsql_allocate_statement(self._isc_status,
                                          self.__get_connection()._db_handle,
                                          self._stmt_handle)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while allocating SQL statement:")
        # prepare statement
        ibase.isc_dsql_prepare(self._isc_status,self.__get_transaction()._tr_handle,
                               self._stmt_handle,len(operation),operation,
                               self.__get_connection().sql_dialect,
                               ctypes.cast(ctypes.pointer(self.out_sqlda),ctypes.POINTER(ibase.XSQLDA)))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while preparing SQL statement:")
        # Determine statement type
        info = ' ' * 20
        ibase.isc_dsql_sql_info(self._isc_status,self._stmt_handle,
                                1,bs([ibase.isc_info_sql_stmt_type]),len(info),info)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while determining SQL statement type:")
        if ord(info[0]) != ibase.isc_info_sql_stmt_type:
            raise InternalError("Cursor.execute, determine statement type:\n"
                                "first byte must be 'isc_info_sql_stmt_type'")
        self.statement_type = bytes_to_int(info[3:3+bytes_to_int(info[1:3])])
        # Init XSQLDA for input parameters
        ibase.isc_dsql_describe_bind(self._isc_status,self._stmt_handle,
                                     self.__get_connection().sql_dialect,
                                     ctypes.cast(ctypes.pointer(self.in_sqlda),ctypes.POINTER(ibase.XSQLDA)))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while determining SQL statement parameters:")
        if self.in_sqlda.sqld > self.in_sqlda.sqln:
            self.in_sqlda = xsqlda_factory(self.in_sqlda.sqld)
            ibase.isc_dsql_describe_bind(self._isc_status,self._stmt_handle,
                                         self.__get_connection().sql_dialect,
                                         ctypes.cast(ctypes.pointer(self.in_sqlda),ctypes.POINTER(ibase.XSQLDA)))
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while determining SQL statement parameters:")
        self.n_input_params = self.in_sqlda.sqld
        # record original type and size information so it can be restored for
        # subsequent executions (mind the implicit string conversions!)
        for sqlvar in self.in_sqlda.sqlvar[:self.n_input_params]:
            self.in_sqlda_save.append((sqlvar.sqltype,sqlvar.sqllen))
        # Init output XSQLDA
        ibase.isc_dsql_describe(self._isc_status,self._stmt_handle,
                                     self.__get_connection().sql_dialect,
                                     ctypes.cast(ctypes.pointer(self.out_sqlda),ctypes.POINTER(ibase.XSQLDA)))
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while determining SQL statement output:")
        if self.out_sqlda.sqld > self.out_sqlda.sqln:
            self.out_sqlda = xsqlda_factory(self.out_sqlda.sqld)
            ibase.isc_dsql_describe(self._isc_status,self._stmt_handle,
                                         self.__get_connection().sql_dialect,
                                         ctypes.cast(ctypes.pointer(self.out_sqlda),ctypes.POINTER(ibase.XSQLDA)))
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while determining SQL statement output:")
        self.n_output_params = self.out_sqlda.sqld
        self.__coerce_XSQLDA(self.out_sqlda)
        self.__prepared = True
        self._name = None
    def __get_connection(self):
        if self.__internal:
            return self.cursor()._connection
        else:
            return self.cursor._connection
    def __get_transaction(self):
        if self.__internal:
            return self.cursor()._transaction
        else:
            return self.cursor._transaction
    def __get_plan(self):
        buf_size = 256
        while True:
            info = ' ' * buf_size
            ibase.isc_dsql_sql_info(self._isc_status,self._stmt_handle,2,
                                    bs([ibase.isc_info_sql_get_plan,ibase.isc_info_end]),
                                    len(info),info)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while determining rowcount:")
            if ord(info[0]) == ibase.isc_info_truncated:
                if buf_size < ibase.SHRT_MAX:
                    buf_size *= 2
                    if buf_size > ibase.SHRT_MAX:
                        buf_size = ibase.SHRT_MAX
                    continue
                else:
                    return "Plan is too big"
            else:
                break
        if ord(info[0]) == ibase.isc_info_end:
            return None
        if ord(info[0]) != ibase.isc_info_sql_get_plan:
            raise IndentationError("Unexpected code in result buffer while querying SQL plan.")
        size = bytes_to_int(info[1:_SIZE_OF_SHORT+1])
        # Skip first byte: a new line
        return ctypes.string_at(info[_SIZE_OF_SHORT+2:],size-1)
    def __is_fixed_point(self,dialect,data_type,subtype,scale):
        return ((data_type in [ibase.SQL_SHORT,ibase.SQL_LONG,ibase.SQL_INT64]
                  and (subtype or scale)
                 )
                 or ((dialect < 3) and scale and 
                     (data_type in [ibase.SQL_DOUBLE,ibase.SQL_D_FLOAT]))
                )
        
    def __get_external_data_type_name(self,dialect,data_type,subtype,scale):
        if data_type == ibase.SQL_TEXT:
            return 'CHAR'
        elif data_type == ibase.SQL_VARYING:
            return 'VARCHAR'
        elif self.__is_fixed_point(dialect,data_type,subtype,scale):
            if subtype == ibase.SUBTYPE_NUMERIC:
                return 'NUMERIC'
            elif subtype == ibase.SUBTYPE_DECIMAL:
                return 'DECIMAL'
            else:
                return 'NUMERIC/DECIMAL'
        elif data_type == ibase.SQL_SHORT:
            return 'SMALLINT'
        elif data_type == ibase.SQL_LONG:
            return 'INTEGER'
        elif data_type == ibase.SQL_INT64:
            return 'BIGINT'
        elif data_type == ibase.SQL_FLOAT:
            return 'FLOAT'
        elif data_type in [ibase.SQL_DOUBLE,ibase.SQL_D_FLOAT]:
            return 'DOUBLE'
        elif data_type == ibase.SQL_TIMESTAMP:
            return 'TIMESTAMP'
        elif data_type == ibase.SQL_TYPE_DATE:
            return 'DATE'
        elif data_type == ibase.SQL_TYPE_TIME:
            return 'TIME'
        elif data_type == ibase.SQL_BLOB:
            return 'BLOB'
        else:
            return 'UNKNOWN'
    def __get_internal_data_type_name(self,data_type):
        if data_type == ibase.SQL_TEXT:
            return 'SQL_TEXT'
        elif data_type == ibase.SQL_VARYING:
            return 'SQL_VARYING'
        elif data_type == ibase.SQL_SHORT:
            return 'SQL_SHORT'
        elif data_type == ibase.SQL_LONG:
            return 'SQL_LONG'
        elif data_type == ibase.SQL_INT64:
            return 'SQL_INT64'
        elif data_type == ibase.SQL_FLOAT:
            return 'SQL_FLOAT'
        elif data_type in [ibase.SQL_DOUBLE,ibase.SQL_D_FLOAT]:
            return 'SQL_DOUBLE'
        elif data_type == ibase.SQL_TIMESTAMP:
            return 'SQL_TIMESTAMP'
        elif data_type == ibase.SQL_TYPE_DATE:
            return 'SQL_TYPE_DATE'
        elif data_type == ibase.SQL_TYPE_TIME:
            return 'SQL_TYPE_TIME'
        elif data_type == ibase.SQL_BLOB:
            return 'SQL_BLOB'
        else:
            return 'UNKNOWN'
    def __get_description(self):
        if not self.__description:
            desc = []
            if self.__executed and (self.out_sqlda.sqld > 0):
                for sqlvar in self.out_sqlda.sqlvar[:self.out_sqlda.sqld]:
                    # Field name (or alias)
                    sqlname = sqlvar.sqlname[:sqlvar.sqlname_length]
                    alias = sqlvar.aliasname[:sqlvar.aliasname_length]
                    if alias != sqlname:
                        sqlname = alias
                    # Type information
                    intsize = sqlvar.sqllen
                    vartype = sqlvar.sqltype & ~1
                    scale = sqlvar.sqlscale
                    precision = 0
                    if vartype in [ibase.SQL_TEXT,ibase.SQL_VARYING]:
                        vtype = types.StringType
                        dispsize = sqlvar.sqllen
                    elif (vartype in [ibase.SQL_SHORT,ibase.SQL_LONG,ibase.SQL_INT64] 
                          and (sqlvar.sqlsubtype or scale)):
                        vtype = decimal.Decimal
                        precision = self.__get_connection()._determine_field_precision(sqlvar)
                        dispsize = 20
                    elif vartype == ibase.SQL_SHORT:
                        vtype = types.IntType
                        dispsize = 6
                    elif vartype == ibase.SQL_LONG:
                        vtype = types.IntType
                        dispsize = 11
                    elif vartype == ibase.SQL_INT64:
                        vtype = types.LongType
                        dispsize = 20
                    elif vartype in [ibase.SQL_FLOAT,ibase.SQL_DOUBLE,ibase.SQL_D_FLOAT]:
                        # Special case, dialect 1 DOUBLE/FLOAT could be Fixed point
                        if (self.__get_connection().sql_dialect < 3) and scale:
                            vtype = decimal.Decimal
                            precision = self.__get_connection()._determine_field_precision(sqlvar)
                        else:
                            vtype = types.FloatType
                        dispsize = 17
                    elif vartype == ibase.SQL_BLOB:
                        scale = sqlvar.sqlsubtype
                        vtype = types.StringType
                        dispsize = 0
                    elif vartype == ibase.SQL_TIMESTAMP:
                        vtype = datetime.datetime
                        dispsize = 22
                    elif vartype == ibase.SQL_TYPE_DATE:
                        vtype= datetime.date
                        dispsize = 10
                    elif vartype == ibase.SQL_TYPE_TIME:
                        vtype = datetime.time
                        dispsize = 11
                    elif vartype == ibase.SQL_ARRAY:
                        vtype = types.ListType
                        dispsize = -1
                    else:
                        vtype = None
                        dispsize = -1
                    desc.append(tuple([sqlname,vtype,dispsize,intsize,precision,scale,bool(sqlvar.sqltype & 1)]))
                self.__description = tuple(desc)
        return self.__description
    def __get_rowcount(self):
        result = -1
        if self.__executed and self.statement_type in [isc_info_sql_stmt_select,
                                                       isc_info_sql_stmt_insert,
                                                       isc_info_sql_stmt_update,
                                                       isc_info_sql_stmt_delete]:
            info = ' ' * 64
            ibase.isc_dsql_sql_info(self._isc_status,self._stmt_handle,2,
                                    bs([ibase.isc_info_sql_records,ibase.isc_info_end]),
                                    len(info),info)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while determining rowcount:")
            if ord(info[0]) != ibase.isc_info_sql_records:
                raise InternalError("Cursor.get_rowcount:\n"
                                    "first byte must be 'isc_info_sql_records'")
            res_walk = 3
            short_size = ctypes.sizeof(ctypes.c_short) 
            while ord(info[res_walk]) != ibase.isc_info_end:
                cur_count_type = ord(info[res_walk])
                res_walk += 1
                size = bytes_to_int(info[res_walk:res_walk+short_size])
                res_walk += short_size
                count = bytes_to_int(info[res_walk:res_walk+size])
                if ((cur_count_type == ibase.isc_info_req_select_count
                     and self.statement_type == ibase.isc_info_sql_stmt_select)
                    or (cur_count_type == ibase.isc_info_req_insert_count
                     and self.statement_type == ibase.isc_info_sql_stmt_insert)
                    or (cur_count_type == ibase.isc_info_req_update_count
                     and self.statement_type == ibase.isc_info_sql_stmt_update)
                    or (cur_count_type == ibase.isc_info_req_delete_count
                     and self.statement_type == ibase.isc_info_sql_stmt_delete)
                    ):
                    result = count
                res_walk += size
        return result
    def _parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_int(raw_value) + 678882
        century = (4 * nday -1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day -3) // 153
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
    def _convert_date(self,v):  # Convert datetime.date to BLR format data
        i = v.month + 9
        jy = v.year + (i // 12) -1
        jm = i % 12
        c = jy // 100
        jy -= 100 * c
        j = (146097*c) // 4 + (1461*jy) // 4 + (153*jm+2) // 5 + v.day - 678882
        return int_to_bytes(j, 4)
    def _convert_time(self,v):  # Convert datetime.time to BLR format time
        t = (v.hour*3600 + v.minute*60 + v.second) *10000 + v.microsecond // 100
        return int_to_bytes(t, 4)
    def _convert_timestamp(self,v):   # Convert datetime.datetime to BLR format timestamp
        return self._convert_date(v.date()) + self._convert_time(v.time())
    def _check_integer_rage(self,value,dialect,data_type,subtype,scale):
        if data_type == ibase.SQL_SHORT:
            vmin = ibase.SHRT_MIN
            vmax = ibase.SHRT_MAX
        elif data_type == ibase.SQL_LONG:
            vmin = ibase.INT_MIN
            vmax = ibase.INT_MAX
        elif data_type == ibase.SQL_INT64:
            vmin = ibase.LONG_MIN
            vmax = ibase.LONG_MAX
        if (value < vmin) or (value > vmax):
            msg = """numeric overflow: value %s (%s scaled for %d decimal places) is of
 too great a magnitude to fit into its internal storage type %s,
 which has range [%s,%s].""" % (str(value),
                                self.__get_external_data_type_name(dialect,data_type,subtype,scale),
                                scale,self.__get_internal_data_type_name(data_type),
                                str(vmin),str(vmax))
            raise ProgrammingError(msg,-802)
    def __coerce_XSQLDA(self,xsqlda):
        """Allocate space for SQLVAR data.
        """
        for sqlvar in xsqlda.sqlvar[:self.out_sqlda.sqld]:
            if sqlvar.sqltype & 1:
                sqlvar.sqlind = ctypes.pointer(ibase.ISC_SHORT(0))
            vartype = sqlvar.sqltype & ~1
            if vartype in [ibase.SQL_TEXT,ibase.SQL_VARYING]:
                sqlvar.sqldata = ctypes.create_string_buffer(sqlvar.sqllen+2)
                #sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen+2),
                                             #buf_pointer)
            elif vartype == ibase.SQL_SHORT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_LONG:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_INT64:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_FLOAT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_DOUBLE:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_D_FLOAT:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_BLOB:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_TIMESTAMP:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_TYPE_DATE:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_TYPE_TIME:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            elif vartype == ibase.SQL_ARRAY:
                sqlvar.sqldata = ctypes.cast(ctypes.create_string_buffer(sqlvar.sqllen),
                                             buf_pointer)
            else:
                pass
    def __XSQLDA2Tuple(self,xsqlda):
        """Move data from output XSQLDA to result tuple.
        """
        values = []
        for sqlvar in xsqlda.sqlvar[:xsqlda.sqld]:
            value = '<NOT_IMPLEMENTED>'
            vartype = sqlvar.sqltype & ~1
            scale = sqlvar.sqlscale
            # NULL handling
            if ((sqlvar.sqltype & 1) != 0) and (bool(sqlvar.sqlind) and sqlvar.sqlind.contents.value == -1):
                value = None
            elif vartype == ibase.SQL_TEXT:
                #value = ctypes.string_at(sqlvar.sqldata,sqlvar.sqllen)
                value = str(sqlvar.sqldata[:sqlvar.sqllen])
                if self.__get_connection().charset:
                    value = value.decode(ibase.charset_map.get(self.__get_connection().charset, self.__get_connection().charset))
            elif vartype == ibase.SQL_VARYING:
                size = bytes_to_int(sqlvar.sqldata[:1])
                #value = ctypes.string_at(sqlvar.sqldata[2],2+size)
                value = str(sqlvar.sqldata[2:2+size])
                if self.__get_connection().charset:
                    value = value.decode(ibase.charset_map.get(self.__get_connection().charset, self.__get_connection().charset))
            elif vartype in [ibase.SQL_SHORT,ibase.SQL_LONG,ibase.SQL_INT64]:
                value = bytes_to_int(sqlvar.sqldata[:sqlvar.sqllen])
                # It's scalled integer?
                if (sqlvar.sqlsubtype or scale):
                    value = decimal.Decimal(value) / _tenTo[abs(scale)]
            elif vartype == ibase.SQL_TYPE_DATE:
                yyyy, mm, dd = self._parse_date(sqlvar.sqldata[:sqlvar.sqllen])
                value = datetime.date(yyyy, mm, dd)
            elif vartype == ibase.SQL_TYPE_TIME:
                h, m, s, ms = self._parse_time(sqlvar.sqldata[:sqlvar.sqllen])
                value = datetime.time(h, m, s, ms)
            elif vartype == ibase.SQL_TIMESTAMP:
                yyyy, mm, dd = self._parse_date(sqlvar.sqldata[:4])
                h, m, s, ms = self._parse_time(sqlvar.sqldata[4:sqlvar.sqllen])
                value = datetime.datetime(yyyy, mm, dd, h, m, s, ms)
            elif vartype == ibase.SQL_FLOAT:
                value = struct.unpack('f', sqlvar.sqldata[:sqlvar.sqllen])[0]
            elif vartype == ibase.SQL_DOUBLE:
                value = struct.unpack('d', sqlvar.sqldata[:sqlvar.sqllen])[0]
            elif vartype == ibase.SQL_BLOB:
                val = sqlvar.sqldata[:sqlvar.sqllen]
                blobid = ibase.ISC_QUAD(bytes_to_int(val[:4]),
                                        bytes_to_int(val[4:sqlvar.sqllen]))
                blob_handle = ibase.isc_blob_handle()
                ibase.isc_open_blob2(self._isc_status, self.__get_connection()._db_handle,
                                     self.__get_transaction()._tr_handle, blob_handle,
                                     blobid, 0, None)
                if db_api_error(self._isc_status):
                    raise exception_from_status(DatabaseError,self._isc_status,
                                          "Cursor.read_output_blob/isc_open_blob2:")
                # Get BLOB total length and max. size of segment
                result = ctypes.cast(ctypes.create_string_buffer(20),
                                             buf_pointer)
                ibase.isc_blob_info(self._isc_status,blob_handle,2,
                                    bs([ibase.isc_info_blob_total_length,
                                        ibase.isc_info_blob_max_segment]),
                                    20,result)
                if db_api_error(self._isc_status):
                    raise exception_from_status(DatabaseError,self._isc_status,
                                          "Cursor.read_output_blob/isc_blob_info:")
                offset = 0
                while ord(result[offset]) != ibase.isc_info_end:
                    code = ord(result[offset])
                    offset += 1
                    if code == ibase.isc_info_blob_total_length:
                        length = bytes_to_int(result[offset:offset+2])
                        blob_length = bytes_to_int(result[offset+2:offset+2+length])
                        offset += length+2
                    elif code == ibase.isc_info_blob_max_segment:
                        length = bytes_to_int(result[offset:offset+2])
                        segment_size = bytes_to_int(result[offset+2:offset+2+length])
                        offset += length+2
                # Load BLOB
                allow_incomplete_segment_read = False
                status = ibase.ISC_STATUS(0)
                blob = ctypes.create_string_buffer(blob_length)
                bytes_read = 0
                bytes_actually_read = ctypes.c_ushort(0)
                while bytes_read < blob_length:
                    status = ibase.isc_get_segment(self._isc_status,blob_handle,
                                                   bytes_actually_read,
                                                   min(segment_size,blob_length-bytes_read),
                                                   ctypes.byref(blob,bytes_read))
                    if status != 0:
                        if (status == ibase.isc_segment) and allow_incomplete_segment_read:
                            bytes_read += bytes_actually_read.value
                        else:
                            raise exception_from_status(DatabaseError,self._isc_status,
                                              "Cursor.read_output_blob/isc_get_segment:")
                    else:
                        bytes_read += bytes_actually_read.value
                # Finish
                ibase.isc_close_blob(self._isc_status,blob_handle)
                value = blob.value
            elif vartype == ibase.SQL_ARRAY:
                value = []
            values.append(value)
            
        return tuple(values)
    def __Tuple2XSQLDA(self,xsqlda,parameters):
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
                # to give BEFORE triggers to act on value without our interference.
                sqlvar.sqlind = ctypes.pointer(ibase.ISC_SHORT(-1))
                sqlvar.sqldata = None
            else:
                # if sqlvar allows null, allocate the null flag
                # I don't know whether it's necessary, but we'll do it anyway for safety
                if ((sqlvar.sqltype & 1) != 0):
                    sqlvar.sqlind = ctypes.pointer(ibase.ISC_SHORT(0))
                # Fill in value by type
                if ((vartype != ibase.SQL_BLOB and isinstance(value,(types.StringTypes))) 
                    or vartype in [ibase.SQL_TEXT,ibase.SQL_VARYING]):
                    # Place for Implicit Conversion of Input Parameters from Strings
                    if isinstance(value,types.UnicodeType):
                        value = value.encode(ibase.charset_map.get(self.__get_connection().charset, self.__get_connection().charset))
                    if not isinstance(value,types.StringType):
                        value = str(value)
                    if len(value) > sqlvar.sqllen:
                        raise ValueError("Value of parameter (%i) is too long, expected %i, found %i" % (i,sqlvar.sqllen,len(value)))
                    sqlvar.sqltype = ibase.SQL_TEXT | (sqlvar.sqltype & 1)
                    sqlvar.sqllen = ctypes.c_short(len(value))
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(value)),buf_pointer)
                elif vartype in [ibase.SQL_SHORT,ibase.SQL_LONG,ibase.SQL_INT64]:
                    # It's scalled integer?
                    if (sqlvar.sqlsubtype or scale):
                        if isinstance(value, decimal.Decimal):
                            value = int((value * _tenTo[abs(scale)]).to_integral())
                        elif isinstance(value, (int, long, float,)):
                            value = int(value * _tenTo[abs(scale)])
                        else:
                            raise TypeError('Objects of type %s are not acceptable input for'
                                ' a fixed-point column.' % str(type(value)))
                    self._check_integer_rage(value,self.__get_connection().sql_dialect,vartype,sqlvar.sqlsubtype,sqlvar.sqlscale)
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(int_to_bytes(value,sqlvar.sqllen))),buf_pointer)
                elif vartype == ibase.SQL_TYPE_DATE:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(self._convert_date(value))),buf_pointer)
                elif vartype == ibase.SQL_TYPE_TIME:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(self._convert_time(value))),buf_pointer)
                elif vartype == ibase.SQL_TIMESTAMP:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(self._convert_timestamp(value))),buf_pointer)
                elif vartype == ibase.SQL_FLOAT:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(struct.pack('f', value))),buf_pointer)
                elif vartype == ibase.SQL_DOUBLE:
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(ctypes.create_string_buffer(struct.pack('d', value))),buf_pointer)
                elif vartype == ibase.SQL_BLOB:
                    blobid = ibase.ISC_QUAD(0,0)
                    blob_handle = ibase.isc_blob_handle()
                    blob = ctypes.create_string_buffer(value)
                    ibase.isc_create_blob2(self._isc_status,
                                           self.__get_connection()._db_handle,
                                           self.__get_transaction()._tr_handle,
                                           blob_handle,blobid,
                                           0,None)
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,self._isc_status,
                                              "Cursor.write_input_blob/isc_create_blob2:")
                    sqlvar.sqldata = ctypes.cast(ctypes.pointer(blobid),buf_pointer)
                    total_size = len(value)
                    bytes_written_so_far = 0
                    bytes_to_write_this_time = ibase.MAX_BLOB_SEGMENT_SIZE
                    while (bytes_written_so_far < total_size):
                        if (total_size - bytes_written_so_far) < ibase.MAX_BLOB_SEGMENT_SIZE:
                            bytes_to_write_this_time = total_size - bytes_written_so_far
                        ibase.isc_put_segment(self._isc_status,blob_handle,
                                              bytes_to_write_this_time,
                                              ctypes.byref(blob,bytes_written_so_far))
                        if db_api_error(self._isc_status):
                            raise exception_from_status(DatabaseError,self._isc_status,
                                                  "Cursor.write_input_blob/isc_put_segment:")
                        bytes_written_so_far += bytes_to_write_this_time
                    ibase.isc_close_blob(self._isc_status,blob_handle)
                    if db_api_error(self._isc_status):
                        raise exception_from_status(DatabaseError,self._isc_status,
                                              "Cursor.write_input_blob/isc_close_blob:")
                elif vartype == ibase.SQL_ARRAY:
                    raise NotImplemented
    def _free_handle(self):
        if self._stmt_handle != None and not self.__closed:
            ibase.isc_dsql_free_statement(self._isc_status,self._stmt_handle,
                                          ibase.DSQL_close)
            self.__executed = False
            self.__closed = True
            self.__output_cache = None
            self._name = None
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while releasing SQL statement handle:")
    def _callproc(self,procname, parameters = None):
        raise NotImplementedError('Cursor.callproc')
    def _close(self):
        if self._stmt_handle != None:
            ibase.isc_dsql_free_statement(self._isc_status,self._stmt_handle,
                                          ibase.DSQL_drop)
            self._stmt_handle = None
            self.__executed = False
            self.__prepared = False
            self.__closed = False
            self.__description = None
            self.__output_cache = None
            self._name = None
            #self.out_buffer = None
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while closing SQL statement:")
    def _execute(self,parameters = None):
        # Bind parameters
        if parameters :
            if not isinstance(parameters,(types.ListType,types.TupleType)):
                raise TypeError("parameters must be list or tuple")
            if len(parameters) > self.in_sqlda.sqln:
                raise ProgrammingError("Statement parameter sequence contains %d parameters, but only %d are allowed" % (len(parameters),self.in_sqlda.sqln))
            # Restore original type and size information for input parameters
            i = 0
            for sqlvar in self.in_sqlda.sqlvar[:self.n_input_params]:
                sqlvar.sqltype,sqlvar.sqllen = self.in_sqlda_save[i]
                i += 1
            self.__Tuple2XSQLDA(self.in_sqlda,parameters)
            xsqlda_in = ctypes.cast(ctypes.pointer(self.in_sqlda),ctypes.POINTER(ibase.XSQLDA))
        else:
            xsqlda_in = None
        # Execute the statement
        if ((self.statement_type == isc_info_sql_stmt_exec_procedure)
            and (self.out_sqlda.sqld > 0)):
            # NOTE: We have to pass xsqlda_out only for statements that return single row
            xsqlda_out = ctypes.cast(ctypes.pointer(self.out_sqlda),ctypes.POINTER(ibase.XSQLDA))
            ibase.isc_dsql_execute2(self._isc_status,self.__get_transaction()._tr_handle,
                                   self._stmt_handle,self.__get_connection().sql_dialect,
                                   xsqlda_in,
                                   xsqlda_out)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while executing Stored Procedure:")
            # The result was returned immediately, but we have to provide it via
            # fetch*() calls as Python DB API requires. However, it's not possible
            # to call fetch on open such statement, so we'll cache the result
            # and return it in fetchone instead calling fetch.
            self.__output_cache = self.__XSQLDA2Tuple(self.out_sqlda)
        else:
            ibase.isc_dsql_execute2(self._isc_status,self.__get_transaction()._tr_handle,
                                   self._stmt_handle,self.__get_connection().sql_dialect,
                                   xsqlda_in,
                                   None)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while executing SQL statement:")
            self.__output_cache = None
        self.__executed = True
        self._last_fetch_status = ibase.ISC_STATUS(self.NO_FETCH_ATTEMPTED_YET)
    def _fetchone(self):
        if self.__executed:
            if self.__output_cache:
                if self._last_fetch_status == self.RESULT_SET_EXHAUSTED:
                    self._free_handle()
                    return None
                else:
                    self._last_fetch_status = self.RESULT_SET_EXHAUSTED
                    return self.__output_cache
            else:
                self._last_fetch_status = ibase.isc_dsql_fetch(self._isc_status,
                        self._stmt_handle,self.__get_connection().sql_dialect,
                        ctypes.cast(ctypes.pointer(self.out_sqlda),ctypes.POINTER(ibase.XSQLDA)))
                if self._last_fetch_status == 0:
                    return self.__XSQLDA2Tuple(self.out_sqlda)
                elif self._last_fetch_status == self.RESULT_SET_EXHAUSTED:
                    self._free_handle()
                    return None
                else:
                    if db_api_error(self._isc_status):
                        raise exception_from_status(ProgrammingError,self._isc_status,
                                              "Cursor.fetchone: Unknown status returned by fetch operation:")
    def _set_cursor_name(self,name):
        ibase.isc_dsql_set_cursor_name(self._isc_status,
                    self._stmt_handle,name,0)
        if db_api_error(self._isc_status):
            raise exception_from_status(OperationalError,self._isc_status,
                                  "Could not set cursor name:")
        self._name = name
    def __del__(self):
        if self._stmt_handle != None:
            self._close()
    def close(self):
        """Drops the resources associated with executed prepared statement, but
keeps it prepared for another execution."""
        self._free_handle()

    description = property(__get_description)
    rowcount = property(__get_rowcount)
    plan = property(__get_plan)

class Cursor(object):
    """
    Represents a database cursor, which is used to manage the context 
    of a fetch operation. 
    """
    arraysize = 1
    
    def __init__(self,connection,transaction):
        self._prepared_statements = {}
        self._connection = connection
        self._transaction = transaction
        self._ps = None # current prepared statement
        
    def next(self):
        row = self.fetchone()
        if row:
            return row
        else:
            raise StopIteration
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
    def __set_name(self,name):
        if name == None or not isinstance(name,types.StringType):
            raise ProgrammingError("The name attribute can only be set to a string, and cannot be deleted")
        if not self._ps:
            raise ProgrammingError("This cursor has not yet executed a statement, so setting its name attribute would be meaningless")
        if self._ps._name:
            raise ProgrammingError("Cursor's name has already been declared in context of currently executed statement")
        self._ps._set_cursor_name(name)
    def callproc(self,procname, parameters = None):
        if not parameters:
            params = []
        else:
            if isinstance(parameters,(types.ListType,types.TupleType)):
                params = parameters
            else:
                raise ProgrammingError("callproc paremetrs must be List or Tuple")
        sql = 'EXECUTE PROCEDURE ' + procname + ' ' + ','.join('?'*len(params))
        self.execute(sql,params)
        return parameters
    def close(self):
        if self._ps != None:
            self._ps._close()
        for ps in self._prepared_statements.values():
            ps._close()
        self._prepared_statements.clear()
        self._name = None
    def execute(self,operation, parameters = None):
        if self._ps != None:
            self._ps._free_handle()
        if self._transaction.closed:
            self._transaction.begin()
        if isinstance(operation,PreparedStatement):
            self._ps = operation
        else:
            self._ps = self._prepared_statements.setdefault(operation,
                                                            PreparedStatement(operation,self,True))
        self._ps._execute(parameters)
    def prep(self,operation):
        if self._transaction.closed:
            self._transaction.begin()
        return PreparedStatement(operation,self,False)
    def executemany(self,operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(operation,parameters)
    def fetchone(self):
        return self._ps._fetchone()
    def fetchmany(self,size=arraysize):
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
        return [row for row in self]
    def fetchonemap(self):
        row = self.fetchone()
        if row:
            row = _RowMapping(self.description,row)
        return row
    def fetchmanymap(self,size=arraysize):
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
        return [row for row in self.itermap()]
    def iter(self):
        return self
    def itermap(self):
        return Iterator(self.fetchonemap,None)
    def setinputsizes(self,sizes):
        pass
    def setoutputsize(self,size, column = None):
        pass
    def __del__(self):
        self.close()

    description = property(__get_description)
    rowcount = property(__get_rowcount)
    name = property(__get_name,__set_name)

class Transaction(object):
    """
    """
    def __init__(self,connections,tpb=None,default_action='commit'):
        self._connections = [weakref.ref(c) for c in connections]
        if tpb == None:
            self.tpb = transaction_parameter_block[ISOLATION_LEVEL_READ_COMMITED]
        else:
            self.tpb = tpb
        self.default_action = default_action
        self._cursors = []  # Weak references to cursors
        self._isc_status = ibase.ISC_STATUS_ARRAY()
        self._tr_handle = None
        
    def __remove_cursor(self,cursor_ref):
        self._cursors.remove(cursor_ref)
    def __get_closed(self):
        return self._tr_handle == None
    def __check_active(self):
        if self._tr_handle == None:
            raise ProgrammingError("Transaction object is not active")
    def _execute_immediate(self,sql):
        self.__check_active()
        sql_len = ctypes.c_short(len(sql))
        ibase.isc_execute_immediate(self._isc_status,self._connections[0]()._db_handle,
                                    self._tr_handle,
                                    sql_len,sql)
#        ibase.isc_execute_immediate(self._isc_status,self._connections[0]()._db_handle,
#                                    self._tr_handle,
#                                    sql_len,sql,self._connections[0]().sql_dialect,None)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while executing SQL statement:")
    def begin(self):
        self.close() # Make sure that previous transaction (if any) is ended
        self._tr_handle = ibase.isc_tr_handle(0)
        if isinstance(self.tpb,TPB):
            _tpb = self.tpb.render()
        elif isinstance(self.tpb,(types.ListType,types.TupleType)):
            _tpb = bs(self.tpb)
        elif isinstance(self.tpb,types.StringType):
            _tpb = self.tpb
        else:
            raise ProgrammingError("TPB must be either string, list/tuple of numeric constants or TPB instance.")
        if _tpb[0] != bs([isc_tpb_version3]):
            _tpb = bs([isc_tpb_version3]) + _tpb
        if len(self._connections) == 1:
            ibase.isc_start_transaction(self._isc_status,self._tr_handle,1,
                                        self._connections[0]()._db_handle,len(_tpb),_tpb)
            if db_api_error(self._isc_status):
                self._tr_handle = None
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while starting transaction:")
        elif len(self._connections) > 1:
            self._tr_handle = None
            raise NotImplementedError("Transaction.begin(multiple connections)")
        else:
            raise ProgrammingError("Transaction.begin requires at least one database handle")
    def commit(self,retaining=False):
        self.__check_active()
        if retaining:
            ibase.isc_commit_retaining(self._isc_status,self._tr_handle)
        else:
            ibase.isc_commit_transaction(self._isc_status,self._tr_handle)
        if db_api_error(self._isc_status):
            raise exception_from_status(DatabaseError,self._isc_status,
                                  "Error while commiting transaction:")
        self._tr_handle = None
    def rollback(self,retaining=False,savepoint=None):
        self.__check_active()
        if retaining and savepoint:
            raise ProgrammingError("Can't rollback to savepoint while retaining context")
        if savepoint:
            self._execute_immediate('rollback to %s' % savepoint)
        else:
            if retaining:
                ibase.isc_rollback_retaining(self._isc_status,self._tr_handle)
            else:
                ibase.isc_rollback_transaction(self._isc_status,self._tr_handle)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while rolling back transaction:")
            self._tr_handle = None
    def close(self):
        if self._tr_handle != None:
            for cursor in self._cursors:
                cursor().close()
            self._cursors = []
            if self.default_action == 'commit':
                self.commit()
            else:
                self.rollback()
    def savepoint(self,name):
        self.__check_active()
        if len(self._connections) > 1:
            raise NotImplementedError("Transaction.savepoint on multiple connections")
        self._execute_immediate('SAVEPOINT %s' % name)
    def cursor(self):
        if len(self._connections) > 1:
            raise NotImplementedError("Transaction.cursor on multiple connections")
        c = Cursor(self._connections[0](),self)
        self._cursors.append(weakref.ref(c,self.__remove_cursor))
        return c

    def trans_info(self,request):
        # We process request as a sequence of info codes, even if only one code
        # was supplied by the caller.
        requestIsSingleton = isinstance(request, int)
        if requestIsSingleton:
            request = (request,)
    
        results = {}
        for infoCode in request:
            # The global().get(...) workaround is here because only recent versions
            # of FB expose constant isc_info_tra_isolation:
            if infoCode == globals().get('isc_info_tra_isolation', -1):
                buf = self.transaction_info(infoCode, 's')
                buf = buf[1 + struct.calcsize('h'):]
                if len(buf) == 1:
                    results[infoCode] = portable_int(buf)
                else:
                    # For isolation level isc_info_tra_read_committed, the
                    # first byte indicates the isolation level
                    # (isc_info_tra_read_committed), while the second indicates
                    # the record version flag (isc_info_tra_rec_version or
                    # isc_info_tra_no_rec_version).
                    isolationLevelByte, recordVersionByte = struct.unpack('cc', buf)
                    isolationLevel = portable_int(isolationLevelByte)
                    recordVersion = portable_int(recordVersionByte)
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
    def transaction_info(self,info_code,result_type):
        self.__check_active()
        request_buffer = bs([info_code])
        buf_size = 256
        while True:
            res_buf = chr(0) * buf_size
            ibase.isc_transaction_info(self._isc_status,self._tr_handle,
                                       len(request_buffer),request_buffer,
                                       len(res_buf),res_buf)
            if db_api_error(self._isc_status):
                raise exception_from_status(DatabaseError,self._isc_status,
                                      "Error while requesting transaction information:")
            i = buf_size - 1
            while i >= 0:
                if res_buf[i] != chr(0):
                    break
                else:
                    i -= 1
            if ord(res_buf[i]) == ibase.isc_info_truncated:
                if buf_size < ibase.SHRT_MAX:
                    buf_size *= 2
                    if buf_size > ibase.SHRT_MAX:
                        buf_size = ibase.SHRT_MAX
                    continue
                else:
                    raise OperationalError("Result is too large to fit into buffer"
                            " of size SHRT_MAX, yet underlying info function only"
                            " accepts buffers with size <= SHRT_MAX.")
            else:
                break
        if ord(res_buf[i]) != ibase.isc_info_end:
            raise InternalError("Exited request loop sucessfuly, but"
                                " res_buf[i] != sc_info_end.")
        if request_buffer[0] != res_buf[0]:
            raise InternalError("Result code does not match request code.")
        if result_type.upper() == 'I':
            return bytes_to_int(res_buf[3:3+bytes_to_int(res_buf[1:3])])
        elif result_type.upper() == 'S':
            return ctypes.string_at(res_buf,i)
        else:
            raise InterfaceError("Unknown result type requested (must be 'i' or 's').")
    def prepare(self):
        raise NotImplemented
    def __del__(self):
        if self._tr_handle != None:
            self.close()
    
    closed = property(__get_closed)

class Iterator(object):
    def __init__(self,method,sentinel):
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

class _RowMapping(object):
    """
      An internal class that wraps a row of results in order to map
    field name to field value.

      We make ABSOLUTELY NO GUARANTEES about the return value of the
    fetch(one|many|all) methods except that it is a sequence indexed by field
    position, and no guarantees about the return value of the
    fetch(one|many|all)map methods except that it is a mapping of field name
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
        return ''.join(self._buffer)
    def clear(self):
        self._buffer = []
    def _extend(self, otherRequestBuilder):
        self._buffer.append(otherRequestBuilder.render())
    def _addRaw(self, rawBuf):
        assert isinstance(rawBuf, str)
        self._buffer.append(rawBuf)
    def _addCode(self, code):
        self._code2reqbuf(self._buffer, code)
    def _code2reqbuf(self,reqBuf, code):
        if isinstance(code, str):
            assert len(code) == 1
            code = ord(code)
    
        # The database engine considers little-endian integers "portable"; they
        # need to have been converted to little-endianness before being sent across
        # the network.
        reqBuf.append(struct.pack('<b', code))
    def _addString(self, code, s):
        _string2reqbuf(self._buffer, code, s)
    def _addNumeric(self, code, n, numCType='I'):
        _numeric2reqbuf(self._buffer, code, n, numCType=numCType)

class TPB(_RequestBufferBuilder):
    def __init__(self):
        _RequestBufferBuilder.__init__(self)
        self._access_mode = isc_tpb_write
        self._isolation_level = isc_tpb_concurrency
        self._lock_resolution = isc_tpb_wait
        self._lock_timeout = None
        self._table_reservation = None
    def copy(self):
        # A shallow copy of self would be entirely safe except that
        # .table_reservation is a complex object that needs to be copied
        # separately.
        import copy
        other = copy.copy(self)
        if self._table_reservation is not None:
            other._table_reservation = copy.copy(self._table_reservation)
        return other
    def render(self):
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
            raise ProgrammingError('Access mode must be one of'
                ' (isc_tpb_read, isc_tpb_write).'
              )
        self._access_mode = access_mode
    access_mode = property(_get_access_mode, _set_access_mode)

    # isolation_level property:
    def _get_isolation_level(self):
        return self._isolation_level
    def _set_isolation_level(self, isolation_level):
        if isinstance(isolation_level, tuple):
            if len(isolation_level) != 2:
                raise ProgrammingError('The tuple variant of isolation level'
                    ' must have two elements:  isc_tpb_read_committed in the'
                    ' first element and one of (isc_tpb_rec_version,'
                    ' isc_tpb_no_rec_version) in the second.')
            isolation_level, suboption = isolation_level
        elif isolation_level == isc_tpb_read_committed:
            suboption = isc_tpb_rec_version

        if isolation_level not in (isc_tpb_concurrency, 
                                   isc_tpb_consistency, 
                                   isc_tpb_read_committed):
            raise ProgrammingError('Isolation level must be one of'
                ' (isc_tpb_concurrency, isc_tpb_consistency,'
                ' isc_tpb_read_committed).')

        if isolation_level == isc_tpb_read_committed:
            if suboption not in (isc_tpb_rec_version, 
                                 isc_tpb_no_rec_version):
                raise ProgrammingError('With isolation level'
                    ' isc_tpb_read_committed, suboption must be one of'
                    ' (isc_tpb_rec_version, isc_tpb_no_rec_version).')
            isolation_level = isolation_level, suboption
        self._isolation_level = isolation_level
    isolation_level = property(_get_isolation_level, _set_isolation_level)

    # lock_resolution property:
    def _get_lock_resolution(self):
        return self._lock_resolution
    def _set_lock_resolution(self, lock_resolution):
        if lock_resolution not in (isc_tpb_wait, isc_tpb_nowait):
            raise ProgrammingError('Lock resolution must be one of (isc_tpb_wait, isc_tpb_nowait).')
        self._lock_resolution = lock_resolution
    lock_resolution = property(_get_lock_resolution, _set_lock_resolution)

    # lock_timeout property:
    def _get_lock_timeout(self):
        return self._lock_timeout
    def _set_lock_timeout(self, lock_timeout):
        if lock_timeout is not None:
            UINT_MAX = 2 ** (struct.calcsize('I') * 8) - 1
            if (not isinstance(lock_timeout, (int, long))) or (
                lock_timeout < 0 or lock_timeout > UINT_MAX):
                raise ProgrammingError('Lock resolution must be either None'
                    ' or a non-negative int number of seconds between 0 and'
                    ' %d.' % UINT_MAX)

        self._lock_timeout = lock_timeout
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
    table_reservation = property(_get_table_reservation, _set_table_reservation_access)


class TableReservation(object):
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
        if not self:
            return ''
        frags = []
        _ = frags.append
        for tableName, resDefs in self.iteritems():
            tableNameLenWithTerm = len(tableName) + 1
            for (sharingMode, accessMode) in resDefs:
                _(chr(accessMode))
                _(struct.pack('<b%ds' % tableNameLenWithTerm,
                    tableNameLenWithTerm, tableName
                  ))
                _(chr(sharingMode))
        return ''.join(frags)
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
                raise KeyError('No table named "%s" is present.' % nonNormalizedKey)
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
    elif not (isinstance(tpb, str) and len(tpb) > 0):
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
