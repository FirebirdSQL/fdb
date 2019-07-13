#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           ibase.py
#   DESCRIPTION:    Python driver for Firebird - Python ctypes interface to Firebird client library
#   CREATED:        6.10.2011
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
#  Contributor(s): Philippe Makowski <pmakowski@ibphoenix.fr>
#                  ______________________________________.
#
# See LICENSE.TXT for details.

#from ctypes import *
from ctypes import c_char_p, c_wchar_p, c_char, c_byte, c_ubyte, c_int, c_uint, c_short, c_ushort, \
     c_long, c_ulong, c_longlong, c_ulonglong, c_void_p, c_int8, c_int16, c_int32, c_int64, c_uint8, \
     c_uint16, c_uint32, c_uint64, POINTER, Structure, CFUNCTYPE, CDLL
from ctypes.util import find_library
import sys
from locale import getpreferredencoding
import types
import operator
import platform
import os

PYTHON_MAJOR_VER = sys.version_info[0]

#-------------------

if PYTHON_MAJOR_VER == 3:
    from queue import PriorityQueue
    def nativestr(st, charset="latin-1"):
        if st is None:
            return st
        elif isinstance(st, bytes):
            return st.decode(charset)
        else:
            return st
    def b(st, charset="latin-1"):
        if st is None:
            return st
        elif isinstance(st, bytes):
            return st
        else:
            try:
                return st.encode(charset)
            except UnicodeEncodeError:
                return st

    def s(st):
        return st

    ord2 = lambda x: x if isinstance(x, IntType) else ord(x)

    if sys.version_info[1] <= 1:
        def int2byte(i):
            return bytes((i,))
    else:
        # This is about 2x faster than the implementation above on 3.2+
        int2byte = operator.methodcaller("to_bytes", 1, "big")

    def mychr(i):
        return i

    mybytes = bytes
    myunicode = str
    mylong = int
    StringType = str
    IntType = int
    LongType = int
    FloatType = float
    ListType = list
    UnicodeType = str
    TupleType = tuple
    xrange = range
    StringTypes = str

else:
    from Queue import PriorityQueue
    def nativestr(st, charset="latin-1"):
        if st is None:
            return st
        elif isinstance(st, unicode):
            return st.encode(charset)
        else:
            return st
    def b(st, charset="latin-1"):
        if st is None:
            return st
        elif isinstance(st, types.StringType):
            return st
        else:
            try:
                return st.encode(charset)
            except UnicodeEncodeError:
                return st

    int2byte = chr
    s = str
    ord2 = ord

    def mychr(i):
        return chr(i)

    mybytes = str
    myunicode = unicode
    mylong = long
    StringType = types.StringType
    IntType = types.IntType
    LongType = types.LongType
    FloatType = types.FloatType
    ListType = types.ListType
    UnicodeType = types.UnicodeType
    TupleType = types.TupleType
    xrange = xrange
    StringTypes = (StringType, UnicodeType)


# Support routines from ctypesgen generated file.

# As of ctypes 1.0, ctypes does not support custom error-checking
# functions on callbacks, nor does it support custom datatypes on
# callbacks, so we must ensure that all callbacks return
# primitive datatypes.
#
# Non-primitive return values wrapped with UNCHECKED won't be
# typechecked, and will be converted to c_void_p.
def UNCHECKED(type):
    if (hasattr(type, "_type_") and isinstance(type._type_, str)
            and type._type_ != "P"):
        return type
    else:
        return c_void_p

# ibase.h

FB_API_VER = 25
MAX_BLOB_SEGMENT_SIZE = 65535

# Event queue operation (and priority) codes

OP_DIE = 1
OP_RECORD_AND_REREGISTER = 2

sys_encoding = getpreferredencoding()

charset_map = {
    # DB CHAR SET NAME    :   PYTHON CODEC NAME (CANONICAL)
    # -------------------------------------------------------------------------
    None                  :   getpreferredencoding(),
    'NONE'                :   getpreferredencoding(),
    'OCTETS'              :   None,  # Allow to pass through unchanged.
    'UNICODE_FSS'         :   'utf_8',
    'UTF8'                :   'utf_8',  # (Firebird 2.0+)
    'ASCII'               :   'ascii',
    'SJIS_0208'           :   'shift_jis',
    'EUCJ_0208'           :   'euc_jp',
    'DOS737'              :   'cp737',
    'DOS437'              :   'cp437',
    'DOS850'              :   'cp850',
    'DOS865'              :   'cp865',
    'DOS860'              :   'cp860',
    'DOS863'              :   'cp863',
    'DOS775'              :   'cp775',
    'DOS862'              :   'cp862',
    'DOS864'              :   'cp864',
    'ISO8859_1'           :   'iso8859_1',
    'ISO8859_2'           :   'iso8859_2',
    'ISO8859_3'           :   'iso8859_3',
    'ISO8859_4'           :   'iso8859_4',
    'ISO8859_5'           :   'iso8859_5',
    'ISO8859_6'           :   'iso8859_6',
    'ISO8859_7'           :   'iso8859_7',
    'ISO8859_8'           :   'iso8859_8',
    'ISO8859_9'           :   'iso8859_9',
    'ISO8859_13'          :   'iso8859_13',
    'KSC_5601'            :   'euc_kr',
    'DOS852'              :   'cp852',
    'DOS857'              :   'cp857',
    'DOS858'              :   'cp858',
    'DOS861'              :   'cp861',
    'DOS866'              :   'cp866',
    'DOS869'              :   'cp869',
    'WIN1250'             :   'cp1250',
    'WIN1251'             :   'cp1251',
    'WIN1252'             :   'cp1252',
    'WIN1253'             :   'cp1253',
    'WIN1254'             :   'cp1254',
    'BIG_5'               :   'big5',
    'GB_2312'             :   'gb2312',
    'WIN1255'             :   'cp1255',
    'WIN1256'             :   'cp1256',
    'WIN1257'             :   'cp1257',
    'GB18030'             :   'gb18030',
    'GBK'                 :   'gbk',
    'KOI8R'               :   'koi8_r',  # (Firebird 2.0+)
    'KOI8U'               :   'koi8_u',  # (Firebird 2.0+)
    'WIN1258'             :   'cp1258',  # (Firebird 2.0+)
    }

DB_CHAR_SET_NAME_TO_PYTHON_ENCODING_MAP = charset_map

# C integer limit constants

SHRT_MIN = -32768
SHRT_MAX = 32767
USHRT_MAX = 65535
INT_MIN = -2147483648
INT_MAX = 2147483647
UINT_MAX = 4294967295
LONG_MIN = -9223372036854775808
LONG_MAX = 9223372036854775807
SSIZE_T_MIN = INT_MIN
SSIZE_T_MAX = INT_MAX

# Constants

DSQL_close = 1
DSQL_drop = 2
DSQL_unprepare = 4
SQLDA_version1 = 1

# Type codes

SQL_TEXT = 452
SQL_VARYING = 448
SQL_SHORT = 500
SQL_LONG = 496
SQL_FLOAT = 482
SQL_DOUBLE = 480
SQL_D_FLOAT = 530
SQL_TIMESTAMP = 510
SQL_BLOB = 520
SQL_ARRAY = 540
SQL_QUAD = 550
SQL_TYPE_TIME = 560
SQL_TYPE_DATE = 570
SQL_INT64 = 580
SQL_BOOLEAN = 32764 # Firebird 3
SQL_NULL = 32766

SUBTYPE_NUMERIC = 1
SUBTYPE_DECIMAL = 2

# Internal type codes (for example used by ARRAY descriptor)

blr_text = 14
blr_text2 = 15
blr_short = 7
blr_long = 8
blr_quad = 9
blr_float = 10
blr_double = 27
blr_d_float = 11
blr_timestamp = 35
blr_varying = 37
blr_varying2 = 38
blr_blob = 261
blr_cstring = 40
blr_cstring2 = 41
blr_blob_id = 45
blr_sql_date = 12
blr_sql_time = 13
blr_int64 = 16
blr_blob2 = 17
# Added in FB 2.1
blr_domain_name = 18
blr_domain_name2 = 19
blr_not_nullable = 20
# Added in FB 2.5
blr_column_name = 21
blr_column_name2 = 22
# Added in FB 3.0
blr_bool = 23
# Rest of BLR is defined in fdb.blr

# Database parameter block stuff

isc_dpb_version1 = 1
isc_dpb_version2 = 2 # Firebird 3
isc_dpb_cdd_pathname = 1
isc_dpb_allocation = 2
#isc_dpb_journal = 3
isc_dpb_page_size = 4
isc_dpb_num_buffers = 5
isc_dpb_buffer_length = 6
isc_dpb_debug = 7
isc_dpb_garbage_collect = 8
isc_dpb_verify = 9
isc_dpb_sweep = 10
#isc_dpb_enable_journal = 11
#isc_dpb_disable_journal = 12
isc_dpb_dbkey_scope = 13
isc_dpb_number_of_users = 14
isc_dpb_trace = 15
isc_dpb_no_garbage_collect = 16
isc_dpb_damaged = 17
#isc_dpb_license = 18
isc_dpb_sys_user_name = 19
isc_dpb_encrypt_key = 20
isc_dpb_activate_shadow = 21
isc_dpb_sweep_interval = 22
isc_dpb_delete_shadow = 23
isc_dpb_force_write = 24
isc_dpb_begin_log = 25
isc_dpb_quit_log = 26
isc_dpb_no_reserve = 27
isc_dpb_user_name = 28
isc_dpb_password = 29
isc_dpb_password_enc = 30
isc_dpb_sys_user_name_enc = 31
isc_dpb_interp = 32
isc_dpb_online_dump = 33
isc_dpb_old_file_size = 34
isc_dpb_old_num_files = 35
isc_dpb_old_file = 36
isc_dpb_old_start_page = 37
isc_dpb_old_start_seqno = 38
isc_dpb_old_start_file = 39
#isc_dpb_drop_walfile = 40
isc_dpb_old_dump_id = 41
#isc_dpb_wal_backup_dir = 42
#isc_dpb_wal_chkptlen = 43
#isc_dpb_wal_numbufs = 44
#isc_dpb_wal_bufsize = 45
#isc_dpb_wal_grp_cmt_wait = 46
isc_dpb_lc_messages = 47
isc_dpb_lc_ctype = 48
isc_dpb_cache_manager = 49
isc_dpb_shutdown = 50
isc_dpb_online = 51
isc_dpb_shutdown_delay = 52
isc_dpb_reserved = 53
isc_dpb_overwrite = 54
isc_dpb_sec_attach = 55
#isc_dpb_disable_wal = 56
isc_dpb_connect_timeout = 57
isc_dpb_dummy_packet_interval = 58
isc_dpb_gbak_attach = 59
isc_dpb_sql_role_name = 60
isc_dpb_set_page_buffers = 61
isc_dpb_working_directory = 62
isc_dpb_sql_dialect = 63
isc_dpb_set_db_readonly = 64
isc_dpb_set_db_sql_dialect = 65
isc_dpb_gfix_attach = 66
isc_dpb_gstat_attach = 67
isc_dpb_set_db_charset = 68
isc_dpb_gsec_attach = 69 # Deprecated in FB3
isc_dpb_address_path = 70
# Added in FB 2.1
isc_dpb_process_id = 71
isc_dpb_no_db_triggers = 72
isc_dpb_trusted_auth = 73
isc_dpb_process_name = 74
# Added in FB 2.5
isc_dpb_trusted_role = 75
isc_dpb_org_filename = 76
isc_dpb_utf8_filename = 77
isc_dpb_ext_call_depth = 78
# Added in FB 3.0
isc_dpb_auth_block = 79
isc_dpb_remote_protocol = 81
isc_dpb_client_version = 80
isc_dpb_host_name = 82
isc_dpb_os_user = 83
isc_dpb_specific_auth_data = 84
isc_dpb_auth_plugin_list = 85
isc_dpb_auth_plugin_name = 86
isc_dpb_config = 87
isc_dpb_nolinger = 88
isc_dpb_reset_icu = 89
isc_dpb_map_attach = 90

# structural codes
isc_info_end = 1
isc_info_truncated = 2
isc_info_error = 3
isc_info_data_not_ready = 4
isc_info_length = 126
isc_info_flag_end = 127

isc_info_req_select_count = 13
isc_info_req_insert_count = 14
isc_info_req_update_count = 15
isc_info_req_delete_count = 16

# DB Info item codes
isc_info_db_id = 4  # [db_filename,site_name[,site_name...]]
isc_info_reads = 5  # number of page reads
isc_info_writes = 6  # number of page writes
isc_info_fetches = 7  # number of reads from the memory buffer cache
isc_info_marks = 8  # number of writes to the memory buffer cache
isc_info_implementation = 11  # (implementation code, implementation class)
isc_info_isc_version = 12  # interbase server version identification string
isc_info_base_level = 13  # capability version of the server
isc_info_page_size = 14
isc_info_num_buffers = 15  # number of memory buffers currently allocated
isc_info_limbo = 16
isc_info_current_memory = 17  # amount of server memory (in bytes) currently in use
isc_info_max_memory = 18  # maximum amount of memory (in bytes) used at one time since the first process attached to the database
# Obsolete 19-20
isc_info_allocation = 21  # number of last database page allocated
isc_info_attachment_id = 22  # attachment id number
# all *_count codes below return {[table_id]=operation_count,...}; table IDs are in the system table RDB$RELATIONS.
isc_info_read_seq_count = 23  # number of sequential table scans (row reads) done on each table since the database was last attached
isc_info_read_idx_count = 24  # number of reads done via an index since the database was last attached
isc_info_insert_count = 25  # number of inserts into the database since the database was last attached
isc_info_update_count = 26  # number of database updates since the database was last attached
isc_info_delete_count = 27  # number of database deletes since the database was last attached
isc_info_backout_count = 28  # number of removals of a version of a record
isc_info_purge_count = 29  # number of removals of old versions of fully mature records (records that are committed, so that older ancestor versions are no longer needed)
isc_info_expunge_count = 30  # number of removals of a record and all of its ancestors, for records whose deletions have been committed
isc_info_sweep_interval = 31  # number of transactions that are committed between sweeps to remove database record versions that are no longer needed
isc_info_ods_version = 32  # On-disk structure (ODS) minor major version number
isc_info_ods_minor_version = 33  # On-disk structure (ODS) minor version number
isc_info_no_reserve = 34  # 20% page space reservation for holding backup versions of modified records: 0=yes, 1=no
# Obsolete 35-51
isc_info_forced_writes = 52  # mode in which database writes are performed: 0=sync, 1=async
isc_info_user_names = 53  # array of names of all the users currently attached to the database
isc_info_page_errors = 54  # number of page level errors validate found
isc_info_record_errors = 55  # number of record level errors validate found
isc_info_bpage_errors = 56  # number of blob page errors validate found
isc_info_dpage_errors = 57  # number of data page errors validate found
isc_info_ipage_errors = 58  # number of index page errors validate found
isc_info_ppage_errors = 59  # number of pointer page errors validate found
isc_info_tpage_errors = 60  # number of transaction page errors validate found
isc_info_set_page_buffers = 61  # number of memory buffers that should be allocated
isc_info_db_sql_dialect = 62  # dialect of currently attached database
isc_info_db_read_only = 63  # whether the database is read-only (1) or not (0)
isc_info_db_size_in_pages = 64  # number of allocated pages
# Values 65 -100 unused to avoid conflict with InterBase
frb_info_att_charset = 101  # charset of current attachment
isc_info_db_class = 102  # server architecture
isc_info_firebird_version = 103  # firebird server version identification string
isc_info_oldest_transaction = 104  # ID of oldest transaction
isc_info_oldest_active = 105  # ID of oldest active transaction
isc_info_oldest_snapshot = 106  # ID of oldest snapshot transaction
isc_info_next_transaction = 107  # ID of next transaction
isc_info_db_provider = 108  # for firebird is 'isc_info_db_code_firebird'
isc_info_active_transactions = 109  # array of active transaction IDs
isc_info_active_tran_count = 110  # number of active transactions
isc_info_creation_date = 111  # time_t struct representing database creation date & time
isc_info_db_file_size = 112 # added in FB 2.1, nbackup-related - size (in pages) of locked db
fb_info_page_contents = 113 # added in FB 2.5, get raw page contents; takes page_number as parameter;
# Added in Firebird 3.0
fb_info_implementation = 114  # (cpu code, OS code, compiler code, flags, implementation class)
fb_info_page_warns = 115  # number of page level warnings validate found
fb_info_record_warns = 116  # number of record level warnings validate found
fb_info_bpage_warns = 117  # number of blob page level warnings validate found
fb_info_dpage_warns = 118  # number of data page level warnings validate found
fb_info_ipage_warns = 119  # number of index page level warnings validate found
fb_info_ppage_warns = 120  # number of pointer page level warnings validate found
fb_info_tpage_warns = 121  # number of trabsaction page level warnings validate found
fb_info_pip_errors = 122  # number of pip page level errors validate found
fb_info_pip_warns = 123  # number of pip page level warnings validate found
isc_info_db_last_value = (fb_info_pip_warns + 1)

isc_info_version = isc_info_isc_version

# flags set in fb_info_crypt_state
fb_info_crypt_encrypted = 0x01,
fb_info_crypt_process = 0x02

# Blob information items
isc_info_blob_num_segments = 4
isc_info_blob_max_segment = 5
isc_info_blob_total_length = 6
isc_info_blob_type = 7

# Transaction information items

isc_info_tra_id = 4  # current tran ID number
isc_info_tra_oldest_interesting = 5  # oldest interesting tran ID when current tran started
isc_info_tra_oldest_snapshot = 6  # min. tran ID of tra_oldest_active
isc_info_tra_oldest_active = 7  # oldest active tran ID when current tran started
isc_info_tra_isolation = 8  # pair: {one of isc_info_tra_isolation_flags, [one of isc_info_tra_read_committed_flags]}
isc_info_tra_access = 9  # 'isc_info_tra_readonly' or 'isc_info_tra_readwrite'
isc_info_tra_lock_timeout = 10  # lock timeout value
# Firebird 3.0
fb_info_tra_dbpath = 11  # db filename for current transaction

# isc_info_tra_isolation responses
isc_info_tra_consistency = 1
isc_info_tra_concurrency = 2
isc_info_tra_read_committed = 3

# isc_info_tra_read_committed options
isc_info_tra_no_rec_version = 0
isc_info_tra_rec_version = 1

# isc_info_tra_access responses
isc_info_tra_readonly = 0
isc_info_tra_readwrite = 1

# SQL information items
isc_info_sql_select = 4
isc_info_sql_bind = 5
isc_info_sql_num_variables = 6
isc_info_sql_describe_vars = 7
isc_info_sql_describe_end = 8
isc_info_sql_sqlda_seq = 9
isc_info_sql_message_seq = 10
isc_info_sql_type = 11
isc_info_sql_sub_type = 12
isc_info_sql_scale = 13
isc_info_sql_length = 14
isc_info_sql_null_ind = 15
isc_info_sql_field = 16
isc_info_sql_relation = 17
isc_info_sql_owner = 18
isc_info_sql_alias = 19
isc_info_sql_sqlda_start = 20
isc_info_sql_stmt_type = 21
isc_info_sql_get_plan = 22
isc_info_sql_records = 23
isc_info_sql_batch_fetch = 24
isc_info_sql_relation_alias = 25
# Added in Firebird 3.0
isc_info_sql_explain_plan = 26
isc_info_sql_stmt_flags = 27

# SQL information return values
isc_info_sql_stmt_select = 1
isc_info_sql_stmt_insert = 2
isc_info_sql_stmt_update = 3
isc_info_sql_stmt_delete = 4
isc_info_sql_stmt_ddl = 5
isc_info_sql_stmt_get_segment = 6
isc_info_sql_stmt_put_segment = 7
isc_info_sql_stmt_exec_procedure = 8
isc_info_sql_stmt_start_trans = 9
isc_info_sql_stmt_commit = 10
isc_info_sql_stmt_rollback = 11
isc_info_sql_stmt_select_for_upd = 12
isc_info_sql_stmt_set_generator = 13
isc_info_sql_stmt_savepoint = 14

# Transaction parameter block stuff
isc_tpb_version1 = 1
isc_tpb_version3 = 3
isc_tpb_consistency = 1
isc_tpb_concurrency = 2
isc_tpb_shared = 3
isc_tpb_protected = 4
isc_tpb_exclusive = 5
isc_tpb_wait = 6
isc_tpb_nowait = 7
isc_tpb_read = 8
isc_tpb_write = 9
isc_tpb_lock_read = 10
isc_tpb_lock_write = 11
isc_tpb_verb_time = 12
isc_tpb_commit_time = 13
isc_tpb_ignore_limbo = 14
isc_tpb_read_committed = 15
isc_tpb_autocommit = 16
isc_tpb_rec_version = 17
isc_tpb_no_rec_version = 18
isc_tpb_restart_requests = 19
isc_tpb_no_auto_undo = 20
isc_tpb_lock_timeout = 21

# BLOB parameter buffer
isc_bpb_version1 = 1
isc_bpb_source_type = 1
isc_bpb_target_type = 2
isc_bpb_type = 3
isc_bpb_source_interp = 4
isc_bpb_target_interp = 5
isc_bpb_filter_parameter = 6
# Added in FB 2.1
isc_bpb_storage = 7

isc_bpb_type_segmented = 0
isc_bpb_type_stream = 1
# Added in FB 2.1
isc_bpb_storage_main = 0
isc_bpb_storage_temp = 2

# BLOB codes

isc_segment = 335544366
isc_segstr_eof = 335544367

# Services API
# Service parameter block stuff
isc_spb_current_version = 2
isc_spb_version = isc_spb_current_version
isc_spb_version3 = 3 # Firebird 3.0
isc_spb_user_name = isc_dpb_user_name
isc_spb_sys_user_name = isc_dpb_sys_user_name
isc_spb_sys_user_name_enc = isc_dpb_sys_user_name_enc
isc_spb_password = isc_dpb_password
isc_spb_password_enc = isc_dpb_password_enc
isc_spb_command_line = 105
isc_spb_dbname = 106
isc_spb_verbose = 107
isc_spb_options = 108
isc_spb_address_path = 109
# Added in FB 2.1
isc_spb_process_id = 110
isc_spb_trusted_auth = 111
isc_spb_process_name = 112
# Added in FB 2.5
isc_spb_trusted_role = 113
# Added in FB 3.0
isc_spb_verbint = 114
isc_spb_auth_block = 115
isc_spb_auth_plugin_name = 116
isc_spb_auth_plugin_list = 117
isc_spb_utf8_filename = 118
isc_spb_client_version = 119
isc_spb_remote_protocol = 120
isc_spb_host_name = 121
isc_spb_os_user = 122
isc_spb_config = 123
isc_spb_expected_db = 124
# This will not be used in protocol 13, therefore may be reused
isc_spb_specific_auth_data = isc_spb_trusted_auth

# Service action items
isc_action_svc_backup = 1           # Starts database backup process on the server
isc_action_svc_restore = 2          # Starts database restore process on the server
isc_action_svc_repair = 3           # Starts database repair process on the server
isc_action_svc_add_user = 4         # Adds a new user to the security database
isc_action_svc_delete_user = 5      # Deletes a user record from the security database
isc_action_svc_modify_user = 6      # Modifies a user record in the security database
isc_action_svc_display_user = 7     # Displays a user record from the security database
isc_action_svc_properties = 8       # Sets database properties
isc_action_svc_add_license = 9      # Adds a license to the license file
isc_action_svc_remove_license = 10  # Removes a license from the license file
isc_action_svc_db_stats = 11        # Retrieves database statistics
isc_action_svc_get_ib_log = 12      # Retrieves the InterBase log file from the server
isc_action_svc_get_fb_log = 12      # Retrieves the Firebird log file from the server
# Added in FB 2.5
isc_action_svc_nbak = 20
isc_action_svc_nrest = 21
isc_action_svc_trace_start = 22
isc_action_svc_trace_stop = 23
isc_action_svc_trace_suspend = 24
isc_action_svc_trace_resume = 25
isc_action_svc_trace_list = 26
isc_action_svc_set_mapping = 27
isc_action_svc_drop_mapping = 28
isc_action_svc_display_user_adm = 29
isc_action_svc_validate = 30 # Firebird 3.0
isc_action_svc_last = 31

# Service information items
isc_info_svc_svr_db_info = 50    # Retrieves the number of attachments and databases */
isc_info_svc_get_config = 53     # Retrieves the parameters and values for IB_CONFIG */
isc_info_svc_version = 54        # Retrieves the version of the services manager */
isc_info_svc_server_version = 55 # Retrieves the version of the Firebird server */
isc_info_svc_implementation = 56 # Retrieves the implementation of the Firebird server */
isc_info_svc_capabilities = 57   # Retrieves a bitmask representing the server's capabilities */
isc_info_svc_user_dbpath = 58    # Retrieves the path to the security database in use by the server */
isc_info_svc_get_env = 59        # Retrieves the setting of $FIREBIRD */
isc_info_svc_get_env_lock = 60   # Retrieves the setting of $FIREBIRD_LCK */
isc_info_svc_get_env_msg = 61    # Retrieves the setting of $FIREBIRD_MSG */
isc_info_svc_line = 62           # Retrieves 1 line of service output per call */
isc_info_svc_to_eof = 63         # Retrieves as much of the server output as will fit in the supplied buffer */
isc_info_svc_timeout = 64        # Sets / signifies a timeout value for reading service information */
isc_info_svc_limbo_trans = 66    # Retrieve the limbo transactions */
isc_info_svc_running = 67        # Checks to see if a service is running on an attachment */
isc_info_svc_get_users = 68      # Returns the user information from isc_action_svc_display_users */
isc_info_svc_auth_block = 69     # FB 3.0: Sets authentication block for service query() call */
isc_info_svc_stdin = 78	         # Returns maximum size of data, needed as stdin for service */

# Parameters for isc_action_{add|del|mod|disp)_user
isc_spb_sec_userid = 5
isc_spb_sec_groupid = 6
isc_spb_sec_username = 7
isc_spb_sec_password = 8
isc_spb_sec_groupname = 9
isc_spb_sec_firstname = 10
isc_spb_sec_middlename = 11
isc_spb_sec_lastname = 12
isc_spb_sec_admin = 13

# Parameters for isc_action_svc_backup
isc_spb_bkp_file = 5
isc_spb_bkp_factor = 6
isc_spb_bkp_length = 7
isc_spb_bkp_skip_data = 8 # Firebird 3.0
isc_spb_bkp_stat = 15 # Firebird 2.5
isc_spb_bkp_ignore_checksums = 0x01
isc_spb_bkp_ignore_limbo = 0x02
isc_spb_bkp_metadata_only = 0x04
isc_spb_bkp_no_garbage_collect = 0x08
isc_spb_bkp_old_descriptions = 0x10
isc_spb_bkp_non_transportable = 0x20
isc_spb_bkp_convert = 0x40
isc_spb_bkp_expand = 0x80
isc_spb_bkp_no_triggers = 0x8000

# Parameters for isc_action_svc_properties
isc_spb_prp_page_buffers = 5
isc_spb_prp_sweep_interval = 6
isc_spb_prp_shutdown_db = 7
isc_spb_prp_deny_new_attachments = 9
isc_spb_prp_deny_new_transactions = 10
isc_spb_prp_reserve_space = 11
isc_spb_prp_write_mode = 12
isc_spb_prp_access_mode = 13
isc_spb_prp_set_sql_dialect = 14
isc_spb_prp_activate = 0x0100
isc_spb_prp_db_online = 0x0200
isc_spb_prp_nolinger = 0x0400 # Firebird 3.0
isc_spb_prp_force_shutdown = 41
isc_spb_prp_attachments_shutdown = 42
isc_spb_prp_transactions_shutdown = 43
isc_spb_prp_shutdown_mode = 44
isc_spb_prp_online_mode = 45

# Parameters for isc_spb_prp_shutdown_mode and isc_spb_prp_online_mode
isc_spb_prp_sm_normal = 0
isc_spb_prp_sm_multi = 1
isc_spb_prp_sm_single = 2
isc_spb_prp_sm_full = 3

# Parameters for isc_spb_prp_reserve_space
isc_spb_prp_res_use_full = 35
isc_spb_prp_res = 36

# Parameters for isc_spb_prp_write_mode
isc_spb_prp_wm_async = 37
isc_spb_prp_wm_sync = 38

# Parameters for isc_action_svc_validate
isc_spb_val_tab_incl = 1
isc_spb_val_tab_excl = 2
isc_spb_val_idx_incl = 3
isc_spb_val_idx_excl = 4
isc_spb_val_lock_timeout = 5

# Parameters for isc_spb_prp_access_mode
isc_spb_prp_am_readonly = 39
isc_spb_prp_am_readwrite = 40

# Parameters for isc_action_svc_repair
isc_spb_rpr_commit_trans = 15
isc_spb_rpr_rollback_trans = 34
isc_spb_rpr_recover_two_phase = 17
isc_spb_tra_id = 18
isc_spb_single_tra_id = 19
isc_spb_multi_tra_id = 20
isc_spb_tra_state = 21
isc_spb_tra_state_limbo = 22
isc_spb_tra_state_commit = 23
isc_spb_tra_state_rollback = 24
isc_spb_tra_state_unknown = 25
isc_spb_tra_host_site = 26
isc_spb_tra_remote_site = 27
isc_spb_tra_db_path = 28
isc_spb_tra_advise = 29
isc_spb_tra_advise_commit = 30
isc_spb_tra_advise_rollback = 31
isc_spb_tra_advise_unknown = 33
# Added in Firebird 3.0
isc_spb_tra_id_64 = 46
isc_spb_single_tra_id_64 = 47
isc_spb_multi_tra_id_64 = 48
isc_spb_rpr_commit_trans_64 = 49
isc_spb_rpr_rollback_trans_64 = 50
isc_spb_rpr_recover_two_phase_64 = 51

isc_spb_rpr_validate_db = 0x01
isc_spb_rpr_sweep_db = 0x02
isc_spb_rpr_mend_db = 0x04
isc_spb_rpr_list_limbo_trans = 0x08
isc_spb_rpr_check_db = 0x10
isc_spb_rpr_ignore_checksum = 0x20
isc_spb_rpr_kill_shadows = 0x40
isc_spb_rpr_full = 0x80
isc_spb_rpr_icu = 0x0800 # Firebird 3.0

# Parameters for isc_action_svc_restore
isc_spb_res_skip_data = isc_spb_bkp_skip_data # Firebird 3.0
isc_spb_res_buffers = 9
isc_spb_res_page_size = 10
isc_spb_res_length = 11
isc_spb_res_access_mode = 12
isc_spb_res_fix_fss_data = 13
isc_spb_res_fix_fss_metadata = 14
isc_spb_res_stat = 15 # Firebird 3.0
isc_spb_res_metadata_only = 0x04
isc_spb_res_deactivate_idx = 0x0100
isc_spb_res_no_shadow = 0x0200
isc_spb_res_no_validity = 0x0400
isc_spb_res_one_at_a_time = 0x0800
isc_spb_res_replace = 0x1000
isc_spb_res_create = 0x2000
isc_spb_res_use_all_space = 0x4000

# Parameters for isc_spb_res_access_mode
isc_spb_res_am_readonly = isc_spb_prp_am_readonly
isc_spb_res_am_readwrite = isc_spb_prp_am_readwrite

# Parameters for isc_info_svc_svr_db_info
isc_spb_num_att = 5
isc_spb_num_db = 6

# Parameters for isc_info_svc_db_stats
isc_spb_sts_data_pages = 0x01
isc_spb_sts_db_log = 0x02
isc_spb_sts_hdr_pages = 0x04
isc_spb_sts_idx_pages = 0x08
isc_spb_sts_sys_relations = 0x10
isc_spb_sts_record_versions = 0x20
isc_spb_sts_table = 0x40
isc_spb_sts_nocreation = 0x80
isc_spb_sts_encryption = 0x100 # Firebird 3.0

# Parameters for isc_action_svc_nbak
isc_spb_nbk_level = 5
isc_spb_nbk_file = 6
isc_spb_nbk_direct = 7
isc_spb_nbk_no_triggers = 0x01

# trace
isc_spb_trc_id = 1
isc_spb_trc_name = 2
isc_spb_trc_cfg = 3

#-------------------

STRING = c_char_p
WSTRING = c_wchar_p

blb_got_eof = 0
blb_got_fragment = -1
blb_got_full_segment = 1
blb_seek_relative = 1
blb_seek_from_tail = 2

# Implementation codes
isc_info_db_impl_rdb_vms = 1
isc_info_db_impl_rdb_eln = 2
isc_info_db_impl_rdb_eln_dev = 3
isc_info_db_impl_rdb_vms_y = 4
isc_info_db_impl_rdb_eln_y = 5
isc_info_db_impl_jri = 6
isc_info_db_impl_jsv = 7
isc_info_db_impl_isc_apl_68K = 25
isc_info_db_impl_isc_vax_ultr = 26
isc_info_db_impl_isc_vms = 27
isc_info_db_impl_isc_sun_68k = 28
isc_info_db_impl_isc_os2 = 29
isc_info_db_impl_isc_sun4 = 30
isc_info_db_impl_isc_hp_ux = 31
isc_info_db_impl_isc_sun_386i = 32
isc_info_db_impl_isc_vms_orcl = 33
isc_info_db_impl_isc_mac_aux = 34
isc_info_db_impl_isc_rt_aix = 35
isc_info_db_impl_isc_mips_ult = 36
isc_info_db_impl_isc_xenix = 37
isc_info_db_impl_isc_dg = 38
isc_info_db_impl_isc_hp_mpexl = 39
isc_info_db_impl_isc_hp_ux68K = 40
isc_info_db_impl_isc_sgi = 41
isc_info_db_impl_isc_sco_unix = 42
isc_info_db_impl_isc_cray = 43
isc_info_db_impl_isc_imp = 44
isc_info_db_impl_isc_delta = 45
isc_info_db_impl_isc_next = 46
isc_info_db_impl_isc_dos = 47
isc_info_db_impl_m88K = 48
isc_info_db_impl_unixware = 49
isc_info_db_impl_isc_winnt_x86 = 50
isc_info_db_impl_isc_epson = 51
isc_info_db_impl_alpha_osf = 52
isc_info_db_impl_alpha_vms = 53
isc_info_db_impl_netware_386 = 54
isc_info_db_impl_win_only = 55
isc_info_db_impl_ncr_3000 = 56
isc_info_db_impl_winnt_ppc = 57
isc_info_db_impl_dg_x86 = 58
isc_info_db_impl_sco_ev = 59
isc_info_db_impl_i386 = 60
isc_info_db_impl_freebsd = 61
isc_info_db_impl_netbsd = 62
isc_info_db_impl_darwin_ppc = 63
isc_info_db_impl_sinixz = 64
isc_info_db_impl_linux_sparc = 65
isc_info_db_impl_linux_amd64 = 66
isc_info_db_impl_freebsd_amd64 = 67
isc_info_db_impl_winnt_amd64 = 68
isc_info_db_impl_linux_ppc = 69
isc_info_db_impl_darwin_x86 = 70
isc_info_db_impl_linux_mipsel = 71 # changed in 2.1, it was isc_info_db_impl_sun_amd64 in 2.0
# Added in FB 2.1
isc_info_db_impl_linux_mips = 72
isc_info_db_impl_darwin_x64 = 73
isc_info_db_impl_sun_amd64 = 74
isc_info_db_impl_linux_arm = 75
isc_info_db_impl_linux_ia64 = 76
isc_info_db_impl_darwin_ppc64 = 77
isc_info_db_impl_linux_s390x = 78
isc_info_db_impl_linux_s390 = 79
isc_info_db_impl_linux_sh = 80
isc_info_db_impl_linux_sheb = 81
# Added in FB 2.5
isc_info_db_impl_linux_hppa = 82
isc_info_db_impl_linux_alpha = 83
isc_info_db_impl_linux_arm64 = 84
isc_info_db_impl_linux_ppc64el = 85
isc_info_db_impl_linux_ppc64 = 86 # Firebird 3.0
isc_info_db_impl_last_value = (isc_info_db_impl_linux_ppc64 + 1)

# Info DB provider
isc_info_db_code_rdb_eln = 1
isc_info_db_code_rdb_vms = 2
isc_info_db_code_interbase = 3
isc_info_db_code_firebird = 4
isc_info_db_code_last_value = (isc_info_db_code_firebird+1)

# Info db class
isc_info_db_class_access = 1
isc_info_db_class_y_valve = 2
isc_info_db_class_rem_int = 3
isc_info_db_class_rem_srvr = 4
isc_info_db_class_pipe_int = 7
isc_info_db_class_pipe_srvr = 8
isc_info_db_class_sam_int = 9
isc_info_db_class_sam_srvr = 10
isc_info_db_class_gateway = 11
isc_info_db_class_cache = 12
isc_info_db_class_classic_access = 13
isc_info_db_class_server_access = 14
isc_info_db_class_last_value = (isc_info_db_class_server_access+1)

# Request information items
isc_info_number_messages = 4
isc_info_max_message = 5
isc_info_max_send = 6
isc_info_max_receive = 7
isc_info_state = 8
isc_info_message_number = 9
isc_info_message_size = 10
isc_info_request_cost = 11
isc_info_access_path = 12
isc_info_req_select_count = 13
isc_info_req_insert_count = 14
isc_info_req_update_count = 15
isc_info_req_delete_count = 16

# Access path items
isc_info_rsb_end = 0
isc_info_rsb_begin = 1
isc_info_rsb_type = 2
isc_info_rsb_relation = 3
isc_info_rsb_plan = 4

# RecordSource (RSB) types
isc_info_rsb_unknown = 1
isc_info_rsb_indexed = 2
isc_info_rsb_navigate = 3
isc_info_rsb_sequential = 4
isc_info_rsb_cross = 5
isc_info_rsb_sort = 6
isc_info_rsb_first = 7
isc_info_rsb_boolean = 8
isc_info_rsb_union = 9
isc_info_rsb_aggregate = 10
isc_info_rsb_merge = 11
isc_info_rsb_ext_sequential = 12
isc_info_rsb_ext_indexed = 13
isc_info_rsb_ext_dbkey = 14
isc_info_rsb_left_cross = 15
isc_info_rsb_select = 16
isc_info_rsb_sql_join = 17
isc_info_rsb_simulate = 18
isc_info_rsb_sim_cross = 19
isc_info_rsb_once = 20
isc_info_rsb_procedure = 21
isc_info_rsb_skip = 22
isc_info_rsb_virt_sequential = 23
isc_info_rsb_recursive = 24
# Firebird 3.0
isc_info_rsb_window = 25
isc_info_rsb_singular = 26
isc_info_rsb_writelock = 27
isc_info_rsb_buffer = 28
isc_info_rsb_hash = 29

# Bitmap expressions
isc_info_rsb_and = 1
isc_info_rsb_or = 2
isc_info_rsb_dbkey = 3
isc_info_rsb_index = 4

isc_info_req_active = 2
isc_info_req_inactive = 3
isc_info_req_send = 4
isc_info_req_receive = 5
isc_info_req_select = 6
isc_info_req_sql_stall = 7

# Blob Subtypes
isc_blob_untyped = 0
# internal subtypes
isc_blob_text = 1
isc_blob_blr = 2
isc_blob_acl = 3
isc_blob_ranges = 4
isc_blob_summary = 5
isc_blob_format = 6
isc_blob_tra = 7
isc_blob_extfile = 8
isc_blob_debug_info = 9
isc_blob_max_predefined_subtype = 10

# Masks for fb_shutdown_callback
fb_shut_confirmation = 1
fb_shut_preproviders = 2
fb_shut_postproviders = 4
fb_shut_finish = 8
fb_shut_exit = 16 # Firebird 3.0

# Shutdown reasons, used by engine
# Users should provide positive values
fb_shutrsn_svc_stopped = -1
fb_shutrsn_no_connection = -2
fb_shutrsn_app_stopped = -3
fb_shutrsn_device_removed = -4 # Not used by FB 3.0
fb_shutrsn_signal = -5
fb_shutrsn_services = -6
fb_shutrsn_exit_called = -7

# Cancel types for fb_cancel_operation
fb_cancel_disable = 1
fb_cancel_enable = 2
fb_cancel_raise = 3
fb_cancel_abort = 4

# Debug information items
fb_dbg_version = 1
fb_dbg_end = 255
fb_dbg_map_src2blr = 2
fb_dbg_map_varname = 3
fb_dbg_map_argument = 4
# Firebird 3.0
fb_dbg_subproc = 5
fb_dbg_subfunc = 6
fb_dbg_map_curname = 7

# sub code for fb_dbg_map_argument
fb_dbg_arg_input = 0
fb_dbg_arg_output = 1

FB_API_HANDLE = c_uint
if platform.architecture() == ('64bit', 'WindowsPE'):
    intptr_t = c_longlong
    uintptr_t = c_ulonglong
else:
    intptr_t = c_long
    uintptr_t = c_ulong

ISC_STATUS = intptr_t
ISC_STATUS_PTR = POINTER(ISC_STATUS)
ISC_STATUS_ARRAY = ISC_STATUS * 20
FB_SQLSTATE_STRING = c_char * (5 + 1)
ISC_LONG = c_int
ISC_ULONG = c_uint
ISC_SHORT = c_short
ISC_USHORT = c_ushort
ISC_UCHAR = c_ubyte
ISC_SCHAR = c_char
ISC_INT64 = c_longlong
ISC_UINT64 = c_ulonglong
ISC_DATE = c_int
ISC_TIME = c_uint
ISC_TRUE = 1
ISC_FALSE = 0

class ISC_TIMESTAMP(Structure):
    pass
ISC_TIMESTAMP._fields_ = [
    ('timestamp_date', ISC_DATE),
    ('timestamp_time', ISC_TIME),
]


class GDS_QUAD_t(Structure):
    pass
GDS_QUAD_t._fields_ = [
    ('gds_quad_high', ISC_LONG),
    ('gds_quad_low', ISC_ULONG),
]
GDS_QUAD = GDS_QUAD_t
ISC_QUAD = GDS_QUAD_t

isc_att_handle = FB_API_HANDLE
isc_blob_handle = FB_API_HANDLE
isc_db_handle = FB_API_HANDLE
isc_req_handle = FB_API_HANDLE
isc_stmt_handle = FB_API_HANDLE
isc_svc_handle = FB_API_HANDLE
isc_tr_handle = FB_API_HANDLE
isc_resv_handle = ISC_LONG

FB_SHUTDOWN_CALLBACK = CFUNCTYPE(UNCHECKED(c_int), c_int, c_int, POINTER(None))
ISC_CALLBACK = CFUNCTYPE(None)
ISC_PRINT_CALLBACK = CFUNCTYPE(None, c_void_p, c_short, STRING)
ISC_VERSION_CALLBACK = CFUNCTYPE(None, c_void_p, STRING)
ISC_EVENT_CALLBACK = CFUNCTYPE(None, POINTER(ISC_UCHAR), c_ushort, POINTER(ISC_UCHAR))


class ISC_ARRAY_BOUND(Structure):
    pass
ISC_ARRAY_BOUND._fields_ = [
    ('array_bound_lower', c_short),
    ('array_bound_upper', c_short),
]


class ISC_ARRAY_DESC(Structure):
    pass
ISC_ARRAY_DESC._fields_ = [
    ('array_desc_dtype', ISC_UCHAR),
    ('array_desc_scale', ISC_UCHAR), ## was ISC_SCHAR),
    ('array_desc_length', c_ushort),
    ('array_desc_field_name', ISC_SCHAR * 32),
    ('array_desc_relation_name', ISC_SCHAR * 32),
    ('array_desc_dimensions', c_short),
    ('array_desc_flags', c_short),
    ('array_desc_bounds', ISC_ARRAY_BOUND * 16),
]


class ISC_BLOB_DESC(Structure):
    pass
ISC_BLOB_DESC._fields_ = [
    ('blob_desc_subtype', c_short),
    ('blob_desc_charset', c_short),
    ('blob_desc_segment_size', c_short),
    ('blob_desc_field_name', ISC_UCHAR * 32),
    ('blob_desc_relation_name', ISC_UCHAR * 32),
]


class isc_blob_ctl(Structure):
    pass
isc_blob_ctl._fields_ = [
    ('ctl_source', CFUNCTYPE(ISC_STATUS)),
    ('ctl_source_handle', POINTER(isc_blob_ctl)),
    ('ctl_to_sub_type', c_short),
    ('ctl_from_sub_type', c_short),
    ('ctl_buffer_length', c_ushort),
    ('ctl_segment_length', c_ushort),
    ('ctl_bpb_length', c_ushort),
    ('ctl_bpb', STRING),
    ('ctl_buffer', POINTER(ISC_UCHAR)),
    ('ctl_max_segment', ISC_LONG),
    ('ctl_number_segments', ISC_LONG),
    ('ctl_total_length', ISC_LONG),
    ('ctl_status', POINTER(ISC_STATUS)),
    ('ctl_data', c_long * 8),
]
ISC_BLOB_CTL = POINTER(isc_blob_ctl)


class bstream(Structure):
    pass
bstream._fields_ = [
    ('bstr_blob', isc_blob_handle),
    ('bstr_buffer', POINTER(c_char)), # STRING
    ('bstr_ptr', POINTER(c_char)), # STRING
    ('bstr_length', c_short),
    ('bstr_cnt', c_short),
    ('bstr_mode', c_char),
]
BSTREAM = bstream
FB_BLOB_STREAM = POINTER(bstream)
# values for enumeration 'blob_lseek_mode'
blob_lseek_mode = c_int  # enum

# values for enumeration 'blob_get_result'
blob_get_result = c_int  # enum


class blobcallback(Structure):
    pass
blobcallback._fields_ = [
    ('blob_get_segment', CFUNCTYPE(c_short, c_void_p, POINTER(ISC_UCHAR),
                                   c_ushort, POINTER(ISC_USHORT))),
    ('blob_handle', c_void_p),
    ('blob_number_segments', ISC_LONG),
    ('blob_max_segment', ISC_LONG),
    ('blob_total_length', ISC_LONG),
    ('blob_put_segment', CFUNCTYPE(None, c_void_p, POINTER(ISC_UCHAR),
                                   c_ushort)),
    ('blob_lseek', CFUNCTYPE(ISC_LONG, c_void_p, c_ushort, c_int)),
]
BLOBCALLBACK = POINTER(blobcallback)


class paramdsc(Structure):
    pass
paramdsc._fields_ = [
    ('dsc_dtype', ISC_UCHAR),
    ('dsc_scale', c_byte),
    ('dsc_length', ISC_USHORT),
    ('dsc_sub_type', c_short),
    ('dsc_flags', ISC_USHORT),
    ('dsc_address', POINTER(ISC_UCHAR)),
]
PARAMDSC = paramdsc


class paramvary(Structure):
    pass
paramvary._fields_ = [
    ('vary_length', ISC_USHORT),
    ('vary_string', ISC_UCHAR * 1),
]
PARAMVARY = paramvary

class ISC_TEB(Structure):
    pass
ISC_TEB._fields_ = [
    ('db_ptr', POINTER(isc_db_handle)),
    ('tpb_len', ISC_SHORT),
    ('tpb_ptr', STRING)
]

class XSQLVAR(Structure):
    pass
XSQLVAR._fields_ = [
    ('sqltype', ISC_SHORT),
    ('sqlscale', ISC_SHORT),
    ('sqlsubtype', ISC_SHORT),
    ('sqllen', ISC_SHORT),
    ('sqldata', POINTER(c_char)),  # STRING),
    ('sqlind', POINTER(ISC_SHORT)),
    ('sqlname_length', ISC_SHORT),
    ('sqlname', ISC_SCHAR * 32),
    ('relname_length', ISC_SHORT),
    ('relname', ISC_SCHAR * 32),
    ('ownname_length', ISC_SHORT),
    ('ownname', ISC_SCHAR * 32),
    ('aliasname_length', ISC_SHORT),
    ('aliasname', ISC_SCHAR * 32),
]


class XSQLDA(Structure):
    pass
XSQLDA._fields_ = [
    ('version', ISC_SHORT),
    ('sqldaid', ISC_SCHAR * 8),
    ('sqldabc', ISC_LONG),
    ('sqln', ISC_SHORT),
    ('sqld', ISC_SHORT),
    ('sqlvar', XSQLVAR * 1),
]

XSQLDA_PTR = POINTER(XSQLDA)

class USER_SEC_DATA(Structure):
    pass
USER_SEC_DATA._fields_ = [
    ('sec_flags', c_short),
    ('uid', c_int),
    ('gid', c_int),
    ('protocol', c_int),
    ('server', STRING),
    ('user_name', STRING),
    ('password', STRING),
    ('group_name', STRING),
    ('first_name', STRING),
    ('middle_name', STRING),
    ('last_name', STRING),
    ('dba_user_name', STRING),
    ('dba_password', STRING),
]

RESULT_VECTOR = ISC_ULONG * 15

# values for enumeration 'db_info_types'
db_info_types = c_int  # enum

# values for enumeration 'info_db_implementations'
info_db_implementations = c_int  # enum

# values for enumeration 'info_db_class'
info_db_class = c_int  # enum

# values for enumeration 'info_db_provider'
info_db_provider = c_int  # enum


class imaxdiv_t(Structure):
    pass
imaxdiv_t._fields_ = [
    ('quot', c_long),
    ('rem', c_long),
]
intmax_t = c_long

int8_t = c_int8
int16_t = c_int16
int32_t = c_int32
int64_t = c_int64
uint8_t = c_uint8
uint16_t = c_uint16
uint32_t = c_uint32
uint64_t = c_uint64
int_least8_t = c_byte
int_least16_t = c_short
int_least32_t = c_int
int_least64_t = c_long
uint_least8_t = c_ubyte
uint_least16_t = c_ushort
uint_least32_t = c_uint
uint_least64_t = c_ulong
int_fast8_t = c_byte
int_fast16_t = c_long
int_fast32_t = c_long
int_fast64_t = c_long
uint_fast8_t = c_ubyte
uint_fast16_t = c_ulong
uint_fast32_t = c_ulong
uint_fast64_t = c_ulong
ptrdiff_t = c_long
size_t = c_ulong
uintmax_t = c_ulong

class fbclient_API(object):
    """Firebird Client API interface object. Loads Firebird Client Library and exposes
    API functions as member methods. Uses :ref:`ctypes <python:module-ctypes>` for bindings.
    """
    def __init__(self, fb_library_name=None):

        def get_key(key, sub_key):
            try:
                return winreg.OpenKey(key, sub_key)
            except:
                return None

        if fb_library_name is None:
            if sys.platform == 'darwin':
                fb_library_name = find_library('Firebird')
            # Next elif is necessary hotfix for ctypes issue
            # http://bugs.python.org/issue16283
            elif sys.platform == 'win32':
                fb_library_name = find_library('fbclient.dll')
                if not fb_library_name:
                    # let's try windows registry
                    if PYTHON_MAJOR_VER == 3:
                        import winreg
                    else:
                        import _winreg as winreg

                    # try find via installed Firebird server
                    key = get_key(winreg.HKEY_LOCAL_MACHINE,
                                  'SOFTWARE\\Firebird Project\\Firebird Server\\Instances')
                    if not key:
                        key = get_key(winreg.HKEY_LOCAL_MACHINE,
                                      'SOFTWARE\\Wow6432Node\\Firebird Project\\Firebird Server\\Instances')
                    if key:
                        instFold = winreg.QueryValueEx(key, 'DefaultInstance')
                        fb_library_name = os.path.join(os.path.join(instFold[0], 'bin'), 'fbclient.dll')
            else:
                fb_library_name = find_library('fbclient')
                if not fb_library_name:
                    try:
                        x = CDLL('libfbclient.so')
                        fb_library_name = 'libfbclient.so'
                    except:
                        pass

            if not fb_library_name:
                raise Exception("The location of Firebird Client Library could not be determined.")
        elif not os.path.exists(fb_library_name):
            path, file_name = os.path.split(fb_library_name)
            file_name = find_library(file_name)
            if not file_name:
                raise Exception("Firebird Client Library '%s' not found" % fb_library_name)
            else:
                fb_library_name = file_name

        if sys.platform in ['win32', 'cygwin', 'os2', 'os2emx']:
            from ctypes import WinDLL
            fb_library = WinDLL(fb_library_name)
        else:
            fb_library = CDLL(fb_library_name)

        #: Firebird client library name
        self.client_library_name = fb_library_name
        #: Firebird client library (loaded by ctypes)
        self.client_library = fb_library

        #: isc_attach_database(POINTER(ISC_STATUS), c_short, STRING, POINTER(isc_db_handle), c_short, STRING)
        self.isc_attach_database = fb_library.isc_attach_database
        self.isc_attach_database.restype = ISC_STATUS
        self.isc_attach_database.argtypes = [POINTER(ISC_STATUS), c_short, STRING,
                                             POINTER(isc_db_handle), c_short, STRING]
        #: isc_array_gen_sdl(POINTER(ISC_STATUS), POINTER(ISC_ARRAY_DESC), POINTER(ISC_SHORT), POINTER(ISC_UCHAR), POINTER(ISC_SHORT))
        self.isc_array_gen_sdl = fb_library.isc_array_gen_sdl
        self.isc_array_gen_sdl.restype = ISC_STATUS
        self.isc_array_gen_sdl.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_ARRAY_DESC),
                                           POINTER(ISC_SHORT), POINTER(ISC_UCHAR),
                                           POINTER(ISC_SHORT)]
        #: isc_array_get_slice(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), POINTER(ISC_ARRAY_DESC), c_void_p, POINTER(ISC_LONG))
        self.isc_array_get_slice = fb_library.isc_array_get_slice
        self.isc_array_get_slice.restype = ISC_STATUS
        self.isc_array_get_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                             POINTER(isc_tr_handle), POINTER(ISC_QUAD),
                                             POINTER(ISC_ARRAY_DESC), c_void_p,
                                             POINTER(ISC_LONG)]
        #: isc_array_lookup_bounds(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, STRING, POINTER(ISC_ARRAY_DESC))
        self.isc_array_lookup_bounds = fb_library.isc_array_lookup_bounds
        self.isc_array_lookup_bounds.restype = ISC_STATUS
        self.isc_array_lookup_bounds.argtypes = [POINTER(ISC_STATUS),
                                                 POINTER(isc_db_handle),
                                                 POINTER(isc_tr_handle), STRING, STRING,
                                                 POINTER(ISC_ARRAY_DESC)]
        #: isc_array_lookup_desc(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, STRING, POINTER(ISC_ARRAY_DESC))
        self.isc_array_lookup_desc = fb_library.isc_array_lookup_desc
        self.isc_array_lookup_desc.restype = ISC_STATUS
        self.isc_array_lookup_desc.argtypes = [POINTER(ISC_STATUS),
                                               POINTER(isc_db_handle),
                                               POINTER(isc_tr_handle), STRING, STRING,
                                               POINTER(ISC_ARRAY_DESC)]
        #: isc_array_set_desc(POINTER(ISC_STATUS), STRING, STRING, POINTER(c_short), POINTER(c_short), POINTER(c_short), POINTER(ISC_ARRAY_DESC))
        self.isc_array_set_desc = fb_library.isc_array_set_desc
        self.isc_array_set_desc.restype = ISC_STATUS
        self.isc_array_set_desc.argtypes = [POINTER(ISC_STATUS), STRING, STRING,
                                            POINTER(c_short), POINTER(c_short),
                                            POINTER(c_short), POINTER(ISC_ARRAY_DESC)]
        #: isc_array_put_slice(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), POINTER(ISC_ARRAY_DESC), c_void_p, POINTER(ISC_LONG))
        self.isc_array_put_slice = fb_library.isc_array_put_slice
        self.isc_array_put_slice.restype = ISC_STATUS
        self.isc_array_put_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                             POINTER(isc_tr_handle), POINTER(ISC_QUAD),
                                             POINTER(ISC_ARRAY_DESC), c_void_p,
                                             POINTER(ISC_LONG)]
        #: isc_blob_default_desc(POINTER(ISC_BLOB_DESC), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR))
        self.isc_blob_default_desc = fb_library.isc_blob_default_desc
        self.isc_blob_default_desc.restype = None
        self.isc_blob_default_desc.argtypes = [POINTER(ISC_BLOB_DESC), POINTER(ISC_UCHAR),
                                               POINTER(ISC_UCHAR)]
        #: isc_blob_gen_bpb(POINTER(ISC_STATUS), POINTER(ISC_BLOB_DESC), POINTER(ISC_BLOB_DESC), c_ushort, POINTER(ISC_UCHAR), POINTER(c_ushort))
        self.isc_blob_gen_bpb = fb_library.isc_blob_gen_bpb
        self.isc_blob_gen_bpb.restype = ISC_STATUS
        self.isc_blob_gen_bpb.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_BLOB_DESC),
                                          POINTER(ISC_BLOB_DESC), c_ushort,
                                          POINTER(ISC_UCHAR), POINTER(c_ushort)]
        #: isc_blob_info(POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_short, STRING, c_short, POINTER(c_char))
        self.isc_blob_info = fb_library.isc_blob_info
        self.isc_blob_info.restype = ISC_STATUS
        self.isc_blob_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
                                       c_short, STRING, c_short, POINTER(c_char)]
        #: isc_blob_lookup_desc(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR), POINTER(ISC_BLOB_DESC), POINTER(ISC_UCHAR))
        self.isc_blob_lookup_desc = fb_library.isc_blob_lookup_desc
        self.isc_blob_lookup_desc.restype = ISC_STATUS
        self.isc_blob_lookup_desc.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                              POINTER(isc_tr_handle), POINTER(ISC_UCHAR),
                                              POINTER(ISC_UCHAR), POINTER(ISC_BLOB_DESC),
                                              POINTER(ISC_UCHAR)]
        #: isc_blob_set_desc(POINTER(ISC_STATUS), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR), c_short, c_short, c_short, POINTER(ISC_BLOB_DESC))
        self.isc_blob_set_desc = fb_library.isc_blob_set_desc
        self.isc_blob_set_desc.restype = ISC_STATUS
        self.isc_blob_set_desc.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_UCHAR),
                                           POINTER(ISC_UCHAR), c_short, c_short, c_short,
                                           POINTER(ISC_BLOB_DESC)]
        #: isc_cancel_blob(POINTER(ISC_STATUS), POINTER(isc_blob_handle))
        self.isc_cancel_blob = fb_library.isc_cancel_blob
        self.isc_cancel_blob.restype = ISC_STATUS
        self.isc_cancel_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle)]
        #: isc_cancel_events(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(ISC_LONG))
        self.isc_cancel_events = fb_library.isc_cancel_events
        self.isc_cancel_events.restype = ISC_STATUS
        self.isc_cancel_events.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                           POINTER(ISC_LONG)]
        #: isc_close_blob(POINTER(ISC_STATUS), POINTER(isc_blob_handle))
        self.isc_close_blob = fb_library.isc_close_blob
        self.isc_close_blob.restype = ISC_STATUS
        self.isc_close_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle)]
        #: isc_commit_retaining(POINTER(ISC_STATUS), POINTER(isc_tr_handle))
        self.isc_commit_retaining = fb_library.isc_commit_retaining
        self.isc_commit_retaining.restype = ISC_STATUS
        self.isc_commit_retaining.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]
        #: isc_commit_transaction(POINTER(ISC_STATUS), POINTER(isc_tr_handle)
        self.isc_commit_transaction = fb_library.isc_commit_transaction
        self.isc_commit_transaction.restype = ISC_STATUS
        self.isc_commit_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]
        #: isc_create_blob(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD))
        self.isc_create_blob = fb_library.isc_create_blob
        self.isc_create_blob.restype = ISC_STATUS
        self.isc_create_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                         POINTER(isc_tr_handle), POINTER(isc_blob_handle),
                                         POINTER(ISC_QUAD)]
        #: isc_create_blob2(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD), c_short, STRING)
        self.isc_create_blob2 = fb_library.isc_create_blob2
        self.isc_create_blob2.restype = ISC_STATUS
        self.isc_create_blob2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                          POINTER(isc_tr_handle), POINTER(isc_blob_handle),
                                          POINTER(ISC_QUAD), c_short, STRING]
        #: isc_create_database(POINTER(ISC_STATUS), c_short, STRING, POINTER(isc_db_handle), c_short, STRING, c_short)
        self.isc_create_database = fb_library.isc_create_database
        self.isc_create_database.restype = ISC_STATUS
        self.isc_create_database.argtypes = [POINTER(ISC_STATUS), c_short, STRING,
                                             POINTER(isc_db_handle), c_short, STRING,
                                             c_short]
        #: isc_database_info(POINTER(ISC_STATUS), POINTER(isc_db_handle), c_short, STRING, c_short, STRING)
        self.isc_database_info = fb_library.isc_database_info
        self.isc_database_info.restype = ISC_STATUS
        self.isc_database_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                           c_short, STRING, c_short, STRING]
        #: isc_decode_date(POINTER(ISC_QUAD), c_void_p)
        self.isc_decode_date = fb_library.isc_decode_date
        self.isc_decode_date.restype = None
        self.isc_decode_date.argtypes = [POINTER(ISC_QUAD), c_void_p]
        #: isc_decode_sql_date(POINTER(ISC_DATE), c_void_p)
        self.isc_decode_sql_date = fb_library.isc_decode_sql_date
        self.isc_decode_sql_date.restype = None
        self.isc_decode_sql_date.argtypes = [POINTER(ISC_DATE), c_void_p]
        #: isc_decode_sql_time(POINTER(ISC_TIME), c_void_p)
        self.isc_decode_sql_time = fb_library.isc_decode_sql_time
        self.isc_decode_sql_time.restype = None
        self.isc_decode_sql_time.argtypes = [POINTER(ISC_TIME), c_void_p]
        #: isc_decode_timestamp(POINTER(ISC_TIMESTAMP), c_void_p)
        self.isc_decode_timestamp = fb_library.isc_decode_timestamp
        self.isc_decode_timestamp.restype = None
        self.isc_decode_timestamp.argtypes = [POINTER(ISC_TIMESTAMP), c_void_p]
        #: isc_detach_database(POINTER(ISC_STATUS), POINTER(isc_db_handle))
        self.isc_detach_database = fb_library.isc_detach_database
        self.isc_detach_database.restype = ISC_STATUS
        self.isc_detach_database.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle)]
        #: isc_drop_database(POINTER(ISC_STATUS), POINTER(isc_db_handle))
        self.isc_drop_database = fb_library.isc_drop_database
        self.isc_drop_database.restype = ISC_STATUS
        self.isc_drop_database.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle)]
        #: isc_dsql_allocate_statement(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_stmt_handle)
        self.isc_dsql_allocate_statement = fb_library.isc_dsql_allocate_statement
        self.isc_dsql_allocate_statement.restype = ISC_STATUS
        self.isc_dsql_allocate_statement.argtypes = [POINTER(ISC_STATUS),
                                                     POINTER(isc_db_handle),
                                                     POINTER(isc_stmt_handle)]
        #: isc_dsql_alloc_statement2(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_stmt_handle))
        self.isc_dsql_alloc_statement2 = fb_library.isc_dsql_alloc_statement2
        self.isc_dsql_alloc_statement2.restype = ISC_STATUS
        self.isc_dsql_alloc_statement2.argtypes = [POINTER(ISC_STATUS),
                                                   POINTER(isc_db_handle),
                                                   POINTER(isc_stmt_handle)]
        #: isc_dsql_describe(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA))
        self.isc_dsql_describe = fb_library.isc_dsql_describe
        self.isc_dsql_describe.restype = ISC_STATUS
        self.isc_dsql_describe.argtypes = [POINTER(ISC_STATUS),
                                           POINTER(isc_stmt_handle),
                                           c_ushort, POINTER(XSQLDA)]
        #: isc_dsql_describe_bind(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)
        self.isc_dsql_describe_bind = fb_library.isc_dsql_describe_bind
        self.isc_dsql_describe_bind.restype = ISC_STATUS
        self.isc_dsql_describe_bind.argtypes = [POINTER(ISC_STATUS),
                                                POINTER(isc_stmt_handle),
                                                c_ushort, POINTER(XSQLDA)]
        #: isc_dsql_exec_immed2(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA))
        self.isc_dsql_exec_immed2 = fb_library.isc_dsql_exec_immed2
        self.isc_dsql_exec_immed2.restype = ISC_STATUS
        self.isc_dsql_exec_immed2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                              POINTER(isc_tr_handle), c_ushort, STRING,
                                              c_ushort, POINTER(XSQLDA), POINTER(XSQLDA)]
        #: isc_dsql_execute(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA))
        self.isc_dsql_execute = fb_library.isc_dsql_execute
        self.isc_dsql_execute.restype = ISC_STATUS
        self.isc_dsql_execute.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                          POINTER(isc_stmt_handle), c_ushort,
                                          POINTER(XSQLDA)]
        #: isc_dsql_execute2(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA), POINTER(XSQLDA))
        self.isc_dsql_execute2 = fb_library.isc_dsql_execute2
        self.isc_dsql_execute2.restype = ISC_STATUS
        self.isc_dsql_execute2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                           POINTER(isc_stmt_handle), c_ushort,
                                           POINTER(XSQLDA), POINTER(XSQLDA)]
        #: isc_dsql_execute_immediate(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA))
        self.isc_dsql_execute_immediate = fb_library.isc_dsql_execute_immediate
        self.isc_dsql_execute_immediate.restype = ISC_STATUS
        self.isc_dsql_execute_immediate.argtypes = [POINTER(ISC_STATUS),
                                                    POINTER(isc_db_handle),
                                                    POINTER(isc_tr_handle),
                                                    c_ushort, STRING, c_ushort,
                                                    POINTER(XSQLDA)]
        #: isc_dsql_fetch(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA))
        self.isc_dsql_fetch = fb_library.isc_dsql_fetch
        self.isc_dsql_fetch.restype = ISC_STATUS
        self.isc_dsql_fetch.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle),
                                        c_ushort, POINTER(XSQLDA)]
        #: isc_dsql_finish(POINTER(isc_db_handle))
        self.isc_dsql_finish = fb_library.isc_dsql_finish
        self.isc_dsql_finish.restype = ISC_STATUS
        self.isc_dsql_finish.argtypes = [POINTER(isc_db_handle)]
        #: isc_dsql_free_statement(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort)
        self.isc_dsql_free_statement = fb_library.isc_dsql_free_statement
        self.isc_dsql_free_statement.restype = ISC_STATUS
        self.isc_dsql_free_statement.argtypes = [POINTER(ISC_STATUS),
                                                 POINTER(isc_stmt_handle), c_ushort]
        #: isc_dsql_insert(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA))
        self.isc_dsql_insert = fb_library.isc_dsql_insert
        self.isc_dsql_insert.restype = ISC_STATUS
        self.isc_dsql_insert.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle),
                                         c_ushort, POINTER(XSQLDA)]
        #: isc_dsql_prepare(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA)
        self.isc_dsql_prepare = fb_library.isc_dsql_prepare
        self.isc_dsql_prepare.restype = ISC_STATUS
        self.isc_dsql_prepare.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                          POINTER(isc_stmt_handle), c_ushort, STRING,
                                          c_ushort, POINTER(XSQLDA)]
        #: isc_dsql_set_cursor_name(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), STRING, c_ushort)
        self.isc_dsql_set_cursor_name = fb_library.isc_dsql_set_cursor_name
        self.isc_dsql_set_cursor_name.restype = ISC_STATUS
        self.isc_dsql_set_cursor_name.argtypes = [POINTER(ISC_STATUS),
                                                  POINTER(isc_stmt_handle), STRING,
                                                  c_ushort]
        #: isc_dsql_sql_info(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_short, STRING, c_short, STRING)
        self.isc_dsql_sql_info = fb_library.isc_dsql_sql_info
        self.isc_dsql_sql_info.restype = ISC_STATUS
        self.isc_dsql_sql_info.argtypes = [POINTER(ISC_STATUS),
                                           POINTER(isc_stmt_handle),
                                           c_short, STRING, c_short, STRING]
        #: isc_encode_date(c_void_p, POINTER(ISC_QUAD))
        self.isc_encode_date = fb_library.isc_encode_date
        self.isc_encode_date.restype = None
        self.isc_encode_date.argtypes = [c_void_p, POINTER(ISC_QUAD)]
        #: isc_encode_sql_date(c_void_p, POINTER(ISC_DATE))
        self.isc_encode_sql_date = fb_library.isc_encode_sql_date
        self.isc_encode_sql_date.restype = None
        self.isc_encode_sql_date.argtypes = [c_void_p, POINTER(ISC_DATE)]
        #: isc_encode_sql_time(c_void_p, POINTER(ISC_TIME))
        self.isc_encode_sql_time = fb_library.isc_encode_sql_time
        self.isc_encode_sql_time.restype = None
        self.isc_encode_sql_time.argtypes = [c_void_p, POINTER(ISC_TIME)]
        #: isc_encode_timestamp(c_void_p, POINTER(ISC_TIMESTAMP))
        self.isc_encode_timestamp = fb_library.isc_encode_timestamp
        self.isc_encode_timestamp.restype = None
        self.isc_encode_timestamp.argtypes = [c_void_p, POINTER(ISC_TIMESTAMP)]
        #: isc_event_counts(POINTER(RESULT_VECTOR), c_short, POINTER(ISC_UCHAR), POINTER(ISC_UCHAR))
        self.isc_event_counts = fb_library.isc_event_counts
        self.isc_event_counts.restype = None
        self.isc_event_counts.argtypes = [POINTER(RESULT_VECTOR), c_short, POINTER(ISC_UCHAR),
                                          POINTER(ISC_UCHAR)]
        #: isc_expand_dpb(POINTER(STRING), POINTER(c_short))
        self.isc_expand_dpb = fb_library.isc_expand_dpb
        self.isc_expand_dpb.restype = None
        self.isc_expand_dpb.argtypes = [POINTER(STRING), POINTER(c_short)]
        #: isc_modify_dpb(POINTER(STRING), POINTER(c_short), c_ushort, STRING, c_short)
        self.isc_modify_dpb = fb_library.isc_modify_dpb
        self.isc_modify_dpb.restype = c_int
        self.isc_modify_dpb.argtypes = [POINTER(STRING), POINTER(c_short), c_ushort,
                                        STRING, c_short]
        #: isc_free(STRING
        self.isc_free = fb_library.isc_free
        self.isc_free.restype = ISC_LONG
        self.isc_free.argtypes = [STRING]
        #: isc_get_segment(POINTER(ISC_STATUS), POINTER(isc_blob_handle), POINTER(c_ushort), c_ushort, c_void_p)
        self.isc_get_segment = fb_library.isc_get_segment
        self.isc_get_segment.restype = ISC_STATUS
        self.isc_get_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
                                         POINTER(c_ushort), c_ushort, c_void_p]
        #self.isc_get_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
        #                            POINTER(c_ushort), c_ushort, POINTER(c_char)]
        #: isc_get_slice(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), c_short, STRING, c_short, POINTER(ISC_LONG), ISC_LONG, c_void_p, POINTER(ISC_LONG))
        self.isc_get_slice = fb_library.isc_get_slice
        self.isc_get_slice.restype = ISC_STATUS
        self.isc_get_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                       POINTER(isc_tr_handle), POINTER(ISC_QUAD),
                                       c_short,
                                       STRING, c_short, POINTER(ISC_LONG), ISC_LONG,
                                       c_void_p, POINTER(ISC_LONG)]
        #: isc_interprete(STRING, POINTER(POINTER(ISC_STATUS)))
        self.isc_interprete = fb_library.isc_interprete
        self.isc_interprete.restype = ISC_LONG
        self.isc_interprete.argtypes = [STRING, POINTER(POINTER(ISC_STATUS))]
        #: fb_interpret(STRING, c_uint, POINTER(POINTER(ISC_STATUS)))
        self.fb_interpret = fb_library.fb_interpret
        self.fb_interpret.restype = ISC_LONG
        self.fb_interpret.argtypes = [STRING, c_uint, POINTER(POINTER(ISC_STATUS))]
        #: isc_open_blob(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD))
        self.isc_open_blob = fb_library.isc_open_blob
        self.isc_open_blob.restype = ISC_STATUS
        self.isc_open_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                       POINTER(isc_tr_handle), POINTER(isc_blob_handle),
                                       POINTER(ISC_QUAD)]
        #: isc_open_blob2(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD), ISC_USHORT, STRING)
        self.isc_open_blob2 = fb_library.isc_open_blob2
        self.isc_open_blob2.restype = ISC_STATUS
        self.isc_open_blob2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                        POINTER(isc_tr_handle), POINTER(isc_blob_handle),
                                        POINTER(ISC_QUAD), ISC_USHORT, STRING] # POINTER(ISC_UCHAR)
        #: isc_prepare_transaction2(POINTER(ISC_STATUS), POINTER(isc_tr_handle), ISC_USHORT, POINTER(ISC_UCHAR))
        self.isc_prepare_transaction2 = fb_library.isc_prepare_transaction2
        self.isc_prepare_transaction2.restype = ISC_STATUS
        self.isc_prepare_transaction2.argtypes = [POINTER(ISC_STATUS),
                                                  POINTER(isc_tr_handle), ISC_USHORT,
                                                  POINTER(ISC_UCHAR)]
        #: isc_print_sqlerror(ISC_SHORT, POINTER(ISC_STATUS))
        self.isc_print_sqlerror = fb_library.isc_print_sqlerror
        self.isc_print_sqlerror.restype = None
        self.isc_print_sqlerror.argtypes = [ISC_SHORT, POINTER(ISC_STATUS)]
        #: isc_print_status(POINTER(ISC_STATUS))
        self.isc_print_status = fb_library.isc_print_status
        self.isc_print_status.restype = ISC_STATUS
        self.isc_print_status.argtypes = [POINTER(ISC_STATUS)]
        #: isc_put_segment(POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_ushort, c_void_p)
        self.isc_put_segment = fb_library.isc_put_segment
        self.isc_put_segment.restype = ISC_STATUS
        self.isc_put_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
                                         c_ushort, c_void_p]
        #self.isc_put_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
        #                            c_ushort, STRING]
        #: isc_put_slice(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), c_short, STRING, c_short, POINTER(ISC_LONG), ISC_LONG, c_void_p)
        self.isc_put_slice = fb_library.isc_put_slice
        self.isc_put_slice.restype = ISC_STATUS
        self.isc_put_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                       POINTER(isc_tr_handle), POINTER(ISC_QUAD),
                                       c_short,
                                       STRING, c_short, POINTER(ISC_LONG), ISC_LONG,
                                       c_void_p]
        #: isc_que_events(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(ISC_LONG), c_short, POINTER(ISC_UCHAR), ISC_EVENT_CALLBACK, POINTER(ISC_UCHAR))
        self.isc_que_events = fb_library.isc_que_events
        self.isc_que_events.restype = ISC_STATUS
        self.isc_que_events.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                        POINTER(ISC_LONG), c_short, POINTER(ISC_UCHAR),
                                        ISC_EVENT_CALLBACK, POINTER(ISC_UCHAR)]
        #: isc_rollback_retaining(POINTER(ISC_STATUS), POINTER(isc_tr_handle))
        self.isc_rollback_retaining = fb_library.isc_rollback_retaining
        self.isc_rollback_retaining.restype = ISC_STATUS
        self.isc_rollback_retaining.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]
        #: isc_rollback_transaction(POINTER(ISC_STATUS), POINTER(isc_tr_handle))
        self.isc_rollback_transaction = fb_library.isc_rollback_transaction
        self.isc_rollback_transaction.restype = ISC_STATUS
        self.isc_rollback_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]
        #: isc_start_multiple(POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short, c_void_p)
        self.isc_start_multiple = fb_library.isc_start_multiple
        self.isc_start_multiple.restype = ISC_STATUS
        self.isc_start_multiple.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                            c_short, c_void_p]

        if sys.platform in ['win32', 'cygwin', 'os2', 'os2emx']:
            P_isc_start_transaction = CFUNCTYPE(ISC_STATUS, POINTER(ISC_STATUS),
                                                POINTER(isc_tr_handle), c_short,
                                                POINTER(isc_db_handle), c_short,
                                                STRING)
            self.isc_start_transaction = P_isc_start_transaction(('isc_start_transaction', fb_library))
        else:
            #: isc_start_transaction(POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short, POINTER(isc_db_handle), c_short, STRING)
            self.isc_start_transaction = fb_library.isc_start_transaction
            self.isc_start_transaction.restype = ISC_STATUS
            self.isc_start_transaction.argtypes = [POINTER(ISC_STATUS),
                                                   POINTER(isc_tr_handle), c_short,
                                                   POINTER(isc_db_handle), c_short, STRING]
        #: isc_sqlcode(POINTER(ISC_STATUS))
        self.isc_sqlcode = fb_library.isc_sqlcode
        self.isc_sqlcode.restype = ISC_LONG
        self.isc_sqlcode.argtypes = [POINTER(ISC_STATUS)]
        #: isc_sql_interprete(c_short, STRING, c_short)
        self.isc_sql_interprete = fb_library.isc_sql_interprete
        self.isc_sql_interprete.restype = None
        self.isc_sql_interprete.argtypes = [c_short, STRING, c_short]
        #: isc_transaction_info(POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short, STRING, c_short, STRING)
        self.isc_transaction_info = fb_library.isc_transaction_info
        self.isc_transaction_info.restype = ISC_STATUS
        self.isc_transaction_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                              c_short, STRING, c_short, STRING]
        #: isc_transact_request(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, STRING, c_ushort, STRING)
        self.isc_transact_request = fb_library.isc_transact_request
        self.isc_transact_request.restype = ISC_STATUS
        self.isc_transact_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                              POINTER(isc_tr_handle), c_ushort, STRING,
                                              c_ushort, STRING, c_ushort, STRING]
        #: isc_vax_integer(STRING, c_short)
        self.isc_vax_integer = fb_library.isc_vax_integer
        self.isc_vax_integer.restype = ISC_LONG
        self.isc_vax_integer.argtypes = [STRING, c_short]
        #: isc_portable_integer(POINTER(ISC_UCHAR), c_short)
        self.isc_portable_integer = fb_library.isc_portable_integer
        self.isc_portable_integer.restype = ISC_INT64
        self.isc_portable_integer.argtypes = [POINTER(ISC_UCHAR), c_short]
        #: isc_add_user(POINTER(ISC_STATUS), POINTER(USER_SEC_DATA))
        self.isc_add_user = fb_library.isc_add_user
        self.isc_add_user.restype = ISC_STATUS
        self.isc_add_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]
        #: isc_delete_user(POINTER(ISC_STATUS), POINTER(USER_SEC_DATA))
        self.isc_delete_user = fb_library.isc_delete_user
        self.isc_delete_user.restype = ISC_STATUS
        self.isc_delete_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]
        #: isc_modify_user(POINTER(ISC_STATUS), POINTER(USER_SEC_DATA))
        self.isc_modify_user = fb_library.isc_modify_user
        self.isc_modify_user.restype = ISC_STATUS
        self.isc_modify_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]
        #: isc_compile_request(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_req_handle), c_short, STRING)
        self.isc_compile_request = fb_library.isc_compile_request
        self.isc_compile_request.restype = ISC_STATUS
        self.isc_compile_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                             POINTER(isc_req_handle), c_short, STRING]
        #: isc_compile_request2(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_req_handle), c_short, STRING)
        self.isc_compile_request2 = fb_library.isc_compile_request2
        self.isc_compile_request2.restype = ISC_STATUS
        self.isc_compile_request2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                              POINTER(isc_req_handle), c_short, STRING]
        #: isc_ddl(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_short, STRING)
        #: This function always returns error since FB 3.0
        self.isc_ddl = fb_library.isc_ddl
        self.isc_ddl.restype = ISC_STATUS
        self.isc_ddl.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                 POINTER(isc_tr_handle), c_short, STRING]
        #: isc_prepare_transaction(POINTER(ISC_STATUS), POINTER(isc_tr_handle))
        self.isc_prepare_transaction = fb_library.isc_prepare_transaction
        self.isc_prepare_transaction.restype = ISC_STATUS
        self.isc_prepare_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]
        #: isc_receive(POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, c_void_p, c_short)
        self.isc_receive = fb_library.isc_receive
        self.isc_receive.restype = ISC_STATUS
        self.isc_receive.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle),
                                     c_short, c_short, c_void_p, c_short]
        #: isc_reconnect_transaction(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_short, STRING)
        self.isc_reconnect_transaction = fb_library.isc_reconnect_transaction
        self.isc_reconnect_transaction.restype = ISC_STATUS
        self.isc_reconnect_transaction.argtypes = [POINTER(ISC_STATUS),
                                                   POINTER(isc_db_handle),
                                                   POINTER(isc_tr_handle), c_short, STRING]
        #: isc_release_request(POINTER(ISC_STATUS), POINTER(isc_req_handle))
        self.isc_release_request = fb_library.isc_release_request
        self.isc_release_request.restype = ISC_STATUS
        self.isc_release_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle)]
        #: isc_request_info(POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, STRING, c_short, STRING)
        self.isc_request_info = fb_library.isc_request_info
        self.isc_request_info.restype = ISC_STATUS
        self.isc_request_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle),
                                          c_short, c_short, STRING, c_short, STRING]
        #: isc_seek_blob(POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_short, ISC_LONG, POINTER(ISC_LONG))
        self.isc_seek_blob = fb_library.isc_seek_blob
        self.isc_seek_blob.restype = ISC_STATUS
        self.isc_seek_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle),
                                       c_short, ISC_LONG, POINTER(ISC_LONG)]
        #: isc_send(POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, c_void_p, c_short)
        self.isc_send = fb_library.isc_send
        self.isc_send.restype = ISC_STATUS
        self.isc_send.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle),
                                  c_short, c_short, c_void_p, c_short]
        #: isc_start_and_send(POINTER(ISC_STATUS), POINTER(isc_req_handle), POINTER(isc_tr_handle), c_short, c_short, c_void_p, c_short)
        self.isc_start_and_send = fb_library.isc_start_and_send
        self.isc_start_and_send.restype = ISC_STATUS
        self.isc_start_and_send.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle),
                                            POINTER(isc_tr_handle), c_short, c_short,
                                            c_void_p, c_short]
        #: isc_start_request(POINTER(ISC_STATUS), POINTER(isc_req_handle), POINTER(isc_tr_handle), c_short)
        self.isc_start_request = fb_library.isc_start_request
        self.isc_start_request.restype = ISC_STATUS
        self.isc_start_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle),
                                           POINTER(isc_tr_handle), c_short]
        #: isc_unwind_request(POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short)
        self.isc_unwind_request = fb_library.isc_unwind_request
        self.isc_unwind_request.restype = ISC_STATUS
        self.isc_unwind_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                            c_short]
        #: isc_wait_for_event(POINTER(ISC_STATUS), POINTER(isc_db_handle), c_short, POINTER(ISC_UCHAR), POINTER(ISC_UCHAR))
        self.isc_wait_for_event = fb_library.isc_wait_for_event
        self.isc_wait_for_event.restype = ISC_STATUS
        self.isc_wait_for_event.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                            c_short, POINTER(ISC_UCHAR), POINTER(ISC_UCHAR)]
        #: isc_close(POINTER(ISC_STATUS), STRING)
        self.isc_close = fb_library.isc_close
        self.isc_close.restype = ISC_STATUS
        self.isc_close.argtypes = [POINTER(ISC_STATUS), STRING]
        #: isc_declare(POINTER(ISC_STATUS), STRING, STRING)
        self.isc_declare = fb_library.isc_declare
        self.isc_declare.restype = ISC_STATUS
        self.isc_declare.argtypes = [POINTER(ISC_STATUS), STRING, STRING]
        #: isc_describe(POINTER(ISC_STATUS), STRING, POINTER(XSQLDA))
        self.isc_describe = fb_library.isc_describe
        self.isc_describe.restype = ISC_STATUS
        self.isc_describe.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]
        #: isc_describe_bind(POINTER(ISC_STATUS), STRING, POINTER(XSQLDA))
        self.isc_describe_bind = fb_library.isc_describe_bind
        self.isc_describe_bind.restype = ISC_STATUS
        self.isc_describe_bind.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]
        #: isc_execute(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, POINTER(XSQLDA))
        self.isc_execute = fb_library.isc_execute
        self.isc_execute.restype = ISC_STATUS
        self.isc_execute.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle),
                                     STRING, POINTER(XSQLDA)]
        #: isc_execute_immediate(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(c_short), STRING)
        self.isc_execute_immediate = fb_library.isc_execute_immediate
        self.isc_execute_immediate.restype = ISC_STATUS
        self.isc_execute_immediate.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                               POINTER(isc_tr_handle), POINTER(c_short), STRING]
        #: isc_fetch(POINTER(ISC_STATUS), STRING, POINTER(XSQLDA))
        self.isc_fetch = fb_library.isc_fetch
        self.isc_fetch.restype = ISC_STATUS
        self.isc_fetch.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]
        #: isc_open(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, POINTER(XSQLDA))
        self.isc_open = fb_library.isc_open
        self.isc_open.restype = ISC_STATUS
        self.isc_open.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, POINTER(XSQLDA)]
        #: isc_prepare(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, POINTER(c_short), STRING, POINTER(XSQLDA))
        self.isc_prepare = fb_library.isc_prepare
        self.isc_prepare.restype = ISC_STATUS
        self.isc_prepare.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle),
                                     POINTER(isc_tr_handle), STRING, POINTER(c_short),
                                     STRING, POINTER(XSQLDA)]
        #: isc_dsql_execute_m(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_execute_m = fb_library.isc_dsql_execute_m
        self.isc_dsql_execute_m.restype = ISC_STATUS
        self.isc_dsql_execute_m.argtypes = [POINTER(ISC_STATUS),
                                            POINTER(isc_tr_handle),
                                            POINTER(isc_stmt_handle), c_ushort,
                                            STRING, c_ushort, c_ushort, STRING]
        #: isc_dsql_execute2_m(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_execute2_m = fb_library.isc_dsql_execute2_m
        self.isc_dsql_execute2_m.restype = ISC_STATUS
        self.isc_dsql_execute2_m.argtypes = [POINTER(ISC_STATUS),
                                             POINTER(isc_tr_handle),
                                             POINTER(isc_stmt_handle), c_ushort,
                                             STRING, c_ushort, c_ushort, STRING,
                                             c_ushort, STRING, c_ushort, c_ushort,
                                             STRING]
        #: isc_dsql_execute_immediate_m(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_execute_immediate_m = fb_library.isc_dsql_execute_immediate_m
        self.isc_dsql_execute_immediate_m.restype = ISC_STATUS
        self.isc_dsql_execute_immediate_m.argtypes = [POINTER(ISC_STATUS),
                                                      POINTER(isc_db_handle),
                                                      POINTER(isc_tr_handle),
                                                      c_ushort, STRING, c_ushort,
                                                      c_ushort, STRING, c_ushort,
                                                      c_ushort, STRING]
        #: isc_dsql_exec_immed3_m(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_exec_immed3_m = fb_library.isc_dsql_exec_immed3_m
        self.isc_dsql_exec_immed3_m.restype = ISC_STATUS
        self.isc_dsql_exec_immed3_m.argtypes = [POINTER(ISC_STATUS),
                                                POINTER(isc_db_handle),
                                                POINTER(isc_tr_handle), c_ushort,
                                                STRING, c_ushort, c_ushort,
                                                STRING, c_ushort, c_ushort,
                                                STRING, c_ushort, STRING,
                                                c_ushort, c_ushort, STRING]
        #: isc_dsql_fetch_m(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_fetch_m = fb_library.isc_dsql_fetch_m
        self.isc_dsql_fetch_m.restype = ISC_STATUS
        self.isc_dsql_fetch_m.argtypes = [POINTER(ISC_STATUS),
                                          POINTER(isc_stmt_handle), c_ushort,
                                          STRING, c_ushort, c_ushort, STRING]
        #: isc_dsql_insert_m(POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING)
        self.isc_dsql_insert_m = fb_library.isc_dsql_insert_m
        self.isc_dsql_insert_m.restype = ISC_STATUS
        self.isc_dsql_insert_m.argtypes = [POINTER(ISC_STATUS),
                                           POINTER(isc_stmt_handle), c_ushort,
                                           STRING, c_ushort, c_ushort, STRING]
        #: isc_dsql_prepare_m(POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING)
        self.isc_dsql_prepare_m = fb_library.isc_dsql_prepare_m
        self.isc_dsql_prepare_m.restype = ISC_STATUS
        self.isc_dsql_prepare_m.argtypes = [POINTER(ISC_STATUS),
                                            POINTER(isc_tr_handle),
                                            POINTER(isc_stmt_handle), c_ushort,
                                            STRING, c_ushort, c_ushort, STRING,
                                            c_ushort, STRING]
        #: isc_dsql_release(POINTER(ISC_STATUS), STRING)
        self.isc_dsql_release = fb_library.isc_dsql_release
        self.isc_dsql_release.restype = ISC_STATUS
        self.isc_dsql_release.argtypes = [POINTER(ISC_STATUS), STRING]
        #: isc_embed_dsql_close(POINTER(ISC_STATUS), STRING)
        self.isc_embed_dsql_close = fb_library.isc_embed_dsql_close
        self.isc_embed_dsql_close.restype = ISC_STATUS
        self.isc_embed_dsql_close.argtypes = [POINTER(ISC_STATUS), STRING]
        #: isc_embed_dsql_declare(POINTER(ISC_STATUS), STRING, STRING)
        self.isc_embed_dsql_declare = fb_library.isc_embed_dsql_declare
        self.isc_embed_dsql_declare.restype = ISC_STATUS
        self.isc_embed_dsql_declare.argtypes = [POINTER(ISC_STATUS), STRING, STRING]
        #: isc_embed_dsql_describe(POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_describe = fb_library.isc_embed_dsql_describe
        self.isc_embed_dsql_describe.restype = ISC_STATUS
        self.isc_embed_dsql_describe.argtypes = [POINTER(ISC_STATUS), STRING,
                                                 c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_describe_bind(POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_describe_bind = fb_library.isc_embed_dsql_describe_bind
        self.isc_embed_dsql_describe_bind.restype = ISC_STATUS
        self.isc_embed_dsql_describe_bind.argtypes = [POINTER(ISC_STATUS), STRING,
                                                      c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_execute(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_execute = fb_library.isc_embed_dsql_execute
        self.isc_embed_dsql_execute.restype = ISC_STATUS
        self.isc_embed_dsql_execute.argtypes = [POINTER(ISC_STATUS),
                                                POINTER(isc_tr_handle),
                                                STRING, c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_execute2(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA))
        self.isc_embed_dsql_execute2 = fb_library.isc_embed_dsql_execute2
        self.isc_embed_dsql_execute2.restype = ISC_STATUS
        self.isc_embed_dsql_execute2.argtypes = [POINTER(ISC_STATUS),
                                                 POINTER(isc_tr_handle),
                                                 STRING, c_ushort, POINTER(XSQLDA),
                                                 POINTER(XSQLDA)]
        #: isc_embed_dsql_execute_immed(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_execute_immed = fb_library.isc_embed_dsql_execute_immed
        self.isc_embed_dsql_execute_immed.restype = ISC_STATUS
        self.isc_embed_dsql_execute_immed.argtypes = [POINTER(ISC_STATUS),
                                                      POINTER(isc_db_handle),
                                                      POINTER(isc_tr_handle),
                                                      c_ushort, STRING, c_ushort,
                                                      POINTER(XSQLDA)]
        #: isc_embed_dsql_fetch(POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_fetch = fb_library.isc_embed_dsql_fetch
        self.isc_embed_dsql_fetch.restype = ISC_STATUS
        self.isc_embed_dsql_fetch.argtypes = [POINTER(ISC_STATUS), STRING,
                                              c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_fetch_a(POINTER(ISC_STATUS), POINTER(c_int), STRING, ISC_USHORT, POINTER(XSQLDA))
        self.isc_embed_dsql_fetch_a = fb_library.isc_embed_dsql_fetch_a
        self.isc_embed_dsql_fetch_a.restype = ISC_STATUS
        self.isc_embed_dsql_fetch_a.argtypes = [POINTER(ISC_STATUS), POINTER(c_int),
                                                STRING, ISC_USHORT, POINTER(XSQLDA)]
        #: isc_embed_dsql_open(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_open = fb_library.isc_embed_dsql_open
        self.isc_embed_dsql_open.restype = ISC_STATUS
        self.isc_embed_dsql_open.argtypes = [POINTER(ISC_STATUS),
                                             POINTER(isc_tr_handle),
                                             STRING, c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_open2(POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA))
        self.isc_embed_dsql_open2 = fb_library.isc_embed_dsql_open2
        self.isc_embed_dsql_open2.restype = ISC_STATUS
        self.isc_embed_dsql_open2.argtypes = [POINTER(ISC_STATUS),
                                              POINTER(isc_tr_handle),
                                              STRING, c_ushort, POINTER(XSQLDA),
                                              POINTER(XSQLDA)]
        #: isc_embed_dsql_insert(POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_insert = fb_library.isc_embed_dsql_insert
        self.isc_embed_dsql_insert.restype = ISC_STATUS
        self.isc_embed_dsql_insert.argtypes = [POINTER(ISC_STATUS), STRING,
                                               c_ushort, POINTER(XSQLDA)]
        #: isc_embed_dsql_prepare(POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, c_ushort, STRING, c_ushort, POINTER(XSQLDA))
        self.isc_embed_dsql_prepare = fb_library.isc_embed_dsql_prepare
        self.isc_embed_dsql_prepare.restype = ISC_STATUS
        self.isc_embed_dsql_prepare.argtypes = [POINTER(ISC_STATUS),
                                                POINTER(isc_db_handle),
                                                POINTER(isc_tr_handle), STRING,
                                                c_ushort, STRING, c_ushort,
                                                POINTER(XSQLDA)]
        #: isc_embed_dsql_release(POINTER(ISC_STATUS), STRING)
        self.isc_embed_dsql_release = fb_library.isc_embed_dsql_release
        self.isc_embed_dsql_release.restype = ISC_STATUS
        self.isc_embed_dsql_release.argtypes = [POINTER(ISC_STATUS), STRING]
        #: BLOB_open(isc_blob_handle, STRING, c_int)
        self.BLOB_open = fb_library.BLOB_open
        self.BLOB_open.restype = POINTER(BSTREAM)
        self.BLOB_open.argtypes = [isc_blob_handle, STRING, c_int]
        #: BLOB_put(ISC_SCHAR, POINTER(BSTREAM))
        self.BLOB_put = fb_library.BLOB_put
        self.BLOB_put.restype = c_int
        self.BLOB_put.argtypes = [ISC_SCHAR, POINTER(BSTREAM)]
        #: BLOB_close(POINTER(BSTREAM))
        self.BLOB_close = fb_library.BLOB_close
        self.BLOB_close.restype = c_int
        self.BLOB_close.argtypes = [POINTER(BSTREAM)]
        #: BLOB_get(POINTER(BSTREAM))
        self.BLOB_get = fb_library.BLOB_get
        self.BLOB_get.restype = c_int
        self.BLOB_get.argtypes = [POINTER(BSTREAM)]
        #: BLOB_display(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_display = fb_library.BLOB_display
        self.BLOB_display.restype = c_int
        self.BLOB_display.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: BLOB_dump(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_dump = fb_library.BLOB_dump
        self.BLOB_dump.restype = c_int
        self.BLOB_dump.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: BLOB_edit(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_edit = fb_library.BLOB_edit
        self.BLOB_edit.restype = c_int
        self.BLOB_edit.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: BLOB_load(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_load = fb_library.BLOB_load
        self.BLOB_load.restype = c_int
        self.BLOB_load.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: BLOB_text_dump(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_text_dump = fb_library.BLOB_text_dump
        self.BLOB_text_dump.restype = c_int
        self.BLOB_text_dump.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: BLOB_text_load(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.BLOB_text_load = fb_library.BLOB_text_load
        self.BLOB_text_load.restype = c_int
        self.BLOB_text_load.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: Bopen(POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING)
        self.Bopen = fb_library.Bopen
        self.Bopen.restype = POINTER(BSTREAM)
        self.Bopen.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]
        #: isc_ftof(STRING, c_ushort, STRING, c_ushort)
        self.isc_ftof = fb_library.isc_ftof
        self.isc_ftof.restype = ISC_LONG
        self.isc_ftof.argtypes = [STRING, c_ushort, STRING, c_ushort]
        #: isc_print_blr(STRING, ISC_PRINT_CALLBACK, c_void_p, c_short)
        self.isc_print_blr = fb_library.isc_print_blr
        self.isc_print_blr.restype = ISC_STATUS
        self.isc_print_blr.argtypes = [STRING, ISC_PRINT_CALLBACK, c_void_p, c_short]
        #: isc_set_debug(c_int)
        self.isc_set_debug = fb_library.isc_set_debug
        self.isc_set_debug.restype = None
        self.isc_set_debug.argtypes = [c_int]
        #: isc_qtoq(POINTER(ISC_QUAD), POINTER(ISC_QUAD))
        self.isc_qtoq = fb_library.isc_qtoq
        self.isc_qtoq.restype = None
        self.isc_qtoq.argtypes = [POINTER(ISC_QUAD), POINTER(ISC_QUAD)]
        #: isc_vtof(STRING, STRING, c_ushort)
        self.isc_vtof = fb_library.isc_vtof
        self.isc_vtof.restype = None
        self.isc_vtof.argtypes = [STRING, STRING, c_ushort]
        #: isc_vtov(STRING, STRING, c_short)
        self.isc_vtov = fb_library.isc_vtov
        self.isc_vtov.restype = None
        self.isc_vtov.argtypes = [STRING, STRING, c_short]
        #: isc_version(POINTER(isc_db_handle), ISC_VERSION_CALLBACK, c_void_p)
        self.isc_version = fb_library.isc_version
        self.isc_version.restype = c_int
        self.isc_version.argtypes = [POINTER(isc_db_handle), ISC_VERSION_CALLBACK, c_void_p]
        #: isc_service_attach(POINTER(ISC_STATUS), c_ushort, STRING, POINTER(isc_svc_handle), c_ushort, STRING)
        self.isc_service_attach = fb_library.isc_service_attach
        self.isc_service_attach.restype = ISC_STATUS
        self.isc_service_attach.argtypes = [POINTER(ISC_STATUS), c_ushort, STRING,
                                            POINTER(isc_svc_handle), c_ushort, STRING]
        #: isc_service_detach(POINTER(ISC_STATUS), POINTER(isc_svc_handle))
        self.isc_service_detach = fb_library.isc_service_detach
        self.isc_service_detach.restype = ISC_STATUS
        self.isc_service_detach.argtypes = [POINTER(ISC_STATUS), POINTER(isc_svc_handle)]
        #: isc_service_query(POINTER(ISC_STATUS), POINTER(isc_svc_handle), POINTER(isc_resv_handle), c_ushort, STRING, c_ushort, STRING, c_ushort, STRING)
        self.isc_service_query = fb_library.isc_service_query
        self.isc_service_query.restype = ISC_STATUS
        self.isc_service_query.argtypes = [POINTER(ISC_STATUS),
                                           POINTER(isc_svc_handle),
                                           POINTER(isc_resv_handle), c_ushort,
                                           STRING, c_ushort, STRING, c_ushort,
                                           STRING]
        #: isc_service_start(POINTER(ISC_STATUS), POINTER(isc_svc_handle), POINTER(isc_resv_handle), c_ushort, STRING)
        self.isc_service_start = fb_library.isc_service_start
        self.isc_service_start.restype = ISC_STATUS
        self.isc_service_start.argtypes = [POINTER(ISC_STATUS),
                                           POINTER(isc_svc_handle),
                                           POINTER(isc_resv_handle),
                                           c_ushort, STRING]
        #: isc_get_client_version(STRING)
        self.isc_get_client_version = fb_library.isc_get_client_version
        self.isc_get_client_version.restype = None
        self.isc_get_client_version.argtypes = [STRING]
        #: isc_get_client_major_version()
        self.isc_get_client_major_version = fb_library.isc_get_client_major_version
        self.isc_get_client_major_version.restype = c_int
        self.isc_get_client_major_version.argtypes = []
        #: isc_get_client_minor_version()
        self.isc_get_client_minor_version = fb_library.isc_get_client_minor_version
        self.isc_get_client_minor_version.restype = c_int
        self.isc_get_client_minor_version.argtypes = []

        #self.imaxabs = fb_library.imaxabs
        #self.imaxabs.restype = intmax_t
        #self.imaxabs.argtypes = [intmax_t]

        #self.imaxdiv = fb_library.imaxdiv
        #self.imaxdiv.restype = imaxdiv_t
        #self.imaxdiv.argtypes = [intmax_t, intmax_t]

        #self.strtoimax = fb_library.strtoimax
        #self.strtoimax.restype = intmax_t
        #self.strtoimax.argtypes = [STRING, POINTER(STRING), c_int]

        #self.strtoumax = fb_library.strtoumax
        #self.strtoumax.restype = uintmax_t
        #self.strtoumax.argtypes = [STRING, POINTER(STRING), c_int]

        #self.wcstoimax = fb_library.wcstoimax
        #self.wcstoimax.restype = intmax_t
        #self.wcstoimax.argtypes = [WSTRING, POINTER(WSTRING), c_int]

        #self.wcstoumax = fb_library.wcstoumax
        #self.wcstoumax.restype = uintmax_t
        #self.wcstoumax.argtypes = [WSTRING, POINTER(WSTRING), c_int]

        self.P_isc_event_block = CFUNCTYPE(ISC_LONG, POINTER(POINTER(ISC_UCHAR)),
                                           POINTER(POINTER(ISC_UCHAR)), ISC_USHORT)
        #: C_isc_event_block(ISC_LONG, POINTER(POINTER(ISC_UCHAR)), POINTER(POINTER(ISC_UCHAR)), ISC_USHORT)
        self.C_isc_event_block = self.P_isc_event_block(('isc_event_block', fb_library))
        self.P_isc_event_block_args = self.C_isc_event_block.argtypes

    def isc_event_block(self, event_buffer, result_buffer, *args):
        "Injects variable number of parameters into C_isc_event_block call"
        if len(args) > 15:
            raise Exception("isc_event_block takes no more than 15 event names")
        newargs = list(self.P_isc_event_block_args)
        for x in args:
            newargs.append(STRING)
        self.C_isc_event_block.argtypes = newargs
        result = self.C_isc_event_block(event_buffer, result_buffer, len(args), *args)
        return result


