#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           ibase.py
#   DESCRIPTION:    Python ctypes interface to Firebird client library
#   CREATED:        6.10.2011
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

from ctypes import *
from ctypes.util import find_library
import sys

fb_library_name = find_library('fbclient')
if 'win' in sys.platform:
    fb_library = WinDLL(fb_library_name)
else:
    fb_library = CDLL(fb_library_name)

#-------------------

MAX_BLOB_SEGMENT_SIZE = 65535

charset_map = {
    # DB CHAR SET NAME    :   PYTHON CODEC NAME (CANONICAL)
    # --------------------------------------------------------------------------
    'OCTETS'              :   None, # Allow to pass through unchanged.
    'UNICODE_FSS'         :   'utf_8',
    'UTF8'                :   'utf_8', # (Firebird 2.0+)
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
    'KOI8-R'              :   'koi8_r', # (Firebird 2.0+)
    'KOI8-U'              :   'koi8_u', # (Firebird 2.0+)
    'WIN1258'             :   'cp1258', # (Firebird 2.0+)
    }

DB_CHAR_SET_NAME_TO_PYTHON_ENCODING_MAP = charset_map

# C integer limit constants

SHRT_MIN = -32767
SHRT_MAX = 32767
USHRT_MAX = 65535
INT_MIN = -2147483648
INT_MAX = 2147483647
LONG_MIN = -9223372036854775808
LONG_MAX = 9223372036854775807
SSIZE_T_MIN = INT_MIN
SSIZE_T_MAX = INT_MAX

# Constants

DSQL_close = 1
DSQL_drop = 2
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

SUBTYPE_NUMERIC = 1
SUBTYPE_DECIMAL = 2

#isc_dpb_activate_shadow = '\x15'
#isc_dpb_address_path = 'F'
#isc_dpb_allocation = '\x02'
#isc_dpb_begin_log = '\x19'
#isc_dpb_buffer_length = '\x06'
#isc_dpb_cache_manager = '1'
#isc_dpb_cdd_pathname = '\x01'
#isc_dpb_connect_timeout = '9'
#isc_dpb_damaged = '\x11'
#isc_dpb_dbkey_scope = '\r'
#isc_dpb_debug = '\x07'
#isc_dpb_delete_shadow = '\x17'
#isc_dpb_disable_journal = '\x0c'
#isc_dpb_disable_wal = '8'
#isc_dpb_drop_walfile = '('
#isc_dpb_dummy_packet_interval = ':'
#isc_dpb_enable_journal = '\x0b'
#isc_dpb_encrypt_key = '\x14'
#isc_dpb_force_write = '\x18'
#isc_dpb_garbage_collect = '\x08'
#isc_dpb_gbak_attach = ';'
#isc_dpb_gfix_attach = 'B'
#isc_dpb_gsec_attach = 'E'
#isc_dpb_gstat_attach = 'C'
#isc_dpb_interp = ' '
#isc_dpb_journal = '\x03'
#isc_dpb_lc_ctype = '0'
#isc_dpb_lc_messages = '/'
#isc_dpb_license = '\x12'
#isc_dpb_no_garbage_collect = '\x10'
#isc_dpb_no_reserve = '\x1b'
#isc_dpb_num_buffers = '\x05'
#isc_dpb_number_of_users = '\x0e'
#isc_dpb_old_dump_id = ')'
#isc_dpb_old_file = '$'
#isc_dpb_old_file_size = '"'
#isc_dpb_old_num_files = '#'
#isc_dpb_old_start_file = "'"
#isc_dpb_old_start_page = '%'
#isc_dpb_old_start_seqno = '&'
#isc_dpb_online = '3'
#isc_dpb_online_dump = '!'
#isc_dpb_overwrite = '6'
#isc_dpb_page_size = '\x04'
#isc_dpb_password = '\x1d'
#isc_dpb_password_enc = '\x1e'
#isc_dpb_quit_log = '\x1a'
#isc_dpb_reserved = '5'
#isc_dpb_sec_attach = '7'
#isc_dpb_set_db_charset = 'D'
#isc_dpb_set_db_readonly = '@'
#isc_dpb_set_db_sql_dialect = 'A'
#isc_dpb_set_page_buffers = '='
#isc_dpb_shutdown = '2'
#isc_dpb_shutdown_delay = '4'
#isc_dpb_sql_dialect = '?'
#isc_dpb_sql_role_name = '<'
#isc_dpb_sweep = '\n'
#isc_dpb_sweep_interval = '\x16'
#isc_dpb_sys_user_name = '\x13'
#isc_dpb_sys_user_name_enc = '\x1f'
#isc_dpb_trace = '\x0f'
#isc_dpb_user_name = '\x1c'
#isc_dpb_verify = '\t'
#isc_dpb_version1 = '\x01'
#isc_dpb_wal_backup_dir = '*'
#isc_dpb_wal_bufsize = '-'
#isc_dpb_wal_chkptlen = '+'
#isc_dpb_wal_grp_cmt_wait = '.'
#isc_dpb_wal_numbufs = ','
#isc_dpb_working_directory = '>'

isc_dpb_activate_shadow = 21
isc_dpb_address_path = 70
isc_dpb_allocation = 2
isc_dpb_begin_log = 25
isc_dpb_buffer_length = 6
isc_dpb_cache_manager = 49
isc_dpb_cdd_pathname = 1
isc_dpb_connect_timeout = 57
isc_dpb_damaged = 17
isc_dpb_dbkey_scope = 13
isc_dpb_debug = 7
isc_dpb_delete_shadow = 23
isc_dpb_disable_journal = 12
isc_dpb_disable_wal = 56
isc_dpb_drop_walfile = 40
isc_dpb_dummy_packet_interval = 58
isc_dpb_enable_journal = 11
isc_dpb_encrypt_key = 20
isc_dpb_force_write = 24
isc_dpb_garbage_collect = 8
isc_dpb_gbak_attach = 59
isc_dpb_gfix_attach = 66
isc_dpb_gsec_attach = 69
isc_dpb_gstat_attach = 67
isc_dpb_interp = 32
isc_dpb_journal = 3
isc_dpb_lc_ctype = 48
isc_dpb_lc_messages = 47
isc_dpb_license = 18
isc_dpb_no_garbage_collect = 16
isc_dpb_no_reserve = 27
isc_dpb_num_buffers = 5
isc_dpb_number_of_users = 14
isc_dpb_old_dump_id = 41
isc_dpb_old_file = 36
isc_dpb_old_file_size = 34
isc_dpb_old_num_files = 35
isc_dpb_old_start_file = 39
isc_dpb_old_start_page = 37
isc_dpb_old_start_seqno = 38
isc_dpb_online = 51
isc_dpb_online_dump = 33
isc_dpb_overwrite = 54
isc_dpb_page_size = 4
isc_dpb_password = 29
isc_dpb_password_enc = 30
isc_dpb_quit_log = 26
isc_dpb_reserved = 53
isc_dpb_sec_attach = 55
isc_dpb_set_db_charset = 68
isc_dpb_set_db_readonly = 64
isc_dpb_set_db_sql_dialect = 65
isc_dpb_set_page_buffers = 61
isc_dpb_shutdown = 50
isc_dpb_shutdown_delay = 52
isc_dpb_sql_dialect = 63
isc_dpb_sql_role_name = 60
isc_dpb_sweep = 10
isc_dpb_sweep_interval = 22
isc_dpb_sys_user_name = 19
isc_dpb_sys_user_name_enc = 31
isc_dpb_trace = 15
isc_dpb_user_name = 28
isc_dpb_verify = 9
isc_dpb_version1 = 1
isc_dpb_wal_backup_dir = 42
isc_dpb_wal_bufsize = 45
isc_dpb_wal_chkptlen = 43
isc_dpb_wal_grp_cmt_wait = 46
isc_dpb_wal_numbufs = 44
isc_dpb_working_directory = 62

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

isc_info_active_tran_count = 110
isc_info_active_transactions = 109
isc_info_allocation = 21
isc_info_attachment_id = 22
isc_info_backout_count = 28
isc_info_base_level = 13
isc_info_bpage_errors = 56
isc_info_creation_date = 111
isc_info_cur_log_part_offset = 37
isc_info_cur_logfile_name = 36
isc_info_current_memory = 17
isc_info_db_SQL_dialect = 62
isc_info_db_class = 102
isc_info_db_id = 4
isc_info_db_provider = 108
isc_info_db_read_only = 63
isc_info_db_size_in_pages = 64
isc_info_db_sql_dialect = 62
isc_info_delete_count = 27
isc_info_dpage_errors = 57
isc_info_expunge_count = 30
isc_info_fetches = 7
isc_info_firebird_version = 103
isc_info_forced_writes = 52
isc_info_implementation = 11
isc_info_insert_count = 25
isc_info_ipage_errors = 58
isc_info_isc_version = 12
isc_info_license = 20
isc_info_limbo = 16
isc_info_logfile = 35
isc_info_marks = 8
isc_info_max_memory = 18
isc_info_next_transaction = 107
isc_info_no_reserve = 34
isc_info_num_buffers = 15
isc_info_num_wal_buffers = 38
isc_info_ods_minor_version = 33
isc_info_ods_version = 32
isc_info_oldest_active = 105
isc_info_oldest_snapshot = 106
isc_info_oldest_transaction = 104
isc_info_page_errors = 54
isc_info_page_size = 14
isc_info_ppage_errors = 59
isc_info_purge_count = 29
isc_info_read_idx_count = 24
isc_info_read_seq_count = 23
isc_info_reads = 5
isc_info_record_errors = 55
isc_info_set_page_buffers = 61

# SQL information items
isc_info_sql_select = 4;
isc_info_sql_bind = 5;
isc_info_sql_num_variables = 6;
isc_info_sql_describe_vars = 7;
isc_info_sql_describe_end = 8;
isc_info_sql_sqlda_seq = 9;
isc_info_sql_message_seq = 10;
isc_info_sql_type = 11;
isc_info_sql_sub_type = 12;
isc_info_sql_scale = 13;
isc_info_sql_length = 14;
isc_info_sql_null_ind = 15;
isc_info_sql_field = 16;
isc_info_sql_relation = 17;
isc_info_sql_owner = 18;
isc_info_sql_alias = 19;
isc_info_sql_sqlda_start = 20;
isc_info_sql_stmt_type = 21;
isc_info_sql_get_plan = 22;
isc_info_sql_records = 23;
isc_info_sql_batch_fetch = 24;

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

isc_info_sweep_interval = 31
isc_info_tpage_errors = 60
isc_info_tra_access = 9
isc_info_tra_concurrency = 2
isc_info_tra_consistency = 1
isc_info_tra_id = 4
isc_info_tra_isolation = 8
isc_info_tra_lock_timeout = 10
isc_info_tra_no_rec_version = 0
isc_info_tra_oldest_active = 7
isc_info_tra_oldest_interesting = 5
isc_info_tra_oldest_snapshot = 6
isc_info_tra_read_committed = 3
isc_info_tra_readonly = 0
isc_info_tra_readwrite = 1
isc_info_tra_rec_version = 1

isc_info_blob_num_segments = 4
isc_info_blob_max_segment = 5
isc_info_blob_total_length = 6
isc_info_blob_type = 7

isc_info_update_count = 26
isc_info_user_names = 53
isc_info_version = 12
isc_info_wal_avg_grpc_size = 51
isc_info_wal_avg_io_size = 49
isc_info_wal_buffer_size = 39
isc_info_wal_ckpt_length = 40
isc_info_wal_cur_ckpt_interval = 41
isc_info_wal_grpc_wait_usecs = 47
isc_info_wal_num_commits = 50
isc_info_wal_num_io = 48
isc_info_wal_prv_ckpt_fname = 42
isc_info_wal_prv_ckpt_poffset = 43
isc_info_wal_recv_ckpt_fname = 44
isc_info_wal_recv_ckpt_poffset = 45
isc_info_window_turns = 19
isc_info_writes = 6

#isc_tpb_autocommit = '\x10'
#isc_tpb_commit_time = '\r'
#isc_tpb_concurrency = '\x02'
#isc_tpb_consistency = '\x01'
#isc_tpb_exclusive = '\x05'
#isc_tpb_ignore_limbo = '\x0e'
#isc_tpb_lock_read = '\n'
#isc_tpb_lock_timeout = '\x15'
#isc_tpb_lock_write = '\x0b'
#isc_tpb_no_auto_undo = '\x14'
#isc_tpb_no_rec_version = '\x12'
#isc_tpb_nowait = '\x07'
#isc_tpb_protected = '\x04'
#isc_tpb_read = '\x08'
#isc_tpb_read_committed = '\x0f'
#isc_tpb_rec_version = '\x11'
#isc_tpb_restart_requests = '\x13'
#isc_tpb_shared = '\x03'
#isc_tpb_verb_time = '\x0c'
#isc_tpb_version3 = '\x03'
#isc_tpb_wait = '\x06'
#isc_tpb_write = '\t'

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

# Services API
# Service parameter block stuff
isc_spb_current_version = 2
isc_spb_version = isc_spb_current_version
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
isc_spb_process_id = 110
isc_spb_trusted_auth = 111
isc_spb_process_name = 112

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

# Parameters for isc_action_{add|del|mod|disp)_user
isc_spb_sec_userid = 5
isc_spb_sec_groupid = 6
isc_spb_sec_username = 7
isc_spb_sec_password = 8
isc_spb_sec_groupname = 9
isc_spb_sec_firstname = 10
isc_spb_sec_middlename = 11
isc_spb_sec_lastname = 12

# Parameters for isc_action_svc_backup
isc_spb_bkp_file = 5
isc_spb_bkp_factor = 6
isc_spb_bkp_length = 7
isc_spb_bkp_ignore_checksums = 0x01
isc_spb_bkp_ignore_limbo = 0x02
isc_spb_bkp_metadata_only = 0x04
isc_spb_bkp_no_garbage_collect = 0x08
isc_spb_bkp_old_descriptions = 0x10
isc_spb_bkp_non_transportable = 0x20
isc_spb_bkp_convert = 0x40
isc_spb_bkp_expand = 0x80

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

# Parameters for isc_spb_prp_reserve_space
isc_spb_prp_res_use_full = 35
isc_spb_prp_res = 36

# Parameters for isc_spb_prp_write_mode
isc_spb_prp_wm_async = 37
isc_spb_prp_wm_sync = 38

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

isc_spb_rpr_validate_db = 0x01
isc_spb_rpr_sweep_db = 0x02
isc_spb_rpr_mend_db = 0x04
isc_spb_rpr_list_limbo_trans = 0x08
isc_spb_rpr_check_db = 0x10
isc_spb_rpr_ignore_checksum = 0x20
isc_spb_rpr_kill_shadows = 0x40
isc_spb_rpr_full = 0x80

# Parameters for isc_action_svc_restore
isc_spb_res_buffers = 9
isc_spb_res_page_size = 10
isc_spb_res_length = 11
isc_spb_res_access_mode = 12
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

#-------------------

STRING = c_char_p
WSTRING = c_wchar_p

frb_info_att_charset = 101

blb_got_eof = 0
blb_got_fragment = -1
blb_got_full_segment = 1
blb_seek_relative = 1
blb_seek_from_tail = 2

isc_info_db_impl_isc_sun_68k = 28
isc_info_db_code_last_value = 5
isc_info_ppage_errors = 59
isc_info_db_impl_linux_ia64 = 76
isc_info_window_turns = 19
isc_info_num_wal_buffers = 38
isc_info_db_impl_i386 = 60
isc_info_wal_prv_ckpt_fname = 42
isc_info_limbo = 16
isc_info_db_impl_sinixz = 64
isc_info_db_impl_isc_dg = 38
isc_info_attachment_id = 22
isc_info_db_impl_isc_delta = 45
isc_info_db_impl_isc_os2 = 29
isc_info_db_impl_last_value = 77
isc_info_user_names = 53
isc_info_wal_buffer_size = 39
isc_info_base_level = 13
isc_info_page_errors = 54
isc_info_db_impl_netbsd = 62
isc_info_db_impl_rdb_eln_y = 5
isc_info_insert_count = 25
isc_info_db_class_cache = 12
isc_info_cur_log_part_offset = 37
isc_info_db_impl_isc_next = 46
isc_info_db_impl_isc_hp_mpexl = 39
isc_info_db_code_rdb_vms = 2
isc_info_db_class_classic_access = 13
isc_info_record_errors = 55
isc_info_db_code_rdb_eln = 1
isc_info_wal_num_commits = 50
isc_info_cur_logfile_name = 36
isc_info_db_impl_isc_dos = 47
isc_info_db_impl_rdb_eln = 2
isc_info_update_count = 26
isc_info_db_id = 4
isc_info_db_class_server_access = 14
isc_info_db_file_size = 112
isc_info_bpage_errors = 56
isc_info_db_impl_m88K = 48
isc_info_tpage_errors = 60
isc_info_wal_grpc_wait_usecs = 47
isc_info_set_page_buffers = 61
isc_info_sweep_interval = 31
isc_info_reads = 5
isc_info_db_class_last_value = 15
isc_info_ods_minor_version = 33
isc_info_wal_ckpt_length = 40
isc_info_db_impl_unixware = 49
isc_info_db_impl_isc_vms_orcl = 33
isc_info_db_size_in_pages = 64
isc_info_no_reserve = 34
isc_info_wal_prv_ckpt_poffset = 43
isc_info_current_memory = 17
isc_info_db_impl_linux_sparc = 65
isc_info_writes = 6
isc_info_db_impl_isc_vax_ultr = 26
isc_info_db_class_access = 1
isc_info_ipage_errors = 58
isc_info_db_impl_isc_sun_386i = 32
isc_info_oldest_snapshot = 106
isc_info_oldest_transaction = 104
isc_info_firebird_version = 103
isc_info_fetches = 7
isc_info_page_size = 14
isc_info_db_impl_freebsd_amd64 = 67
isc_info_db_class_sam_srvr = 10
isc_info_read_idx_count = 24
isc_info_db_impl_isc_epson = 51
isc_info_oldest_active = 105
isc_info_dpage_errors = 57
isc_info_db_impl_isc_rt_aix = 35
isc_info_db_impl_winnt_amd64 = 68
isc_info_wal_avg_grpc_size = 51
isc_info_implementation = 11
isc_info_db_impl_sco_ev = 59
isc_info_db_impl_alpha_osf = 52
isc_info_delete_count = 27
isc_info_db_class_pipe_int = 7
isc_info_db_impl_isc_vms = 27
isc_info_db_impl_rdb_eln_dev = 3
isc_info_db_impl_isc_sun4 = 30
isc_info_db_impl_isc_hp_ux = 31
isc_info_db_impl_linux_ppc = 69
isc_info_db_class_y_valve = 2
isc_info_wal_num_io = 48
isc_info_db_impl_ncr_3000 = 56
isc_info_db_impl_isc_xenix = 37
isc_info_db_class_sam_int = 9
isc_info_db_impl_rdb_vms_y = 4
isc_info_db_impl_isc_mips_ult = 36
isc_info_db_impl_darwin_x86 = 70
isc_info_db_impl_netware_386 = 54
isc_info_db_read_only = 63
isc_info_wal_recv_ckpt_fname = 44
isc_info_max_memory = 18
isc_info_db_impl_linux_amd64 = 66
isc_info_db_impl_alpha_vms = 53
isc_info_db_code_firebird = 4
isc_info_db_impl_linux_mipsel = 71
isc_info_db_class_rem_srvr = 4
isc_info_license = 20
isc_info_db_impl_win_only = 55
isc_info_expunge_count = 30
isc_info_next_transaction = 107
isc_info_active_transactions = 109
isc_info_db_impl_jri = 6
isc_info_db_class = 102
isc_info_wal_cur_ckpt_interval = 41
isc_info_backout_count = 28
isc_info_num_buffers = 15
isc_info_db_impl_darwin_ppc = 63
isc_info_db_impl_linux_mips = 72
isc_info_db_impl_isc_winnt_x86 = 50
isc_info_db_impl_jsv = 7
isc_info_db_class_gateway = 11
isc_info_read_seq_count = 23
isc_info_active_tran_count = 110
isc_info_db_sql_dialect = 62
isc_info_db_impl_isc_hp_ux68K = 40
isc_info_db_impl_darwin_x64 = 73
isc_info_forced_writes = 52
isc_info_isc_version = 12
isc_info_db_impl_winnt_ppc = 57
isc_info_db_impl_isc_mac_aux = 34
isc_info_db_class_pipe_srvr = 8
isc_info_db_impl_isc_apl_68K = 25
isc_info_allocation = 21
isc_info_db_code_interbase = 3
isc_info_db_impl_sun_amd64 = 74
isc_info_db_impl_dg_x86 = 58
isc_info_wal_avg_io_size = 49
isc_info_logfile = 35
isc_info_db_impl_isc_sco_unix = 42
isc_info_purge_count = 29
isc_info_db_impl_isc_imp = 44
isc_info_db_impl_rdb_vms = 1
isc_info_db_class_rem_int = 3
isc_info_db_impl_freebsd = 61
isc_info_db_impl_linux_arm = 75
isc_info_creation_date = 111
isc_info_marks = 8
isc_info_db_last_value = 113
isc_info_db_impl_isc_cray = 43
isc_info_db_provider = 108
isc_info_wal_recv_ckpt_poffset = 45
isc_info_ods_version = 32
isc_info_db_impl_isc_sgi = 41

# status codes

isc_segment     = 335544366L

FB_API_HANDLE = c_uint
intptr_t = c_long
ISC_STATUS = intptr_t
ISC_STATUS_PTR = POINTER(ISC_STATUS)
ISC_STATUS_ARRAY = ISC_STATUS * 20
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
isc_callback = CFUNCTYPE(None)
isc_resv_handle = ISC_LONG

ISC_PRINT_CALLBACK = CFUNCTYPE(None, c_void_p, c_short, STRING)
ISC_VERSION_CALLBACK = CFUNCTYPE(None, c_void_p, STRING)
ISC_EVENT_CALLBACK = CFUNCTYPE(None, c_void_p, c_ushort, POINTER(ISC_UCHAR))

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
    ('array_desc_scale', ISC_SCHAR),
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
    ('bstr_buffer', STRING),
    ('bstr_ptr', STRING),
    ('bstr_length', c_short),
    ('bstr_cnt', c_short),
    ('bstr_mode', c_char),
]
BSTREAM = bstream

# values for enumeration 'blob_lseek_mode'
blob_lseek_mode = c_int # enum

# values for enumeration 'blob_get_result'
blob_get_result = c_int # enum

class blobcallback(Structure):
    pass
blobcallback._fields_ = [
    ('blob_get_segment', CFUNCTYPE(c_short, c_void_p, POINTER(ISC_UCHAR), c_ushort, POINTER(ISC_USHORT))),
    ('blob_handle', c_void_p),
    ('blob_number_segments', ISC_LONG),
    ('blob_max_segment', ISC_LONG),
    ('blob_total_length', ISC_LONG),
    ('blob_put_segment', CFUNCTYPE(None, c_void_p, POINTER(ISC_UCHAR), c_ushort)),
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

class XSQLVAR(Structure):
    pass
XSQLVAR._fields_ = [
    ('sqltype', ISC_SHORT),
    ('sqlscale', ISC_SHORT),
    ('sqlsubtype', ISC_SHORT),
    ('sqllen', ISC_SHORT),
    ('sqldata', POINTER(c_char)),#STRING),
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

isc_attach_database = fb_library.isc_attach_database
isc_attach_database.restype = ISC_STATUS
isc_attach_database.argtypes = [POINTER(ISC_STATUS), c_short, STRING, POINTER(isc_db_handle), c_short, STRING]

isc_array_gen_sdl = fb_library.isc_array_gen_sdl
isc_array_gen_sdl.restype = ISC_STATUS
isc_array_gen_sdl.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_ARRAY_DESC), POINTER(ISC_SHORT), POINTER(ISC_UCHAR), POINTER(ISC_SHORT)]

isc_array_get_slice = fb_library.isc_array_get_slice
isc_array_get_slice.restype = ISC_STATUS
isc_array_get_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), POINTER(ISC_ARRAY_DESC), c_void_p, POINTER(ISC_LONG)]

isc_array_lookup_bounds = fb_library.isc_array_lookup_bounds
isc_array_lookup_bounds.restype = ISC_STATUS
isc_array_lookup_bounds.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, STRING, POINTER(ISC_ARRAY_DESC)]

isc_array_lookup_desc = fb_library.isc_array_lookup_desc
isc_array_lookup_desc.restype = ISC_STATUS
isc_array_lookup_desc.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, STRING, POINTER(ISC_ARRAY_DESC)]

isc_array_set_desc = fb_library.isc_array_set_desc
isc_array_set_desc.restype = ISC_STATUS
isc_array_set_desc.argtypes = [POINTER(ISC_STATUS), STRING, STRING, POINTER(c_short), POINTER(c_short), POINTER(c_short), POINTER(ISC_ARRAY_DESC)]

isc_array_put_slice = fb_library.isc_array_put_slice
isc_array_put_slice.restype = ISC_STATUS
isc_array_put_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), POINTER(ISC_ARRAY_DESC), c_void_p, POINTER(ISC_LONG)]

isc_blob_default_desc = fb_library.isc_blob_default_desc
isc_blob_default_desc.restype = None
isc_blob_default_desc.argtypes = [POINTER(ISC_BLOB_DESC), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR)]

isc_blob_gen_bpb = fb_library.isc_blob_gen_bpb
isc_blob_gen_bpb.restype = ISC_STATUS
isc_blob_gen_bpb.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_BLOB_DESC), POINTER(ISC_BLOB_DESC), c_ushort, POINTER(ISC_UCHAR), POINTER(c_ushort)]

isc_blob_info = fb_library.isc_blob_info
isc_blob_info.restype = ISC_STATUS
isc_blob_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_short, STRING, c_short, POINTER(c_char)]

isc_blob_lookup_desc = fb_library.isc_blob_lookup_desc
isc_blob_lookup_desc.restype = ISC_STATUS
isc_blob_lookup_desc.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR), POINTER(ISC_BLOB_DESC), POINTER(ISC_UCHAR)]

isc_blob_set_desc = fb_library.isc_blob_set_desc
isc_blob_set_desc.restype = ISC_STATUS
isc_blob_set_desc.argtypes = [POINTER(ISC_STATUS), POINTER(ISC_UCHAR), POINTER(ISC_UCHAR), c_short, c_short, c_short, POINTER(ISC_BLOB_DESC)]

isc_cancel_blob = fb_library.isc_cancel_blob
isc_cancel_blob.restype = ISC_STATUS
isc_cancel_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle)]

isc_cancel_events = fb_library.isc_cancel_events
isc_cancel_events.restype = ISC_STATUS
isc_cancel_events.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(ISC_LONG)]

isc_close_blob = fb_library.isc_close_blob
isc_close_blob.restype = ISC_STATUS
isc_close_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle)]

isc_commit_retaining = fb_library.isc_commit_retaining
isc_commit_retaining.restype = ISC_STATUS
isc_commit_retaining.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]

isc_commit_transaction = fb_library.isc_commit_transaction
isc_commit_transaction.restype = ISC_STATUS
isc_commit_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]

isc_create_blob = fb_library.isc_create_blob
isc_create_blob.restype = ISC_STATUS
isc_create_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD)]

isc_create_blob2 = fb_library.isc_create_blob2
isc_create_blob2.restype = ISC_STATUS
isc_create_blob2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD), c_short, STRING]

isc_create_database = fb_library.isc_create_database
isc_create_database.restype = ISC_STATUS
isc_create_database.argtypes = [POINTER(ISC_STATUS), c_short, STRING, POINTER(isc_db_handle), c_short, STRING, c_short]

isc_database_info = fb_library.isc_database_info
isc_database_info.restype = ISC_STATUS
isc_database_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), c_short, STRING, c_short, STRING]

isc_decode_date = fb_library.isc_decode_date
isc_decode_date.restype = None
isc_decode_date.argtypes = [POINTER(ISC_QUAD), c_void_p]

isc_decode_sql_date = fb_library.isc_decode_sql_date
isc_decode_sql_date.restype = None
isc_decode_sql_date.argtypes = [POINTER(ISC_DATE), c_void_p]

isc_decode_sql_time = fb_library.isc_decode_sql_time
isc_decode_sql_time.restype = None
isc_decode_sql_time.argtypes = [POINTER(ISC_TIME), c_void_p]

isc_decode_timestamp = fb_library.isc_decode_timestamp
isc_decode_timestamp.restype = None
isc_decode_timestamp.argtypes = [POINTER(ISC_TIMESTAMP), c_void_p]

isc_detach_database = fb_library.isc_detach_database
isc_detach_database.restype = ISC_STATUS
isc_detach_database.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle)]

isc_drop_database = fb_library.isc_drop_database
isc_drop_database.restype = ISC_STATUS
isc_drop_database.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle)]

isc_dsql_allocate_statement = fb_library.isc_dsql_allocate_statement
isc_dsql_allocate_statement.restype = ISC_STATUS
isc_dsql_allocate_statement.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_stmt_handle)]

isc_dsql_alloc_statement2 = fb_library.isc_dsql_alloc_statement2
isc_dsql_alloc_statement2.restype = ISC_STATUS
isc_dsql_alloc_statement2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_stmt_handle)]

isc_dsql_describe = fb_library.isc_dsql_describe
isc_dsql_describe.restype = ISC_STATUS
isc_dsql_describe.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)]

isc_dsql_describe_bind = fb_library.isc_dsql_describe_bind
isc_dsql_describe_bind.restype = ISC_STATUS
isc_dsql_describe_bind.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)]

isc_dsql_exec_immed2 = fb_library.isc_dsql_exec_immed2
isc_dsql_exec_immed2.restype = ISC_STATUS
isc_dsql_exec_immed2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA)]

isc_dsql_execute = fb_library.isc_dsql_execute
isc_dsql_execute.restype = ISC_STATUS
isc_dsql_execute.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)]

isc_dsql_execute2 = fb_library.isc_dsql_execute2
isc_dsql_execute2.restype = ISC_STATUS
isc_dsql_execute2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA), POINTER(XSQLDA)]

isc_dsql_execute_immediate = fb_library.isc_dsql_execute_immediate
isc_dsql_execute_immediate.restype = ISC_STATUS
isc_dsql_execute_immediate.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA)]

isc_dsql_fetch = fb_library.isc_dsql_fetch
isc_dsql_fetch.restype = ISC_STATUS
isc_dsql_fetch.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)]

isc_dsql_finish = fb_library.isc_dsql_finish
isc_dsql_finish.restype = ISC_STATUS
isc_dsql_finish.argtypes = [POINTER(isc_db_handle)]

isc_dsql_free_statement = fb_library.isc_dsql_free_statement
isc_dsql_free_statement.restype = ISC_STATUS
isc_dsql_free_statement.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort]

isc_dsql_insert = fb_library.isc_dsql_insert
isc_dsql_insert.restype = ISC_STATUS
isc_dsql_insert.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, POINTER(XSQLDA)]

isc_dsql_prepare = fb_library.isc_dsql_prepare
isc_dsql_prepare.restype = ISC_STATUS
isc_dsql_prepare.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA)]

isc_dsql_set_cursor_name = fb_library.isc_dsql_set_cursor_name
isc_dsql_set_cursor_name.restype = ISC_STATUS
isc_dsql_set_cursor_name.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), STRING, c_ushort]

isc_dsql_sql_info = fb_library.isc_dsql_sql_info
isc_dsql_sql_info.restype = ISC_STATUS
isc_dsql_sql_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_short, STRING, c_short, STRING]

isc_encode_date = fb_library.isc_encode_date
isc_encode_date.restype = None
isc_encode_date.argtypes = [c_void_p, POINTER(ISC_QUAD)]

isc_encode_sql_date = fb_library.isc_encode_sql_date
isc_encode_sql_date.restype = None
isc_encode_sql_date.argtypes = [c_void_p, POINTER(ISC_DATE)]

isc_encode_sql_time = fb_library.isc_encode_sql_time
isc_encode_sql_time.restype = None
isc_encode_sql_time.argtypes = [c_void_p, POINTER(ISC_TIME)]

isc_encode_timestamp = fb_library.isc_encode_timestamp
isc_encode_timestamp.restype = None
isc_encode_timestamp.argtypes = [c_void_p, POINTER(ISC_TIMESTAMP)]

isc_event_block = fb_library.isc_event_block
isc_event_block.restype = ISC_LONG
isc_event_block.argtypes = [POINTER(POINTER(ISC_UCHAR)), POINTER(POINTER(ISC_UCHAR)), ISC_USHORT]

isc_event_counts = fb_library.isc_event_counts
isc_event_counts.restype = None
isc_event_counts.argtypes = [POINTER(ISC_ULONG), c_short, POINTER(ISC_UCHAR), POINTER(ISC_UCHAR)]

isc_expand_dpb = fb_library.isc_expand_dpb
isc_expand_dpb.restype = None
isc_expand_dpb.argtypes = [POINTER(STRING), POINTER(c_short)]

isc_modify_dpb = fb_library.isc_modify_dpb
isc_modify_dpb.restype = c_int
isc_modify_dpb.argtypes = [POINTER(STRING), POINTER(c_short), c_ushort, STRING, c_short]

isc_free = fb_library.isc_free
isc_free.restype = ISC_LONG
isc_free.argtypes = [STRING]

isc_get_segment = fb_library.isc_get_segment
isc_get_segment.restype = ISC_STATUS
isc_get_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), POINTER(c_ushort), c_ushort, c_void_p]
#isc_get_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), POINTER(c_ushort), c_ushort, POINTER(c_char)]

isc_get_slice = fb_library.isc_get_slice
isc_get_slice.restype = ISC_STATUS
isc_get_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), c_short, STRING, c_short, POINTER(ISC_LONG), ISC_LONG, c_void_p, POINTER(ISC_LONG)]

isc_interprete = fb_library.isc_interprete
isc_interprete.restype = ISC_LONG
isc_interprete.argtypes = [STRING, POINTER(POINTER(ISC_STATUS))]

fb_interpret = fb_library.fb_interpret
fb_interpret.restype = ISC_LONG
fb_interpret.argtypes = [STRING, c_uint, POINTER(POINTER(ISC_STATUS))]

isc_open_blob = fb_library.isc_open_blob
isc_open_blob.restype = ISC_STATUS
isc_open_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD)]

isc_open_blob2 = fb_library.isc_open_blob2
isc_open_blob2.restype = ISC_STATUS
isc_open_blob2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(isc_blob_handle), POINTER(ISC_QUAD), ISC_USHORT, POINTER(ISC_UCHAR)]

isc_prepare_transaction2 = fb_library.isc_prepare_transaction2
isc_prepare_transaction2.restype = ISC_STATUS
isc_prepare_transaction2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), ISC_USHORT, POINTER(ISC_UCHAR)]

isc_print_sqlerror = fb_library.isc_print_sqlerror
isc_print_sqlerror.restype = None
isc_print_sqlerror.argtypes = [ISC_SHORT, POINTER(ISC_STATUS)]

isc_print_status = fb_library.isc_print_status
isc_print_status.restype = ISC_STATUS
isc_print_status.argtypes = [POINTER(ISC_STATUS)]

isc_put_segment = fb_library.isc_put_segment
isc_put_segment.restype = ISC_STATUS
isc_put_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_ushort, c_void_p]
#isc_put_segment.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_ushort, STRING]

isc_put_slice = fb_library.isc_put_slice
isc_put_slice.restype = ISC_STATUS
isc_put_slice.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(ISC_QUAD), c_short, STRING, c_short, POINTER(ISC_LONG), ISC_LONG, c_void_p]

isc_que_events = fb_library.isc_que_events
isc_que_events.restype = ISC_STATUS
isc_que_events.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(ISC_LONG), c_short, POINTER(ISC_UCHAR), ISC_EVENT_CALLBACK, c_void_p]

isc_rollback_retaining = fb_library.isc_rollback_retaining
isc_rollback_retaining.restype = ISC_STATUS
isc_rollback_retaining.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]

isc_rollback_transaction = fb_library.isc_rollback_transaction
isc_rollback_transaction.restype = ISC_STATUS
isc_rollback_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]

isc_start_multiple = fb_library.isc_start_multiple
isc_start_multiple.restype = ISC_STATUS
isc_start_multiple.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short, c_void_p]

### 
if 'win' in sys.platform:
    P_isc_start_transaction = CFUNCTYPE(ISC_STATUS,POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short,POINTER(isc_db_handle),c_short, STRING)
    isc_start_transaction = P_isc_start_transaction(('isc_start_transaction',fb_library))
else:
    isc_start_transaction = fb_library.isc_start_transaction
    isc_start_transaction.restype = ISC_STATUS
    isc_start_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short,POINTER(isc_db_handle),c_short, STRING]



isc_sqlcode = fb_library.isc_sqlcode
isc_sqlcode.restype = ISC_LONG
isc_sqlcode.argtypes = [POINTER(ISC_STATUS)]

isc_sql_interprete = fb_library.isc_sql_interprete
isc_sql_interprete.restype = None
isc_sql_interprete.argtypes = [c_short, STRING, c_short]

isc_transaction_info = fb_library.isc_transaction_info
isc_transaction_info.restype = ISC_STATUS
isc_transaction_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short, STRING, c_short, STRING]

isc_transact_request = fb_library.isc_transact_request
isc_transact_request.restype = ISC_STATUS
isc_transact_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, STRING, c_ushort, STRING]

isc_vax_integer = fb_library.isc_vax_integer
isc_vax_integer.restype = ISC_LONG
isc_vax_integer.argtypes = [STRING, c_short]

isc_portable_integer = fb_library.isc_portable_integer
isc_portable_integer.restype = ISC_INT64
isc_portable_integer.argtypes = [POINTER(ISC_UCHAR), c_short]

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

isc_add_user = fb_library.isc_add_user
isc_add_user.restype = ISC_STATUS
isc_add_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]

isc_delete_user = fb_library.isc_delete_user
isc_delete_user.restype = ISC_STATUS
isc_delete_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]

isc_modify_user = fb_library.isc_modify_user
isc_modify_user.restype = ISC_STATUS
isc_modify_user.argtypes = [POINTER(ISC_STATUS), POINTER(USER_SEC_DATA)]

isc_compile_request = fb_library.isc_compile_request
isc_compile_request.restype = ISC_STATUS
isc_compile_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_req_handle), c_short, STRING]

isc_compile_request2 = fb_library.isc_compile_request2
isc_compile_request2.restype = ISC_STATUS
isc_compile_request2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_req_handle), c_short, STRING]

isc_ddl = fb_library.isc_ddl
isc_ddl.restype = ISC_STATUS
isc_ddl.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_short, STRING]

isc_prepare_transaction = fb_library.isc_prepare_transaction
isc_prepare_transaction.restype = ISC_STATUS
isc_prepare_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle)]

isc_receive = fb_library.isc_receive
isc_receive.restype = ISC_STATUS
isc_receive.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, c_void_p, c_short]

isc_reconnect_transaction = fb_library.isc_reconnect_transaction
isc_reconnect_transaction.restype = ISC_STATUS
isc_reconnect_transaction.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_short, STRING]

isc_release_request = fb_library.isc_release_request
isc_release_request.restype = ISC_STATUS
isc_release_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle)]

isc_request_info = fb_library.isc_request_info
isc_request_info.restype = ISC_STATUS
isc_request_info.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, STRING, c_short, STRING]

isc_seek_blob = fb_library.isc_seek_blob
isc_seek_blob.restype = ISC_STATUS
isc_seek_blob.argtypes = [POINTER(ISC_STATUS), POINTER(isc_blob_handle), c_short, ISC_LONG, POINTER(ISC_LONG)]

isc_send = fb_library.isc_send
isc_send.restype = ISC_STATUS
isc_send.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle), c_short, c_short, c_void_p, c_short]

isc_start_and_send = fb_library.isc_start_and_send
isc_start_and_send.restype = ISC_STATUS
isc_start_and_send.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle), POINTER(isc_tr_handle), c_short, c_short, c_void_p, c_short]

isc_start_request = fb_library.isc_start_request
isc_start_request.restype = ISC_STATUS
isc_start_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_req_handle), POINTER(isc_tr_handle), c_short]

isc_unwind_request = fb_library.isc_unwind_request
isc_unwind_request.restype = ISC_STATUS
isc_unwind_request.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), c_short]

isc_wait_for_event = fb_library.isc_wait_for_event
isc_wait_for_event.restype = ISC_STATUS
isc_wait_for_event.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), c_short, POINTER(ISC_UCHAR), POINTER(ISC_UCHAR)]

isc_close = fb_library.isc_close
isc_close.restype = ISC_STATUS
isc_close.argtypes = [POINTER(ISC_STATUS), STRING]

isc_declare = fb_library.isc_declare
isc_declare.restype = ISC_STATUS
isc_declare.argtypes = [POINTER(ISC_STATUS), STRING, STRING]

isc_describe = fb_library.isc_describe
isc_describe.restype = ISC_STATUS
isc_describe.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]

isc_describe_bind = fb_library.isc_describe_bind
isc_describe_bind.restype = ISC_STATUS
isc_describe_bind.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]

isc_execute = fb_library.isc_execute
isc_execute.restype = ISC_STATUS
isc_execute.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, POINTER(XSQLDA)]

isc_execute_immediate = fb_library.isc_execute_immediate
isc_execute_immediate.restype = ISC_STATUS
isc_execute_immediate.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), POINTER(c_short), STRING]

isc_fetch = fb_library.isc_fetch
isc_fetch.restype = ISC_STATUS
isc_fetch.argtypes = [POINTER(ISC_STATUS), STRING, POINTER(XSQLDA)]

isc_open = fb_library.isc_open
isc_open.restype = ISC_STATUS
isc_open.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, POINTER(XSQLDA)]

isc_prepare = fb_library.isc_prepare
isc_prepare.restype = ISC_STATUS
isc_prepare.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, POINTER(c_short), STRING, POINTER(XSQLDA)]

isc_dsql_execute_m = fb_library.isc_dsql_execute_m
isc_dsql_execute_m.restype = ISC_STATUS
isc_dsql_execute_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_execute2_m = fb_library.isc_dsql_execute2_m
isc_dsql_execute2_m.restype = ISC_STATUS
isc_dsql_execute2_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_execute_immediate_m = fb_library.isc_dsql_execute_immediate_m
isc_dsql_execute_immediate_m.restype = ISC_STATUS
isc_dsql_execute_immediate_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_exec_immed3_m = fb_library.isc_dsql_exec_immed3_m
isc_dsql_exec_immed3_m.restype = ISC_STATUS
isc_dsql_exec_immed3_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_fetch_m = fb_library.isc_dsql_fetch_m
isc_dsql_fetch_m.restype = ISC_STATUS
isc_dsql_fetch_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_insert_m = fb_library.isc_dsql_insert_m
isc_dsql_insert_m.restype = ISC_STATUS
isc_dsql_insert_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING]

isc_dsql_prepare_m = fb_library.isc_dsql_prepare_m
isc_dsql_prepare_m.restype = ISC_STATUS
isc_dsql_prepare_m.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), POINTER(isc_stmt_handle), c_ushort, STRING, c_ushort, c_ushort, STRING, c_ushort, STRING]

isc_dsql_release = fb_library.isc_dsql_release
isc_dsql_release.restype = ISC_STATUS
isc_dsql_release.argtypes = [POINTER(ISC_STATUS), STRING]

isc_embed_dsql_close = fb_library.isc_embed_dsql_close
isc_embed_dsql_close.restype = ISC_STATUS
isc_embed_dsql_close.argtypes = [POINTER(ISC_STATUS), STRING]

isc_embed_dsql_declare = fb_library.isc_embed_dsql_declare
isc_embed_dsql_declare.restype = ISC_STATUS
isc_embed_dsql_declare.argtypes = [POINTER(ISC_STATUS), STRING, STRING]

isc_embed_dsql_describe = fb_library.isc_embed_dsql_describe
isc_embed_dsql_describe.restype = ISC_STATUS
isc_embed_dsql_describe.argtypes = [POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_describe_bind = fb_library.isc_embed_dsql_describe_bind
isc_embed_dsql_describe_bind.restype = ISC_STATUS
isc_embed_dsql_describe_bind.argtypes = [POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_execute = fb_library.isc_embed_dsql_execute
isc_embed_dsql_execute.restype = ISC_STATUS
isc_embed_dsql_execute.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_execute2 = fb_library.isc_embed_dsql_execute2
isc_embed_dsql_execute2.restype = ISC_STATUS
isc_embed_dsql_execute2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA)]

isc_embed_dsql_execute_immed = fb_library.isc_embed_dsql_execute_immed
isc_embed_dsql_execute_immed.restype = ISC_STATUS
isc_embed_dsql_execute_immed.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), c_ushort, STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_fetch = fb_library.isc_embed_dsql_fetch
isc_embed_dsql_fetch.restype = ISC_STATUS
isc_embed_dsql_fetch.argtypes = [POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_fetch_a = fb_library.isc_embed_dsql_fetch_a
isc_embed_dsql_fetch_a.restype = ISC_STATUS
isc_embed_dsql_fetch_a.argtypes = [POINTER(ISC_STATUS), POINTER(c_int), STRING, ISC_USHORT, POINTER(XSQLDA)]

isc_embed_dsql_open = fb_library.isc_embed_dsql_open
isc_embed_dsql_open.restype = ISC_STATUS
isc_embed_dsql_open.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_open2 = fb_library.isc_embed_dsql_open2
isc_embed_dsql_open2.restype = ISC_STATUS
isc_embed_dsql_open2.argtypes = [POINTER(ISC_STATUS), POINTER(isc_tr_handle), STRING, c_ushort, POINTER(XSQLDA), POINTER(XSQLDA)]

isc_embed_dsql_insert = fb_library.isc_embed_dsql_insert
isc_embed_dsql_insert.restype = ISC_STATUS
isc_embed_dsql_insert.argtypes = [POINTER(ISC_STATUS), STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_prepare = fb_library.isc_embed_dsql_prepare
isc_embed_dsql_prepare.restype = ISC_STATUS
isc_embed_dsql_prepare.argtypes = [POINTER(ISC_STATUS), POINTER(isc_db_handle), POINTER(isc_tr_handle), STRING, c_ushort, STRING, c_ushort, POINTER(XSQLDA)]

isc_embed_dsql_release = fb_library.isc_embed_dsql_release
isc_embed_dsql_release.restype = ISC_STATUS
isc_embed_dsql_release.argtypes = [POINTER(ISC_STATUS), STRING]

BLOB_open = fb_library.BLOB_open
BLOB_open.restype = POINTER(BSTREAM)
BLOB_open.argtypes = [isc_blob_handle, STRING, c_int]

BLOB_put = fb_library.BLOB_put
BLOB_put.restype = c_int
BLOB_put.argtypes = [ISC_SCHAR, POINTER(BSTREAM)]

BLOB_close = fb_library.BLOB_close
BLOB_close.restype = c_int
BLOB_close.argtypes = [POINTER(BSTREAM)]

BLOB_get = fb_library.BLOB_get
BLOB_get.restype = c_int
BLOB_get.argtypes = [POINTER(BSTREAM)]

BLOB_display = fb_library.BLOB_display
BLOB_display.restype = c_int
BLOB_display.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

BLOB_dump = fb_library.BLOB_dump
BLOB_dump.restype = c_int
BLOB_dump.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

BLOB_edit = fb_library.BLOB_edit
BLOB_edit.restype = c_int
BLOB_edit.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

BLOB_load = fb_library.BLOB_load
BLOB_load.restype = c_int
BLOB_load.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

BLOB_text_dump = fb_library.BLOB_text_dump
BLOB_text_dump.restype = c_int
BLOB_text_dump.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

BLOB_text_load = fb_library.BLOB_text_load
BLOB_text_load.restype = c_int
BLOB_text_load.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

Bopen = fb_library.Bopen
Bopen.restype = POINTER(BSTREAM)
Bopen.argtypes = [POINTER(ISC_QUAD), isc_db_handle, isc_tr_handle, STRING]

isc_ftof = fb_library.isc_ftof
isc_ftof.restype = ISC_LONG
isc_ftof.argtypes = [STRING, c_ushort, STRING, c_ushort]

isc_print_blr = fb_library.isc_print_blr
isc_print_blr.restype = ISC_STATUS
isc_print_blr.argtypes = [STRING, ISC_PRINT_CALLBACK, c_void_p, c_short]

isc_set_debug = fb_library.isc_set_debug
isc_set_debug.restype = None
isc_set_debug.argtypes = [c_int]

isc_qtoq = fb_library.isc_qtoq
isc_qtoq.restype = None
isc_qtoq.argtypes = [POINTER(ISC_QUAD), POINTER(ISC_QUAD)]

isc_vtof = fb_library.isc_vtof
isc_vtof.restype = None
isc_vtof.argtypes = [STRING, STRING, c_ushort]

isc_vtov = fb_library.isc_vtov
isc_vtov.restype = None
isc_vtov.argtypes = [STRING, STRING, c_short]

isc_version = fb_library.isc_version
isc_version.restype = c_int
isc_version.argtypes = [POINTER(isc_db_handle), ISC_VERSION_CALLBACK, c_void_p]

isc_reset_fpe = fb_library.isc_reset_fpe
isc_reset_fpe.restype = ISC_LONG
isc_reset_fpe.argtypes = [ISC_USHORT]

isc_service_attach = fb_library.isc_service_attach
isc_service_attach.restype = ISC_STATUS
isc_service_attach.argtypes = [POINTER(ISC_STATUS), c_ushort, STRING, POINTER(isc_svc_handle), c_ushort, STRING]

isc_service_detach = fb_library.isc_service_detach
isc_service_detach.restype = ISC_STATUS
isc_service_detach.argtypes = [POINTER(ISC_STATUS), POINTER(isc_svc_handle)]

isc_service_query = fb_library.isc_service_query
isc_service_query.restype = ISC_STATUS
isc_service_query.argtypes = [POINTER(ISC_STATUS), POINTER(isc_svc_handle), POINTER(isc_resv_handle), c_ushort, STRING, c_ushort, STRING, c_ushort, STRING]

isc_service_start = fb_library.isc_service_start
isc_service_start.restype = ISC_STATUS
isc_service_start.argtypes = [POINTER(ISC_STATUS), POINTER(isc_svc_handle), POINTER(isc_resv_handle), c_ushort, STRING]

isc_get_client_version = fb_library.isc_get_client_version
isc_get_client_version.restype = None
isc_get_client_version.argtypes = [STRING]

isc_get_client_major_version = fb_library.isc_get_client_major_version
isc_get_client_major_version.restype = c_int
isc_get_client_major_version.argtypes = []

isc_get_client_minor_version = fb_library.isc_get_client_minor_version
isc_get_client_minor_version.restype = c_int
isc_get_client_minor_version.argtypes = []

# values for enumeration 'db_info_types'
db_info_types = c_int # enum

# values for enumeration 'info_db_implementations'
info_db_implementations = c_int # enum

# values for enumeration 'info_db_class'
info_db_class = c_int # enum

# values for enumeration 'info_db_provider'
info_db_provider = c_int # enum
class imaxdiv_t(Structure):
    pass
imaxdiv_t._fields_ = [
    ('quot', c_long),
    ('rem', c_long),
]
intmax_t = c_long

#imaxabs = fb_library.imaxabs
#imaxabs.restype = intmax_t
#imaxabs.argtypes = [intmax_t]

#imaxdiv = fb_library.imaxdiv
#imaxdiv.restype = imaxdiv_t
#imaxdiv.argtypes = [intmax_t, intmax_t]

#strtoimax = fb_library.strtoimax
#strtoimax.restype = intmax_t
#strtoimax.argtypes = [STRING, POINTER(STRING), c_int]

uintmax_t = c_ulong

#strtoumax = fb_library.strtoumax
#strtoumax.restype = uintmax_t
#strtoumax.argtypes = [STRING, POINTER(STRING), c_int]

#wcstoimax = fb_library.wcstoimax
#wcstoimax.restype = intmax_t
#wcstoimax.argtypes = [WSTRING, POINTER(WSTRING), c_int]

#wcstoumax = fb_library.wcstoumax
#wcstoumax.restype = uintmax_t
#wcstoumax.argtypes = [WSTRING, POINTER(WSTRING), c_int]

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
uintptr_t = c_ulong
ptrdiff_t = c_long
size_t = c_ulong

__all__ = ['isc_info_base_level', 'isc_start_and_send',
           'int_fast32_t', 'BLOBCALLBACK', 'BLOB_dump',
           'isc_info_db_impl_sco_ev', 'isc_info_purge_count',
           'isc_create_database', 'isc_info_max_memory', 'uint8_t',
           'isc_service_start', 'ISC_BLOB_CTL', 'isc_cancel_events',
           'isc_info_db_class', 'isc_info_db_impl_sun_amd64',
           'isc_info_db_impl_alpha_vms', 'isc_decode_timestamp',
           'isc_info_db_impl_linux_sparc',
           'isc_info_db_impl_alpha_osf', 'isc_info_creation_date',
           'isc_prepare_transaction2', 'isc_info_db_impl_isc_imp',
           'isc_info_db_impl_netware_386',
           'isc_info_db_impl_linux_arm', 'uint_fast16_t',
           'ISC_ARRAY_DESC', 'isc_decode_sql_time',
           'isc_array_get_slice', 'isc_info_current_memory',
           'isc_encode_sql_date', 'db_info_types',
           'isc_info_num_buffers', 'isc_dsql_allocate_statement',
           'isc_info_oldest_active', 'Bopen',
           'isc_info_firebird_version', 'blb_got_full_segment',
           'info_db_class', 'isc_info_wal_ckpt_length',
           'isc_info_db_impl_isc_mac_aux', 'isc_info_wal_avg_io_size',
           'isc_portable_integer', 'isc_open_blob',
           'isc_print_sqlerror', 'isc_dsql_exec_immed3_m',
           'uint_least32_t', 'ISC_BLOB_DESC',
           'isc_info_db_impl_rdb_vms_y', 'ISC_UCHAR',
           'blb_seek_relative', 'uint_fast8_t', 'PARAMDSC',
           'ISC_USHORT', 'isc_embed_dsql_execute',
           'isc_info_db_impl_isc_os2', 'isc_unwind_request',
           'isc_attach_database', 'isc_info_db_impl_winnt_amd64',
           'isc_info_db_code_rdb_vms', 'isc_info_db_read_only',
           'isc_info_db_impl_freebsd_amd64', 'isc_qtoq',
           'isc_info_db_impl_linux_ppc', 'isc_info_limbo',
           'isc_decode_date', 'isc_info_db_impl_isc_sun_386i',
           'isc_info_db_impl_last_value', 'isc_dsql_finish',
           'isc_info_db_impl_netbsd', 'isc_transact_request',
           'int_least64_t', 'isc_info_db_impl_isc_hp_ux',
           'isc_info_db_class_access', 'isc_info_page_errors',
           'BLOB_display', 'isc_info_db_impl_isc_cray',
           'isc_info_bpage_errors', 'isc_print_blr',
           'isc_dsql_release', 'isc_info_forced_writes',
           'isc_embed_dsql_describe', 'isc_service_detach',
           'isc_array_lookup_desc', 'isc_compile_request2',
           'isc_info_db_impl_isc_vms', 'isc_info_db_impl_i386',
           'isc_info_db_impl_m88K', 'blobcallback',
           'isc_info_wal_prv_ckpt_fname', 'isc_ddl',
           'isc_dsql_free_statement', 'isc_info_isc_version',
           'isc_blob_gen_bpb', 'isc_free', 'blb_seek_from_tail',
           'blb_got_eof', 'GDS_QUAD_t', 'isc_info_allocation',
           'isc_info_db_impl_rdb_eln_y', 'BSTREAM',
           'isc_info_db_impl_darwin_ppc', 'isc_dsql_prepare',
           'isc_embed_dsql_open', 'BLOB_edit', 'ISC_TIMESTAMP',
           'isc_blob_ctl', 'isc_event_block',
           'isc_info_wal_recv_ckpt_poffset', 'isc_set_debug',
           'BLOB_get', 'isc_dsql_execute', 'isc_request_info',
           'isc_info_db_impl_darwin_x64', 'isc_dsql_describe_bind',
           'isc_tr_handle', 'isc_info_db_class_rem_srvr',
           'isc_cancel_blob', 'size_t', 'BLOB_load', 'isc_vtof',
           'XSQLDA', 'isc_info_db_class_rem_int', 'uint_least16_t',
           'isc_vtov', 'isc_get_client_version', 'ISC_EVENT_CALLBACK',
           'ISC_INT64', 'isc_embed_dsql_release',
           'isc_commit_transaction', 'isc_info_tpage_errors',
           'isc_info_db_impl_linux_mipsel',
           'isc_info_wal_grpc_wait_usecs', 'isc_print_status',
           'GDS_QUAD', 'isc_array_set_desc', 'isc_array_put_slice',
           'isc_info_sweep_interval', 'isc_array_lookup_bounds',
           'info_db_implementations', 'isc_info_db_impl_jsv',
           'isc_info_wal_avg_grpc_size', 'isc_blob_set_desc',
           'isc_info_db_impl_win_only', 'isc_embed_dsql_open2',
           'int_fast64_t', 'uint_fast32_t', 'FB_API_HANDLE',
           'isc_blob_handle', 'uint_least8_t',
           'isc_info_db_impl_isc_apl_68K', 'isc_detach_database',
           'isc_info_cur_log_part_offset', 'isc_sqlcode',
           'isc_dsql_alloc_statement2', 'isc_embed_dsql_prepare',
           'isc_db_handle', 'isc_info_wal_buffer_size',
           'isc_info_db_impl_isc_dos', 'isc_info_oldest_transaction',
           'isc_att_handle', 'isc_info_db_class_cache', 'isc_declare',
           'isc_info_db_class_pipe_srvr', 'isc_info_dpage_errors',
           'fb_interpret', 'isc_info_db_impl_isc_vax_ultr',
           'isc_dsql_execute2_m', 'ISC_DATE', 'isc_get_segment',
           'isc_info_window_turns', 'isc_info_db_class_pipe_int',
           'uint64_t', 'ISC_SHORT', 'ISC_STATUS_ARRAY',
           'isc_info_db_impl_isc_delta', 'isc_create_blob',
           'isc_describe', 'isc_close', 'isc_info_db_impl_ncr_3000',
           'isc_info_user_names', 'isc_info_no_reserve',
           'isc_info_db_id', 'ISC_TIME', 'isc_info_insert_count',
           'isc_info_db_class_y_valve', 'isc_info_active_tran_count',
           'ISC_UINT64', 'isc_info_reads', 'isc_release_request',
           'isc_describe_bind', 'isc_info_db_class_last_value',
           'isc_info_wal_recv_ckpt_fname', 'blob_get_result',
           'isc_info_db_impl_isc_epson', 'isc_info_db_class_gateway',
           'isc_info_db_impl_sinixz', 'isc_info_db_impl_darwin_x86',
           'isc_delete_user', 'isc_dsql_execute2',
           'isc_info_db_impl_jri', 'isc_info_ods_minor_version',
           'isc_resv_handle', 'isc_wait_for_event',
           'isc_info_db_impl_isc_hp_ux68K', 'intptr_t',
           'isc_info_writes', 'int_fast8_t', 'isc_modify_dpb',
           'isc_info_ppage_errors', 'isc_info_db_last_value',
           'isc_get_client_minor_version', 'isc_embed_dsql_close',
           'isc_callback', 'bstream', 'isc_array_gen_sdl',
           'isc_reset_fpe', 'isc_info_db_code_firebird',
           'isc_req_handle', 'isc_info_db_class_sam_srvr',
           'frb_info_att_charset', 'isc_info_attachment_id',
           'paramvary', 'USER_SEC_DATA',
           'isc_info_db_impl_linux_amd64', 'isc_commit_retaining',
           'int16_t', 'ISC_LONG', 'isc_info_db_size_in_pages',
           'isc_embed_dsql_execute_immed', 'isc_dsql_exec_immed2',
           'isc_dsql_prepare_m', 'isc_seek_blob',
           'isc_info_ods_version', 'isc_svc_handle',
           'isc_info_db_impl_rdb_eln', 'isc_open',
           'isc_sql_interprete', 'ISC_PRINT_CALLBACK',
           'isc_info_wal_num_io', 'isc_info_db_impl_freebsd',
           'isc_info_db_code_last_value', 'isc_info_set_page_buffers',
           'isc_database_info', 'isc_encode_date', 'uint16_t',
           'isc_encode_timestamp', 'wcstoumax', 'blb_got_fragment',
           'isc_start_request', 'int32_t',
           'isc_info_db_impl_linux_ia64', 'isc_dsql_sql_info',
           'isc_start_multiple', 'strtoimax',
           'isc_info_wal_num_commits', 'strtoumax', 'ISC_QUAD',
           'isc_prepare_transaction', 'isc_info_wal_prv_ckpt_poffset',
           'isc_info_fetches', 'isc_fetch', 'isc_que_events',
           'isc_compile_request', 'isc_embed_dsql_fetch',
           'isc_dsql_insert_m', 'isc_execute',
           'isc_info_db_class_sam_int', 'isc_info_oldest_snapshot',
           'isc_info_db_impl_isc_sgi', 'uintmax_t', 'XSQLVAR',
           'isc_info_db_impl_unixware', 'isc_reconnect_transaction',
           'int_fast16_t', 'isc_info_db_impl_rdb_eln_dev',
           'isc_decode_sql_date', 'isc_embed_dsql_describe_bind',
           'isc_stmt_handle', 'isc_embed_dsql_fetch_a',
           'isc_info_db_impl_winnt_ppc', 'isc_info_marks',
           'isc_info_license', 'isc_prepare', 'uint_fast64_t',
           'isc_info_db_impl_dg_x86', 'uint32_t',
           'isc_rollback_transaction', 'paramdsc', 'ISC_ULONG',
           'isc_dsql_insert', 'isc_encode_sql_time',
           'isc_dsql_fetch_m', 'isc_info_db_code_rdb_eln',
           'isc_info_db_impl_isc_sun_68k', 'isc_blob_info',
           'isc_service_query', 'isc_open_blob2', 'isc_expand_dpb',
           'isc_info_db_provider', 'imaxabs', 'int_least32_t',
           'isc_dsql_set_cursor_name',
           'isc_info_db_impl_isc_winnt_x86',
           'isc_dsql_execute_immediate', 'isc_vax_integer',
           'ISC_SCHAR', 'isc_info_implementation',
           'isc_info_db_class_server_access', 'isc_interprete',
           'isc_info_wal_cur_ckpt_interval', 'isc_create_blob2',
           'isc_transaction_info', 'isc_close_blob',
           'isc_info_db_class_classic_access',
           'isc_embed_dsql_declare', 'isc_info_db_code_interbase',
           'isc_dsql_describe', 'isc_info_next_transaction',
           'isc_info_read_idx_count', 'PARAMVARY',
           'isc_info_backout_count', 'isc_start_transaction',
           'isc_blob_lookup_desc', 'int_least16_t',
           'isc_service_attach', 'isc_info_db_impl_isc_sco_unix',
           'ISC_VERSION_CALLBACK', 'isc_put_slice', 'isc_get_slice',
           'BLOB_put', 'isc_receive', 'ptrdiff_t', 'uint_least64_t',
           'isc_rollback_retaining', 'isc_info_expunge_count',
           'isc_info_record_errors', 'isc_info_db_impl_isc_hp_mpexl',
           'isc_info_db_impl_isc_sun4', 'isc_embed_dsql_execute2',
           'isc_info_db_impl_isc_dg', 'isc_dsql_execute_m',
           'isc_execute_immediate', 'isc_info_read_seq_count',
           'isc_info_ipage_errors', 'isc_dsql_execute_immediate_m',
           'isc_event_counts', 'isc_info_db_impl_linux_mips',
           'uintptr_t', 'isc_info_num_wal_buffers', 'int8_t',
           'blob_lseek_mode', 'isc_embed_dsql_insert',
           'BLOB_text_load', 'info_db_provider', 'wcstoimax',
           'isc_version', 'isc_info_cur_logfile_name',
           'isc_info_delete_count', 'isc_info_db_sql_dialect',
           'isc_info_logfile', 'imaxdiv', 'isc_add_user',
           'isc_blob_default_desc', 'isc_info_active_transactions',
           'isc_info_db_impl_isc_next', 'isc_drop_database',
           'isc_ftof', 'int64_t', 'BLOB_text_dump',
           'isc_info_page_size', 'isc_send', 'isc_put_segment',
           'BLOB_open', 'imaxdiv_t', 'isc_info_db_impl_isc_vms_orcl',
           'isc_info_db_file_size', 'isc_info_db_impl_isc_xenix',
           'ISC_ARRAY_BOUND', 'isc_info_db_impl_rdb_vms',
           'ISC_STATUS', 'isc_info_db_impl_isc_mips_ult',
           'BLOB_close', 'intmax_t', 'isc_modify_user',
           'isc_info_db_impl_isc_rt_aix',
           'isc_get_client_major_version', 'isc_dsql_fetch',
           'isc_info_update_count', 'int_least8_t']
