#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           __init__.py
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
#  Copyright (c) Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): ______________________________________.
#
# See LICENSE.TXT for details.

from fdb.fbcore import *
from fdb.fbcore import __version__
from fdb import services
from fdb import blr
from fdb import trace
from fdb import gstat

__all__ = (# Common with KInterbasDB
    'BINARY', 'Binary', 'BlobReader', 'Connection', 'ConnectionGroup',
    'Cursor', 'DATETIME', 'DBAPITypeObject', 'DESCRIPTION_DISPLAY_SIZE',
    'DESCRIPTION_INTERNAL_SIZE', 'DESCRIPTION_NAME', 'DESCRIPTION_NULL_OK',
    'DESCRIPTION_PRECISION', 'DESCRIPTION_SCALE', 'DESCRIPTION_TYPE_CODE',
    'DIST_TRANS_MAX_DATABASES', 'DataError', 'DatabaseError', 'Date',
    'DateFromTicks', 'Error', 'EventConduit', 'IntegrityError',
    'InterfaceError', 'InternalError', 'NUMBER', 'NotSupportedError',
    'OperationalError', 'PreparedStatement', 'ProgrammingError', 'ROWID',
    'STRING', 'TPB', 'TableReservation', 'ParameterBuffer', 'Time', 'TimeFromTicks',
    'TimestampFromTicks', 'Transaction', 'TransactionConflict',
    '__version__', 'apilevel', 'connect', 'create_database',
    'frb_info_att_charset', 'isc_dpb_activate_shadow', 'isc_dpb_address_path',
    'isc_dpb_allocation', 'isc_dpb_begin_log', 'isc_dpb_buffer_length',
    'isc_dpb_cache_manager', 'isc_dpb_cdd_pathname', 'isc_dpb_connect_timeout',
    'isc_dpb_damaged', 'isc_dpb_dbkey_scope', 'isc_dpb_debug',
    'isc_dpb_delete_shadow',
    'isc_dpb_dummy_packet_interval',
    'isc_dpb_encrypt_key', 'isc_dpb_force_write',
    'isc_dpb_garbage_collect', 'isc_dpb_gbak_attach', 'isc_dpb_gfix_attach',
    'isc_dpb_gsec_attach', 'isc_dpb_gstat_attach', 'isc_dpb_interp',
    'isc_dpb_lc_ctype', 'isc_dpb_lc_messages',
    'isc_dpb_no_garbage_collect', 'isc_dpb_no_reserve',
    'isc_dpb_num_buffers', 'isc_dpb_number_of_users', 'isc_dpb_old_dump_id',
    'isc_dpb_old_file', 'isc_dpb_old_file_size', 'isc_dpb_old_num_files',
    'isc_dpb_old_start_file', 'isc_dpb_old_start_page', 'isc_dpb_old_start_seqno',
    'isc_dpb_online', 'isc_dpb_online_dump', 'isc_dpb_overwrite',
    'isc_dpb_page_size', 'isc_dpb_password', 'isc_dpb_password_enc',
    'isc_dpb_quit_log', 'isc_dpb_reserved', 'isc_dpb_sec_attach',
    'isc_dpb_set_db_charset', 'isc_dpb_set_db_readonly',
    'isc_dpb_set_db_sql_dialect', 'isc_dpb_set_page_buffers',
    'isc_dpb_shutdown', 'isc_dpb_shutdown_delay', 'isc_dpb_sql_dialect',
    'isc_dpb_sql_role_name', 'isc_dpb_sweep', 'isc_dpb_sweep_interval',
    'isc_dpb_sys_user_name', 'isc_dpb_sys_user_name_enc', 'isc_dpb_trace',
    'isc_dpb_user_name', 'isc_dpb_verify', 'isc_dpb_version1',
    'isc_dpb_working_directory', 'isc_info_active_tran_count', 'isc_info_active_transactions',
    'isc_info_allocation', 'isc_info_attachment_id', 'isc_info_backout_count',
    'isc_info_base_level', 'isc_info_bpage_errors', 'isc_info_creation_date',
    'isc_info_cur_log_part_offset', 'isc_info_cur_logfile_name',
    'isc_info_current_memory', 'isc_info_db_class', 'fb_info_page_contents',
    'isc_info_db_id', 'isc_info_db_provider', 'isc_info_db_read_only',
    'isc_info_db_size_in_pages', 'isc_info_db_sql_dialect',
    'isc_info_delete_count', 'isc_info_dpage_errors', 'isc_info_expunge_count',
    'isc_info_fetches', 'isc_info_firebird_version', 'isc_info_forced_writes',
    'isc_info_implementation', 'isc_info_insert_count', 'isc_info_ipage_errors',
    'isc_info_isc_version', 'isc_info_license', 'isc_info_limbo',
    'isc_info_logfile', 'isc_info_marks', 'isc_info_max_memory',
    'isc_info_next_transaction', 'isc_info_no_reserve', 'isc_info_num_buffers',
    'isc_info_num_wal_buffers', 'isc_info_ods_minor_version',
    'isc_info_ods_version', 'isc_info_oldest_active', 'isc_info_oldest_snapshot',
    'isc_info_oldest_transaction', 'isc_info_page_errors', 'isc_info_page_size',
    'isc_info_ppage_errors', 'isc_info_purge_count', 'isc_info_read_idx_count',
    'isc_info_read_seq_count', 'isc_info_reads', 'isc_info_record_errors',
    'isc_info_set_page_buffers', 'isc_info_sql_stmt_commit',
    'isc_info_sql_stmt_ddl', 'isc_info_sql_stmt_delete',
    'isc_info_sql_stmt_exec_procedure', 'isc_info_sql_stmt_get_segment',
    'isc_info_sql_stmt_insert', 'isc_info_sql_stmt_put_segment',
    'isc_info_sql_stmt_rollback', 'isc_info_sql_stmt_savepoint',
    'isc_info_sql_stmt_select', 'isc_info_sql_stmt_select_for_upd',
    'isc_info_sql_stmt_set_generator', 'isc_info_sql_stmt_start_trans',
    'isc_info_sql_stmt_update', 'isc_info_sweep_interval', 'isc_info_tpage_errors',
    'isc_info_tra_access', 'isc_info_tra_concurrency', 'isc_info_tra_consistency',
    'isc_info_tra_id', 'isc_info_tra_isolation', 'isc_info_tra_lock_timeout',
    'isc_info_tra_no_rec_version', 'isc_info_tra_oldest_active',
    'isc_info_tra_oldest_interesting', 'isc_info_tra_oldest_snapshot',
    'isc_info_tra_read_committed', 'isc_info_tra_readonly', 'fb_info_tra_dbpath',
    'isc_info_tra_readwrite', 'isc_info_tra_rec_version', 'isc_info_update_count',
    'isc_info_user_names', 'isc_info_version', 'isc_info_wal_avg_grpc_size',
    'isc_info_wal_avg_io_size', 'isc_info_wal_buffer_size',
    'isc_info_wal_ckpt_length', 'isc_info_wal_cur_ckpt_interval',
    'isc_info_wal_grpc_wait_usecs', 'isc_info_wal_num_commits',
    'isc_info_wal_num_io', 'isc_info_wal_prv_ckpt_fname',
    'isc_info_wal_prv_ckpt_poffset', 'isc_info_wal_recv_ckpt_fname',
    'isc_info_wal_recv_ckpt_poffset', 'isc_info_window_turns',
    'isc_info_writes', 'isc_tpb_autocommit', 'isc_tpb_commit_time',
    'isc_tpb_concurrency', 'isc_tpb_consistency', 'isc_tpb_exclusive',
    'isc_tpb_ignore_limbo', 'isc_tpb_lock_read', 'isc_tpb_lock_timeout',
    'isc_tpb_lock_write', 'isc_tpb_no_auto_undo', 'isc_tpb_no_rec_version',
    'isc_tpb_nowait', 'isc_tpb_protected', 'isc_tpb_read',
    'isc_tpb_read_committed', 'isc_tpb_rec_version', 'isc_tpb_restart_requests',
    'isc_tpb_shared', 'isc_tpb_verb_time', 'isc_tpb_version3', 'isc_tpb_wait',
    'isc_tpb_write', 'paramstyle', 'threadsafety',
    # New in FDB
    'ISOLATION_LEVEL_READ_COMMITED', 'ISOLATION_LEVEL_READ_COMMITED_LEGACY',
    'ISOLATION_LEVEL_REPEATABLE_READ', 'ISOLATION_LEVEL_SERIALIZABLE',
    'ISOLATION_LEVEL_SNAPSHOT', 'ISOLATION_LEVEL_SNAPSHOT_TABLE_STABILITY',
    'ISOLATION_LEVEL_READ_COMMITED_RO',
    'MAX_BLOB_SEGMENT_SIZE',
    'SQL_ARRAY', 'SQL_BLOB', 'SQL_DOUBLE', 'SQL_D_FLOAT', 'SQL_FLOAT',
    'SQL_INT64', 'SQL_LONG', 'SQL_QUAD', 'SQL_SHORT', 'SQL_TEXT',
    'SQL_TIMESTAMP', 'SQL_TYPE_DATE', 'SQL_TYPE_TIME', 'SQL_VARYING',
    'SUBTYPE_DECIMAL', 'SUBTYPE_NUMERIC', 'SQL_BOOLEAN',
    'charset_map', 'load_api',
    'isc_info_end', 'bs', 'ConnectionWithSchema',  # 'isc_sqlcode',
    'ODS_FB_20', 'ODS_FB_21', 'ODS_FB_25', 'ODS_FB_30')
