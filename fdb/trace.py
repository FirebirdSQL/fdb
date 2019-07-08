#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           trace.py
#   DESCRIPTION:    Python driver for Firebird - Firebird Trace & Audit
#   CREATED:        10.12.2017
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

import fdb
import datetime
import decimal
import collections
from . import utils
try:
    from sys import intern
except ImportError:
    pass

#: Trace event status codes
STATUS_OK = ' '
STATUS_FAILED = 'F'
STATUS_UNAUTHORIZED = 'U'
STATUS_UNKNOWN = '?'

#: Trace event codes, also works as index to EVENTS list
EVENT_TRACE_INIT = 0
EVENT_TRACE_SUSPEND = 1
EVENT_TRACE_END = 2
EVENT_CREATE_DATABASE = 3
EVENT_DROP_DATABASE = 4
EVENT_ATTACH = 5
EVENT_DETACH = 6
EVENT_TRANSACTION_START = 7
EVENT_COMMIT = 8
EVENT_ROLLBACK = 9
EVENT_COMMIT_R = 10
EVENT_ROLLBACK_R = 11
EVENT_STMT_PREPARE = 12
EVENT_STMT_START = 13
EVENT_STMT_END = 14
EVENT_STMT_FREE = 15
EVENT_STMT_CLOSE = 16
EVENT_TRG_START = 17
EVENT_TRG_END = 18
EVENT_PROC_START = 19
EVENT_PROC_END = 20
EVENT_SVC_START = 21
EVENT_SVC_ATTACH = 22
EVENT_SVC_DETACH = 23
EVENT_SVC_QUERY = 24
EVENT_SET_CONTEXT = 25
EVENT_ERROR = 26
EVENT_WARNING = 27
EVENT_SWEEP_START = 28
EVENT_SWEEP_PROGRESS = 29
EVENT_SWEEP_FINISH = 30
EVENT_SWEEP_FAILED = 31
EVENT_BLR_COMPILE = 32
EVENT_BLR_EXECUTE = 33
EVENT_DYN_EXECUTE = 34
EVENT_UNKNOWN = 35

#: List of trace event names in order matching their numeric codes
EVENTS = ['TRACE_INIT', 'TRACE_SUSPENDED', 'TRACE_FINI',
          'CREATE_DATABASE', 'DROP_DATABASE', 'ATTACH_DATABASE', 'DETACH_DATABASE',
          'START_TRANSACTION', 'COMMIT_TRANSACTION', 'ROLLBACK_TRANSACTION', 'COMMIT_RETAINING', 'ROLLBACK_RETAINING',
          'PREPARE_STATEMENT', 'EXECUTE_STATEMENT_START', 'EXECUTE_STATEMENT_FINISH', 'FREE_STATEMENT', 'CLOSE_CURSOR',
          'EXECUTE_TRIGGER_START', 'EXECUTE_TRIGGER_FINISH',
          'EXECUTE_PROCEDURE_START', 'EXECUTE_PROCEDURE_FINISH',
          'START_SERVICE', 'ATTACH_SERVICE', 'DETACH_SERVICE', 'QUERY_SERVICE',
          'SET_CONTEXT', 'ERROR', 'WARNING',
          'SWEEP_START', 'SWEEP_PROGRESS', 'SWEEP_FINISH', 'SWEEP_FAILED',
          'COMPILE_BLR', 'EXECUTE_BLR', 'EXECUTE_DYN',
          'UNKNOWN']

#
# Named tuples for individual trace events
AttachmentInfo = collections.namedtuple('AttachmentInfo', 'attachment_id,database,charset,protocol,address,user,role,remote_process,remote_pid')
TransactionInfo = collections.namedtuple('TransactionInfo', 'attachment_id,transaction_id,options')
ServiceInfo = collections.namedtuple('ServiceInfo', 'service_id,user,protocol,address,remote_process,remote_pid')
SQLInfo = collections.namedtuple('SQLInfo', 'sql_id,sql,plan')
ParamInfo = collections.namedtuple('ParamInfo', 'par_id,params')
#
AccessTuple = collections.namedtuple('AccessTuple', 'table,natural,index,update,insert,delete,backout,purge,expunge')
#
EventTraceInit = collections.namedtuple('EventTraceInit', 'event_id,timestamp,session_name')
EventTraceSuspend = collections.namedtuple('EventTraceSuspend', 'event_id,timestamp,session_name')
EventTraceFinish = collections.namedtuple('EventTraceFinish', 'event_id,timestamp,session_name')
#
EventCreate = collections.namedtuple('EventCreate', 'event_id,timestamp,status,attachment_id,database,charset,protocol,address,user,role,remote_process,remote_pid')
EventDrop = collections.namedtuple('EventDrop', 'event_id,timestamp,status,attachment_id,database,charset,protocol,address,user,role,remote_process,remote_pid')
EventAttach = collections.namedtuple('EventAttach', 'event_id,timestamp,status,attachment_id,database,charset,protocol,address,user,role,remote_process,remote_pid')
EventDetach = collections.namedtuple('EventDetach', 'event_id,timestamp,status,attachment_id,database,charset,protocol,address,user,role,remote_process,remote_pid')
#
EventTransactionStart = collections.namedtuple('EventTransactionStart', 'event_id,timestamp,status,attachment_id,transaction_id,options')
EventCommit = collections.namedtuple('EventCommit', 'event_id,timestamp,status,attachment_id,transaction_id,options,run_time,reads,writes,fetches,marks')
EventRollback = collections.namedtuple('EventRollback', 'event_id,timestamp,status,attachment_id,transaction_id,options,run_time,reads,writes,fetches,marks')
EventCommitRetaining = collections.namedtuple('EventCommitRetaining', 'event_id,timestamp,status,attachment_id,transaction_id,options,run_time,reads,writes,fetches,marks')
EventRollbackRetaining = collections.namedtuple('EventRollbackRetaining', 'event_id,timestamp,status,attachment_id,transaction_id,options,run_time,reads,writes,fetches,marks')
#
EventPrepareStatement = collections.namedtuple('EventPrepareStatement', 'event_id,timestamp,status,attachment_id,transaction_id,statement_id,sql_id,prepare_time')
EventStatementStart = collections.namedtuple('EventStatementStart', 'event_id,timestamp,status,attachment_id,transaction_id,statement_id,sql_id,param_id')
EventStatementFinish = collections.namedtuple('EventStatementFinish', 'event_id,timestamp,status,attachment_id,transaction_id,statement_id,sql_id,param_id,records,run_time,reads,writes,fetches,marks,access')
EventFreeStatement = collections.namedtuple('EventFreeStatement', 'event_id,timestamp,attachment_id,transaction_id,statement_id,sql_id')
EventCloseCursor = collections.namedtuple('EventCloseCursor', 'event_id,timestamp,attachment_id,transaction_id,statement_id,sql_id')
#
EventTriggerStart = collections.namedtuple('EventTriggerStart', 'event_id,timestamp,status,attachment_id,transaction_id,trigger,table,event')
EventTriggerFinish = collections.namedtuple('EventTriggerFinish', 'event_id,timestamp,status,attachment_id,transaction_id,trigger,table,event,run_time,reads,writes,fetches,marks,access')
#
EventProcedureStart = collections.namedtuple('EventProcedureStart', 'event_id,timestamp,status,attachment_id,transaction_id,procedure,param_id')
EventProcedureFinish = collections.namedtuple('EventProcedureFinish', 'event_id,timestamp,status,attachment_id,transaction_id,procedure,param_id,run_time,reads,writes,fetches,marks,access')
#
EventServiceAttach = collections.namedtuple('EventServiceAttach', 'event_id,timestamp,status,service_id')
EventServiceDetach = collections.namedtuple('EventServiceDetach', 'event_id,timestamp,status,service_id')
EventServiceStart = collections.namedtuple('EventServiceStart', 'event_id,timestamp,status,service_id,action,parameters')
EventServiceQuery = collections.namedtuple('EventServiceQuery', 'event_id,timestamp,status,service_id,action,parameters')
#
EventSetContext = collections.namedtuple('EventSetContext', 'event_id,timestamp,attachment_id,transaction_id,context,key,value')
#
EventError = collections.namedtuple('EventError', 'event_id,timestamp,attachment_id,place,details')
EventServiceError = collections.namedtuple('EventServiceError', 'event_id,timestamp,service_id,place,details')
EventWarning = collections.namedtuple('EventWarning', 'event_id,timestamp,attachment_id,place,details')
EventServiceWarning = collections.namedtuple('EventServiceWarning', 'event_id,timestamp,service_id,place,details')
#
EventSweepStart = collections.namedtuple('EventSweepStart', 'event_id,timestamp,attachment_id,oit,oat,ost,next')
EventSweepProgress = collections.namedtuple('EventSweepProgress', 'event_id,timestamp,attachment_id,run_time,reads,writes,fetches,marks,access')
EventSweepFinish = collections.namedtuple('EventSweepFinish', 'event_id,timestamp,attachment_id,oit,oat,ost,next,run_time,reads,writes,fetches,marks')

EventSweepFailed = collections.namedtuple('EventSweepFailed', 'event_id,timestamp,attachment_id')
#
EventBLRCompile = collections.namedtuple('EventBLRCompile', 'event_id,timestamp,status,attachment_id,statement_id,content,prepare_time')
EventBLRExecute = collections.namedtuple('EventBLRExecute', 'event_id,timestamp,status,attachment_id,transaction_id,statement_id,content,run_time,reads,writes,fetches,marks,access')
EventDYNExecute = collections.namedtuple('EventDYNExecute', 'event_id,timestamp,status,attachment_id,transaction_id,content,run_time')
#
EventUnknown = collections.namedtuple('EventUnknown', 'event_id,timestamp,data')

class TraceParser(object):
    """Parser for standard textual trace log. Produces named tuples describing individual trace log entries/events.

    Attributes:
        seen_attachments (set): Set of attachment ids that were already processed.
        seen_transactions (set): Set of transaction ids that were already processed.
        seen_services (set): Set of service ids that were already processed.
        sqlinfo_map (dict): Dictionary that maps (sql_cmd,plan) keys to internal ids
        param_map (dict): Dictionary that maps parameters (statement or procedure) keys to internal ids
        next_event_id (int): Sequence id that would be assigned to next parsed event (starts with 1).
        next_sql_id (int): Sequence id that would be assigned to next parsed unique SQL command (starts with 1).
        next_param_id (int): Sequence id that would be assigned to next parsed unique parameter (starts with 1).
"""
    def __init__(self):
        self.seen_attachments = set()
        self.seen_transactions = set()
        self.seen_services = set()
        self.sqlinfo_map = {}
        self.param_map = {}
        self.next_event_id = 1
        self.next_sql_id = 1
        self.next_param_id = 1
        #
        self.__buffer = []
        self.__current_event = None
        self.__current_block = None
        self.__last_timestamp = None
        self.__event_values = {}
        self.__parse_map = {EVENT_TRACE_INIT: self.__parser_trace_init,
                            EVENT_TRACE_END: self.__parser_trace_finish,
                            EVENT_TRANSACTION_START: self.__parser_start_transaction,
                            EVENT_COMMIT: self.__parser_commit_transaction,
                            EVENT_ROLLBACK: self.__parser_rollback_transaction,
                            EVENT_COMMIT_R: self.__parser_commit_retaining,
                            EVENT_ROLLBACK_R: self.__parser_rollback_retaining,
                            EVENT_STMT_PREPARE: self.__parser_prepare_statement,
                            EVENT_STMT_START: self.__parser_execute_statement_start,
                            EVENT_STMT_END: self.__parser_execute_statement_finish,
                            EVENT_STMT_FREE: self.__parser_free_statement,
                            EVENT_STMT_CLOSE: self.__parser_close_cursor,
                            EVENT_TRG_START: self.__parser_trigger_start,
                            EVENT_TRG_END: self.__parser_trigger_finish,
                            EVENT_PROC_START: self.__parser_procedure_start,
                            EVENT_PROC_END: self.__parser_procedure_finish,
                            EVENT_CREATE_DATABASE: self.__parser_create_db,
                            EVENT_DROP_DATABASE: self.__parser_drop_db,
                            EVENT_ATTACH: self.__parser_attach,
                            EVENT_DETACH: self.__parser_detach,
                            EVENT_SVC_START: self.__parser_service_start,
                            EVENT_SVC_ATTACH: self.__parser_service_attach,
                            EVENT_SVC_DETACH: self.__parser_service_detach,
                            EVENT_SVC_QUERY: self.__parser_service_query,
                            EVENT_SET_CONTEXT: self.__parser_set_context,
                            EVENT_ERROR: self.__parser_error,
                            EVENT_WARNING: self.__parser_warning,
                            EVENT_SWEEP_START: self.__parser_sweep_start,
                            EVENT_SWEEP_PROGRESS: self.__parser_sweep_progress,
                            EVENT_SWEEP_FINISH: self.__parser_sweep_finish,
                            EVENT_SWEEP_FAILED: self.__parser_sweep_failed,
                            EVENT_BLR_COMPILE: self.__parser_blr_compile,
                            EVENT_BLR_EXECUTE: self.__parser_blr_execute,
                            EVENT_DYN_EXECUTE: self.__parser_dyn_execute,
                            EVENT_UNKNOWN: self.__parser_unknown}
    def _is_entry_header(self, line):
        """Returns True if parameter is trace log entry header. This version only checks that first item is a timestamp in valid format.

    :param string line: Line of text to be checked.
"""
        items = line.split()
        try:
            timestamp = datetime.datetime.strptime(items[0], '%Y-%m-%dT%H:%M:%S.%f')
            return True
        except:
            return False
    def _is_session_suspended(self, line):
        """Returns True if parameter is trace log message that trace session was suspended due to full log.

    :param string line: Line of text to be checked.
"""
        return line.rfind('is suspended as its log is full ---') >= 0
    def _is_plan_separator(self, line):
        """Returns True if parameter is statement plan separator.

    :param string line: Line of text to be checked.
"""
        return line == '^'*79
    def _is_perf_start(self, line):
        """Returns True if parameter is first item of statement performance information.

    :param string line: Line of text to be checked.
"""
        return line.endswith(' records fetched')
    def _is_blr_perf_start(self, line):
        """Returns True if parameter is first item of BLR/DYN performance information.

    :param string line: Line of text to be checked.
"""
        parts = line.split()
        return 'ms' in parts or 'fetch(es)' in parts or 'mark(s)' in parts or 'read(s)' in parts or 'write(s)' in parts
    def _is_param_start(self, line):
        """Returns True if parameter is first item in list of parameters.

    :param string line: Line of text to be checked.
"""
        return line.startswith('param0 = ')
    def _iter_trace_blocks(self, ilines):
        lines = []
        for line in ilines:
            line = line.strip()
            if line:
                if not lines:
                    if self._is_entry_header(line):
                        lines.append(line)
                else:
                    if self._is_entry_header(line) or self._is_session_suspended(line):
                        yield lines
                        lines = [line]
                    else:
                        lines.append(line)
        if lines:
            yield lines
    def _parse_header(self, line):
        """Parses trace entry header into 3-item tuple.

    :param string line: Line of text to be parsed.

    :returns: Tuple with items: (timestamp, status, trace_entry_type_id)

    :raises `~fdb.ParseError`: When event is not recognized
"""
        items = line.split()
        timestamp = datetime.datetime.strptime(items[0], '%Y-%m-%dT%H:%M:%S.%f')
        if (len(items) == 3) or (items[2] in ['ERROR', 'WARNING']):
            return (timestamp, STATUS_OK, EVENTS.index(items[2]) if items[2] in EVENTS else EVENT_UNKNOWN)
        else:
            if items[2] == 'UNAUTHORIZED':
                return (timestamp, STATUS_UNAUTHORIZED, EVENTS.index(items[3]))
            elif items[2] == 'FAILED':
                return (timestamp, STATUS_FAILED, EVENTS.index(items[3]))
            elif items[2] == 'Unknown':
                return (timestamp, STATUS_UNKNOWN, EVENT_UNKNOWN)  # ' '.join(items[3:]))
            else:
                raise fdb.ParseError('Unrecognized event header: "%s"' % line)
    def _parse_attachment_info(self, values, check=True):
        line = self.__current_block.popleft()
        database, sep, attachment = line.partition(' (')
        values['database'] = database
        attachment_id, user_role, charset, protocol_address = attachment.strip('()').split(',')
        pad, s = attachment_id.split('_')
        values['attachment_id'] = int(s)
        values['charset'] = charset.strip()
        #
        protocol_address = protocol_address.strip()
        if protocol_address == '<internal>':
            protocol = address = protocol_address
        else:
            protocol, address = protocol_address.split(':', 1)
        values['protocol'] = protocol
        values['address'] = address
        if ':' in user_role:
            a, b = user_role.strip().split(':')
        else:
            a = user_role.strip()
            b = 'NONE'
        values['user'] = a
        values['role'] = b
        if protocol_address == '<internal>':
            values['remote_process'] = None
            values['remote_pid'] = None
        elif len(self.__current_block) > 0 and not (self.__current_block[0].startswith('(TRA') or
                                                    ' ms,' in self.__current_block[0] or
                                                    'Transaction counters:' in self.__current_block[0]):
            remote_process_id = self.__current_block.popleft()
            remote_process, remote_pid = remote_process_id.rsplit(':', 1)
            values['remote_process'] = remote_process
            values['remote_pid'] = int(remote_pid)
        else:
            values['remote_process'] = None
            values['remote_pid'] = None
        #
        if check and values['attachment_id'] not in self.seen_attachments:
            self.__buffer.append(AttachmentInfo(**values))
        self.seen_attachments.add(values['attachment_id'])
    def _parse_transaction_info(self, values, check=True):
        # Transaction parameters
        transaction_id, transaction_options = self.__current_block.popleft().strip('\t ()').split(',')
        pad, s = transaction_id.split('_')
        values['attachment_id'] = values['attachment_id']
        values['transaction_id'] = int(s)
        values['options'] = [intern(x.strip()) for x in transaction_options.split('|')]
        if check and values['transaction_id'] not in self.seen_transactions:
            self.__buffer.append(TransactionInfo(**values))
        self.seen_transactions.add(values['transaction_id'])
    def _parse_transaction_performance(self):
        self.__event_values['run_time'] = None
        self.__event_values['reads'] = None
        self.__event_values['writes'] = None
        self.__event_values['fetches'] = None
        self.__event_values['marks'] = None
        if self.__current_block:
            values = self.__current_block.popleft().split(',')
            while values:
                value, val_type = values.pop().split()
                if 'ms' in val_type:
                    self.__event_values['run_time'] = int(value)
                elif 'read' in val_type:
                    self.__event_values['reads'] = int(value)
                elif 'write' in val_type:
                    self.__event_values['writes'] = int(value)
                elif 'fetch' in val_type:
                    self.__event_values['fetches'] = int(value)
                elif 'mark' in val_type:
                    self.__event_values['marks'] = int(value)
                else:
                    raise fdb.ParseError("Unhandled performance parameter %s" % val_type)
    def _parse_attachment_and_transaction(self):
        # Attachment
        att_values = {}
        self._parse_attachment_info(att_values)
        # Transaction
        tr_values = {}
        tr_values['attachment_id'] = att_values['attachment_id']
        self._parse_transaction_info(tr_values)
        self.__event_values['attachment_id'] = tr_values['attachment_id']
        self.__event_values['transaction_id'] = tr_values['transaction_id']
    def _parse_statement_id(self):
        self.__event_values['plan'] = None
        self.__event_values['sql'] = None
        pad, s = self.__current_block.popleft().split()
        self.__event_values['statement_id'] = int(s[:-1])
        if self.__current_block.popleft() != '-'*79:
            raise fdb.ParseError("Separator '-'*79 line expected")
    def _parse_blr_statement_id(self):
        line = self.__current_block[0].strip()
        if line.startswith('Statement ') and line[-1] == ':':
            pad, s = self.__current_block.popleft().split()
            self.__event_values['statement_id'] = int(s[:-1])
        else:
            self.__event_values['statement_id'] = None
    def _parse_blrdyn_content(self):
        if self.__current_block[0] == '-'*79:
            self.__current_block.popleft()
            content = []
            line = self.__current_block.popleft()
            while line and not self._is_blr_perf_start(line):
                content.append(line)
                if self.__current_block:
                    line = self.__current_block.popleft()
                else:
                    line = None
            if line:
                self.__current_block.appendleft(line)
            self.__event_values['content'] = '\n'.join(content)
        else:
            self.__event_values['content'] = None
    def _parse_prepare_time(self):
        if self.__current_block and self.__current_block[-1].endswith(' ms'):
            run_time = self.__current_block.pop()
            time, measure = run_time.split()
            self.__event_values['prepare_time'] = int(time)
        else:
            self.__event_values['prepare_time'] = None
    def _parse_sql_statement(self):
        if not self.__current_block:
            return
        line = self.__current_block.popleft()
        sql = []
        while line and not (self._is_plan_separator(line) or self._is_perf_start(line) or self._is_param_start(line)):
            sql.append(line)
            if self.__current_block:
                line = self.__current_block.popleft()
            else:
                line = None
        if line:
            self.__current_block.appendleft(line)
        self.__event_values['sql'] = '\n'.join(sql)
    def _parse_plan(self):
        if not self.__current_block:
            return
        line = self.__current_block.popleft()
        if self._is_perf_start(line):
            self.__current_block.appendleft(line)
            return
        if self._is_param_start(line):
            self.__current_block.appendleft(line)
            return
        if not self._is_plan_separator(line):
            raise fdb.ParseError("Separator '^'*79 line expected")
        line = self.__current_block.popleft()
        plan = []
        while line and not (self._is_perf_start(line) or self._is_param_start(line)):
            plan.append(line)
            if self.__current_block:
                line = self.__current_block.popleft()
            else:
                line = None
        if line:
            self.__current_block.appendleft(line)
        self.__event_values['plan'] = '\n'.join(plan)
    def _parse_parameters(self, for_procedure=False):
        parameters = []
        while self.__current_block and self.__current_block[0].startswith('param'):
            line = self.__current_block.popleft()
            param_id, param_def = line.split(' = ')
            param_type, param_value = param_def.rsplit(',', 1)
            param_value = param_value.strip(' "')
            if param_value == '<NULL>':
                param_value = None
            elif param_type in ['smallint', 'integer', 'bigint']:
                param_value = int(param_value)
            elif param_type == 'timestamp':
                param_value = datetime.datetime.strptime(param_value, '%Y-%m-%dT%H:%M:%S.%f')
            elif param_type == 'date':
                param_value = datetime.datetime.strptime(param_value, '%Y-%m-%d')
            elif param_type == 'time':
                param_value = datetime.datetime.strptime(param_value, '%H:%M:%S.%f')
            elif param_type in ['float', 'double precision']:
                param_value = decimal.Decimal(param_value)
            parameters.append((param_type, param_value,))
        while self.__current_block and self.__current_block[0].endswith('more arguments skipped...'):
            self.__current_block.popleft()
        #
        param_id = None
        if len(parameters) > 0:
            key = tuple(parameters)
            if key in self.param_map:
                param_id = self.param_map[key]
            else:
                param_id = self.next_param_id
                self.next_param_id += 1
                self.param_map[key] = param_id
                self.__buffer.append(ParamInfo(**{'par_id': param_id, 'params': parameters}))
        #
        self.__event_values['param_id'] = param_id
    def _parse_performance(self):
        self.__event_values['run_time'] = None
        self.__event_values['reads'] = None
        self.__event_values['writes'] = None
        self.__event_values['fetches'] = None
        self.__event_values['marks'] = None
        self.__event_values['access'] = None
        if not self.__current_block:
            return
        if 'records fetched' in self.__current_block[0]:
            line = self.__current_block.popleft()
            self.__event_values['records'] = int(line.split()[0])
        values = self.__current_block.popleft().split(',')
        while values:
            value, val_type = values.pop().split()
            if 'ms' in val_type:
                self.__event_values['run_time'] = int(value)
            elif 'read' in val_type:
                self.__event_values['reads'] = int(value)
            elif 'write' in val_type:
                self.__event_values['writes'] = int(value)
            elif 'fetch' in val_type:
                self.__event_values['fetches'] = int(value)
            elif 'mark' in val_type:
                self.__event_values['marks'] = int(value)
            else:
                raise fdb.ParseError("Unhandled performance parameter %s" % val_type)
        if self.__current_block:
            self.__event_values['access'] = []
            if self.__current_block.popleft() != "Table                             Natural     Index    Update    Insert    Delete   Backout     Purge   Expunge":
                raise fdb.ParseError("Performance table header expected")
            if self.__current_block.popleft() != "*"*111:
                raise fdb.ParseError("Performance table header separator expected")
            while self.__current_block:
                entry = self.__current_block.popleft()
                self.__event_values['access'].append(AccessTuple._make((intern(entry[:32].strip()),
                                                                        utils.safe_int(entry[32:41].strip()),
                                                                        utils.safe_int(entry[41:51].strip()),
                                                                        utils.safe_int(entry[51:61].strip()),
                                                                        utils.safe_int(entry[61:71].strip()),
                                                                        utils.safe_int(entry[71:81].strip()),
                                                                        utils.safe_int(entry[81:91].strip()),
                                                                        utils.safe_int(entry[91:101].strip()),
                                                                        utils.safe_int(entry[101:111].strip()))))
    def _parse_sql_info(self):
        plan = self.__event_values['plan']
        sql = self.__event_values['sql']
        key = (sql, plan)
        #
        if key in self.sqlinfo_map:
            sql_id = self.sqlinfo_map[key]
        else:
            sql_id = self.next_sql_id
            self.next_sql_id += 1
            self.sqlinfo_map[key] = sql_id
            self.__buffer.append(SQLInfo(**{'sql_id': sql_id, 'sql': sql, 'plan': plan,}))
        #
        del self.__event_values['plan']
        del self.__event_values['sql']
        self.__event_values['sql_id'] = sql_id
    def _parse_trigger(self):
        trigger, event = self.__current_block.popleft().split('(')
        if ' FOR ' in trigger:
            a, b = trigger.split(' FOR ')
            self.__event_values['trigger'] = a
            self.__event_values['table'] = b.strip()
        else:
            self.__event_values['trigger'] = trigger.strip()
            self.__event_values['table'] = None
        self.__event_values['event'] = event.strip('()')
    def _parse_service(self):
        line = self.__current_block.popleft()
        if 'service_mgr' not in line:
            raise fdb.ParseError("Service connection description expected.")
        pad, sep, s = line.partition(' (')
        svc_id, user, protocol_address, remote_process_id = s.strip('()').split(',')
        pad, svc_id = svc_id.split(' ')
        svc_id = int(svc_id if svc_id.startswith('0x') else '0x%s' % svc_id, 0)
        if svc_id not in self.seen_services:
            svc_values = {}
            svc_values['service_id'] = svc_id
            svc_values['user'] = user.strip()
            protocol_address = protocol_address.strip()
            if protocol_address == '<internal>':
                protocol = address = protocol_address
            else:
                protocol, address = protocol_address.split(':')
            svc_values['protocol'] = protocol
            svc_values['address'] = address
            remote_process_id = remote_process_id.strip()
            remote_process, remote_pid = remote_process_id.rsplit(':', 1)
            svc_values['remote_process'] = remote_process
            svc_values['remote_pid'] = int(remote_pid)
            self.__buffer.append(ServiceInfo(**svc_values))
            self.seen_services.add(svc_id)
        self.__event_values['service_id'] = svc_id
    def _parse_sweep_attachment(self):
        att_values = {}
        self._parse_attachment_info(att_values)
        self.__event_values['attachment_id'] = att_values['attachment_id']
        #values = {'remote_process': None, 'remote_pid': None,}
        #line = self.__current_block.popleft()
        #database, sep, attachment = line.partition(' (')
        #values['database'] = database
        #attachment_id, user_role, charset, protocol_address = attachment.strip('()').split(',')
        #pad, s = attachment_id.split('_')
        #self.__event_values['attachment_id'] = values['attachment_id'] = int(s)
        #values['charset'] = charset.strip()
        ##
        #protocol_address = protocol_address.strip()
        #if protocol_address == '<internal>':
            #protocol = address = protocol_address
        #else:
            #protocol, address = protocol_address.split(':')
        #values['protocol'] = protocol
        #values['address'] = address
        #if ':' in user_role:
            #a, b = user_role.strip().split(':')
        #else:
            #a = user_role.strip()
            #b = 'NONE'
        #values['user'] = a
        #values['role'] = b
        #if values['attachment_id'] not in self.seen_attachments:
            #self.__writer.write(AttachmentInfo(**values))
        #self.seen_attachments.add(values['attachment_id'])
    def _parse_sweep_tr_counters(self):
        line = self.__current_block.popleft()
        if not line:
            line = self.__current_block.popleft()
        if 'Transaction counters:' not in line:
            raise fdb.ParseError("Transaction counters expected")
        while len(self.__current_block) > 0:
            line = self.__current_block.popleft()
            if 'Oldest interesting' in line:
                self.__event_values['oit'] = int(line.rsplit(' ', 1)[1])
            elif 'Oldest active' in line:
                self.__event_values['oat'] = int(line.rsplit(' ', 1)[1])
            elif 'Oldest snapshot' in line:
                self.__event_values['ost'] = int(line.rsplit(' ', 1)[1])
            elif 'Next transaction' in line:
                self.__event_values['next'] = int(line.rsplit(' ', 1)[1])
            elif 'ms' in line and len(self.__current_block) == 0:
                # Put back performance counters
                self.__current_block.appendleft(line)
                break
    def __parse_trace_header(self):
        self.__last_timestamp, status, self.__current_event = self._parse_header(self.__current_block.popleft())
        self.__event_values['event_id'] = self.next_event_id
        self.next_event_id += 1
        self.__event_values['status'] = status
        self.__event_values['timestamp'] = self.__last_timestamp
    def __parser_trace_suspend(self):
        # Session was suspended because log was full, so we will create fake event to note that
        line = self.__current_block.popleft()
        self.__event_values['timestamp'] = self.__last_timestamp
        self.__event_values['event_id'] = self.next_event_id
        session_name = line[4:line.find(' is suspended')]
        self.__event_values['session_name'] = session_name.replace(' ', '_').upper()
        self.next_event_id += 1
        return EventTraceSuspend(**self.__event_values)
    def __parser_trace_init(self):
        self.__parse_trace_header()
        del self.__event_values['status']
        self.__event_values['session_name'] = self.__current_block.popleft()
        return EventTraceInit(**self.__event_values)
    def __parser_trace_finish(self):
        self.__parse_trace_header()
        del self.__event_values['status']
        self.__event_values['session_name'] = self.__current_block.popleft()
        return EventTraceFinish(**self.__event_values)
    def __parser_start_transaction(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        return EventTransactionStart(**self.__event_values)
    def __parser_commit_transaction(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        return EventCommit(**self.__event_values)
    def __parser_rollback_transaction(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        return EventRollback(**self.__event_values)
    def __parser_commit_retaining(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        return EventCommitRetaining(**self.__event_values)
    def __parser_rollback_retaining(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # Transaction parameters
        self._parse_transaction_info(self.__event_values, check=False)
        self._parse_transaction_performance()
        return EventRollbackRetaining(**self.__event_values)
    def __parser_prepare_statement(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_prepare_time()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        return EventPrepareStatement(**self.__event_values)
    def __parser_execute_statement_start(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_parameters()
        self._parse_sql_info()
        return EventStatementStart(**self.__event_values)
    def __parser_execute_statement_finish(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_parameters()
        self.__event_values['records'] = None
        self._parse_performance()
        self._parse_sql_info()
        return EventStatementFinish(**self.__event_values)
    def __parser_free_statement(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        del self.__event_values['status']
        return EventFreeStatement(**self.__event_values)
    def __parser_close_cursor(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_statement_id()
        self._parse_sql_statement()
        self._parse_plan()
        self._parse_sql_info()
        del self.__event_values['status']
        return EventCloseCursor(**self.__event_values)
    def __parser_trigger_start(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_trigger()
        return EventTriggerStart(**self.__event_values)
    def __parser_trigger_finish(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        self._parse_trigger()
        self._parse_performance()
        return EventTriggerFinish(**self.__event_values)
    def __parser_procedure_start(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        pad, s = self.__current_block.popleft().split()
        self.__event_values['procedure'] = s[:-1]
        self._parse_parameters(for_procedure=True)
        return EventProcedureStart(**self.__event_values)
    def __parser_procedure_finish(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        pad, s = self.__current_block.popleft().split()
        self.__event_values['procedure'] = s[:-1]
        self._parse_parameters(for_procedure=True)
        self._parse_performance()
        return EventProcedureFinish(**self.__event_values)
    def __parser_create_db(self):
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventCreate(**self.__event_values)
    def __parser_drop_db(self):
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventDrop(**self.__event_values)
    def __parser_attach(self):
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        #self.__event_values['unauthorized'] = False
        return EventAttach(**self.__event_values)
    def __parser_detach(self):
        self.__parse_trace_header()
        # Attachment parameters
        self._parse_attachment_info(self.__event_values, check=False)
        return EventDetach(**self.__event_values)
    def __parser_service_start(self):
        self.__parse_trace_header()
        self._parse_service()
        # service parameters
        action = self.__current_block.popleft().strip('"')
        self.__event_values['action'] = action
        parameters = []
        while len(self.__current_block) > 0:
            parameters.append(self.__current_block.popleft())
        self.__event_values['parameters'] = parameters
        #
        return EventServiceStart(**self.__event_values)
    def __parser_service_attach(self):
        self.__parse_trace_header()
        self._parse_service()
        return EventServiceAttach(**self.__event_values)
    def __parser_service_detach(self):
        self.__parse_trace_header()
        self._parse_service()
        return EventServiceDetach(**self.__event_values)
    def __parser_service_query(self):
        self.__parse_trace_header()
        self._parse_service()
        # service parameters
        line = self.__current_block.popleft().strip()
        if line[0] == '"' and line[-1] == '"':
            action = line.strip('"')
            self.__event_values['action'] = action
        else:
            self.__event_values['action'] = None
        parameters = []
        while len(self.__current_block) > 0:
            parameters.append(self.__current_block.popleft())
        self.__event_values['parameters'] = parameters
        #
        return EventServiceQuery(**self.__event_values)
    def __parser_set_context(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        line = self.__current_block.popleft()
        context, line = line.split(']', 1)
        key, value = line.split('=', 1)
        self.__event_values['context'] = context[1:]
        self.__event_values['key'] = key.strip()
        self.__event_values['value'] = value.strip()
        del self.__event_values['status']
        return EventSetContext(**self.__event_values)
    def __parser_error(self):
        self.__event_values['place'] = self.__current_block[0].split(' AT ')[1]
        self.__parse_trace_header()
        att_values = {}
        if 'service_mgr' in self.__current_block[0]:
            event_class = EventServiceError
            self._parse_service()
        else:
            event_class = EventError
            self._parse_attachment_info(att_values)
            self.__event_values['attachment_id'] = att_values['attachment_id']
        details = []
        while len(self.__current_block) > 0:
            details.append(self.__current_block.popleft())
        self.__event_values['details'] = details
        del self.__event_values['status']
        return event_class(**self.__event_values)
    def __parser_warning(self):
        self.__event_values['place'] = self.__current_block[0].split(' AT ')[1]
        self.__parse_trace_header()
        att_values = {}
        if 'service_mgr' in self.__current_block[0]:
            event_class = EventServiceWarning
            self._parse_service()
        else:
            event_class = EventWarning
            self._parse_attachment_info(att_values)
            self.__event_values['attachment_id'] = att_values['attachment_id']
        details = []
        while len(self.__current_block) > 0:
            details.append(self.__current_block.popleft())
        self.__event_values['details'] = details
        del self.__event_values['status']
        return event_class(**self.__event_values)
    def __parser_sweep_start(self):
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_sweep_tr_counters()
        del self.__event_values['status']
        return EventSweepStart(**self.__event_values)
    def __parser_sweep_progress(self):
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_performance()
        del self.__event_values['status']
        return EventSweepProgress(**self.__event_values)
    def __parser_sweep_finish(self):
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        self._parse_sweep_tr_counters()
        self._parse_performance()
        del self.__event_values['status']
        del self.__event_values['access']
        return EventSweepFinish(**self.__event_values)
    def __parser_sweep_failed(self):
        self.__parse_trace_header()
        self._parse_sweep_attachment()
        del self.__event_values['status']
        return EventSweepFailed(**self.__event_values)
    def __parser_blr_compile(self):
        self.__parse_trace_header()
        # Attachment
        values = {}
        self._parse_attachment_info(values)
        self.__event_values['attachment_id'] = values['attachment_id']
        # BLR
        self._parse_blr_statement_id()
        self._parse_blrdyn_content()
        self._parse_prepare_time()
        return EventBLRCompile(**self.__event_values)
    def __parser_blr_execute(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        # BLR
        self._parse_blr_statement_id()
        self._parse_blrdyn_content()
        self._parse_performance()
        return EventBLRExecute(**self.__event_values)
    def __parser_dyn_execute(self):
        self.__parse_trace_header()
        self._parse_attachment_and_transaction()
        # DYN
        self._parse_blrdyn_content()
        value, ms = self.__current_block.popleft().split()
        self.__event_values['run_time'] = int(value)
        return EventDYNExecute(**self.__event_values)
    def __parser_unknown(self):
        items = self.__current_block[0].split()
        self.__parse_trace_header()
        self.__current_block.appendleft(' '.join(items[2:]))
        del self.__event_values['status']
        self.__event_values['data'] = '\n'.join(self.__current_block)
        return EventUnknown(**self.__event_values)
    def _parse_block(self, parser):
        self.__event_values.clear()
        result = parser()
        return result
    def parse_event(self, trace_block):
        """Parse single trace event.

        Args:
            trace_block (list): List with trace entry lines for single trace event.

        Returns:
            Named tuple with parsed event.
"""
        self.__current_block = collections.deque(trace_block)
        if self._is_session_suspended(self.__current_block[0]):
            record_parser = self.__parser_trace_suspend
        else:
            timestamp, status, trace_event = self._parse_header(self.__current_block[0])
            record_parser = self.__parse_map[trace_event]
        #
        return self._parse_block(record_parser)
    def parse(self, lines):
        """Parse output from Firebird trace session.

        Args:
            lines (iterable): Iterable that return lines produced by firebird trace session.

        Yields:
            Named tuples describing individual trace log entries/events.

        Raises:
            fdb.ParseError: When any problem is found in input stream.
"""
        for rec in (self.parse_event(x) for x in self._iter_trace_blocks(lines)):
            while len(self.__buffer) > 0:
                yield self.__buffer.pop(0)
            yield rec
