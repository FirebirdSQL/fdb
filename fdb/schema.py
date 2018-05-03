#coding:utf-8
#
#   PROGRAM:     fdb
#   MODULE:      schema.py
#   DESCRIPTION: Python driver for Firebird - Database schema
#   CREATED:     10.5.2013
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
from fdb.utils import LateBindingProperty, ObjectList, Visitable
import string
import weakref
from itertools import groupby
import collections

# Firebird field type codes
FBT_SMALLINT = 7
FBT_INTEGER = 8
FBT_QUAD = 9
FBT_FLOAT = 10
FBT_CHAR = 14
FBT_DOUBLE_PRECISION = 27
FBT_DATE = 35
FBT_VARCHAR = 37
FBT_CSTRING = 40
FBT_BLOB_ID = 45
FBT_BLOB = 261
FBT_SQL_DATE = 12
FBT_SQL_TIME = 13
FBT_SQL_TIMESTAMP = 35
FBT_BIGINT = 16
FBT_BOOLEAN = 23

MAX_INTSUBTYPES = 2
MAX_BLOBSUBTYPES = 8
# Trigger masks
TRIGGER_TYPE_SHIFT = 13
TRIGGER_TYPE_MASK = (0x3 << TRIGGER_TYPE_SHIFT)
TRIGGER_TYPE_DML = (0 << TRIGGER_TYPE_SHIFT)
TRIGGER_TYPE_DB	= (1 << TRIGGER_TYPE_SHIFT)
TRIGGER_TYPE_DDL = (2 << TRIGGER_TYPE_SHIFT)

# Trigger type codes
DDL_TRIGGER_ANY = 4611686018427375615 # 9223372036854751229
DDL_TRIGGER_CREATE_TABLE = 1
DDL_TRIGGER_ALTER_TABLE = 2
DDL_TRIGGER_DROP_TABLE = 3
DDL_TRIGGER_CREATE_PROCEDURE = 4
DDL_TRIGGER_ALTER_PROCEDURE = 5
DDL_TRIGGER_DROP_PROCEDURE = 6
DDL_TRIGGER_CREATE_FUNCTION = 7
DDL_TRIGGER_ALTER_FUNCTION = 8
DDL_TRIGGER_DROP_FUNCTION = 9
DDL_TRIGGER_CREATE_TRIGGER = 10
DDL_TRIGGER_ALTER_TRIGGER = 11
DDL_TRIGGER_DROP_TRIGGER = 12
# gap for TRIGGER_TYPE_MASK - 3 bits
DDL_TRIGGER_CREATE_EXCEPTION = 16
DDL_TRIGGER_ALTER_EXCEPTION = 17
DDL_TRIGGER_DROP_EXCEPTION = 18
DDL_TRIGGER_CREATE_VIEW = 19
DDL_TRIGGER_ALTER_VIEW = 20
DDL_TRIGGER_DROP_VIEW = 21
DDL_TRIGGER_CREATE_DOMAIN = 22
DDL_TRIGGER_ALTER_DOMAIN = 23
DDL_TRIGGER_DROP_DOMAIN = 24
DDL_TRIGGER_CREATE_ROLE = 25
DDL_TRIGGER_ALTER_ROLE = 26
DDL_TRIGGER_DROP_ROLE = 27
DDL_TRIGGER_CREATE_INDEX = 28
DDL_TRIGGER_ALTER_INDEX = 29
DDL_TRIGGER_DROP_INDEX = 30
DDL_TRIGGER_CREATE_SEQUENCE = 31
DDL_TRIGGER_ALTER_SEQUENCE = 32
DDL_TRIGGER_DROP_SEQUENCE = 33
DDL_TRIGGER_CREATE_USER = 34
DDL_TRIGGER_ALTER_USER = 35
DDL_TRIGGER_DROP_USER = 36
DDL_TRIGGER_CREATE_COLLATION = 37
DDL_TRIGGER_DROP_COLLATION = 38
DDL_TRIGGER_ALTER_CHARACTER_SET = 39
DDL_TRIGGER_CREATE_PACKAGE = 40
DDL_TRIGGER_ALTER_PACKAGE = 41
DDL_TRIGGER_DROP_PACKAGE = 42
DDL_TRIGGER_CREATE_PACKAGE_BODY = 43
DDL_TRIGGER_DROP_PACKAGE_BODY = 44
DDL_TRIGGER_CREATE_MAPPING = 45
DDL_TRIGGER_ALTER_MAPPING = 46
DDL_TRIGGER_DROP_MAPPING = 47

# Lists and disctionary maps
COLUMN_TYPES = {None: 'UNKNOWN', FBT_SMALLINT: 'SMALLINT', FBT_INTEGER: 'INTEGER',
                FBT_QUAD: 'QUAD', FBT_FLOAT: 'FLOAT', FBT_CHAR: 'CHAR',
                FBT_DOUBLE_PRECISION: 'DOUBLE PRECISION', FBT_VARCHAR: 'VARCHAR',
                FBT_CSTRING: 'CSTRING', FBT_BLOB_ID: 'BLOB_ID', FBT_BLOB: 'BLOB',
                FBT_SQL_TIME: 'TIME', FBT_SQL_DATE: 'DATE', FBT_SQL_TIMESTAMP: 'TIMESTAMP',
                FBT_BIGINT: 'BIGINT', FBT_BOOLEAN: 'BOOLEAN'}
INTEGRAL_SUBTYPES = ('UNKNOWN', 'NUMERIC', 'DECIMAL')
BLOB_SUBTYPES = ('BINARY', 'TEXT', 'BLR', 'ACL', 'RANGES', 'SUMMARY',
                 'FORMAT', 'TRANSACTION_DESCRIPTION', 'EXTERNAL_FILE_DESCRIPTION',
                 'DEBUG_INFORMATION')

TRIGGER_PREFIX_TYPES = ['BEFORE', 'AFTER']
TRIGGER_SUFFIX_TYPES = ['', 'INSERT', 'UPDATE', 'DELETE']
TRIGGER_DB_TYPES = ['CONNECT', 'DISCONNECT', 'TRANSACTION START',
                    'TRANSACTION COMMIT', 'TRANSACTION ROLLBACK']
TRIGGER_DDL_TYPES = [None, "CREATE TABLE", "ALTER TABLE", "DROP TABLE",
                     "CREATE PROCEDURE", "ALTER PROCEDURE", "DROP PROCEDURE",
                     "CREATE FUNCTION", "ALTER FUNCTION", "DROP FUNCTION",
                     "CREATE TRIGGER", "ALTER TRIGGER", "DROP TRIGGER",
                     None, None, None,	# gap for TRIGGER_TYPE_MASK - 3 bits
                     "CREATE EXCEPTION", "ALTER EXCEPTION", "DROP EXCEPTION",
                     "CREATE VIEW", "ALTER VIEW", "DROP VIEW",
                     "CREATE DOMAIN", "ALTER DOMAIN", "DROP DOMAIN",
                     "CREATE ROLE", "ALTER ROLE", "DROP ROLE",
                     "CREATE INDEX", "ALTER INDEX", "DROP INDEX",
                     "CREATE SEQUENCE", "ALTER SEQUENCE", "DROP SEQUENCE",
                     "CREATE USER", "ALTER USER", "DROP USER",
                     "CREATE COLLATION", "DROP COLLATION", "ALTER CHARACTER SET",
                     "CREATE PACKAGE", "ALTER PACKAGE", "DROP PACKAGE",
                     "CREATE PACKAGE BODY", "DROP PACKAGE BODY",
                     "CREATE MAPPING", "ALTER MAPPING", "DROP MAPPING"]


# Collation parameters codes
COLLATION_PAD_SPACE = 1
COLLATION_CASE_INSENSITIVE = 2
COLLATION_ACCENT_INSENSITIVE = 4

# Index type names
INDEX_TYPE_ASCENDING = 'ASCENDING'
INDEX_TYPE_DESCENDING = 'DESCENDING'
INDEX_TYPES = [INDEX_TYPE_ASCENDING, INDEX_TYPE_DESCENDING]

# Relation type codes
RELATION_TYPE_TABLE = 0
RELATION_TYPE_VIEW = 1
RELATION_TYPE_GTT = 5
RELATION_TYPE_GTT_PRESERVE = 4
RELATION_TYPE_GTT_DELETE = 5

# Procedure parameter type codes
PROCPAR_DATATYPE = 0
PROCPAR_DOMAIN = 1
PROCPAR_TYPE_OF_DOMAIN = 2
PROCPAR_TYPE_OF_COLUMN = 3

# Section codes for Schema.get_metadata_ddl()
SCRIPT_COLLATIONS = 1
SCRIPT_CHARACTER_SETS = 2
SCRIPT_UDFS = 3
SCRIPT_GENERATORS = 4
SCRIPT_EXCEPTIONS = 5
SCRIPT_DOMAINS = 6
SCRIPT_PACKAGE_DEFS = 7
SCRIPT_FUNCTION_DEFS = 8
SCRIPT_PROCEDURE_DEFS = 9
SCRIPT_TABLES = 10
SCRIPT_PRIMARY_KEYS = 11
SCRIPT_UNIQUE_CONSTRAINTS = 12
SCRIPT_CHECK_CONSTRAINTS = 13
SCRIPT_FOREIGN_CONSTRAINTS = 14
SCRIPT_INDICES = 15
SCRIPT_VIEWS = 16
SCRIPT_PACKAGE_BODIES = 17
SCRIPT_PROCEDURE_BODIES = 18
SCRIPT_FUNCTION_BODIES = 19
SCRIPT_TRIGGERS = 20
SCRIPT_ROLES = 21
SCRIPT_GRANTS = 22
SCRIPT_COMMENTS = 23
SCRIPT_SHADOWS = 24
SCRIPT_SET_GENERATORS = 25
SCRIPT_INDEX_DEACTIVATIONS = 26
SCRIPT_INDEX_ACTIVATIONS = 27
SCRIPT_TRIGGER_DEACTIVATIONS = 28
SCRIPT_TRIGGER_ACTIVATIONS = 29

# Schema information collection codes
SCHEMA_TABLES = 1
SCHEMA_VIEWS = 2
SCHEMA_DOMAINS = 3
SCHEMA_INDICES = 4
SCHEMA_DEPENDENCIES = 5
SCHEMA_GENERATORS = 6
SCHEMA_SEQUENCES = 6
SCHEMA_TRIGGERS = 7
SCHEMA_PROCEDURES = 8
SCHEMA_CONSTRAINTS = 9
SCHEMA_COLLATIONS = 10
SCHEMA_CHARACTER_SETS = 11
SCHEMA_EXCEPTIONS = 12
SCHEMA_ROLES = 13
SCHEMA_FUNCTIONS = 14
SCHEMA_FILES = 15
SCHEMA_SHADOWS = 16
SCHEMA_PRIVILEGES = 17
SCHEMA_USERS = 18
SCHEMA_PACKAGES = 19
SCHEMA_BACKUP_HISTORY = 20
SCHEMA_FILTERS = 21

# List of default sections (in order) for Schema.get_metadata_ddl()
SCRIPT_DEFAULT_ORDER = [SCRIPT_COLLATIONS, SCRIPT_CHARACTER_SETS,
                        SCRIPT_UDFS, SCRIPT_GENERATORS,
                        SCRIPT_EXCEPTIONS, SCRIPT_DOMAINS,
                        SCRIPT_PACKAGE_DEFS,
                        SCRIPT_FUNCTION_DEFS, SCRIPT_PROCEDURE_DEFS,
                        SCRIPT_TABLES, SCRIPT_PRIMARY_KEYS,
                        SCRIPT_UNIQUE_CONSTRAINTS,
                        SCRIPT_CHECK_CONSTRAINTS,
                        SCRIPT_FOREIGN_CONSTRAINTS, SCRIPT_INDICES,
                        SCRIPT_VIEWS, SCRIPT_PACKAGE_BODIES,
                        SCRIPT_PROCEDURE_BODIES,
                        SCRIPT_FUNCTION_BODIES, SCRIPT_TRIGGERS,
                        SCRIPT_GRANTS, SCRIPT_ROLES, SCRIPT_COMMENTS,
                        SCRIPT_SHADOWS, SCRIPT_SET_GENERATORS]

# List of reserved Firebird words
RESERVED = ['ACTIVE', 'ADD', 'ADMIN', 'AFTER', 'ALL', 'ALTER', 'AND',
            'ANY', 'ARE', 'AS', 'ASC', 'ASCENDING', 'AT', 'AUTO', 'AUTODDL', 'AVG',
            'BASED', 'BASE_NAME', 'BEFORE', 'BEGIN', 'BETWEEN', 'BIGINT', 'BIT_LENGTH',
            'BLOB', 'BLOBEDIT', 'BOTH', 'BUFFER', 'BY', 'BOOLEAN',
            'CASE', 'CAST', 'CHAR', 'CHARACTER', 'CHAR_LENGTH', 'CHARACTER_LENGTH',
            'CHECK', 'CHECK_POINT_LENGTH', 'CLOSE', 'COALESCE', 'COLLATE', 'COLLATION',
            'COLUMN', 'COMMIT', 'COMMITTED', 'COMPILETIME', 'COMPUTED', 'CONDITIONAL',
            'CONNECT', 'CONSTRAINT', 'CONTAINING', 'CONTINUE', 'COUNT', 'CREATE', 'CROSS',
            'CSTRING', 'CURRENT', 'CURRENT_CONNECTION', 'CURRENT_DATE', 'CURRENT_ROLE',
            'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_TRANSACTION', 'CURRENT_USER',
            'CORR', 'COVAR_POP', 'COVAR_SAMP',
            'DATABASE', 'DATE', 'DAY', 'DB_KEY', 'DEBUG', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT',
            'DELETE', 'DELETING', 'DESC', 'DESCENDING', 'DESCRIBE', 'DISCONNECT',
            'DISPLAY', 'DISTINCT', 'DO', 'DOMAIN', 'DOUBLE', 'DROP', 'DETERMINISTIC',
            'ECHO', 'EDIT', 'ELSE', 'END', 'ENTRY_POINT', 'ESCAPE', 'EVENT', 'EXCEPTION', 'EXECUTE',
            'EXISTS', 'EXIT', 'EXTERN', 'EXTERNAL', 'EXTRACT',
            'FETCH', 'FILE', 'FILTER', 'FLOAT', 'FOR', 'FOREIGN', 'FOUND', 'FROM', 'FULL',
            'FUNCTION', 'FALSE',
            'GDSCODE', 'GENERATOR', 'GEN_ID', 'GOTO', 'GRANT', 'GROUP', 'GROUP_COMMIT_WAIT_TIME',
            'GLOBAL',
            'HAVING', 'HEADING', 'HELP', 'HOUR',
            'IF', 'IMMEDIATE', 'IN', 'INACTIVE', 'INDEX', 'INDICATOR', 'INIT', 'INNER', 'INPUT',
            'INPUT_TYPE', 'INSERT', 'INSERTING', 'INT', 'INTEGER', 'INTO', 'IS', 'ISOLATION',
            'INSENSITIVE',
            'JOIN',
            'KEY',
            'LAST', 'LC_MESSAGES', 'LC_TYPE', 'LEADING', 'LEAVE', 'LEFT', 'LENGTH',
            'LEVEL', 'LIKE', 'LOCK', 'LOG_BUFFER_SIZE', 'LONG', 'LOWER',
            'MANUAL', 'MAX', 'MAXIMUM', 'MAXIMUM_SEGMENT', 'MAX_SEGMENT', 'MERGE', 'MESSAGE',
            'MIN', 'MINIMUM', 'MINUTE', 'MODULE_NAME', 'MONTH',
            'NAMES', 'NATIONAL', 'NATURAL', 'NCHAR', 'NO', 'NOAUTO', 'NOT', 'NULL', 'NULLIF',
            'NULLS', 'NUM_LOG_BUFFERS', 'NUMERIC',
            'OCTET_LENGTH', 'OF', 'ON', 'ONLY', 'OPEN', 'OPTION', 'OR', 'ORDER', 'OUTER', 'OUTPUT',
            'OUTPUT_TYPE', 'OVERFLOW', 'OFFSET', 'OVER',
            'PAGE', 'PAGELENGTH', 'PAGES', 'PAGE_SIZE', 'PARAMETER', 'PASSWORD', 'PERCENT',
            'PLAN', 'POSITION', 'POST_EVENT', 'PRECISION', 'PREPARE', 'PRIMARY', 'PRIVILEGES',
            'PROCEDURE', 'PUBLIC',
            'QUIT',
            'RDB$DB_KEY', 'READ', 'REAL', 'RECORD_VERSION', 'RECREATE', 'REFERENCES', 'RELEASE',
            'RESERV', 'RESERVING', 'RETAIN', 'RETURN', 'RETURNING_VALUES', 'RETURNS', 'REVOKE',
            'RIGHT', 'ROLLBACK', 'ROW_COUNT', 'ROWS', 'RUNTIME', 'RECURSIVE', 'RDB$RECORD_VERSION',
            'REGR_AVGX', 'REGR_AVGY', 'REGR_COUNT', 'REGR_INTERCEPT', 'REGR_R2', 'REGR_SLOPE',
            'REGR_SXX', 'REGR_SXY', 'REGR_SYY', 'ROW',
            'SAVEPOINT', 'SCHEMA', 'SECOND', 'SELECT', 'SET', 'SHADOW', 'SHARED', 'SHELL',
            'SHOW', 'SIMILAR', 'SINGULAR', 'SIZE', 'SMALLINT', 'SNAPSHOT', 'SOME', 'SORT', 'SQL',
            'SQLCODE', 'SQLERROR', 'SQLWARNING', 'STABILITY', 'STARTING', 'STARTS', 'STATEMENT',
            'STATIC', 'STATISTICS', 'SUB_TYPE', 'SUM', 'SUSPEND', 'SENSITIVE', 'START', 'SCROLL',
            'SQLSTATE', 'STDDEV_POP', 'STDDEV_SAMP',
            'TABLE', 'TERM', 'TERMINATOR', 'THEN', 'TIES', 'TIME', 'TIMESTAMP', 'TO', 'TRAILING',
            'TRANSACTION', 'TRANSLATE', 'TRANSLATION', 'TRIGGER', 'TRIM', 'TRUE',
            'UNCOMMITTED', 'UNION', 'UNIQUE', 'UNKNOWN', 'UPDATE', 'UPDATING', 'UPPER', 'USER',
            'USING',
            'VALUE', 'VALUES', 'VARCHAR', 'VARIABLE', 'VARYING', 'VERSION', 'VIEW', 'VAR_POP',
            'VAR_SAMP',
            'WAIT', 'WHEN', 'WHENEVER', 'WHERE', 'WHILE', 'WITH', 'WORK', 'WRITE',
            'YEAR']
# List of non-reserved Firebird words
NON_RESERVED = ['ABS', 'ACCENT', 'ACOS', 'ALWAYS', 'ASCII_CHAR', 'ASCII_VAL', 'ASIN', 'ATAN',
                'ATAN2', 'AUTONOMOUS', 'ACTION', 'ABSOLUTE', 'ACOSH', 'ASINH', 'ATANH',
                'BIN_AND', 'BIN_OR', 'BIN_NOT', 'BIN_SHL', 'BIN_SHR', 'BIN_XOR',
                'BLOCK', 'BACKUP', 'BREAK', 'BODY',
                #removed 'BASENAME',
                'CALLER', 'CEIL', 'CEILING', 'CHAR_TO_UUID', 'CASCADE', 'COMMENT', 'COMMON',
                'COS', 'COSH', 'COT', 'CURSOR', 'CONTINUE',
                #removed 'CACHE','CHECK_POINT_LEN',
                'DATEADD', 'DATEDIFF', 'DECODE', 'DIFFERENCE', 'DATA', 'DESCRIPTOR', 'DDL',
                'DECRYPT', 'DENSE_RANK',
                'EXP', 'ENCRYPT', 'ENGINE',
                'FIRSTNAME', 'FLOOR', 'FIRST', 'FREE_IT', 'FIRST_VALUE',
                'GEN_UUID', 'GENERATED', 'GRANTED',
                #removed 'GROUP_COMMIT_WAIT',
                'HASH',
                'IGNORE', 'IIF', 'IDENTITY', 'INCREMENT',
                'LIMBO', 'LIST', 'LN', 'LOG', 'LOG10', 'LPAD', 'LASTNAME', 'LAST_VALUE', 'LAG',
                'LEAD', 'LINGER',
                #removed 'LOGFILE','LOG_BUF_SEZE',
                'MAPPING', 'MATCHED', 'MATCHING', 'MAXVALUE', 'MIDDLENAME', 'MILLISECOND',
                'MINVALUE', 'MOD',
                'NEXT', 'NAME', 'NTH_VALUE',
                #removed 'NUM_LOG_BUFS',
                'OS_NAME', 'OVERLAY',
                'PI', 'PLACING', 'POWER', 'PROTECTED', 'PAD', 'PRESERVE', 'PACKAGE', 'PARTITION',
                'PLUGIN', 'PRIOR',
                'REPLACE', 'REQUESTS', 'RESTART', 'RETURNING', 'REVERSE', 'ROUND', 'RPAD', 'RAND',
                'RESTRICT', 'ROLE', 'RANK', 'RELATIVE', 'ROW_NUMBER',
                #removed 'RAW_PARTITIONS',
                'SEGMENT', 'SEQUENCE', 'SIGN', 'SIN', 'SINH', 'SOURCE', 'SPACE', 'SQLSTATE', 'SQRT',
                'SCALAR_ARRAY', 'SKIP', 'SUBSTRING', 'SERVERWIDE',
                'TIMEOUT', 'TRUNC', 'TWO_PHASE', 'TAN', 'TANH', 'TYPE', 'TEMPORARY', 'TAGS',
                'TRUSTED',
                'UUID_TO_CHAR', 'UNDO', 'USAGE',
                'WEEK', 'WEEKDAY',
                'YEARDAY']

#--- Functions
def get_grants(privileges, grantors=None):
    """Get list of minimal set of SQL GRANT statamenets necessary to grant
    specified privileges.

    Args:
        privileges (list): List of :class:`Privilege` instances.

    Keyword Args:
        grantors (list): List of standard grantor names. Generates GRANTED BY
            clause for privileges granted by user that's not in list.
    """
    tp = {'S':'SELECT', 'I':'INSERT', 'U':'UPDATE', 'D':'DELETE', 'R':'REFERENCES'}

    def skey(item):
        return (item.user_name, item.user_type, item.grantor_name,
                item.subject_name, item.subject_type, item.has_grant(),
                item.privilege in tp, item.privilege, str(item.field_name),)
    def gkey(item):
        return (item.user_name, item.user_type, item.grantor_name,
                item.subject_name, item.subject_type, item.has_grant(),
                item.privilege in tp,)
    def gkey2(item):
        return item.privilege

    grants = []
    p = list(privileges)
    p.sort(key=skey)
    for k, g in groupby(p, gkey):
        g = list(g)
        item = g[0]
        if item.has_grant():
            admin_option = ' WITH %s OPTION' % ('ADMIN' if item.privilege == 'M'
                                                else 'GRANT')
        else:
            admin_option = ''
        uname = item.user_name
        user = item.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        sname = item.subject_name
        if (grantors is not None) and (item.grantor_name not in grantors):
            granted_by = ' GRANTED BY %s' % item.grantor_name
        else:
            granted_by = ''
        priv_list = []
        for k, items in groupby(g, gkey2):
            items = list(items)
            item = items[0]
            if item.privilege in tp:
                privilege = tp[item.privilege]
                if len(items) > 1:
                    privilege += '(%s)' % ','.join(i.field_name for i in items if i.field_name)
                elif item.field_name is not None:
                    privilege += '(%s)' % item.field_name
                priv_list.append(privilege)
            elif item.privilege == 'X': # procedure
                privilege = 'EXECUTE ON PROCEDURE '
            else: # role membership
                privilege = ''
        if priv_list:
            privilege = ', '.join(priv_list)
            privilege += ' ON '
        grants.append('GRANT %s%s TO %s%s%s%s' % (privilege, sname, utype, uname, admin_option,
                                                  granted_by))
    return grants
def iskeyword(ident):
    "Return True if `ident` is (any) Firebird keyword."
    return (ident in RESERVED) or (ident in NON_RESERVED)
def escape_single_quotes(text):
    "Return `text` with any single quotes escaped (doubled)."
    return text.replace("'", "''")


#--- Classes
class Schema(Visitable):
    """This class represents database schema.
    """

    #: option switch: Always quote db object names on output
    opt_always_quote = False
    #: option switch: Keyword for generator/sequence
    opt_generator_keyword = 'SEQUENCE'
    #: Datatype declaration methods for procedure parameters: key = numID, value = name
    enum_param_type_from = {PROCPAR_DATATYPE: 'DATATYPE',
                            PROCPAR_DOMAIN: 'DOMAIN',
                            PROCPAR_TYPE_OF_DOMAIN: 'TYPE OF DOMAIN',
                            PROCPAR_TYPE_OF_COLUMN: 'TYPE OF COLUMN'}
    #: Object types: key = numID, value = type_name
    enum_object_types = dict()
    #: Object type codes: key = type_name, value = numID
    enum_object_type_codes = dict()
    #: Character set names: key = numID, value = charset_name
    enum_character_set_names = dict()
    #: Field types: key = numID, value = type_name
    enum_field_types = dict()
    #: Field sub types: key = numID, value = type_name
    enum_field_subtypes = dict()
    #: Function types: key = numID, value = type_name
    enum_function_types = dict()
    #: Mechanism Types: key = numID, value = type_name
    enum_mechanism_types = dict()
    #: Parameter Mechanism Types: key = numID, value = type_name
    enum_parameter_mechanism_types = dict()
    #: Procedure Types: key = numID, value = type_name
    enum_procedure_types = dict()
    #: Relation Types: key = numID, value = type_name
    enum_relation_types = dict()
    #: System Flag Types: key = numID, value = type_name
    enum_system_flag_types = dict()
    #: Transaction State Types: key = numID, value = type_name
    enum_transaction_state_types = dict()
    #: Trigger Types: key = numID, value = type_name
    enum_trigger_types = dict()
    # Firebird 3.0
    #: Parameter Types: key = numID, value = type_name
    enum_parameter_types = dict()
    #: Index activity status: key = numID, value = flag_name
    enum_index_activity_flags = dict()
    #: Index uniqueness: key = numID, value = flag_name
    enum_index_unique_flags = dict()
    #: Trigger activity status: key = numID, value = flag_name_name
    enum_trigger_activity_flags = dict()
    #: Grant option: key = numID, value = option_name
    enum_grant_options = dict()
    #: Page type: key = numID, value = type_name
    enum_page_types = dict()
    #: Privacy flags: numID, value = flag_name
    enum_privacy_flags = dict()
    #: Legacy flags: numID, value = flag_name
    enum_legacy_flags = dict()
    #: Determinism flags: numID, value = flag_name
    enum_determinism_flags = dict()

    def __init__(self):
        self._con = None
        self._ic = None
        self.__internal = False
        self.__clear()
    def __del__(self):
        if not self.closed:
            self._close()
    def __fail_if_closed(self):
        if self.closed:
            raise fdb.ProgrammingError("Schema is not binded to connection.")
    def _close(self):
        self._ic.close()
        self._con = None
        self._ic = None
    def _set_as_internal(self):
        """Mark this instance as `internal` (embedded). This blocks calls to
        :meth:`bind` and :meth:`close`."""
        self.__internal = True
        self._con = weakref.proxy(self._con)
    def __object_by_name(self, _list, name):
        if name is None:
            return None
        for o in _list:
            if o.name == name:
                return o
        return None

    def __clear(self, data=None):
        if data:
            if not isinstance(data, collections.Iterable):
                data = (data, )
        else:
            data = range(1, SCHEMA_FILTERS + 1)
        for item in data:
            if item == SCHEMA_TABLES:
                self.__tables = None
            elif item == SCHEMA_VIEWS:
                self.__views = None
            elif item == SCHEMA_DOMAINS:
                self.__domains = None
            elif item == SCHEMA_INDICES:
                self.__indices = None
                self.__constraint_indices = None
            elif item == SCHEMA_DEPENDENCIES:
                self.__dependencies = None
            elif item == SCHEMA_GENERATORS:
                self.__generators = None
            elif item == SCHEMA_TRIGGERS:
                self.__triggers = None
            elif item == SCHEMA_PROCEDURES:
                self.__procedures = None
            elif item == SCHEMA_CONSTRAINTS:
                self.__constraints = None
            elif item == SCHEMA_COLLATIONS:
                self.__collations = None
            elif item == SCHEMA_CHARACTER_SETS:
                self.__character_sets = None
            elif item == SCHEMA_EXCEPTIONS:
                self.__exceptions = None
            elif item == SCHEMA_ROLES:
                self.__roles = None
            elif item == SCHEMA_FUNCTIONS:
                self.__functions = None
            elif item == SCHEMA_FILES:
                self.__files = None
            elif item == SCHEMA_SHADOWS:
                self.__shadows = None
            elif item == SCHEMA_PRIVILEGES:
                self.__privileges = None
            elif item == SCHEMA_USERS:
                self.__users = None
            elif item == SCHEMA_PACKAGES:
                self.__packages = None
            elif item == SCHEMA_BACKUP_HISTORY:
                self.__backup_history = None
            elif item == SCHEMA_FILTERS:
                self.__filters = None
            else:
                raise fdb.ProgrammingError("Unknown metadata category '%d'" % item)

    #--- protected

    def _select_row(self, cmd, params=None):
        if params:
            self._ic.execute(cmd, params)
        else:
            self._ic.execute(cmd)
        return self._ic.fetchonemap()
    def _select(self, cmd, params=None):
        if params:
            self._ic.execute(cmd, params)
        else:
            self._ic.execute(cmd)
        return self._ic.itermap()
    def _get_field_dimensions(self, field):
        return [(r[0], r[1]) for r in
                self._ic.execute("""SELECT r.RDB$LOWER_BOUND, r.RDB$UPPER_BOUND
FROM RDB$FIELD_DIMENSIONS r
where r.RDB$FIELD_NAME = '%s'
order by r.RDB$DIMENSION""" % field.name)]
    def _get_item(self, name, itype, subname=None):
        if itype == 0: # Relation
            return self.get_table(name)
        elif itype == 1: # View
            return self.get_view(name)
        elif itype == 2: # Trigger
            return self.get_trigger(name)
        elif itype == 5: # Procedure
            return self.get_procedure(name)
        elif itype == 8: # User
            result = self.__object_by_name(self._get_users(), name)
            if not result:
                result = fdb.services.User(name)
                self.__users.append(result)
            return result
        elif itype == 9: # Field
            return self.get_table(name).get_column(subname)
        elif itype == 10: # Index
            return self.get_index(name)
        elif itype == 13: # Role
            return self.get_role(name)
        elif itype == 14: # Generator
            return self.get_sequence(name)
        elif itype == 15: # UDF
            return self.get_function(name)
        elif itype == 17: # Collation
            return self.get_collation(name)
        elif itype in [17, 18]: # Package
            return self.get_package(name)
        else:
            raise fdb.ProgrammingError('Unsupported subject type')

    #--- special attribute access methods

    def _get_default_character_set(self):
        return self.get_character_set(self._default_charset_name)
    def _get_collations(self):
        if self.__collations is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$collations")
            self.__collations = ObjectList((Collation(self, row) for row in self._ic.itermap()),
                                           Collation, 'item.name')
            self.__collations.freeze()
        return self.__collations
    def _get_character_sets(self):
        if self.__character_sets is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$character_sets")
            self.__character_sets = ObjectList((CharacterSet(self, row) for row in self._ic.itermap()),
                                               CharacterSet, 'item.name')
            self.__character_sets.freeze()
        return self.__character_sets
    def _get_exceptions(self):
        if self.__exceptions is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$exceptions")
            self.__exceptions = ObjectList((DatabaseException(self, row) for row in self._ic.itermap()),
                                           DatabaseException, 'item.name')
            self.__exceptions.freeze()
        return self.__exceptions
    def _get_all_domains(self):
        if self.__domains is None:
            self.__fail_if_closed()
            cols = ['RDB$FIELD_NAME', 'RDB$VALIDATION_SOURCE', 'RDB$COMPUTED_SOURCE',
                    'RDB$DEFAULT_SOURCE', 'RDB$FIELD_LENGTH', 'RDB$FIELD_SCALE',
                    'RDB$FIELD_TYPE', 'RDB$FIELD_SUB_TYPE', 'RDB$DESCRIPTION',
                    'RDB$SYSTEM_FLAG', 'RDB$SEGMENT_LENGTH', 'RDB$EXTERNAL_LENGTH',
                    'RDB$EXTERNAL_SCALE', 'RDB$EXTERNAL_TYPE', 'RDB$DIMENSIONS',
                    'RDB$NULL_FLAG', 'RDB$CHARACTER_LENGTH', 'RDB$COLLATION_ID',
                    'RDB$CHARACTER_SET_ID', 'RDB$FIELD_PRECISION']
            if self._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$SECURITY_CLASS', 'RDB$OWNER_NAME'])
            self._ic.execute("""select %s from RDB$FIELDS""" % ','.join(cols))
            sys_domains, user_domains = ObjectList((Domain(self, row) for row in self._ic.itermap()),
                                                   Domain, 'item.name').split(lambda item:
                                                                              item.issystemobject())
            sys_domains.freeze()
            user_domains.freeze()
            self.__domains = (user_domains, sys_domains)
        return self.__domains
    def _get_domains(self):
        return self._get_all_domains()[0]
    def _get_sysdomains(self):
        return self._get_all_domains()[1]
    def _get_all_tables(self):
        if self.__tables is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$relations where rdb$view_blr is null")
            sys_tables, user_tables = ObjectList((Table(self, row) for row in self._ic.itermap()),
                                                 Table, 'item.name').split(lambda item:
                                                                           item.issystemobject())
            sys_tables.freeze()
            user_tables.freeze()
            self.__tables = (user_tables, sys_tables)
        return self.__tables
    def _get_tables(self):
        return self._get_all_tables()[0]
    def _get_systables(self):
        return self._get_all_tables()[1]
    def _get_all_views(self):
        if self.__views is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$relations where rdb$view_blr is not null")
            sys_views, user_views = ObjectList((View(self, row) for row in self._ic.itermap()),
                                               View, 'item.name').split(lambda item:
                                                                        item.issystemobject())
            sys_views.freeze()
            user_views.freeze()
            self.__views = (user_views, sys_views)
        return self.__views
    def _get_views(self):
        return self._get_all_views()[0]
    def _get_sysviews(self):
        return self._get_all_views()[1]
    def _get_constraint_indices(self):
        if self.__constraint_indices is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$INDEX_NAME, RDB$CONSTRAINT_NAME
from RDB$RELATION_CONSTRAINTS where RDB$INDEX_NAME is not null""")
            self.__constraint_indices = dict((key.strip(), value.strip()) for key, value in self._ic)
        return self.__constraint_indices
    def _get_all_indices(self):
        if self.__indices is None:
            self.__fail_if_closed()
            # Dummy call to _get_constraint_indices() is necessary as
            # Index.issystemobject() that is called in Index.__init__() will
            # drop result from internal cursor and we'll not load all indices.
            self._get_constraint_indices()
            self._ic.execute("""select RDB$INDEX_NAME, RDB$RELATION_NAME,
RDB$INDEX_ID, RDB$UNIQUE_FLAG, RDB$DESCRIPTION, RDB$SEGMENT_COUNT,
RDB$INDEX_INACTIVE, RDB$INDEX_TYPE, RDB$FOREIGN_KEY, RDB$SYSTEM_FLAG,
RDB$EXPRESSION_SOURCE, RDB$STATISTICS from RDB$INDICES""")
            sys_indices, user_indices = ObjectList((Index(self, row) for row in self._ic.itermap()),
                                                   Index, 'item.name').split(lambda item:
                                                                             item.issystemobject())
            sys_indices.freeze()
            user_indices.freeze()
            self.__indices = (user_indices, sys_indices)
        return self.__indices
    def _get_indices(self):
        return self._get_all_indices()[0]
    def _get_sysindices(self):
        return self._get_all_indices()[1]
    def _get_all_generators(self):
        if self.__generators is None:
            self.__fail_if_closed()
            cols = ['RDB$GENERATOR_NAME', 'RDB$GENERATOR_ID', 'RDB$DESCRIPTION',
                    'RDB$SYSTEM_FLAG']
            if self._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$SECURITY_CLASS', 'RDB$OWNER_NAME', 'RDB$INITIAL_VALUE',
                             'RDB$GENERATOR_INCREMENT'])
            self._ic.execute("select %s from rdb$generators" % ','.join(cols))
            sys_generators, user_generators = ObjectList((Sequence(self, row) for row in self._ic.itermap()),
                                                         Sequence, 'item.name').split(lambda item:
                                                                                      item.issystemobject())
            sys_generators.freeze()
            user_generators.freeze()
            self.__generators = (user_generators, sys_generators)
        return self.__generators
    def _get_generators(self):
        return self._get_all_generators()[0]
    def _get_sysgenerators(self):
        return self._get_all_generators()[1]
    def _get_all_triggers(self):
        if self.__triggers is None:
            self.__fail_if_closed()
            cols = ['RDB$TRIGGER_NAME', 'RDB$RELATION_NAME', 'RDB$TRIGGER_SEQUENCE',
                    'RDB$TRIGGER_TYPE', 'RDB$TRIGGER_SOURCE', 'RDB$DESCRIPTION',
                    'RDB$TRIGGER_INACTIVE', 'RDB$SYSTEM_FLAG', 'RDB$FLAGS']
            if self._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$VALID_BLR', 'RDB$ENGINE_NAME', 'RDB$ENTRYPOINT'])
            self._ic.execute("select %s from RDB$TRIGGERS" % ','.join(cols))
            sys_triggers, user_triggers = ObjectList((Trigger(self, row) for row in self._ic.itermap()),
                                                     Trigger, 'item.name').split(lambda item:
                                                                                 item.issystemobject())
            sys_triggers.freeze()
            user_triggers.freeze()
            self.__triggers = (user_triggers, sys_triggers)
        return self.__triggers
    def _get_triggers(self):
        return self._get_all_triggers()[0]
    def _get_systriggers(self):
        return self._get_all_triggers()[1]
    def _get_all_procedures(self):
        if self.__procedures is None:
            self.__fail_if_closed()
            cols = ['RDB$PROCEDURE_NAME', 'RDB$PROCEDURE_ID', 'RDB$PROCEDURE_INPUTS',
                    'RDB$PROCEDURE_OUTPUTS', 'RDB$DESCRIPTION', 'RDB$PROCEDURE_SOURCE',
                    'RDB$SECURITY_CLASS', 'RDB$OWNER_NAME', 'RDB$SYSTEM_FLAG']
            if self._con.ods >= fdb.ODS_FB_21:
                cols.extend(['RDB$PROCEDURE_TYPE', 'RDB$VALID_BLR'])
            if self._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$ENGINE_NAME', 'RDB$ENTRYPOINT',
                             'RDB$PACKAGE_NAME', 'RDB$PRIVATE_FLAG'])
            self._ic.execute("select %s from rdb$procedures" % ','.join(cols))
            sys_procedures, user_procedures = ObjectList((Procedure(self, row) for row in self._ic.itermap()),
                                                         Procedure, 'item.name').split(lambda item:
                                                                                       item.issystemobject())
            sys_procedures.freeze()
            user_procedures.freeze()
            self.__procedures = (user_procedures, sys_procedures)
        return self.__procedures
    def _get_procedures(self):
        return self._get_all_procedures()[0]
    def _get_sysprocedures(self):
        return self._get_all_procedures()[1]
    def _get_constraints(self):
        if self.__constraints is None:
            self.__fail_if_closed()
            # Dummy call to _get_all_tables() is necessary as
            # Constraint.issystemobject() that is called in Constraint.__init__()
            # will drop result from internal cursor and we'll not load all constraints.
            self._get_all_tables()
            self._ic.execute("""select c.RDB$CONSTRAINT_NAME,
c.RDB$CONSTRAINT_TYPE, c.RDB$RELATION_NAME, c.RDB$DEFERRABLE,
c.RDB$INITIALLY_DEFERRED, c.RDB$INDEX_NAME, r.RDB$CONST_NAME_UQ,
r.RDB$MATCH_OPTION,r.RDB$UPDATE_RULE,r.RDB$DELETE_RULE,
k.RDB$TRIGGER_NAME from rdb$relation_constraints C
left outer join rdb$ref_constraints R on C.rdb$constraint_name = R.rdb$constraint_name
left outer join rdb$check_constraints K on (C.rdb$constraint_name = K.rdb$constraint_name)
and (c.RDB$CONSTRAINT_TYPE in ('CHECK','NOT NULL'))""")
            self.__constraints = ObjectList((Constraint(self, row) for row in self._ic.itermap()),
                                            Constraint, 'item.name')
            # Check constrains need special care because they're doubled
            # (select above returns two records for them with different trigger names)
            checks = self.__constraints.extract(lambda item: item.ischeck())
            dchecks = {}
            for check in checks:
                dchecks.setdefault(check.name, list()).append(check)
            for checklist in dchecks.values():
                names = [c._attributes['RDB$TRIGGER_NAME'] for c in checklist]
                check = checklist[0]
                check._attributes['RDB$TRIGGER_NAME'] = names
                self.__constraints.append(check)
            self.__constraints.freeze()
        return self.__constraints
    def _get_roles(self):
        if self.__roles is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$roles")
            self.__roles = ObjectList((Role(self, row) for row in self._ic.itermap()),
                                      Role, 'item.name')
            self.__roles.freeze()
        return self.__roles
    def _get_dependencies(self):
        if self.__dependencies is None:
            self.__fail_if_closed()
            self._ic.execute("select * from rdb$dependencies")
            self.__dependencies = ObjectList((Dependency(self, row) for row in self._ic.itermap()),
                                             Dependency)
        return self.__dependencies
    def _get_all_functions(self):
        if self.__functions is None:
            self.__fail_if_closed()
            cols = ['RDB$FUNCTION_NAME', 'RDB$FUNCTION_TYPE', 'RDB$MODULE_NAME',
                    'RDB$ENTRYPOINT', 'RDB$DESCRIPTION', 'RDB$RETURN_ARGUMENT',
                    'RDB$SYSTEM_FLAG']
            if self._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$ENGINE_NAME', 'RDB$PACKAGE_NAME', 'RDB$PRIVATE_FLAG',
                             'RDB$FUNCTION_SOURCE', 'RDB$FUNCTION_ID', 'RDB$VALID_BLR',
                             'RDB$SECURITY_CLASS', 'RDB$OWNER_NAME', 'RDB$LEGACY_FLAG',
                             'RDB$DETERMINISTIC_FLAG'])
            self._ic.execute("select %s from rdb$functions" % ','.join(cols))
            sys_functions, user_functions = ObjectList((Function(self, row) for row in self._ic.itermap()),
                                                       Function, 'item.name').split(lambda item:
                                                                                    item.issystemobject())
            sys_functions.freeze()
            user_functions.freeze()
            self.__functions = (user_functions, sys_functions)
        return self.__functions
    def _get_functions(self):
        return self._get_all_functions()[0]
    def _get_sysfunctions(self):
        return self._get_all_functions()[1]
    def _get_files(self):
        if self.__files is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$FILE_NAME, RDB$FILE_SEQUENCE,
RDB$FILE_START, RDB$FILE_LENGTH from RDB$FILES
where RDB$SHADOW_NUMBER = 0
order by RDB$FILE_SEQUENCE""")
            self.__files = ObjectList((DatabaseFile(self, row) for row in self._ic.itermap()),
                                      DatabaseFile, 'item.name')
            self.__files.freeze()
        return self.__files
    def _get_shadows(self):
        if self.__shadows is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$FILE_FLAGS, RDB$SHADOW_NUMBER
from RDB$FILES
where RDB$SHADOW_NUMBER > 0 AND RDB$FILE_SEQUENCE = 0
order by RDB$SHADOW_NUMBER""")
            self.__shadows = ObjectList((Shadow(self, row) for row in self._ic.itermap()),
                                        Shadow, 'item.name')
            self.__shadows.freeze()
        return self.__shadows
    def _get_privileges(self):
        if self.__privileges is None:
            self.__fail_if_closed()
            self._ic.execute("""select RDB$USER, RDB$GRANTOR, RDB$PRIVILEGE,
RDB$GRANT_OPTION, RDB$RELATION_NAME, RDB$FIELD_NAME, RDB$USER_TYPE, RDB$OBJECT_TYPE
FROM RDB$USER_PRIVILEGES""")
            self.__privileges = ObjectList((Privilege(self, row) for row in self._ic.itermap()),
                                           Privilege)
        return self.__privileges
    def _get_backup_history(self):
        if self.__backup_history is None:
            self.__fail_if_closed()
            self._ic.execute("""SELECT RDB$BACKUP_ID, RDB$TIMESTAMP,
RDB$BACKUP_LEVEL, RDB$GUID, RDB$SCN, RDB$FILE_NAME
FROM RDB$BACKUP_HISTORY""")
            self.__backup_history = ObjectList((BackupHistory(self, row) for row in self._ic.itermap()),
                                               BackupHistory, 'item.name')
            self.__backup_history.freeze()
        return self.__backup_history
    def _get_filters(self):
        if self.__filters is None:
            self.__fail_if_closed()
            self._ic.execute("""SELECT RDB$FUNCTION_NAME, RDB$DESCRIPTION,
RDB$MODULE_NAME, RDB$ENTRYPOINT, RDB$INPUT_SUB_TYPE, RDB$OUTPUT_SUB_TYPE, RDB$SYSTEM_FLAG
FROM RDB$FILTERS""")
            self.__filters = ObjectList((Filter(self, row) for row in self._ic.itermap()),
                                        Filter, 'item.name')
            self.__filters.freeze()
        return self.__filters
    def _get_users(self):
        if self.__users is None:
            self.__fail_if_closed()
            self._ic.execute("select distinct(RDB$USER) FROM RDB$USER_PRIVILEGES")
            self.__users = ObjectList((fdb.services.User(row[0].strip()) for row in self._ic), fdb.services.User, 'item.name')
        return self.__users
    def _get_packages(self):
        if self.__packages is None:
            if self._con.ods >= fdb.ODS_FB_30:
                self.__fail_if_closed()
                self._ic.execute("""select RDB$PACKAGE_NAME, RDB$PACKAGE_HEADER_SOURCE,
    RDB$PACKAGE_BODY_SOURCE, RDB$VALID_BODY_FLAG, RDB$SECURITY_CLASS, RDB$OWNER_NAME,
    RDB$SYSTEM_FLAG, RDB$DESCRIPTION
                FROM RDB$PACKAGES""")
                self.__packages = ObjectList((Package(self, row) for row in self._ic.itermap()),
                                             Package, 'item.name')
            else:
                self.__packages = ObjectList(_cls=Package, key_expr='item.name')
            self.__packages.freeze()
        return self.__packages

    #--- Properties

    #: True if link to :class:`~fdb.Connection` is closed.
    closed = property(lambda self: self._con is None)
    description = property(lambda self: self.__description, doc="Database description or None if it doesn't have a description.")
    owner_name = property(lambda self: self.__owner, doc="Database owner name.")
    default_character_set = LateBindingProperty(_get_default_character_set, doc="Default :class:`CharacterSet` for database")
    security_class = property(lambda self: self.__security_class, doc="Can refer to the security class applied as databasewide access control limits.")
    collations = LateBindingProperty(_get_collations, doc=":class:`~fdb.utils.ObjectList` of all collations in database.\nItems are :class:`Collation` objects.")
    character_sets = LateBindingProperty(_get_character_sets, doc=":class:`~fdb.utils.ObjectList` of all character sets in database.\nItems are :class:`CharacterSet` objects.")
    exceptions = LateBindingProperty(_get_exceptions, doc=":class:`~fdb.utils.ObjectList` of all exceptions in database.\nItems are :class:`DatabaseException` objects.")
    generators = LateBindingProperty(_get_generators, doc=":class:`~fdb.utils.ObjectList` of all user generators in database.\nItems are :class:`Sequence` objects.")
    sysgenerators = LateBindingProperty(_get_sysgenerators, doc=":class:`~fdb.utils.ObjectList` of all system generators in database.\nItems are :class:`Sequence` objects.")
    sequences = LateBindingProperty(_get_generators, doc=":class:`~fdb.utils.ObjectList` of all user generators in database.\nItems are :class:`Sequence` objects.")
    syssequences = LateBindingProperty(_get_sysgenerators, doc=":class:`~fdb.utils.ObjectList` of all system generators in database.\nItems are :class:`Sequence` objects.")
    domains = LateBindingProperty(_get_domains, doc=":class:`~fdb.utils.ObjectList` of all user domains in database.\nItems are :class:`Domain` objects.")
    sysdomains = LateBindingProperty(_get_sysdomains, doc=":class:`~fdb.utils.ObjectList` of all system domains in database.\nItems are :class:`Domain` objects.")
    indices = LateBindingProperty(_get_indices, doc=":class:`~fdb.utils.ObjectList` of all user indices in database.\nItems are :class:`Index` objects.")
    sysindices = LateBindingProperty(_get_sysindices, doc=":class:`~fdb.utils.ObjectList` of all system indices in database.\nItems are :class:`Index` objects.")
    tables = LateBindingProperty(_get_tables, doc=":class:`~fdb.utils.ObjectList` of all user tables in database.\nItems are :class:`Table` objects.")
    systables = LateBindingProperty(_get_systables, doc=":class:`~fdb.utils.ObjectList` of all system tables in database.\nItems are :class:`Table` objects.")
    views = LateBindingProperty(_get_views, doc=":class:`~fdb.utils.ObjectList` of all user views in database.\nItems are :class:`View` objects.")
    sysviews = LateBindingProperty(_get_sysviews, doc=":class:`~fdb.utils.ObjectList` of all system views in database.\nItems are :class:`View` objects.")
    triggers = LateBindingProperty(_get_triggers, doc=":class:`~fdb.utils.ObjectList` of all user triggers in database.\nItems are :class:`Trigger` objects.")
    systriggers = LateBindingProperty(_get_systriggers, doc=":class:`~fdb.utils.ObjectList` of all system triggers in database.\nItems are :class:`Trigger` objects.")
    procedures = LateBindingProperty(_get_procedures, doc=":class:`~fdb.utils.ObjectList` of all user procedures in database.\nItems are :class:`Procedure` objects.")
    sysprocedures = LateBindingProperty(_get_sysprocedures, doc=":class:`~fdb.utils.ObjectList` of all system procedures in database.\nItems are :class:`Procedure` objects.")
    constraints = LateBindingProperty(_get_constraints, doc=":class:`~fdb.utils.ObjectList` of all constraints in database.\nItems are :class:`Constraint` objects.")
    roles = LateBindingProperty(_get_roles, doc=":class:`~fdb.utils.ObjectList` of all roles in database.\nItems are :class:`Role` objects.")
    dependencies = LateBindingProperty(_get_dependencies, doc=":class:`~fdb.utils.ObjectList` of all dependencies in database.\nItems are :class:`Dependency` objects.")
    functions = LateBindingProperty(_get_functions, doc=":class:`~fdb.utils.ObjectList` of all user functions defined in database.\nItems are :class:`Function` objects.")
    sysfunctions = LateBindingProperty(_get_sysfunctions, doc=":class:`~fdb.utils.ObjectList` of all system functions defined in database.\nItems are :class:`Function` objects.")
    files = LateBindingProperty(_get_files, doc=":class:`~fdb.utils.ObjectList` of all extension files defined for database.\nItems are :class:`DatabaseFile` objects.")
    shadows = LateBindingProperty(_get_shadows, doc=":class:`~fdb.utils.ObjectList` of all shadows defined for database.\nItems are :class:`Shadow` objects.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of all privileges defined for database.\nItems are :class:`Privilege` objects.")
    backup_history = LateBindingProperty(_get_backup_history, doc=":class:`~fdb.utils.ObjectList` of all nbackup hisotry records.\nItems are :class:`BackupHistory` objects.")
    filters = LateBindingProperty(_get_filters, doc=":class:`~fdb.utils.ObjectList` of all user-defined BLOB filters.\nItems are :class:`Filter` objects.")
    # FB 3
    packages = LateBindingProperty(_get_packages, doc=":class:`~fdb.utils.ObjectList` of all packages defined for database.\nItems are :class:`Package` objects.")
    linger = property(lambda self: self.__linger, doc="Database linger value.")

    #--- Public

    def bind(self, connection):
        """Bind this instance to specified :class:`~fdb.Connection`.

        Args:
            connection: :class:`~fdb.Connection` instance.

        Raises:
            fdb.ProgrammingError: If Schema object was set as internal (via :meth:`_set_as_internal`).
        """
        if self.__internal:
            raise fdb.ProgrammingError("Call to 'bind' not allowed for embedded Schema.")
        if self._con:
            self.close()
        self._con = connection
        self._ic = self._con.query_transaction.cursor()

        self.__clear()

        self._ic.execute('select * from RDB$DATABASE')
        row = self._ic.fetchonemap()
        self.__description = row['RDB$DESCRIPTION']
        self.__linger = row.get('RDB$LINGER')
        self._default_charset_name = row['RDB$CHARACTER_SET_NAME'].strip()
        self.__security_class = row['RDB$SECURITY_CLASS']
        if self.__security_class:
            self.__security_class = self.__security_class.strip()
        self._ic.execute("select RDB$OWNER_NAME from RDB$RELATIONS where RDB$RELATION_NAME = 'RDB$DATABASE'")
        self.__owner = self._ic.fetchone()[0].strip()
        # Load enumerate types defined in RDB$TYPES table
        enum_select = 'select RDB$TYPE, RDB$TYPE_NAME from RDB$TYPES where RDB$FIELD_NAME = ?'
        def enum_dict(enum_type):
            return dict((key, value.strip()) for key, value
                        in self._ic.execute(enum_select, (enum_type,)))
        # Object types
        self.enum_object_types = enum_dict('RDB$OBJECT_TYPE')
        # Object type codes
        self.enum_object_type_codes = dict(((value, key) for key, value
                                            in self.enum_object_types.items()))
        # Character set names
        self.enum_character_set_names = enum_dict('RDB$CHARACTER_SET_NAME')
        # Field types
        self.enum_field_types = enum_dict('RDB$FIELD_TYPE')
        # Field sub types
        self.enum_field_subtypes = enum_dict('RDB$FIELD_SUB_TYPE')
        # Function types
        self.enum_function_types = enum_dict('RDB$FUNCTION_TYPE')
        # Mechanism Types
        self.enum_mechanism_types = enum_dict('RDB$MECHANISM')
        # Parameter Mechanism Types
        self.enum_parameter_mechanism_types = enum_dict('RDB$PARAMETER_MECHANISM')
        # Procedure Types
        self.enum_procedure_types = enum_dict('RDB$PROCEDURE_TYPE')
        if not self.enum_procedure_types:
            self.enum_procedure_types = {0: 'LEGACY', 1: 'SELECTABLE', 2: 'EXECUTABLE'}
        # Relation Types
        self.enum_relation_types = enum_dict('RDB$RELATION_TYPE')
        # System Flag Types
        self.enum_system_flag_types = enum_dict('RDB$SYSTEM_FLAG')
        # Transaction State Types
        self.enum_transaction_state_types = enum_dict('RDB$TRANSACTION_STATE')
        # Trigger Types
        self.enum_trigger_types = enum_dict('RDB$TRIGGER_TYPE')
        # Firebird 3.0
        # Parameter Types
        self.enum_parameter_types = enum_dict('RDB$PARAMETER_TYPE')
        # Index activity
        self.enum_index_activity_flags = enum_dict('RDB$INDEX_INACTIVE')
        # Index uniqueness
        self.enum_index_unique_flags = enum_dict('RDB$UNIQUE_FLAG')
        # Trigger activity
        self.enum_trigger_activity_flags = enum_dict('RDB$TRIGGER_INACTIVE')
        # Grant options
        self.enum_grant_options = enum_dict('RDB$GRANT_OPTION')
        # Page types
        self.enum_page_types = enum_dict('RDB$PAGE_TYPE')
        # Privacy
        self.enum_privacy_flags = enum_dict('RDB$PRIVATE_FLAG')
        # Legacy
        self.enum_legacy_flags = enum_dict('RDB$LEGACY_FLAG')
        # Determinism
        self.enum_determinism_flags = enum_dict('RDB$DETERMINISTIC_FLAG')

    def close(self):
        """Sever link to :class:`~fdb.Connection`.

        Raises:
            fdb.ProgrammingError: If Schema object was set as internal (via
                :meth:`_set_as_internal`).
        """
        if self.__internal:
            raise fdb.ProgrammingError("Call to 'close' not allowed for embedded Schema.")
        self._close()
        self.__clear()

    #--- Basic Database manipulation routines

    def clear(self):
        "Drop all cached metadata objects."
        self.__clear()
    def reload(self, data=None):
        """Drop all or specified category of cached metadata objects, so they're
        reloaded from database on next reference.

        Keyword Args:
            data: `None`, metadata category code or iterable with category codes.

        Note:
            Category codes are defined by `SCHEMA_*` globals.

            Also commits query transaction!

        Raises:
            fdb.ProgrammingError: For undefined metadata category.
        """
        self.__clear(data)
        if not self.closed:
            self._ic.transaction.commit()

    def get_metadata_ddl(self, sections=SCRIPT_DEFAULT_ORDER):
        """Return list of DDL SQL commands for creation of specified categories of database objects.

        Keyword Args:
            sections (list): List of section identifiers.

        Returns:
            List with SQL commands.

        Sections identifiers are represented by `SCRIPT_*` contants defined in schema module.

        Sections are created in the order of occerence in list. Uses `SCRIPT_DEFAULT_ORDER` list when sections are not specified.
"""
        def order_by_dependency(items, get_dependencies):
            ordered = []
            wlist = list(items)
            while len(wlist) > 0:
                item = wlist.pop(0)
                add = True
                for dep in get_dependencies(item):
                    if isinstance(dep.depended_on, View) and dep.depended_on not in ordered:
                        wlist.append(item)
                        add = False
                        break
                if add:
                    ordered.append(item)
            return ordered
        def view_dependencies(item):
            return [x for x in item.get_dependencies()
                    if x.depended_on_type == 1]
        #
        script = []
        for section in sections:
            if section == SCRIPT_COLLATIONS:
                for collation in self.collations:
                    if not collation.issystemobject():
                        script.append(collation.get_sql_for('create'))
            elif section == SCRIPT_CHARACTER_SETS:
                for charset in self.character_sets:
                    if charset.name != charset.default_collate.name:
                        script.append(charset.get_sql_for('alter',
                                                          collation=charset.default_collate.name))
            elif section == SCRIPT_UDFS:
                for udf in self.functions:
                    if udf.isexternal():
                        script.append(udf.get_sql_for('declare'))
            elif section == SCRIPT_GENERATORS:
                for generator in self.generators:
                    script.append(generator.get_sql_for('create'))
            elif section == SCRIPT_EXCEPTIONS:
                for e in self.exceptions:
                    script.append(e.get_sql_for('create'))
            elif section == SCRIPT_DOMAINS:
                for domain in self.domains:
                    script.append(domain.get_sql_for('create'))
            elif section == SCRIPT_PACKAGE_DEFS:
                for package in self.packages:
                    script.append(package.get_sql_for('create'))
            elif section == SCRIPT_FUNCTION_DEFS:
                for func in (x for x in self.functions if
                             not x.isexternal() and
                             not x.ispackaged()):
                    script.append(func.get_sql_for('create', no_code=True))
            elif section == SCRIPT_PROCEDURE_DEFS:
                for proc in (x for x in self.procedures if not x.ispackaged()):
                    script.append(proc.get_sql_for('create', no_code=True))
            elif section == SCRIPT_TABLES:
                for table in self.tables:
                    script.append(table.get_sql_for('create', no_pk=True, no_unique=True))
            elif section == SCRIPT_PRIMARY_KEYS:
                for constraint in (x for x in self.constraints if x.ispkey()):
                    script.append(constraint.get_sql_for('create'))
            elif section == SCRIPT_UNIQUE_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.isunique()):
                        script.append(constraint.get_sql_for('create'))
            elif section == SCRIPT_CHECK_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.ischeck()):
                        script.append(constraint.get_sql_for('create'))
            elif section == SCRIPT_FOREIGN_CONSTRAINTS:
                for table in self.tables:
                    for constraint in (x for x in table.constraints if x.isfkey()):
                        script.append(constraint.get_sql_for('create'))
            elif section == SCRIPT_INDICES:
                for table in self.tables:
                    for index in (x for x in table.indices
                                  if not x.isenforcer()):
                        script.append(index.get_sql_for('create'))
            elif section == SCRIPT_VIEWS:
                for view in order_by_dependency(self.views, view_dependencies):
                    script.append(view.get_sql_for('create'))
            elif section == SCRIPT_PACKAGE_BODIES:
                for package in self.packages:
                    script.append(package.get_sql_for('create', body=True))
            elif section == SCRIPT_PROCEDURE_BODIES:
                for proc in (x for x in self.procedures if not x.ispackaged()):
                    script.append('ALTER' + proc.get_sql_for('create')[6:])
            elif section == SCRIPT_FUNCTION_BODIES:
                for func in (x for x in self.functions if
                             not x.isexternal() and
                             not x.ispackaged()):
                    script.append('ALTER' + func.get_sql_for('create')[6:])
            elif section == SCRIPT_TRIGGERS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('create'))
            elif section == SCRIPT_ROLES:
                for role in (x for x in self.roles if not x.issystemobject()):
                    script.append(role.get_sql_for('create'))
            elif section == SCRIPT_GRANTS:
                for priv in (x for x in self.privileges
                             if x.user_name != 'SYSDBA'
                             and not x.subject.issystemobject()):
                    script.append(priv.get_sql_for('grant'))
            elif section == SCRIPT_COMMENTS:
                for objects in [self.character_sets, self.collations,
                                self.exceptions, self.domains,
                                self.generators, self.tables,
                                self.indices, self.views,
                                self.triggers, self.procedures,
                                self.functions, self.roles]:
                    for obj in objects:
                        if obj.description is not None:
                            script.append(obj.get_sql_for('comment'))
                        if isinstance(obj, (Table, View)):
                            for col in obj.columns:
                                if col.description is not None:
                                    script.append(col.get_sql_for('comment'))
                        elif isinstance(obj, Procedure):
                            if isinstance(obj, (Table, View)):
                                for par in obj.input_params:
                                    if par.description is not None:
                                        script.append(par.get_sql_for('comment'))
                                for par in obj.output_params:
                                    if par.description is not None:
                                        script.append(par.get_sql_for('comment'))
            elif section == SCRIPT_SHADOWS:
                for shadow in self.shadows:
                    script.append(shadow.get_sql_for('create'))
            elif section == SCRIPT_INDEX_DEACTIVATIONS:
                for index in self.indices:
                    script.append(index.get_sql_for('deactivate'))
            elif section == SCRIPT_INDEX_ACTIVATIONS:
                for index in self.indices:
                    script.append(index.get_sql_for('activate'))
            elif section == SCRIPT_SET_GENERATORS:
                for generator in self.generators:
                    script.append(generator.get_sql_for('alter', value=generator.value))
            elif section == SCRIPT_TRIGGER_DEACTIVATIONS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('alter', active=False))
            elif section == SCRIPT_TRIGGER_ACTIVATIONS:
                for trigger in self.triggers:
                    script.append(trigger.get_sql_for('alter', active=True))
            else:
                raise ValueError("Unknown section code %s" % section)
        return script

    def ismultifile(self):
        "Returns true if database has multiple files."
        return len(self.files) > 0
    def get_collation(self, name):
        """Get :class:`Collation` by name.

        Args:
            name (str): Collation name.

        Returns:
            :class:`Collation` with specified name or `None`.
        """
        return self.collations.get(name)
    def get_character_set(self, name):
        """Get :class:`CharacterSet` by name.

        Args:
            name (str): Character set name.

        Returns:
            :class:`CharacterSet` with specified name or `None`.
        """
        return self.character_sets.get(name)
    def get_exception(self, name):
        """Get :class:`DatabaseException` by name.

        Args:
            name (str): Exception name.

        Returns:
            :class:`DatabaseException` with specified name or `None`.
        """
        return self.exceptions.get(name)
    def get_generator(self, name):
        """Get :class:`Sequence` by name.

        Args:
            name (str): Sequence name.

        Returns:
            :class:`Sequence` with specified name or `None`.
        """
        generator = self.generators.get(name)
        if generator is None:
            generator = self.sysgenerators.get(name)
        return generator
    get_sequence = get_generator
    def get_index(self, name):
        """Get :class:`Index` by name.

        Args:
            name (str): Index name.

        Returns:
            :class:`Index` with specified name or `None`.
        """
        index = self.indices.get(name)
        if index is None:
            index = self.sysindices.get(name)
        return index
    def get_domain(self, name):
        """Get :class:`Domain` by name.

        Args:
            name (str): Domain name.

        Returns:
            :class:`Domain` with specified name or `None`.
        """
        domain = self.domains.get(name)
        if domain is None:
            domain = self.sysdomains.get(name)
        return domain
    def get_table(self, name):
        """Get :class:`Table` by name.

        Args:
            name (str): Table name.

        Returns:
            :class:`Table` with specified name or `None`.
        """
        table = self.tables.get(name)
        if table is None:
            table = self.systables.get(name)
        return table
    def get_view(self, name):
        """Get :class:`View` by name.

        Args:
            name (str): View name.

        Returns:
            :class:`View` with specified name or `None`.
        """
        view = self.views.get(name)
        if view is None:
            view = self.sysviews.get(name)
        return view
    def get_trigger(self, name):
        """Get :class:`Trigger` by name.

        Args:
            name (str): Trigger name.

        Returns:
            :class:`Trigger` with specified name or `None`.
        """
        trigger = self.triggers.get(name)
        if trigger is None:
            trigger = self.systriggers.get(name)
        return trigger
    def get_procedure(self, name):
        """Get :class:`Procedure` by name.

        Args:
            name (str): Procedure name.

        Returns:
            :class:`Procedure` with specified name or `None`.
        """
        procedure = self.procedures.get(name)
        if procedure is None:
            procedure = self.sysprocedures.get(name)
        return procedure
    def get_constraint(self, name):
        """Get :class:`Constraint` by name.

        Args:
            name (str): Constraint name.

        Returns:
            :class:`Constraint` with specified name or `None`.
        """
        return self.constraints.get(name)
    def get_role(self, name):
        """Get :class:`Role` by name.

        Args:
            name (str): Role name.

        Returns:
            :class:`Role` with specified name or `None`.
        """
        return self.roles.get(name)
    def get_function(self, name):
        """Get :class:`Function` by name.

        Args:
            name (str): Function name.

        Returns:
            :class:`Function` with specified name or `None`.
        """
        function = self.functions.get(name)
        if function is None:
            function = self.sysfunctions.get(name)
        return function
    def get_collation_by_id(self, charset_id, collation_id):
        """Get :class:`Collation` by ID.

        Args:
            charset_id (int): Character set ID.
            collation_id (int): Collation ID.

        Returns:
            :class:`Collation` with specified ID or `None`.
        """
        for collation in self.collations:
            if (collation._attributes['RDB$CHARACTER_SET_ID'] == charset_id) and (collation.id == collation_id):
                return collation
        else:
            return None
    def get_character_set_by_id(self, id):
        """Get :class:`CharacterSet` by ID.

        Args:
            id (int): CharacterSet ID.

        Returns:
            :class:`CharacterSet` with specified ID or `None`.
        """
        for charset in self.character_sets:
            if charset.id == id:
                return charset
        else:
            return None
    def get_privileges_of(self, user, user_type=None):
        """Get list of all privileges granted to user/database object.

        Args:
            user: User name or instance of class that represents possible user.
                Allowed classes are :class:`~fdb.services.User`, :class:`Table`,
                :class:`View`, :class:`Procedure`, :class:`Trigger` or :class:`Role`.

        Keyword Args:
            user_type (int): **Required if** `user` is provided as string name.
                Numeric code for user type, see :attr:`Schema.enum_object_types`.

        Returns:
            List of :class:`Privilege` objects.

        Raises:
            fdb.ProgrammingError: For unknown `user_type` code.
        """
        if isinstance(user, (fdb.StringType, fdb.UnicodeType)):
            if (user_type is None) or (user_type not in self.enum_object_types):
                raise fdb.ProgrammingError("Unknown user_type code.")
            else:
                uname = user
                utype = [user_type]
        elif isinstance(user, (Table, View, Procedure, Trigger, Role)):
            uname = user.name
            utype = user._type_code
        elif isinstance(user, fdb.services.User):
            uname = user.name
            utype = [8]
        return [p for p in self.privileges
                if (p.user_name == uname) and (p.user_type in utype)]
    def get_package(self, name):
        """Get :class:`Package` by name.

        Args:
            name (str): Package name.

        Returns:
            :class:`Package` with specified name or `None`.
        """
        return self.packages.get(name)

class BaseSchemaItem(Visitable):
    """Base class for all database schema objects."""
    #: Weak reference to parent :class:`Schema` instance.
    schema = None
    def __init__(self, schema, attributes):
        self.schema = schema if isinstance(schema, weakref.ProxyType) else weakref.proxy(schema)
        self._type_code = []
        self._attributes = dict(attributes)
        self._actions = []

    #--- protected
    def _strip_attribute(self, attr):
        if self._attributes.get(attr):
            self._attributes[attr] = self._attributes[attr].strip()
    def _check_params(self, params, param_names):
        p = set(params.keys())
        n = set(param_names)
        if not p.issubset(n):
            raise fdb.ProgrammingError("Unsupported parameter(s) '%s'" %
                                       ','.join(p.difference(n)))
    def _needs_quoting(self, ident):
        if not ident:
            return False
        if self.schema.opt_always_quote:
            return True
        if len(ident) >= 1 and ident[0] not in string.ascii_uppercase:
            return True
        for char in ident:
            if char not in string.ascii_uppercase + string.digits + '$_':
                return True
        return iskeyword(ident)
    def _get_quoted_ident(self, ident):
        if self._needs_quoting(ident):
            return '"%s"' % ident
        else:
            return ident
    def _get_name(self):
        return None
    def _get_description(self):
        return self._attributes.get('RDB$DESCRIPTION')
    def _get_actions(self):
        return self._actions
    def _get_recreate_sql(self, **params):
        return 'RE'+self._get_create_sql(**params)
    def _get_create_or_alter_sql(self, **params):
        return 'CREATE OR ALTER' + self._get_create_sql(**params)[6:]

    #--- properties
    name = LateBindingProperty(_get_name, doc="Database object name or None if object doesn't have a name.")
    description = LateBindingProperty(_get_description, doc="Database object description or None if object doesn't have a description.")
    actions = LateBindingProperty(_get_actions, doc="List of supported SQL operations on metadata object instance.")
    #--- Public
    def issystemobject(self):
        "Returns True if this database object is system object."
        return True if self._attributes.get('RDB$SYSTEM_FLAG', False) else False
    def get_quoted_name(self):
        "Returns quoted (if necessary) name."
        return self._get_quoted_ident(self.name)
    def get_dependents(self):
        "Returns list of all database objects that depend on this one."
        return [d for d in self.schema.dependencies if d.depended_on_name == self.name and
                d.depended_on_type in self._type_code]
    def get_dependencies(self):
        "Returns list of database objects that this object depend on."
        return [d for d in self.schema.dependencies if d.dependent_name == self.name and
                d.dependent_type in self._type_code]
    def get_sql_for(self, action, **params):
        """Returns SQL command for specified action on metadata object.

        Supported actions are defined by :attr:`actions` list.

        Raises:
            fdb.ProgrammingError: For unsupported action or wrong parameters passed.
        """
        _action = action.lower()
        if _action in self._actions:
            _call = getattr(self, '_get_%s_sql' % _action)
            return _call(**params)
        else:
            raise fdb.ProgrammingError("Unsupported action '%s'" % action)

class Collation(BaseSchemaItem):
    """Represents collation.

    Supported SQL actions:
        - User collation: `create`, `drop`, `comment`
        - System collation: `comment`
    """
    def __init__(self, schema, attributes):
        super(Collation, self).__init__(schema, attributes)
        self._type_code = [17,]

        self._strip_attribute('RDB$COLLATION_NAME')
        self._strip_attribute('RDB$BASE_COLLATION_NAME')
        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'drop'])

    #--- Protected
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP COLLATION %s' % self.get_quoted_name()
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        base_sql = """CREATE COLLATION %s
   FOR %s
   %s
   %s
   %s
   %s
   %s""" % (self.get_quoted_name(),
            self.character_set.get_quoted_name(),
            ("FROM EXTERNAL ('%s')" % self._attributes['RDB$BASE_COLLATION_NAME']
             if self.isbasedonexternal()
             else "FROM %s" % self.base_collation.get_quoted_name()),
            'PAD SPACE' if self.ispadded() else 'NO PAD',
            'CASE INSENSITIVE' if self.iscaseinsensitive() else 'CASE SENSITIVE',
            'ACCENT INSENSITIVE' if self.isaccentinsensitive() else 'ACCENT SENSITIVE',
            "'%s'" % self.specific_attributes if self.specific_attributes else '')
        return base_sql.strip()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON COLLATION %s IS %s' % (self.get_quoted_name(),
                                                  'NULL' if self.description is None else
                                                  "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$COLLATION_NAME']
    def _get_id(self):
        return self._attributes['RDB$COLLATION_ID']
    def _get_character_set(self):
        """Return :class:`CharacterSet` object to which this collation belongs.
        """
        return self.schema.get_character_set_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    def _get_base_collation(self):
        base_name = self._attributes['RDB$BASE_COLLATION_NAME']
        return self.schema.get_collation(base_name) if base_name else None
    def _get_attributes(self):
        return self._attributes['RDB$COLLATION_ATTRIBUTES']
    def _get_specific_attributes(self):
        return self._attributes['RDB$SPECIFIC_ATTRIBUTES']
    def _get_function_name(self):
        return self._attributes['RDB$FUNCTION_NAME']
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Collation ID.")
    character_set = LateBindingProperty(_get_character_set, doc="Character set object associated with collation.")
    base_collation = LateBindingProperty(_get_base_collation, doc="Base Collation object that's extended by this one or None.")
    attributes = LateBindingProperty(_get_attributes, doc="Collation attributes.")
    specific_attributes = LateBindingProperty(_get_specific_attributes, doc="Collation specific attributes.")
    function_name = LateBindingProperty(_get_function_name, doc="Not currently used.")
    # FB 3.0
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Creator user name.")
    #--- Public
    def ispadded(self):
        """Returns True if collation has PAD SPACE attribute."""
        return bool(self.attributes & COLLATION_PAD_SPACE)
    def iscaseinsensitive(self):
        "Returns True if collation has CASE INSENSITIVE attribute."
        return bool(self.attributes & COLLATION_CASE_INSENSITIVE)
    def isaccentinsensitive(self):
        "Returns True if collation has ACCENT INSENSITIVE attribute."
        return bool(self.attributes & COLLATION_ACCENT_INSENSITIVE)
    def isbasedonexternal(self):
        "Returns True if collation is based on external collation definition."
        return self._attributes['RDB$BASE_COLLATION_NAME'] and not self.base_collation

class CharacterSet(BaseSchemaItem):
    """Represents character set.

    Supported SQL actions:
        `alter` (collation=Collation instance or collation name), `comment`
    """
    def __init__(self, schema, attributes):
        super(CharacterSet, self).__init__(schema, attributes)
        self._type_code = [11,]

        self._strip_attribute('RDB$CHARACTER_SET_NAME')
        self._strip_attribute('RDB$DEFAULT_COLLATE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self._actions = ['alter', 'comment']

    #--- protected
    def _get_alter_sql(self, **params):
        self._check_params(params, ['collation'])
        collation = params.get('collation')
        if collation:
            return ('ALTER CHARACTER SET %s SET DEFAULT COLLATION %s' % (self.get_quoted_name(),
                                                                         collation.get_quoted_name()
                                                                         if isinstance(collation, Collation)
                                                                         else collation))
        else:
            raise fdb.ProgrammingError("Missing required parameter: 'collation'.")
    def _get_comment_sql(self, **params):
        return 'COMMENT ON CHARACTER SET %s IS %s' % (self.get_quoted_name(),
                                                      'NULL' if self.description is None
                                                      else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$CHARACTER_SET_NAME']
    def _get_id(self):
        return self._attributes['RDB$CHARACTER_SET_ID']
    def _get_bytes_per_character(self):
        return self._attributes['RDB$BYTES_PER_CHARACTER']
    def _get_default_collate(self):
        return self.get_collation(self._attributes['RDB$DEFAULT_COLLATE_NAME'])
    def _get_collations(self):
        return self.schema.collations.filter(lambda item: item._attributes['RDB$CHARACTER_SET_ID'] == self.id)
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')

    #--- properties
    id = LateBindingProperty(_get_id, doc="Character set ID.")
    bytes_per_character = LateBindingProperty(_get_bytes_per_character, doc="Size of characters in bytes.")
    default_collate = LateBindingProperty(_get_default_collate, doc="Collate object of default collate.")
    collations = LateBindingProperty(_get_collations, doc=":class:`~fdb.utils.ObjectList` of Collations associated with character set.")
    # FB 3.0
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Creator user name.")
    #--- Public
    def get_collation(self, name):
        """Return :class:`Collation` object with specified name that belongs to
this character set.
        """
        return self.collations.get(name)
    def get_collation_by_id(self, id):
        """Return :class:`Collation` object with specified id that belongs to
this character set.
        """
        for col in self.collations:
            if col.id == id:
                return col
        return None

class DatabaseException(BaseSchemaItem):
    """Represents database exception.

    Supported SQL actions:
        - User exception: `create`, `recreate`, `alter` (message=string), `create_or_alter`, `drop`, `comment`
        - System exception: `comment`
    """
    def __init__(self, schema, attributes):
        super(DatabaseException, self).__init__(schema, attributes)
        self._type_code = [7,]

        self._strip_attribute('RDB$EXCEPTION_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])


    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        return "CREATE EXCEPTION %s '%s'" % (self.get_quoted_name(),
                                             escape_single_quotes(self.message))
    def _get_alter_sql(self, **params):
        self._check_params(params, ['message'])
        message = params.get('message')
        if message:
            return "ALTER EXCEPTION %s '%s'" % (self.get_quoted_name(),
                                                escape_single_quotes(message))
        else:
            raise fdb.ProgrammingError("Missing required parameter: 'message'.")
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP EXCEPTION %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON EXCEPTION %s IS %s' % (self.get_quoted_name(),
                                                  'NULL' if self.description is None
                                                  else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$EXCEPTION_NAME']
    def _get_id(self):
        return self._attributes['RDB$EXCEPTION_NUMBER']
    def _get_message(self):
        return self._attributes['RDB$MESSAGE']
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')

    #--- Properties
    id = LateBindingProperty(_get_id, doc="System-assigned unique exception number.")
    message = LateBindingProperty(_get_message, doc="Custom message text.")
    # FB 3.0
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Creator user name.")

class Sequence(BaseSchemaItem):
    """Represents database generator/sequence.

    Supported SQL actions:
        - User sequence: `create`, `alter` (value=number), `drop`, `comment`
        - System sequence: `comment`
    """
    def __init__(self, schema, attributes):
        super(Sequence, self).__init__(schema, attributes)
        self._type_code = [14,]

        self._strip_attribute('RDB$GENERATOR_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'alter', 'drop'])

    #--- protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        return 'CREATE %s %s' % (self.schema.opt_generator_keyword,
                                 self.get_quoted_name())
    def _get_alter_sql(self, **params):
        self._check_params(params, ['value'])
        value = params.get('value')
        if value is not None:
            return "ALTER %s %s RESTART WITH %d" % (self.schema.opt_generator_keyword,
                                                    self.get_quoted_name(),
                                                    value)
        else:
            raise fdb.ProgrammingError("Missing required parameter: 'value'.")
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP %s %s' % (self.schema.opt_generator_keyword,
                               self.get_quoted_name())
    def _get_comment_sql(self, **params):
        return 'COMMENT ON %s %s IS %s' % (self.schema.opt_generator_keyword,
                                           self.get_quoted_name(),
                                           'NULL' if self.description is None
                                           else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$GENERATOR_NAME']
    def _get_id(self):
        return self._attributes['RDB$GENERATOR_ID']
    def _get_value(self):
        return self.schema._select_row("select GEN_ID(%s,0) from RDB$DATABASE" % self.get_quoted_name())['GEN_ID']
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')
    def _get_inital_value(self):
        return self._attributes.get('RDB$INITIAL_VALUE')
    def _get_increment(self):
        return self._attributes.get('RDB$GENERATOR_INCREMENT')

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Internal ID number of the sequence.")
    value = LateBindingProperty(_get_value, doc="Current sequence value.")
    # FB 3.0
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Creator user name.")
    inital_value = LateBindingProperty(_get_inital_value, doc="Initial sequence value.")
    increment = LateBindingProperty(_get_increment, doc="Sequence increment.")
    #--- Public
    def isidentity(self):
        "Returns True for system generators created for IDENTITY columns."
        return self._attributes['RDB$SYSTEM_FLAG'] == 6

class TableColumn(BaseSchemaItem):
    """Represents table column.

    Supported SQL actions:
        - User column: `drop`, `comment`,
          `alter` (name=string, datatype=string_SQLTypeDef, position=number,
          expression=computed_by_expr, restart=None_or_init_value)
        - System column: `comment`
    """
    def __init__(self, schema, table, attributes):
        super(TableColumn, self).__init__(schema, attributes)
        self._type_code = [3, 9]

        self.__table = weakref.proxy(table)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$GENERATOR_NAME')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['alter', 'drop'])

    #--- Protected
    def _get_alter_sql(self, **params):
        self._check_params(params, ['expression', 'datatype', 'name', 'position', 'restart'])
        new_expr = params.get('expression')
        new_type = params.get('datatype')
        new_name = params.get('name')
        new_position = params.get('position')
        if new_expr and not self.iscomputed():
            raise fdb.ProgrammingError("Change from persistent column to computed"
                                       " is not allowed.")
        elif self.iscomputed() and (new_type and not new_expr):
            raise fdb.ProgrammingError("Change from computed column to persistent"
                                       " is not allowed.")
        sql = 'ALTER TABLE %s ALTER COLUMN %s' % (self.table.get_quoted_name(),
                                                  self.get_quoted_name())
        if new_name:
            return '%s TO %s' % (sql, self._get_quoted_ident(new_name))
        elif new_position:
            return '%s POSITION %d' % (sql, new_position)
        elif new_type or new_expr:
            result = sql
            if new_type:
                result += ' TYPE %s' % new_type
            if new_expr:
                result += ' COMPUTED BY %s' % new_expr
            return result
        elif 'restart' in params:
            restart = params.get('restart')
            sql += ' RESTART'
            if restart is not None:
                sql += ' WITH %d' % restart
            return sql
        else:
            raise fdb.ProgrammingError("Parameter required.")
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'ALTER TABLE %s DROP %s' % (self.table.get_quoted_name(),
                                           self.get_quoted_name())
    def _get_comment_sql(self, **params):
        return 'COMMENT ON COLUMN %s.%s IS %s' % (self.table.get_quoted_name(),
                                                  self.get_quoted_name(),
                                                  'NULL' if self.description is None
                                                  else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$FIELD_NAME']
    def _get_id(self):
        return self._attributes['RDB$FIELD_ID']
    def _get_table(self):
        return self.__table
    def _get_domain(self):
        return self.schema.get_domain(self._attributes['RDB$FIELD_SOURCE'])
    def _get_position(self):
        return self._attributes['RDB$FIELD_POSITION']
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_default(self):
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    def _get_collation(self):
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    def _get_datatype(self):
        return self.domain.datatype
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: (p.subject_name == self.table.name and
                                                        p.field_name == self.name and
                                                        p.subject_type in self.table._type_code))
    def _get_generator(self):
        return self.schema.get_generator(self._attributes.get('RDB$GENERATOR_NAME'))
    def _get_identity_type(self):
        return self._attributes.get('RDB$IDENTITY_TYPE')

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Internam number ID for the column.")
    table = LateBindingProperty(_get_table, doc="The Table object this column belongs to.")
    domain = LateBindingProperty(_get_domain, doc="Domain object this column is based on.")
    position = LateBindingProperty(_get_position, doc="Column's sequence number in row.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    default = LateBindingProperty(_get_default, doc="Default value for column or None.")
    collation = LateBindingProperty(_get_collation, doc="Collation object or None.")
    datatype = LateBindingProperty(_get_datatype, doc="Comlete SQL datatype definition.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    # FB 3.0
    generator = LateBindingProperty(_get_generator, doc="Internal flags.")
    identity_type = LateBindingProperty(_get_identity_type, doc="Internal flags.")
    #--- Public
    def get_dependents(self):
        "Return list of all database objects that depend on this one."
        return [d for d in self.schema.dependencies
                if d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 0 and d.field_name == self.name]
    def get_dependencies(self):
        "Return list of database objects that this object depend on."
        return [d for d in self.schema.dependencies
                if d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 0 and d.field_name == self.name]
    def get_computedby(self):
        "Returns (string) extression for column computation or None."
        return self.domain.expression
    def iscomputed(self):
        "Returns True if column is computed."
        return bool(self.domain.expression)
    def isdomainbased(self):
        "Returns True if column is based on user domain."
        return not self.domain.issystemobject()
    def isnullable(self):
        "Returns True if column can accept NULL values."
        return not self._attributes['RDB$NULL_FLAG']
    def iswritable(self):
        "Returns True if column is writable (i.e. it's not computed etc.)."
        return bool(self._attributes['RDB$UPDATE_FLAG'])
    def isidentity(self):
        "Returns True for identity type column."
        return self._attributes.get('RDB$IDENTITY_TYPE') is not None
    def has_default(self):
        "Returns True if column has default value."
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))

class Index(BaseSchemaItem):
    """Represents database index.

    Supported SQL actions:
        - User index: `create`, `activate`, `deactivate`, `recompute`, `drop`, `comment`
        - System index: `activate`, `recompute`, `comment`
    """
    def __init__(self, schema, attributes):
        super(Index, self).__init__(schema, attributes)
        self._type_code = [6, 10]

        self.__segment_names = None
        self.__segment_statistics = None
        self._strip_attribute('RDB$INDEX_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FOREIGN_KEY')

        self._actions = ['activate', 'recompute', 'comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'deactivate', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        return """CREATE %s%s INDEX %s ON %s %s""" % ('UNIQUE ' if self.isunique() else '',
                                                      self.index_type, self.get_quoted_name(), self.table.name,
                                                      'COMPUTED BY %s' % self.expression if self.isexpression()
                                                      else '(%s)' % ','.join(self.segment_names))
    def _get_activate_sql(self, **params):
        self._check_params(params, [])
        return 'ALTER INDEX %s ACTIVE' % self.get_quoted_name()
    def _get_deactivate_sql(self, **params):
        self._check_params(params, [])
        return 'ALTER INDEX %s INACTIVE' % self.get_quoted_name()
    def _get_recompute_sql(self, **params):
        self._check_params(params, [])
        return 'SET STATISTICS INDEX %s' % self.get_quoted_name()
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP INDEX %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON INDEX %s IS %s' % (self.get_quoted_name(),
                                              'NULL' if self.description is None
                                              else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$INDEX_NAME']
    def _get_table(self):
        return self.schema.get_table(self._attributes['RDB$RELATION_NAME'])
    def _get_id(self):
        return self._attributes['RDB$INDEX_ID']
    def _get_index_type(self):
        return (INDEX_TYPE_DESCENDING if self._attributes['RDB$INDEX_TYPE'] == 1
                else INDEX_TYPE_ASCENDING)
    def _get_partner_index(self):
        pname = self._attributes['RDB$FOREIGN_KEY']
        return self.schema.get_index(pname) if pname else None
    def _get_expression(self):
        return self._attributes['RDB$EXPRESSION_SOURCE']
    def _get_statistics(self):
        return self._attributes['RDB$STATISTICS']
    def _get_segments(self):
        return ObjectList(self.table.get_column(colname) for colname in self.segment_names)
    def _get_segment_names(self):
        if self.__segment_names is None:
            if self._attributes['RDB$SEGMENT_COUNT'] > 0:
                self.__segment_names = [r['RDB$FIELD_NAME'].strip() for r
                                        in self.schema._select("""select rdb$field_name
from rdb$index_segments where rdb$index_name = ? order by rdb$field_position""", (self.name,))]
            else:
                self.__segment_names = []
        return self.__segment_names
    def _get_segment_statistics(self):
        if self.__segment_statistics is None:
            if self._attributes['RDB$SEGMENT_COUNT'] > 0:
                if self.schema._con.ods >= fdb.ODS_FB_21:
                    self.__segment_statistics = [r['RDB$STATISTICS'] for r
                                                 in self.schema._select("""select RDB$STATISTICS
from rdb$index_segments where rdb$index_name = ? order by rdb$field_position""", (self.name,))]
                else:
                    self.__segment_statistics = [None for x in range(self._attributes['RDB$SEGMENT_COUNT'])]
            else:
                self.__segment_statistics = []
        return self.__segment_statistics
    def _get_constraint(self):
        const_name = self.schema._get_constraint_indices().get(self.name)
        if const_name:
            return self.schema.get_constraint(const_name)
        else:
            return None

    #--- Properties
    table = LateBindingProperty(_get_table, doc="The :class:`Table` instance the index applies to.")
    id = LateBindingProperty(_get_id, doc="Internal number ID of the index.")
    index_type = LateBindingProperty(_get_index_type, doc="ASCENDING or DESCENDING.")
    partner_index = LateBindingProperty(_get_partner_index, doc="Associated unique/primary key :class:`Index` instance, or None.")
    expression = LateBindingProperty(_get_expression, doc="Source of an expression or None.")
    statistics = LateBindingProperty(_get_statistics, doc="Latest selectivity of the index.")
    segment_names = LateBindingProperty(_get_segment_names, doc="List of index segment names.")
    segment_statistics = LateBindingProperty(_get_segment_statistics, doc="List of index segment statistics (for ODS 11.1 and higher).")
    segments = LateBindingProperty(_get_segments, doc=":class:`~fdb.utils.ObjectList` of index segments as :class:`TableColumn` instances.")
    constraint = LateBindingProperty(_get_constraint, doc=":class:`Constraint` instance that uses this index or None.")
    #--- Public
    def issystemobject(self):
        "Returns True if this database object is system object."
        return bool(self._attributes['RDB$SYSTEM_FLAG']
                    or (self.isenforcer() and self.name.startswith('RDB$')))
    def isexpression(self):
        "Returns True if index is expression index."
        return not self.segments
    def isunique(self):
        "Returns True if index is UNIQUE."
        return self._attributes['RDB$UNIQUE_FLAG'] == 1
    def isinactive(self):
        "Returns True if index is INACTIVE."
        return self._attributes['RDB$INDEX_INACTIVE'] == 1
    def isenforcer(self):
        "Returns True if index is used to enforce a constraint."
        return self.name in self.schema._get_constraint_indices()

class ViewColumn(BaseSchemaItem):
    """Represents view column.

    Supported SQL actions:
        `comment`
    """
    def __init__(self, schema, view, attributes):
        super(ViewColumn, self).__init__(schema, attributes)
        self._type_code = [3, 9]

        self.__view = weakref.proxy(view)
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$BASE_FIELD')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('BASE_RELATION')

        self._actions = ['comment']

    #--- Protected
    def _get_comment_sql(self, **params):
        return 'COMMENT ON COLUMN %s.%s IS %s' % (self.view.get_quoted_name(),
                                                  self.get_quoted_name(),
                                                  'NULL' if self.description is None
                                                  else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$FIELD_NAME']
    def _get_base_field(self):
        bfield = self._attributes['RDB$BASE_FIELD']
        if bfield:
            brel = self._attributes['BASE_RELATION']
            relation = self.schema.get_table(brel)
            if relation:
                return relation.get_column(bfield)
            relation = self.schema.get_view(brel)
            if relation:
                return relation.get_column(bfield)
            relation = self.schema.get_procedure(brel)
            if relation:
                return relation.get_outparam(bfield)
            raise fdb.OperationalError("Can't locate base relation.")
        return None
    def _get_view(self):
        return self.__view
    def _get_domain(self):
        return self.schema.get_domain(self._attributes['RDB$FIELD_SOURCE'])
    def _get_position(self):
        return self._attributes['RDB$FIELD_POSITION']
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_collation(self):
        return self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    def _get_datatype(self):
        return self.domain.datatype
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: (p.subject_name == self.view.name and
                                                        p.field_name == self.name and
                                                        p.subject_type == 0)) # Views are logged as Tables in RDB$USER_PRIVILEGES

    #--- Properties
    base_field = LateBindingProperty(_get_base_field, doc="The source column from the base relation. Result could be either "
                                     ":class:`TableColumn`, :class:`ViewColumn` or :class:`ProcedureParameter` "
                                     "instance or None.")
    view = LateBindingProperty(_get_view, doc="View object this column belongs to.")
    domain = LateBindingProperty(_get_domain, doc="Domain object this column is based on.")
    position = LateBindingProperty(_get_position, doc="Column's sequence number in row.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    collation = LateBindingProperty(_get_collation, doc="Collation object or None.")
    datatype = LateBindingProperty(_get_datatype, doc="Comlete SQL datatype definition.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    #--- Public
    def get_dependents(self):
        "Return list of all database objects that depend on this one."
        return [d for d in self.schema.dependencies
                if d.depended_on_name == self._attributes['RDB$RELATION_NAME']
                and d.depended_on_type == 1 and d.field_name == self.name]
    def get_dependencies(self):
        "Return list of database objects that this object depend on."
        return [d for d in self.schema.dependencies
                if d.dependent_name == self._attributes['RDB$RELATION_NAME']
                and d.dependent_type == 1 and d.field_name == self.name]
    def isnullable(self):
        "Returns True if column is NULLABLE."
        return not self._attributes['RDB$NULL_FLAG']
    def iswritable(self):
        "Returns True if column is writable."
        return bool(self._attributes['RDB$UPDATE_FLAG'])

class Domain(BaseSchemaItem):
    """Represents SQl Domain.

    Supported SQL actions:
        - User domain: `create`, `drop`, `comment`,
          `alter` (name=string, default=string_definition_or_None,
          check=string_definition_or_None, datatype=string_SQLTypeDef)
        - System domain: `comment`
    """
    def __init__(self, schema, attributes):
        super(Domain, self).__init__(schema, attributes)
        self._type_code = [9]

        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'alter', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        sql = 'CREATE DOMAIN %s AS %s' % (self.get_quoted_name(), self.datatype)
        if self.has_default():
            sql += ' DEFAULT %s' % self.default
        if not self.isnullable():
            sql += ' NOT NULL'
        if self.isvalidated():
            sql += ' ' + self.validation
        if self._attributes['RDB$COLLATION_ID']:
            #sql += ' COLLATE %s' % self.collation.get_quoted_name()
            if self.character_set._attributes['RDB$DEFAULT_COLLATE_NAME'] != self.collation.name:
                sql += ' COLLATE %s' % self.collation.get_quoted_name()
        return sql
    def _get_alter_sql(self, **params):
        self._check_params(params, ['name', 'default', 'check', 'datatype'])
        new_name = params.get('name')
        new_default = params.get('default', '')
        new_constraint = params.get('check', '')
        new_type = params.get('datatype')
        sql = 'ALTER DOMAIN %s' % self.get_quoted_name()
        if len(params) > 1:
            raise fdb.ProgrammingError("Only one parameter allowed.")
        if new_name:
            return '%s TO %s' % (sql, self._get_quoted_ident(new_name))
        elif new_default != '':
            return ('%s SET DEFAULT %s' % (sql, new_default) if new_default
                    else '%s DROP DEFAULT' % sql)
        elif new_constraint != '':
            return ('%s ADD CHECK (%s)' % (sql, new_constraint) if new_constraint
                    else '%s DROP CONSTRAINT' % sql)
        elif new_type:
            return '%s TYPE %s' % (sql, new_type)
        else:
            raise fdb.ProgrammingError("Parameter required.")
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP DOMAIN %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON DOMAIN %s IS %s' % (self.get_quoted_name(),
                                               'NULL' if self.description is None
                                               else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$FIELD_NAME']
    def _get_expression(self):
        return self._attributes['RDB$COMPUTED_SOURCE']
    def _get_validation(self):
        return self._attributes['RDB$VALIDATION_SOURCE']
    def _get_default(self):
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    def _get_length(self):
        return self._attributes['RDB$FIELD_LENGTH']
    def _get_scale(self):
        return self._attributes['RDB$FIELD_SCALE']
    def _get_field_type(self):
        return self._attributes['RDB$FIELD_TYPE']
    def _get_sub_type(self):
        return self._attributes['RDB$FIELD_SUB_TYPE']
    def _get_segment_length(self):
        return self._attributes['RDB$SEGMENT_LENGTH']
    def _get_external_length(self):
        return self._attributes['RDB$EXTERNAL_LENGTH']
    def _get_external_scale(self):
        return self._attributes['RDB$EXTERNAL_SCALE']
    def _get_external_type(self):
        return self._attributes['RDB$EXTERNAL_TYPE']
    def _get_dimensions(self):
        if self._attributes['RDB$DIMENSIONS']:
            return self.schema._get_field_dimensions(self)
        else:
            return []
    def _get_character_length(self):
        return self._attributes['RDB$CHARACTER_LENGTH']
    def _get_collation(self):
        return self.schema.get_collation_by_id(self._attributes['RDB$CHARACTER_SET_ID'],
                                               self._attributes['RDB$COLLATION_ID'])
    def _get_character_set(self):
        return self.schema.get_character_set_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    def _get_precision(self):
        return self._attributes['RDB$FIELD_PRECISION']
    def _get_datatype(self):
        l = []
        precision_known = False
        if self.field_type in (FBT_SMALLINT, FBT_INTEGER, FBT_BIGINT):
            if self.precision != None:
                if (self.sub_type > 0) and (self.sub_type <= MAX_INTSUBTYPES):
                    l.append('%s(%d, %d)' % \
                      (INTEGRAL_SUBTYPES[self.sub_type], self.precision, -self.scale))
                    precision_known = True
        if not precision_known:
            if (self.field_type == FBT_SMALLINT) and (self.scale < 0):
                l.append('NUMERIC(4, %d)' % -self.scale)
            elif (self.field_type == FBT_INTEGER) and (self.scale < 0):
                l.append('NUMERIC(9, %d)' % -self.scale)
            elif (self.field_type == FBT_DOUBLE_PRECISION) and (self.scale < 0):
                l.append('NUMERIC(15, %d)' % -self.scale)
            else:
                l.append(COLUMN_TYPES[self.field_type])
        if self.field_type in (FBT_CHAR, FBT_VARCHAR):
            l.append('(%d)' % (self.length if self.character_length is None else self.character_length))
        if self._attributes['RDB$DIMENSIONS'] != None:
            l.append('[%s]' % ', '.join('%d' % u if l == 1
                                        else '%d:%d' % (l, u)
                                        for l, u in self.dimensions))
        if self.field_type == FBT_BLOB:
            if self.sub_type >= 0 and self.sub_type <= MAX_BLOBSUBTYPES:
                l.append(' SUB_TYPE %s' % BLOB_SUBTYPES[self.sub_type])
            else:
                l.append(' SUB_TYPE %d' % self.sub_type)
            l.append(' SEGMENT SIZE %d' % self.segment_length)
        if self.field_type in (FBT_CHAR, FBT_VARCHAR, FBT_BLOB):
            if self._attributes['RDB$CHARACTER_SET_ID'] is not None and \
              (self.character_set.name != self.schema.default_character_set.name) or \
              self._attributes['RDB$COLLATION_ID']:
                if self._attributes['RDB$CHARACTER_SET_ID'] is not None:
                    l.append(' CHARACTER SET %s' % self.character_set.name)
        return ''.join(l)
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')

    #--- Properties
    expression = LateBindingProperty(_get_expression, doc="Expression that defines the COMPUTED BY column or None.")
    validation = LateBindingProperty(_get_validation, doc="CHECK constraint for the domain or None.")
    default = LateBindingProperty(_get_default, doc="Expression that defines the default value or None.")
    length = LateBindingProperty(_get_length, doc="Length of the column in bytes.")
    scale = LateBindingProperty(_get_scale, doc="Negative number representing the scale of NUMBER and DECIMAL column.")
    field_type = LateBindingProperty(_get_field_type, doc="Number code of the data type defined for the column.")
    sub_type = LateBindingProperty(_get_sub_type, doc="BLOB subtype.")
    segment_length = LateBindingProperty(_get_segment_length, doc="For BLOB columns, a suggested length for BLOB buffers.")
    external_length = LateBindingProperty(_get_external_length, doc="Length of field as it is in an external table. Always 0 for regular tables.")
    external_scale = LateBindingProperty(_get_external_scale, doc="Scale factor of an integer field as it is in an external table.")
    external_type = LateBindingProperty(_get_external_type, doc="Data type of the field as it is in an external table.")
    dimensions = LateBindingProperty(_get_dimensions, doc="List of dimension definition pairs if column is an array type. Always empty for non-array columns.")
    character_length = LateBindingProperty(_get_character_length, doc="Length of CHAR and VARCHAR column, in characters (not bytes).")
    collation = LateBindingProperty(_get_collation, doc="Collation object for a character column or None.")
    character_set = LateBindingProperty(_get_character_set, doc="CharacterSet object for a character or text BLOB column, or None.")
    precision = LateBindingProperty(_get_precision, doc="Indicates the number of digits of precision available to the data type of the column.")
    datatype = LateBindingProperty(_get_datatype, doc="Comlete SQL datatype definition.")
    # FB 3.0
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Creator user name.")
    #--- Public
    def issystemobject(self):
        "Return True if this database object is system object."
        return (self._attributes['RDB$SYSTEM_FLAG'] == 1) or self.name.startswith('RDB$')
    def isnullable(self):
        "Returns True if domain is not defined with NOT NULL."
        return not self._attributes['RDB$NULL_FLAG']
    def iscomputed(self):
        "Returns True if domain is computed."
        return bool(self._attributes['RDB$COMPUTED_SOURCE'])
    def isvalidated(self):
        "Returns True if domain has validation constraint."
        return bool(self._attributes['RDB$VALIDATION_SOURCE'])
    def isarray(self):
        "Returns True if domain defines an array."
        return bool(self._attributes['RDB$DIMENSIONS'])
    def has_default(self):
        "Returns True if domain has default value."
        return bool(self._attributes['RDB$DEFAULT_SOURCE'])

class Dependency(BaseSchemaItem):
    """Maps dependency between database objects.

    Supported SQL actions:
        `none`
    """
    def __init__(self, schema, attributes):
        super(Dependency, self).__init__(schema, attributes)

        self._strip_attribute('RDB$DEPENDENT_NAME')
        self._strip_attribute('RDB$DEPENDED_ON_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')

    #--- Protected
    def _get_dependent_name(self):
        return self._attributes['RDB$DEPENDENT_NAME']
    def _get_dependent_type(self):
        return self._attributes['RDB$DEPENDENT_TYPE']
    def _get_field_name(self):
        return self._attributes['RDB$FIELD_NAME']
    def _get_depended_on_name(self):
        return self._attributes['RDB$DEPENDED_ON_NAME']
    def _get_depended_on_type(self):
        return self._attributes['RDB$DEPENDED_ON_TYPE']
    def _get_dependent(self):
        if self.dependent_type == 0: # TABLE
            t = self.schema.get_table(self.dependent_name)
        elif self.dependent_type == 1: # VIEW
            return self.schema.get_view(self.dependent_name)
        elif self.dependent_type == 2: # TRIGGER
            return self.schema.get_trigger(self.dependent_name)
        elif self.dependent_type == 3: # COMPUTED FIELD (i.e. DOMAIN)
            return self.schema.get_domain(self.dependent_name)
        elif self.dependent_type == 4:
            ## ToDo: Implement handler for VALIDATION if necessary
            return None
        elif self.dependent_type == 5: #PROCEDURE
            return self.schema.get_procedure(self.dependent_name)
        elif self.dependent_type == 6: # EXPRESSION INDEX
            return self.schema.get_index(self.dependent_name)
        elif self.dependent_type == 7: # EXCEPTION
            return self.schema.get_exception(self.dependent_name)
        elif self.dependent_type == 8:
            ## ToDo: Implement handler for USER if necessary
            return None
        elif self.dependent_type == 9: # FIELD (i.e. DOMAIN)
            return self.schema.get_domain(self.dependent_name)
        elif self.dependent_type == 10: # INDEX
            return self.schema.get_index(self.dependent_name)
        elif self.dependent_type == 11:
            ## ToDo: Implement handler for DEPENDENT COUNT if necessary
            return None
        elif self.dependent_type == 12:
            ## ToDo: Implement handler for USER GROUP if necessary
            return None
        elif self.dependent_type == 13: # ROLE
            return self.schema.get_role(self.dependent_name)
        elif self.dependent_type == 14: # GENERATOR
            return self.schema.get_generator(self.dependent_name)
        elif self.dependent_type == 15: # UDF
            return self.schema.get_function(self.dependent_name)
        elif self.dependent_type == 16:
            ## ToDo: Implement handler for BLOB_FILTER
            return None
        elif self.dependent_type == 17: # Collation
            return self.schema.get_collation(self.dependent_name)
        elif self.dependent_type in [18, 19]: # Package + package body
            return self.schema.get_package(self.dependent_name)
        return None
    def _get_depended_on(self):
        if self.depended_on_type == 0: # TABLE
            t = self.schema.get_table(self.depended_on_name)
            if self.field_name:
                return t.get_column(self.field_name)
            else:
                return t
        elif self.depended_on_type == 1: # VIEW
            t = self.schema.get_view(self.depended_on_name)
            if self.field_name:
                return t.get_column(self.field_name)
            else:
                return t
        elif self.depended_on_type == 2: # TRIGGER
            return self.schema.get_trigger(self.depended_on_name)
        elif self.depended_on_type == 3: # COMPUTED FIELD (i.e. DOMAIN)
            return self.schema.get_domain(self.depended_on_name)
        elif self.depended_on_type == 4:
            ## ToDo: Implement handler for VALIDATION if necessary
            return None
        elif self.depended_on_type == 5: #PROCEDURE
            return self.schema.get_procedure(self.depended_on_name)
        elif self.depended_on_type == 6: # EXPRESSION INDEX
            return self.schema.get_index(self.depended_on_name)
        elif self.depended_on_type == 7: # EXCEPTION
            return self.schema.get_exception(self.depended_on_name)
        elif self.depended_on_type == 8:
            ## ToDo: Implement handler for USER if necessary
            return None
        elif self.depended_on_type == 9: # FIELD (i.e. DOMAIN)
            return self.schema.get_domain(self.depended_on_name)
        elif self.depended_on_type == 10: # INDEX
            return self.schema.get_index(self.depended_on_name)
        elif self.depended_on_type == 11:
            ## ToDo: Implement handler for DEPENDENT COUNT if necessary
            return None
        elif self.depended_on_type == 12:
            ## ToDo: Implement handler for USER GROUP if necessary
            return None
        elif self.depended_on_type == 13: # ROLE
            return self.schema.get_role(self.depended_on_name)
        elif self.depended_on_type == 14: # GENERATOR
            return self.schema.get_generator(self.depended_on_name)
        elif self.depended_on_type == 15: # UDF
            return self.schema.get_function(self.depended_on_name)
        elif self.depended_on_type == 16:
            ## ToDo: Implement handler for BLOB_FILTER
            return None
        return None
    def _get_package(self):
        return self.schema.get_package(self._attributes.get('RDB$PACKAGE_NAME'))

    #--- Properties
    dependent = LateBindingProperty(_get_dependent, doc="Dependent database object.")
    dependent_name = LateBindingProperty(_get_dependent_name, doc="Dependent database object name.")
    dependent_type = LateBindingProperty(_get_dependent_type, doc="Dependent database object type.")
    field_name = LateBindingProperty(_get_field_name, doc="Name of one column in `depended on` object.")
    depended_on = LateBindingProperty(_get_depended_on, doc="Database object on which dependent depends.")
    depended_on_name = LateBindingProperty(_get_depended_on_name, doc="Name of db object on which dependent depends.")
    depended_on_type = LateBindingProperty(_get_depended_on_type, doc="Type of db object on which dependent depends.")
    # FB 3.0
    package = LateBindingProperty(_get_package, doc=":class:`Package` instance if dependent depends on object in package or None.")
    #--- Public
    def issystemobject(self):
        "Returns True as dependency entries are considered as system objects."
        return True
    def get_dependents(self):
        "Returns empty list because Dependency object never has dependents."
        return []
    def get_dependencies(self):
        "Returns empty list because Dependency object never has dependencies."
        return []
    def ispackaged(self):
        "Returns True if dependency is defined in package."
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))

class Constraint(BaseSchemaItem):
    """Represents table or column constraint.

    Supported SQL actions:
        - Constraint on user table except NOT NULL constraint: `create`, `drop`
        - Constraint on system table: `none`
    """
    def __init__(self, schema, attributes):
        super(Constraint, self).__init__(schema, attributes)

        self._strip_attribute('RDB$CONSTRAINT_NAME')
        self._strip_attribute('RDB$CONSTRAINT_TYPE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$DEFERRABLE')
        self._strip_attribute('RDB$INITIALLY_DEFERRED')
        self._strip_attribute('RDB$INDEX_NAME')
        self._strip_attribute('RDB$TRIGGER_NAME')
        self._strip_attribute('RDB$CONST_NAME_UQ')
        self._strip_attribute('RDB$MATCH_OPTION')
        self._strip_attribute('RDB$UPDATE_RULE')
        self._strip_attribute('RDB$DELETE_RULE')

        if not (self.issystemobject() or self.isnotnull()):
            self._actions = ['create', 'drop']

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        const_def = 'ALTER TABLE %s ADD ' % self.table.get_quoted_name()
        if not self.name.startswith('INTEG_'):
            const_def += 'CONSTRAINT %s\n  ' % self.get_quoted_name()
        if self.ischeck():
            const_def += self.triggers[0].source
        elif self.ispkey() or self.isunique():
            const_def += 'PRIMARY KEY' if self.ispkey() else 'UNIQUE'
            i = self.index
            const_def += ' (%s)' % ','.join(i.segment_names)
            if not i.issystemobject():
                const_def += '\n  USING %s INDEX %s' % (i.index_type, i.get_quoted_name())
        elif self.isfkey():
            const_def += 'FOREIGN KEY (%s)\n  ' % ','.join(self.index.segment_names)
            p = self.partner_constraint
            const_def += 'REFERENCES %s (%s)' % (p.table.get_quoted_name(),
                                                 ','.join(p.index.segment_names))
            if self.delete_rule != 'RESTRICT':
                const_def += '\n  ON DELETE %s' % self.delete_rule
            if self.update_rule != 'RESTRICT':
                const_def += '\n  ON UPDATE %s' % self.update_rule
            i = self.index
            if not i.issystemobject():
                const_def += '\n  USING %s INDEX %s' % (i.index_type, i.get_quoted_name())
        else:
            raise fdb.OperationalError("Unrecognized constraint type '%s'" % self.constraint_type)
        return const_def
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'ALTER TABLE %s DROP CONSTRAINT %s' % (self.table.get_quoted_name(),
                                                      self.get_quoted_name())
    def _get_name(self):
        return self._attributes['RDB$CONSTRAINT_NAME']
    def _get_constraint_type(self):
        return self._attributes['RDB$CONSTRAINT_TYPE']
    def _get_table(self):
        return self.schema.get_table(self._attributes['RDB$RELATION_NAME'])
    def _get_index(self):
        return self.schema.get_index(self._attributes['RDB$INDEX_NAME'])
    def _get_trigger_names(self):
        if self.ischeck():
            return self._attributes['RDB$TRIGGER_NAME']
        else:
            return []
    def _get_triggers(self):
        return [self.schema.get_trigger(tname) for tname in self.trigger_names]
    def _get_column_name(self):
        if self.isnotnull():
            return self._attributes['RDB$TRIGGER_NAME']
        else:
            return None
    def _get_partner_constraint(self):
        return self.schema.get_constraint(self._attributes['RDB$CONST_NAME_UQ'])
    def _get_match_option(self):
        return self._attributes['RDB$MATCH_OPTION']
    def _get_update_rule(self):
        return self._attributes['RDB$UPDATE_RULE']
    def _get_delete_rule(self):
        return self._attributes['RDB$DELETE_RULE']

    #--- Properties
    constraint_type = LateBindingProperty(_get_constraint_type, doc="primary key/unique/foreign key/check/not null.")
    table = LateBindingProperty(_get_table, doc=":class:`Table` instance this constraint applies to.")
    index = LateBindingProperty(_get_index, doc=":class:`Index` instance that enforces the constraint.\n`None` if constraint is not primary key/unique or foreign key.")
    trigger_names = LateBindingProperty(_get_trigger_names, doc="For a CHECK constraint contains trigger names that enforce the constraint.")
    triggers = LateBindingProperty(_get_triggers, doc="For a CHECK constraint contains :class:`Trigger` instances that enforce the constraint.")
    column_name = LateBindingProperty(_get_column_name, doc="For a NOT NULL constraint, this is the name of the column to which the constraint applies.")
    partner_constraint = LateBindingProperty(_get_partner_constraint, doc="For a FOREIGN KEY constraint, this is the unique or primary key :class:`Constraint` referred.")
    match_option = LateBindingProperty(_get_match_option, doc="For a FOREIGN KEY constraint only. Current value is FULL in all cases.")
    update_rule = LateBindingProperty(_get_update_rule, doc="For a FOREIGN KEY constraint, this is the action applicable to when primary key is updated.")
    delete_rule = LateBindingProperty(_get_delete_rule, doc="For a FOREIGN KEY constraint, this is the action applicable to when primary key is deleted.")
    #--- Public
    def issystemobject(self):
        "Returns True if this database object is system object."
        return self.schema.get_table(self._attributes['RDB$RELATION_NAME']).issystemobject()
    def isnotnull(self):
        "Returns True if it's NOT NULL constraint."
        return self.constraint_type == 'NOT NULL'
    def ispkey(self):
        "Returns True if it's PRIMARY KEY constraint."
        return self.constraint_type == 'PRIMARY KEY'
    def isfkey(self):
        "Returns True if it's FOREIGN KEY constraint."
        return self.constraint_type == 'FOREIGN KEY'
    def isunique(self):
        "Returns True if it's UNIQUE constraint."
        return self.constraint_type == 'UNIQUE'
    def ischeck(self):
        "Returns True if it's CHECK constraint."
        return self.constraint_type == 'CHECK'
    def isdeferrable(self):
        "Returns True if it's DEFERRABLE constraint."
        return self._attributes['RDB$DEFERRABLE'] != 'NO'
    def isdeferred(self):
        "Returns True if it's INITIALLY DEFERRED constraint."
        return self._attributes['RDB$INITIALLY_DEFERRED'] != 'NO'

class Table(BaseSchemaItem):
    """Represents Table in database.

    Supported SQL actions:
        - User table: `create` (no_pk=bool, no_unique=bool),
                      `recreate` (no_pk=bool, no_unique=bool),
                      `drop`, `comment`
        - System table: `comment`
    """
    def __init__(self, schema, attributes):
        super(Table, self).__init__(schema, attributes)
        self._type_code = [0,]

        self.__columns = None

        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$DEFAULT_CLASS')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'recreate', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        try:
            self._check_params(params, ['no_pk', 'no_unique'])
            no_pk = params.get('no_pk', False)
            no_unique = params.get('no_unique', False)
            #
            tabdef = 'CREATE %sTABLE %s' % ('GLOBAL TEMPORARY ' if self.isgtt() else '',
                                            self.get_quoted_name())
            if self.isexternal():
                tabdef += "  EXTERNAL FILE '%s'\n" % self.external_file
            tabdef += ' ('
            partdefs = []
            for col in self.columns:
                coldef = '\n  %s ' % col.get_quoted_name()
                collate = ''
                if col.isdomainbased():
                    coldef += '%s' % col.domain.get_quoted_name()
                elif col.iscomputed():
                    coldef += 'COMPUTED BY %s' % col.get_computedby()
                else:
                    datatype = col.datatype
                    if datatype.rfind(' COLLATE ') > 0:
                        datatype, collate = datatype.split(' COLLATE ')
                    coldef += '%s' % datatype
                if col.isidentity():
                    coldef += ' GENERATED BY DEFAULT AS IDENTITY'
                    if col.generator.inital_value != 0:
                        coldef += ' (START WITH %d)' % col.generator.inital_value
                else:
                    if col.has_default():
                        coldef += ' DEFAULT %s' % col.default
                    if not col.isnullable():
                        coldef += ' NOT NULL'
                    if col._attributes['RDB$COLLATION_ID'] is not None:
                        # Sometimes RDB$COLLATION_ID has a garbage value
                        if col.collation is not None:
                            cname = col.collation.name
                            if col.domain.character_set._attributes['RDB$DEFAULT_COLLATE_NAME'] != cname:
                                collate = cname
                if collate:
                    coldef += ' COLLATE %s' % collate
                partdefs.append(coldef)
            if self.has_pkey() and not no_pk:
                pk = self.primary_key
                pkdef = '\n  '
                if not pk.name.startswith('INTEG_'):
                    pkdef += 'CONSTRAINT %s\n  ' % pk.get_quoted_name()
                i = pk.index
                pkdef += 'PRIMARY KEY (%s)' % ','.join(i.segment_names)
                if not i.issystemobject():
                    pkdef += '\n    USING %s INDEX %s' % (i.index_type, i.get_quoted_name())
                partdefs.append(pkdef)
            if not no_unique:
                for uq in self.constraints:
                    if uq.isunique():
                        uqdef = '\n  '
                        if not uq.name.startswith('INTEG_'):
                            uqdef += 'CONSTRAINT %s\n  ' % uq.get_quoted_name()
                        i = uq.index
                        uqdef += 'UNIQUE (%s)' % ','.join(i.segment_names)
                        if not i.issystemobject():
                            uqdef += '\n    USING %s INDEX %s' % (i.index_type, i.get_quoted_name())
                        partdefs.append(uqdef)
            tabdef += ','.join(partdefs)
            tabdef += '\n)'
            return tabdef
        except Exception as e:
            raise e
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP TABLE %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON TABLE %s IS %s' % (self.get_quoted_name(),
                                              'NULL' if self.description is None
                                              else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$RELATION_NAME']
    def _get_id(self):
        return self._attributes['RDB$RELATION_ID']
    def _get_dbkey_length(self):
        return self._attributes['RDB$DBKEY_LENGTH']
    def _get_format(self):
        return self._attributes['RDB$FORMAT']
    def _get_table_type(self):
        return self.schema.enum_relation_types.get(self._attributes.get('RDB$RELATION_TYPE'),
                                                   'PERSISTENT')
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_external_file(self):
        return self._attributes['RDB$EXTERNAL_FILE']
    def _get_owner_name(self):
        return self._attributes['RDB$OWNER_NAME']
    def _get_default_class(self):
        return self._attributes['RDB$DEFAULT_CLASS']
    def _get_flags(self):
        return self._attributes['RDB$FLAGS']
    def _get_indices(self):
        user_indices, sys_indices = self.schema._get_all_indices()
        result = user_indices.filter(lambda i: i._attributes['RDB$RELATION_NAME'] == self.name)
        result.extend(sys_indices.filter(lambda i: i._attributes['RDB$RELATION_NAME'] == self.name))
        return result
    def _get_triggers(self):
        return self.schema.triggers.filter(lambda t: t._attributes['RDB$RELATION_NAME'] == self.name)
    def _get_constraints(self):
        return self.schema.constraints.filter(lambda c: c._attributes['RDB$RELATION_NAME'] == self.name)
    def _get_columns(self):
        if self.__columns is None:
            cols = ['RDB$FIELD_NAME', 'RDB$RELATION_NAME', 'RDB$FIELD_SOURCE',
                    'RDB$FIELD_POSITION', 'RDB$UPDATE_FLAG', 'RDB$FIELD_ID',
                    'RDB$DESCRIPTION', 'RDB$SECURITY_CLASS', 'RDB$SYSTEM_FLAG',
                    'RDB$NULL_FLAG', 'RDB$DEFAULT_SOURCE', 'RDB$COLLATION_ID']
            if self.schema._con.ods >= fdb.ODS_FB_30:
                cols.extend(['RDB$GENERATOR_NAME', 'RDB$IDENTITY_TYPE'])
            self.__columns = ObjectList((TableColumn(self.schema, self, row) for row in
                                         self.schema._select("""select %s from RDB$RELATION_FIELDS
where RDB$RELATION_NAME = ? order by RDB$FIELD_POSITION""" % ','.join(cols), (self.name,))), TableColumn, 'item.name')
            self.__columns.freeze()
        return self.__columns
    def _get_primary_key(self):
        for const in self.constraints:
            if const.ispkey():
                return const
        return None
    def _get_foreign_keys(self):
        return self.constraints.filter(lambda c: c.isfkey())
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: ((p.subject_name == self.name) and
                                                        (p.subject_type in self._type_code)))

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Internam number ID for the table.")
    dbkey_length = LateBindingProperty(_get_dbkey_length, doc="Length of the RDB$DB_KEY column in bytes.")
    format = LateBindingProperty(_get_format, doc="Internal format ID for the table.")
    table_type = LateBindingProperty(_get_table_type, doc="Table type.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class that define access limits to the table.")
    external_file = LateBindingProperty(_get_external_file, doc="Full path to the external data file, if any.")
    owner_name = LateBindingProperty(_get_owner_name, doc="User name of table's creator.")
    default_class = LateBindingProperty(_get_default_class, doc="Default security class.")
    flags = LateBindingProperty(_get_flags, doc="Internal flags.")
    primary_key = LateBindingProperty(_get_primary_key, doc="PRIMARY KEY :class:`Constraint` for this table or None.")
    foreign_keys = LateBindingProperty(_get_foreign_keys, doc=":class:`~fdb.utils.ObjectList` of FOREIGN KEY :class:`Constraint` instances for this table.")
    columns = LateBindingProperty(_get_columns, doc="Returns :class:`~fdb.utils.ObjectList` of columns defined for table.\nItems are :class:`TableColumn` objects.")
    constraints = LateBindingProperty(_get_constraints, doc="Returns :class:`~fdb.utils.ObjectList` of constraints defined for table.\nItems are :class:`Constraint` objects.")
    indices = LateBindingProperty(_get_indices, doc="Returns :class:`~fdb.utils.ObjectList` of indices defined for table.\nItems are :class:`Index` objects.")
    triggers = LateBindingProperty(_get_triggers, doc="Returns :class:`~fdb.utils.ObjectList` of triggers defined for table.\nItems are :class:`Trigger` objects.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    #--- Public
    def get_column(self, name):
        "Return :class:`TableColumn` object with specified name."
        return self.columns.get(name)
        #for col in self.columns:
            #if col.name == name:
                #return col
        #return None
    def isgtt(self):
        "Returns True if table is GLOBAL TEMPORARY table."
        return self.table_type.startswith('GLOBAL_TEMPORARY')
    def ispersistent(self):
        "Returns True if table is persistent one."
        return self.table_type in ['PERSISTENT', 'EXTERNAL']
    def isexternal(self):
        "Returns True if table is external table."
        return bool(self.external_file)
    def has_pkey(self):
        "Returns True if table has PRIMARY KEY defined."
        for const in self.constraints:
            if const.ispkey():
                return True
        return False
    def has_fkey(self):
        "Returns True if table has any FOREIGN KEY constraint."
        for const in self.constraints:
            if const.isfkey():
                return True
        return False

class View(BaseSchemaItem):
    """Represents database View.

    Supported SQL actions:
        - User views: `create`, `recreate`, `alter` (columns=string_or_list, query=string,check=bool),
             `create_or_alter`, `drop`, `comment`
        - System views: `comment`
    """
    def __init__(self, schema, attributes):
        super(View, self).__init__(schema, attributes)
        self._type_code = [1,]

        self.__columns = None

        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$VIEW_SOURCE')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$DEFAULT_CLASS')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        return "CREATE VIEW %s (%s)\n   AS\n     %s" % (self.get_quoted_name(),
                                                        ','.join([col.get_quoted_name()
                                                                  for col in self.columns]),
                                                        self.sql)
    def _get_alter_sql(self, **params):
        self._check_params(params, ['columns', 'query', 'check'])
        columns = params.get('columns')
        if isinstance(columns, (list, tuple)):
            columns = ','.join(columns)
        query = params.get('query')
        check = params.get('check', False)
        if query:
            return "ALTER VIEW %s %s\n   AS\n     %s" % (self.get_quoted_name(),
                                                         '(%s)' % columns if columns else '',
                                                         '%s\n     WITH CHECK OPTION' % query if check
                                                         else query)
        else:
            raise fdb.ProgrammingError("Missing required parameter: 'query'.")
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP VIEW %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON VIEW %s IS %s' % (self.get_quoted_name(),
                                             'NULL' if self.description is None
                                             else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$RELATION_NAME']
    def _get_sql(self):
        return self._attributes['RDB$VIEW_SOURCE']
    def _get_id(self):
        return self._attributes['RDB$RELATION_ID']
    def _get_dbkey_length(self):
        return self._attributes['RDB$DBKEY_LENGTH']
    def _get_format(self):
        return self._attributes['RDB$FORMAT']
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_owner_name(self):
        return self._attributes['RDB$OWNER_NAME']
    def _get_default_class(self):
        return self._attributes['RDB$DEFAULT_CLASS']
    def _get_flags(self):
        return self._attributes['RDB$FLAGS']
    def _get_triggers(self):
        return self.schema.triggers.filter(lambda t: t._attributes['RDB$RELATION_NAME'] == self.name)
    def _get_columns(self):
        if self.__columns is None:
            self.__columns = ObjectList((ViewColumn(self.schema, self, row) for row in
                                         self.schema._select("""select r.RDB$FIELD_NAME, r.RDB$RELATION_NAME,
r.RDB$FIELD_SOURCE, r.RDB$FIELD_POSITION, r.RDB$UPDATE_FLAG, r.RDB$FIELD_ID,
r.RDB$DESCRIPTION, r.RDB$SYSTEM_FLAG, r.RDB$SECURITY_CLASS, r.RDB$NULL_FLAG,
r.RDB$DEFAULT_SOURCE, r.RDB$COLLATION_ID, r.RDB$BASE_FIELD,
v.RDB$RELATION_NAME as BASE_RELATION
    from RDB$RELATION_FIELDS r
    left join RDB$VIEW_RELATIONS v on r.RDB$VIEW_CONTEXT = v.RDB$VIEW_CONTEXT and v.rdb$view_name = ?
    where r.RDB$RELATION_NAME = ?
    order by RDB$FIELD_POSITION""", (self.name, self.name))), ViewColumn, 'item.name')
            self.__columns.freeze()
        return self.__columns
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: ((p.subject_name == self.name) and
                                                        (p.subject_type == 0))) # Views are logged as Tables in RDB$USER_PRIVILEGES

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Internal number ID for the view.")
    sql = LateBindingProperty(_get_sql, doc="The query specification.")
    dbkey_length = LateBindingProperty(_get_dbkey_length, doc="Length of the RDB$DB_KEY column in bytes.")
    format = LateBindingProperty(_get_format, doc="Internal format ID for the view.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class that define access limits to the view.")
    owner_name = LateBindingProperty(_get_owner_name, doc="User name of view's creator.")
    default_class = LateBindingProperty(_get_default_class, doc="Default security class.")
    flags = LateBindingProperty(_get_flags, doc="Internal flags.")
    columns = LateBindingProperty(_get_columns, doc="Returns :class:`~fdb.utils.ObjectList` of columns defined for view.\nItems are :class:`ViewColumn` objects.")
    triggers = LateBindingProperty(_get_triggers, doc="Returns :class:`~fdb.utils.ObjectList` of triggers defined for view.\nItems are :class:`Trigger` objects.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    #--- Public
    def get_column(self, name):
        "Return :class:`TableColumn` object with specified name."
        for col in self.columns:
            if col.name == name:
                return col
        return None
    def get_trigger(self, name):
        "Return :class:`Trigger` object with specified name."
        for t in self.triggers:
            if t.name == name:
                return t
        return None
    def has_checkoption(self):
        "Returns True if View has WITH CHECK OPTION defined."
        return "WITH CHECK OPTION" in self.sql.upper()

class Trigger(BaseSchemaItem):
    """Represents trigger.

    Supported SQL actions:
        - User trigger: `create` (inactive=bool), `recreate`, `create_or_alter`, `drop`, `comment`,
          `alter` (fire_on=string, active=bool,sequence=int, declare=string_or_list, code=string_or_list)
        - System trigger: `comment`
    """
    def __init__(self, schema, attributes):
        super(Trigger, self).__init__(schema, attributes)
        self._type_code = [2,]

        self._strip_attribute('RDB$TRIGGER_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, ['inactive'])
        inactive = params.get('inactive', False)
        result = 'CREATE TRIGGER %s' % self.get_quoted_name()
        if self._attributes['RDB$RELATION_NAME']:
            result += ' FOR %s' % self.relation.get_quoted_name()
        result += ' %s\n%s POSITION %d\n%s' % ('ACTIVE' if self.isactive() and not inactive else 'INACTIVE',
                                               self.get_type_as_string(),
                                               self.sequence, self.source)
        return result
    def _get_alter_sql(self, **params):
        self._check_params(params, ['fire_on', 'active', 'sequence', 'declare', 'code'])
        action = params.get('fire_on')
        active = params.get('active')
        sequence = params.get('sequence')
        declare = params.get('declare')
        code = params.get('code')
        #
        header = ''
        if active is not None:
            header += ' ACTIVE' if active else ' INACTIVE'
        if action is not None:
            dbaction = action.upper().startswith('ON ')
            if (dbaction and not self.isdbtrigger()) or (not dbaction and self.isdbtrigger()):
                raise fdb.ProgrammingError("Trigger type change is not allowed.")
            header += '\n  %s' % action
        if sequence is not None:
            header += '\n  POSITION %d' % sequence
        #
        if code is not None:
            if declare is None:
                d = ''
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += '  %s\n' % x
            else:
                d = '%s\n' % declare
            if isinstance(code, (list, tuple)):
                c = ''
                for x in code:
                    c += '  %s\n' % x
            else:
                c = '%s\n' % code
            body = '\nAS\n%sBEGIN\n%sEND' % (d, c)
        else:
            body = ''
        #
        if not (header or body):
            raise fdb.ProgrammingError("Header or body definition required.")
        return 'ALTER TRIGGER %s%s%s' % (self.get_quoted_name(), header, body)
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP TRIGGER %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON TRIGGER %s IS %s' % (self.get_quoted_name(),
                                                'NULL' if self.description is None
                                                else "'%s'" % escape_single_quotes(self.description))
    def _get_action_time(self):
        if self.isddltrigger():
            return (self.trigger_type) & 1
        else:
            return (self.trigger_type + 1) & 1
    def _get_action_type(self, slot):
        if self.isddltrigger():
            return (self.trigger_type & ~TRIGGER_TYPE_DDL) >> 1
        else:
            return ((self.trigger_type + 1) >> (slot * 2 - 1)) & 3
    def _get_name(self):
        return self._attributes['RDB$TRIGGER_NAME']
    def _get_relation(self):
        relname = self._attributes['RDB$RELATION_NAME']
        rel = self.schema.get_table(relname)
        if not rel:
            rel = self.schema.get_view(relname)
        return rel
    def _get_sequence(self):
        return self._attributes['RDB$TRIGGER_SEQUENCE']
    def _get_trigger_type(self):
        return self._attributes['RDB$TRIGGER_TYPE']
    def _get_source(self):
        return self._attributes['RDB$TRIGGER_SOURCE']
    def _get_flags(self):
        return self._attributes['RDB$FLAGS']
    def _get_valid_blr(self):
        result = self._attributes.get('RDB$VALID_BLR')
        return bool(result) if result is not None else None
    def _get_engine_name(self):
        return self._attributes.get('RDB$ENGINE_NAME')
    def _get_entrypoint(self):
        return self._attributes.get('RDB$ENTRYPOINT')
    def _istype(self, type_code):
        atype = self._get_action_type(1)
        if atype == type_code:
            return True
        atype = self._get_action_type(2)
        if atype and atype == type_code:
            return True
        atype = self._get_action_type(3)
        if atype and atype == type_code:
            return True
        return False

    #--- Properties
    relation = LateBindingProperty(_get_relation, doc=":class:`Table` or :class:`View` that the trigger is for, or None for database triggers")
    sequence = LateBindingProperty(_get_sequence, doc="Sequence (position) of trigger. Zero usually means no sequence defined.")
    trigger_type = LateBindingProperty(_get_trigger_type, doc="Numeric code for trigger type that define what event and when are covered by trigger.")
    source = LateBindingProperty(_get_source, doc="PSQL source code.")
    flags = LateBindingProperty(_get_flags, doc="Internal flags.")
    valid_blr = LateBindingProperty(_get_valid_blr, doc="Trigger BLR invalidation flag. Coul be True/False or None.")
    # FB 3
    engine_name = LateBindingProperty(_get_engine_name, doc="Engine name.")
    entrypoint = LateBindingProperty(_get_entrypoint, doc="Entrypoint.")
    #--- Public
    def isactive(self):
        "Returns True if this trigger is active."
        return self._attributes['RDB$TRIGGER_INACTIVE'] == 0
    def isbefore(self):
        "Returns True if this trigger is set for BEFORE action."
        return self._get_action_time() == 0
    def isafter(self):
        "Returns True if this trigger is set for AFTER action."
        return self._get_action_time() == 1
    def isdbtrigger(self):
        "Returns True if this trigger is database trigger."
        return (self.trigger_type & TRIGGER_TYPE_MASK) == TRIGGER_TYPE_DB
    def isddltrigger(self):
        "Returns True if this trigger is DDL trigger."
        return (self.trigger_type & TRIGGER_TYPE_MASK) == TRIGGER_TYPE_DDL
    def isinsert(self):
        "Returns True if this trigger is set for INSERT operation."
        return self._istype(1)
    def isupdate(self):
        "Returns True if this trigger is set for UPDATE operation."
        return self._istype(2)
    def isdelete(self):
        "Returns True if this trigger is set for DELETE operation."
        return self._istype(3)
    def get_type_as_string(self):
        "Return string with action and operation specification."
        l = []
        if self.isddltrigger():
            l.append(TRIGGER_PREFIX_TYPES[self._get_action_time()])
            code = self._get_action_type(1)
            l.append('ANY DDL STATEMENT' if code == DDL_TRIGGER_ANY else TRIGGER_DDL_TYPES[code])
        elif self.isdbtrigger():
            l.append('ON '+TRIGGER_DB_TYPES[self.trigger_type & ~TRIGGER_TYPE_DB])
        else:
            l.append(TRIGGER_PREFIX_TYPES[self._get_action_time()])
            l.append(TRIGGER_SUFFIX_TYPES[self._get_action_type(1)])
            sufix = self._get_action_type(2)
            if sufix:
                l.append('OR')
                l.append(TRIGGER_SUFFIX_TYPES[sufix])
            sufix = self._get_action_type(3)
            if sufix:
                l.append('OR')
                l.append(TRIGGER_SUFFIX_TYPES[sufix])
        return ' '.join(l)

class ProcedureParameter(BaseSchemaItem):
    """Represents procedure parameter.

    Supported SQL actions:
        `comment`
    """
    def __init__(self, schema, proc, attributes):
        super(ProcedureParameter, self).__init__(schema, attributes)

        self.__proc = proc
        self._strip_attribute('RDB$PARAMETER_NAME')
        self._strip_attribute('RDB$PROCEDURE_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')

        self._actions = ['comment']

    #--- Protected
    def _get_comment_sql(self, **params):
        return 'COMMENT ON PARAMETER %s.%s IS %s' % (self.procedure.get_quoted_name(),
                                                     self.get_quoted_name(),
                                                     'NULL' if self.description is None
                                                     else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$PARAMETER_NAME']
    def _get_procedure(self):
        return self.schema.get_procedure(self._attributes['RDB$PROCEDURE_NAME'])
    def _get_sequence(self):
        return self._attributes['RDB$PARAMETER_NUMBER']
    def _get_domain(self):
        return self.schema.get_domain(self._attributes['RDB$FIELD_SOURCE'])
    def _get_datatype(self):
        return self.domain.datatype
    def _get_type_from(self):
        m = self.mechanism
        if m is None:
            return PROCPAR_DATATYPE
        elif m == 0:
            return PROCPAR_DATATYPE if self.domain.issystemobject() else PROCPAR_DOMAIN
        elif m == 1:
            if self._attributes.get('RDB$RELATION_NAME') is None:
                return PROCPAR_TYPE_OF_DOMAIN
            else:
                return PROCPAR_TYPE_OF_COLUMN
        else:
            raise fdb.InternalError("Unknown parameter mechanism code: %d" % m)
    def _get_default(self):
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    def _get_collation(self):
        cid = self._attributes.get('RDB$COLLATION_ID')
        return (None if cid is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    def _get_mechanism(self):
        return self._attributes.get('RDB$PARAMETER_MECHANISM')
    def _get_column(self):
        rname = self._attributes.get('RDB$RELATION_NAME')
        return (None if rname is None
                else self.schema.get_table(rname).get_column(self._attributes['RDB$FIELD_NAME']))
    def _get_package(self):
        return self.schema.get_package(self._attributes.get('RDB$PACKAGE_NAME'))

    #--- Properties
    procedure = LateBindingProperty(_get_procedure, doc="Name of the stored procedure.")
    sequence = LateBindingProperty(_get_sequence, doc="Sequence (position) of parameter.")
    domain = LateBindingProperty(_get_domain, doc=":class:`Domain` for this parameter.")
    datatype = LateBindingProperty(_get_datatype, doc="Comlete SQL datatype definition.")
    type_from = LateBindingProperty(_get_type_from, doc="Numeric code. See :attr:`Schema.enum_param_type_from`.`")
    # FB 2.1
    default = LateBindingProperty(_get_default, doc="Default value.")
    collation = LateBindingProperty(_get_collation, doc=":class:`collation` for this parameter.")
    mechanism = LateBindingProperty(_get_mechanism, doc="Parameter mechanism code.")
    # FB 2.5
    column = LateBindingProperty(_get_column, doc=":class:`TableColumn` for this parameter.")
    # FB 3.0
    package = LateBindingProperty(_get_package, doc="Package this procedure belongs to. \nObject is :class:`Package` instance or None.")
    #--- Public
    def get_sql_definition(self):
        "Returns SQL definition for parameter."
        typedef = self.datatype
        if self.type_from == PROCPAR_DOMAIN:
            typedef = self.domain.get_quoted_name()
        elif self.type_from == PROCPAR_TYPE_OF_DOMAIN:
            typedef = 'TYPE OF %s' % self.domain.get_quoted_name()
        elif self.type_from == PROCPAR_TYPE_OF_COLUMN:
            typedef = 'TYPE OF COLUMN %s.%s' % (self.column.table.get_quoted_name(),
                                                self.column.get_quoted_name())
        result = '%s %s%s' % (self.get_quoted_name(), typedef,
                              '' if self.isnullable() else ' NOT NULL')
        c = self.collation
        if c is not None:
            result += ' COLLATE %s' % c.get_quoted_name()
        if self.isinput() and self.has_default():
            result += ' = %s' % self.default
        return result
    def isinput(self):
        "Returns True if parameter is INPUT parameter."
        return self._attributes['RDB$PARAMETER_TYPE'] == 0
    def isnullable(self):
        "Returns True if parameter allows NULL."
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self):
        "Returns True if parameter has default value."
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def ispackaged(self):
        "Returns True if procedure parameter is defined in package."
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))

class Procedure(BaseSchemaItem):
    """Represents stored procedure.

    Supported SQL actions:
        - User procedure: `create` (no_code=bool), `recreate`  no_code=bool),
          `create_or_alter` (no_code=bool), `drop`, `comment`
          `alter` (input=string_or_list, output=string_or_list, declare=string_or_list, code=string_or_list)
        - System procedure: `comment`
    """
    def __init__(self, schema, attributes):
        super(Procedure, self).__init__(schema, attributes)
        self._type_code = [5,]

        self.__input_params = self.__output_params = None

        self._strip_attribute('RDB$PROCEDURE_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        self._strip_attribute('RDB$PACKAGE_NAME')

        self.__ods = schema._con.ods

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'recreate', 'alter', 'create_or_alter', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, ['no_code'])
        no_code = params.get('no_code')
        result = 'CREATE PROCEDURE %s' % self.get_quoted_name()
        if self.has_input():
            if self._attributes['RDB$PROCEDURE_INPUTS'] == 1:
                result += ' (%s)\n' % self.input_params[0].get_sql_definition()
            else:
                result += ' (\n'
                for p in self.input_params:
                    result += '  %s%s\n' % (p.get_sql_definition(),
                                            '' if p.sequence+1 == self._attributes['RDB$PROCEDURE_INPUTS']
                                            else ',')
                result += ')\n'
        else:
            result += '\n'
        if self.has_output():
            if self._attributes['RDB$PROCEDURE_OUTPUTS'] == 1:
                result += 'RETURNS (%s)\n' % self.output_params[0].get_sql_definition()
            else:
                result += 'RETURNS (\n'
                for p in self.output_params:
                    result += '  %s%s\n' % (p.get_sql_definition(),
                                            '' if p.sequence+1 == self._attributes['RDB$PROCEDURE_OUTPUTS']
                                            else ',')
                result += ')\n'
        return result+'AS\n'+(('BEGIN\nEND' if self.proc_type != 1
                               else 'BEGIN\n  SUSPEND;\nEND')
                              if no_code else self.source)
    def _get_alter_sql(self, **params):
        self._check_params(params, ['input', 'output', 'declare', 'code'])
        inpars = params.get('input')
        outpars = params.get('output')
        declare = params.get('declare')
        code = params.get('code')
        if 'code' not in params:
            raise fdb.ProgrammingError("Missing required parameter: 'code'.")
        #
        header = ''
        if inpars is not None:
            if isinstance(inpars, (list, tuple)):
                numpars = len(inpars)
                if numpars == 1:
                    header = ' (%s)\n' % inpars
                else:
                    header = ' (\n'
                    i = 1
                    for p in inpars:
                        header += '  %s%s\n' % (p, '' if i == numpars else ',')
                        i += 1
                    header += ')\n'
            else:
                header = ' (%s)\n' % inpars
        #
        if outpars is not None:
            if not header:
                header += '\n'
            if isinstance(outpars, (list, tuple)):
                numpars = len(outpars)
                if numpars == 1:
                    header += 'RETURNS (%s)\n' % outpars
                else:
                    header += 'RETURNS (\n'
                    i = 1
                    for p in outpars:
                        header += '  %s%s\n' % (p, '' if i == numpars else ',')
                        i += 1
                    header += ')\n'
            else:
                header += 'RETURNS (%s)\n' % outpars
        #
        if code:
            if declare is None:
                d = ''
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += '  %s\n' % x
            else:
                d = '%s\n' % declare
            if isinstance(code, (list, tuple)):
                c = ''
                for x in code:
                    c += '  %s\n' % x
            else:
                c = '%s\n' % code
            body = '%sAS\n%sBEGIN\n%sEND' % ('' if header else '\n', d, c)
        else:
            body = '%sAS\nBEGIN\nEND' % ('' if header else '\n')
        #
        return 'ALTER PROCEDURE %s%s%s' % (self.get_quoted_name(), header, body)
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP PROCEDURE %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON PROCEDURE %s IS %s' % (self.get_quoted_name(),
                                                  'NULL' if self.description is None
                                                  else "'%s'" % escape_single_quotes(self.description))
    def __param_columns(self):
        cols = ['RDB$PARAMETER_NAME', 'RDB$PROCEDURE_NAME', 'RDB$PARAMETER_NUMBER',
                'RDB$PARAMETER_TYPE', 'RDB$FIELD_SOURCE', 'RDB$DESCRIPTION',
                'RDB$SYSTEM_FLAG']
        if self.__ods >= fdb.ODS_FB_21:
            cols.extend(['RDB$DEFAULT_SOURCE', 'RDB$COLLATION_ID', 'RDB$NULL_FLAG',
                         'RDB$PARAMETER_MECHANISM'])
        if self.__ods >= fdb.ODS_FB_25:
            cols.extend(['RDB$FIELD_NAME', 'RDB$RELATION_NAME'])
        if self.__ods >= fdb.ODS_FB_30:
            cols.extend(['RDB$PACKAGE_NAME'])
        return ','.join(cols)
    def _get_name(self):
        return self._attributes['RDB$PROCEDURE_NAME']
    def _get_id(self):
        return self._attributes['RDB$PROCEDURE_ID']
    def _get_source(self):
        return self._attributes['RDB$PROCEDURE_SOURCE']
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_owner_name(self):
        return self._attributes['RDB$OWNER_NAME']
    def _get_input_params(self):
        sql = """select %s from rdb$procedure_parameters where rdb$procedure_name = ?
and rdb$parameter_type = 0 order by rdb$parameter_number"""
        if self.__input_params is None:
            if self.has_input():
                self.__input_params = ObjectList((ProcedureParameter(self.schema, self, row) for row in
                                                  self.schema._select(sql % self.__param_columns(), (self.name,))),
                                                 ProcedureParameter, 'item.name')
            else:
                self.__input_params = ObjectList()
            self.__input_params.freeze()
        return self.__input_params
    def _get_output_params(self):
        sql = """select %s from rdb$procedure_parameters where rdb$procedure_name = ?
and rdb$parameter_type = 1 order by rdb$parameter_number"""
        if self.__output_params is None:
            if self.has_output():
                self.__output_params = ObjectList((ProcedureParameter(self.schema, self, row) for row in
                                                   self.schema._select(sql % self.__param_columns(), (self.name,))),
                                                  ProcedureParameter, 'item.name')
            else:
                self.__output_params = ObjectList()
            self.__output_params.freeze()
        return self.__output_params
    def _get_proc_type(self):
        return self._attributes.get('RDB$PROCEDURE_TYPE', 0)
    def _get_valid_blr(self):
        result = self._attributes.get('RDB$VALID_BLR')
        return bool(result) if result is not None else None
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: ((p.subject_name == self.name) and
                                                        (p.subject_type in self._type_code)))
    def _get_engine_name(self):
        return self._attributes.get('RDB$ENGINE_NAME')
    def _get_entrypoint(self):
        return self._attributes.get('RDB$ENTRYPOINT')
    def _get_package(self):
        return self.schema.get_package(self._attributes.get('RDB$PACKAGE_NAME'))
    def _get_privacy(self):
        return self._attributes.get('RDB$PRIVATE_FLAG')

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Internal unique ID number.")
    source = LateBindingProperty(_get_source, doc="PSQL source code.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class that define access limits to the procedure.")
    owner_name = LateBindingProperty(_get_owner_name, doc="User name of procedure's creator.")
    input_params = LateBindingProperty(_get_input_params, doc=":class:`~fdb.utils.ObjectList` of input parameters.\nInstances are :class:`ProcedureParameter` instances.")
    output_params = LateBindingProperty(_get_output_params, doc=":class:`~fdb.utils.ObjectList` of output parameters.\nInstances are :class:`ProcedureParameter` instances.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    # FB 2.1
    proc_type = LateBindingProperty(_get_proc_type, doc="Procedure type code. See :attr:`~fdb.schema.Schema.enum_procedure_types`.")
    valid_blr = LateBindingProperty(_get_valid_blr, doc="Procedure BLR invalidation flag. Coul be True/False or None.")
    # FB 3.0
    engine_name = LateBindingProperty(_get_engine_name, doc="Engine name.")
    entrypoint = LateBindingProperty(_get_entrypoint, doc="Entrypoint.")
    package = LateBindingProperty(_get_package, doc="Package this procedure belongs to. \nObject is :class:`Package` instance or None.")
    privacy = LateBindingProperty(_get_privacy, doc="Privacy flag.")
    #--- Public
    def get_param(self, name):
        "Returns :class:`ProcedureParameter` with specified name or None"
        for p in self.output_params:
            if p.name == name:
                return p
        for p in self.input_params:
            if p.name == name:
                return p
        return None
    def has_input(self):
        "Returns True if procedure has any input parameters."
        return bool(self._attributes['RDB$PROCEDURE_INPUTS'])
    def has_output(self):
        "Returns True if procedure has any output parameters."
        return bool(self._attributes['RDB$PROCEDURE_OUTPUTS'])
    def ispackaged(self):
        "Returns True if procedure is defined in package."
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))

class Role(BaseSchemaItem):
    """Represents user role.

    Supported SQL actions:
        - User role: `create`, `drop`, `comment`
        - System role: `comment`
    """
    def __init__(self, schema, attributes):
        super(Role, self).__init__(schema, attributes)
        self._type_code = [13,]

        self._strip_attribute('RDB$ROLE_NAME')
        self._strip_attribute('RDB$OWNER_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')

        self._actions = ['comment']
        if not self.issystemobject():
            self._actions.extend(['create', 'drop'])

    #--- Protected
    def _get_create_sql(self, **params):
        self._check_params(params, [])
        return 'CREATE ROLE %s' % self.get_quoted_name()
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP ROLE %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON ROLE %s IS %s' % (self.get_quoted_name(),
                                             'NULL' if self.description is None
                                             else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$ROLE_NAME']
    def _get_owner_name(self):
        return self._attributes['RDB$OWNER_NAME']
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_privileges(self):
        return self.schema.privileges.filter(lambda p: ((p.user_name == self.name) and
                                                        (p.user_type in self._type_code)))

    #--- Properties
    owner_name = LateBindingProperty(_get_owner_name, doc="User name of role owner.")
    privileges = LateBindingProperty(_get_privileges, doc=":class:`~fdb.utils.ObjectList` of :class:`Privilege` objects granted to this object.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")

class FunctionArgument(BaseSchemaItem):
    """Represets UDF argument.

    Supported SQL actions:
        `none`
    """
    def __init__(self, schema, function, attributes):
        super(FunctionArgument, self).__init__(schema, attributes)
        self._type_code = [15,]
        self.__function = function

        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$ARGUMENT_NAME')
        self._strip_attribute('RDB$FIELD_SOURCE')
        self._strip_attribute('RDB$DEFAULT_SOURCE')
        self._strip_attribute('RDB$FIELD_NAME')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$DESCRIPTION')

    #--- Protected
    def _get_name(self):
        return self.argument_name if self.argument_name else (self.function.name+
                                                              '_'+str(self._get_position()))
    def _get_function(self):
        return self.__function
        #return self.schema.get_function(self._attributes['RDB$FUNCTION_NAME'])
    def _get_position(self):
        return self._attributes['RDB$ARGUMENT_POSITION']
    def _get_mechanism(self):
        x = self._attributes['RDB$MECHANISM']
        return None if x is None else abs(x)
    def _get_length(self):
        return self._attributes['RDB$FIELD_LENGTH']
    def _get_scale(self):
        return self._attributes['RDB$FIELD_SCALE']
    def _get_field_type(self):
        return self._attributes['RDB$FIELD_TYPE']
    def _get_sub_type(self):
        return self._attributes['RDB$FIELD_SUB_TYPE']
    def _get_character_length(self):
        return self._attributes['RDB$CHARACTER_LENGTH']
    def _get_character_set(self):
        return self.schema.get_character_set_by_id(self._attributes['RDB$CHARACTER_SET_ID'])
    def _get_precision(self):
        return self._attributes['RDB$FIELD_PRECISION']
    def _get_datatype(self):
        if self.field_type is None:
            # FB3 PSQL function, datatype defined via internal domain
            return self.domain.datatype
        else:
            # Classic external UDF
            l = []
            precision_known = False
            if self.field_type in (FBT_SMALLINT, FBT_INTEGER, FBT_BIGINT):
                if self.precision != None:
                    if (self.sub_type > 0) and (self.sub_type <= MAX_INTSUBTYPES):
                        l.append('%s(%d, %d)' % \
                          (INTEGRAL_SUBTYPES[self.sub_type], self.precision, -self.scale))
                        precision_known = True
            if not precision_known:
                if (self.field_type == FBT_SMALLINT) and (self.scale < 0):
                    l.append('NUMERIC(4, %d)' % -self.scale)
                elif (self.field_type == FBT_INTEGER) and (self.scale < 0):
                    l.append('NUMERIC(9, %d)' % -self.scale)
                elif (self.field_type == FBT_DOUBLE_PRECISION) and (self.scale < 0):
                    l.append('NUMERIC(15, %d)' % -self.scale)
                else:
                    l.append(COLUMN_TYPES[self.field_type])
            if self.field_type in (FBT_CHAR, FBT_VARCHAR, FBT_CSTRING):
                l.append('(%d)' % (self.length if (self.character_length is None)
                                   else self.character_length))
            if self.field_type == FBT_BLOB:
                if self.sub_type >= 0 and self.sub_type <= MAX_BLOBSUBTYPES:
                    if self.sub_type > 0:
                        l.append(' SUB_TYPE %s' % BLOB_SUBTYPES[self.sub_type])
                else:
                    l.append(' SUB_TYPE %d' % self.sub_type)
            if self.field_type in (FBT_CHAR, FBT_VARCHAR, FBT_CSTRING, FBT_BLOB):
                if self._attributes['RDB$CHARACTER_SET_ID'] is not None and \
                  (self.character_set.name != self.schema.default_character_set.name):
                    l.append(' CHARACTER SET %s' % self.character_set.name)
            return ''.join(l)
    def _get_package(self):
        return self.schema.get_package(self._attributes.get('RDB$PACKAGE_NAME'))
    def _get_argument_name(self):
        return self._attributes.get('RDB$ARGUMENT_NAME')
    def _get_domain(self):
        return self.schema.get_domain(self._attributes.get('RDB$FIELD_SOURCE'))
    def _get_default(self):
        result = self._attributes.get('RDB$DEFAULT_SOURCE')
        if result:
            if result.upper().startswith('= '):
                result = result[2:]
            elif result.upper().startswith('DEFAULT '):
                result = result[8:]
        return result
    def _get_collation(self):
        cid = self._attributes.get('RDB$COLLATION_ID')
        return (None if cid is None
                else self.schema.get_collation_by_id(self.domain._attributes['RDB$CHARACTER_SET_ID'], cid))
    def _get_argument_mechanism(self):
        return self._attributes.get('RDB$ARGUMENT_MECHANISM')
    def _get_column(self):
        rname = self._attributes.get('RDB$RELATION_NAME')
        return (None if rname is None
                else self.schema.get_table(rname).get_column(self._attributes['RDB$FIELD_NAME']))
    def _get_type_from(self):
        m = self.argument_mechanism
        if m is None:
            return PROCPAR_DATATYPE
        elif m == 0:
            return PROCPAR_DATATYPE if self.domain.issystemobject() else PROCPAR_DOMAIN
        elif m == 1:
            if self._attributes.get('RDB$RELATION_NAME') is None:
                return PROCPAR_TYPE_OF_DOMAIN
            else:
                return PROCPAR_TYPE_OF_COLUMN
        else:
            raise fdb.InternalError("Unknown parameter mechanism code: %d" % m)

    #--- Properties
    function = LateBindingProperty(_get_function, doc=":class:`Function` to which this argument belongs.")
    position = LateBindingProperty(_get_position, doc="Argument position.")
    mechanism = LateBindingProperty(_get_mechanism, doc="How argument is passed.")
    field_type = LateBindingProperty(_get_field_type, doc="Number code of the data type defined for the argument.")
    length = LateBindingProperty(_get_length, doc="Length of the argument in bytes.")
    scale = LateBindingProperty(_get_scale, doc="Negative number representing the scale of NUMBER and DECIMAL argument.")
    precision = LateBindingProperty(_get_precision, doc="Indicates the number of digits of precision available to the data type of the argument.")
    sub_type = LateBindingProperty(_get_sub_type, doc="BLOB subtype.")
    character_length = LateBindingProperty(_get_character_length, doc="Length of CHAR and VARCHAR column, in characters (not bytes).")
    character_set = LateBindingProperty(_get_character_set, doc=":class:`CharacterSet` for a character/text BLOB argument, or None.")
    datatype = LateBindingProperty(_get_datatype, doc="Comlete SQL datatype definition.")
    # FB 3.0
    argument_name = LateBindingProperty(_get_argument_name, doc="Argument name.")
    domain = LateBindingProperty(_get_domain, doc=":class:`Domain` for this parameter.")
    default = LateBindingProperty(_get_default, doc="Default value.")
    collation = LateBindingProperty(_get_collation, doc=":class:`Collation` for this parameter.")
    argument_mechanism = LateBindingProperty(_get_argument_mechanism, doc="Argument mechanism.")
    column = LateBindingProperty(_get_column, doc=":class:`TableColumn` for this parameter.")
    type_from = LateBindingProperty(_get_type_from, doc="Numeric code. See :attr:`Schema.enum_param_type_from`.`")
    package = LateBindingProperty(_get_package, doc="Package this function belongs to.\nObject is :class:`Package` instance or None.")
    #--- Public
    def get_sql_definition(self):
        "Returns SQL definition for parameter."
        if self.function.isexternal():
            return '%s%s%s' % (self.datatype,
                               ' BY DESCRIPTOR' if self.isbydescriptor() else '',
                               ' BY VALUE' if self.isbyvalue() and self.isreturning() else '',)
        else:
            typedef = self.datatype
            if self.type_from == PROCPAR_DOMAIN:
                typedef = self.domain.get_quoted_name()
            elif self.type_from == PROCPAR_TYPE_OF_DOMAIN:
                typedef = 'TYPE OF %s' % self.domain.get_quoted_name()
            elif self.type_from == PROCPAR_TYPE_OF_COLUMN:
                typedef = 'TYPE OF COLUMN %s.%s' % (self.column.table.get_quoted_name(),
                                                    self.column.get_quoted_name())
            result = '%s%s%s' % (self.get_quoted_name()+' ' if not self.isreturning() else '',
                                 typedef,
                                 '' if self.isnullable() else ' NOT NULL')
            c = self.collation
            if c is not None:
                result += ' COLLATE %s' % c.get_quoted_name()
            if not self.isreturning() and self.has_default():
                result += ' = %s' % self.default
            return result
    def isbyvalue(self):
        "Returns True if argument is passed by value."
        return self.mechanism == 0
    def isbyreference(self):
        "Returns True if argument is passed by reference."
        return self.mechanism in [1, 5]
    def isbydescriptor(self, any=False):
        """Returns True if argument is passed by descriptor.

        :param bool any: If True, method returns True if any kind of descriptor
          is used (including BLOB and ARRAY descriptors).
        """
        return self.mechanism in [2, 3, 4] if any else self.mechanism == 2
    def iswithnull(self):
        "Returns True if argument is passed by reference with NULL support."
        return self.mechanism == 5
    def isfreeit(self):
        "Returns True if (return) argument is declared as FREE_IT."
        return self._attributes['RDB$MECHANISM'] < 0
    def isreturning(self):
        "Returns True if argument represents return value for function."
        return self.position == self.function._attributes['RDB$RETURN_ARGUMENT']
    # Firebird 3.0
    def isnullable(self):
        "Returns True if parameter allows NULL."
        return not bool(self._attributes.get('RDB$NULL_FLAG'))
    def has_default(self):
        "Returns True if parameter has default value."
        return bool(self._attributes.get('RDB$DEFAULT_SOURCE'))
    def ispackaged(self):
        "Returns True if function argument is defined in package."
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))

class Function(BaseSchemaItem):
    """Represents user defined function.

    Supported SQL actions:
        - External UDF: `declare`, `drop`, `comment`
        - PSQL UDF (FB 3, not declared in package): `create` (no_code=bool),
          `recreate` (no_code=bool), `create_or_alter` (no_code=bool), `drop`,
          `alter` (arguments=string_or_list, returns=string, declare=string_or_list, code=string_or_list)
        - System UDF: `none`
    """
    def __init__(self, schema, attributes):
        super(Function, self).__init__(schema, attributes)
        self._type_code = [15,]
        self.__arguments = None
        self.__returns = None

        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$MODULE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')
        self._strip_attribute('RDB$ENGINE_NAME')
        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

        self.__ods = schema._con.ods

        if not self.issystemobject():
            if self.isexternal():
                self._actions = ['comment', 'declare', 'drop']
            else:
                if self._attributes.get('RDB$PACKAGE_NAME') is None:
                    self._actions = ['create', 'recreate', 'alter', 'create_or_alter', 'drop']

    #--- Protected
    def _get_declare_sql(self, **params):
        self._check_params(params, [])
        fdef = 'DECLARE EXTERNAL FUNCTION %s\n' % self.get_quoted_name()
        for p in self.arguments:
            fdef += '  %s%s\n' % (p.get_sql_definition(),
                                  '' if p.position == len(self.arguments) else ',')
        if self.has_return():
            fdef += 'RETURNS %s%s\n' % ('PARAMETER %d' % self._attributes['RDB$RETURN_ARGUMENT']
                                        if self.has_return_argument()
                                        else self.returns.get_sql_definition(),
                                        ' FREE_IT' if self.returns.isfreeit() else '')
        return "%sENTRY_POINT '%s'\nMODULE_NAME '%s'" % (fdef, self.entrypoint, self.module_name)
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP%s FUNCTION %s' % (' EXTERNAL' if self.isexternal() else '',
                                       self.get_quoted_name())
    def _get_comment_sql(self, **params):
        return 'COMMENT ON EXTERNAL FUNCTION %s IS %s' % (self.get_quoted_name(),
                                                          'NULL' if self.description is None
                                                          else "'%s'" % escape_single_quotes(self.description))
    def _get_create_sql(self, **params):
        self._check_params(params, ['no_code'])
        no_code = params.get('no_code')
        result = 'CREATE FUNCTION %s' % self.get_quoted_name()
        if self.has_arguments():
            if len(self.arguments) == 1:
                result += ' (%s)\n' % self.arguments[0].get_sql_definition()
            else:
                result += ' (\n'
                for p in self.arguments:
                    result += '  %s%s\n' % (p.get_sql_definition(),
                                            '' if p.position == len(self.arguments) else ',')
                result += ')\n'
        else:
            result += '\n'
        result += 'RETURNS %s\n' % self.returns.get_sql_definition()
        return result+'AS\n'+('BEGIN\nEND' if no_code else self.source)
    def _get_alter_sql(self, **params):
        self._check_params(params, ['arguments', 'returns', 'declare', 'code'])
        arguments = params.get('arguments')
        returns = params.get('returns')
        if returns is None:
            raise fdb.ProgrammingError("Missing required parameter: 'returns'.")
        declare = params.get('declare')
        code = params.get('code')
        if 'code' not in params:
            raise fdb.ProgrammingError("Missing required parameter: 'code'.")
        #
        header = ''
        if arguments is not None:
            if isinstance(arguments, (list, tuple)):
                numpars = len(arguments)
                if numpars == 1:
                    header = ' (%s)\n' % arguments
                else:
                    header = ' (\n'
                    i = 1
                    for p in arguments:
                        header += '  %s%s\n' % (p, '' if i == numpars else ',')
                        i += 1
                    header += ')\n'
            else:
                header = ' (%s)\n' % arguments
        #
        if not header:
            header += '\n'
        header += 'RETURNS %s\n' % returns
        #
        if code:
            if declare is None:
                d = ''
            elif isinstance(declare, (list, tuple)):
                d = ''
                for x in declare:
                    d += '  %s\n' % x
            else:
                d = '%s\n' % declare
            if isinstance(code, (list, tuple)):
                c = ''
                for x in code:
                    c += '  %s\n' % x
            else:
                c = '%s\n' % code
            body = '%sAS\n%sBEGIN\n%sEND' % ('' if header else '\n', d, c)
        else:
            body = '%sAS\nBEGIN\nEND' % ('' if header else '\n')
        #
        return 'ALTER FUNCTION %s%s%s' % (self.get_quoted_name(), header, body)
    def _load_arguments(self, mock=None):
        cols = ['RDB$FUNCTION_NAME', 'RDB$ARGUMENT_POSITION', 'RDB$MECHANISM',
                'RDB$FIELD_TYPE', 'RDB$FIELD_SCALE', 'RDB$FIELD_LENGTH',
                'RDB$FIELD_SUB_TYPE', 'RDB$CHARACTER_SET_ID', 'RDB$FIELD_PRECISION',
                'RDB$CHARACTER_LENGTH']
        if self.__ods >= fdb.ODS_FB_30:
            cols.extend(['RDB$PACKAGE_NAME', 'RDB$ARGUMENT_NAME', 'RDB$FIELD_SOURCE',
                         'RDB$DEFAULT_SOURCE', 'RDB$COLLATION_ID', 'RDB$NULL_FLAG',
                         'RDB$ARGUMENT_MECHANISM', 'RDB$FIELD_NAME', 'RDB$RELATION_NAME',
                         'RDB$SYSTEM_FLAG', 'RDB$DESCRIPTION'])
        self.__arguments = ObjectList((FunctionArgument(self.schema, self, row) for row in
                                       (mock if mock else
                                        self.schema._select("""select %s from rdb$function_arguments
where rdb$function_name = ? order by rdb$argument_position""" % ','.join(cols), (self.name,)))),
                                      FunctionArgument)
        self.__arguments.freeze()
        rarg = self._attributes['RDB$RETURN_ARGUMENT']
        if rarg is not None:
            for a in self.__arguments:
                if a.position == rarg:
                    self.__returns = weakref.ref(a)
    def _get_name(self):
        return self._attributes['RDB$FUNCTION_NAME']
    def _get_module_name(self):
        return self._attributes['RDB$MODULE_NAME']
    def _get_entrypoint(self):
        return self._attributes['RDB$ENTRYPOINT']
    def _get_returns(self):
        if self.__arguments is None:
            self._load_arguments()
        return self.__returns if self.__returns is None else self.__returns()
    def _get_arguments(self):
        if self.__arguments is None:
            self._load_arguments()
        return self.__arguments.filter(lambda a: a.position != 0)
    def _get_engine_mame(self):
        return self._attributes.get('RDB$ENGINE_NAME')
    def _get_package(self):
        return self.schema.get_package(self._attributes.get('RDB$PACKAGE_NAME'))
    def _get_private_flag(self):
        return self._attributes.get('RDB$PRIVATE_FLAG')
    def _get_source(self):
        return self._attributes.get('RDB$FUNCTION_SOURCE')
    def _get_id(self):
        return self._attributes.get('RDB$FUNCTION_ID')
    def _get_valid_blr(self):
        result = self._attributes.get('RDB$VALID_BLR')
        return bool(result) if result is not None else None
    def _get_security_class(self):
        return self._attributes.get('RDB$SECURITY_CLASS')
    def _get_owner_name(self):
        return self._attributes.get('RDB$OWNER_NAME')
    def _get_legacy_flag(self):
        return self._attributes.get('RDB$LEGACY_FLAG')
    def _get_deterministic_flag(self):
        return self._attributes.get('RDB$DETERMINISTIC_FLAG')

    #--- Properties
    module_name = LateBindingProperty(_get_module_name, doc="Module name.")
    entrypoint = LateBindingProperty(_get_entrypoint, doc="Entrypoint in module.")
    returns = LateBindingProperty(_get_returns, doc="Returning :class:`FunctionArgument` or None.")
    arguments = LateBindingProperty(_get_arguments,
                                    doc=":class:`~fdb.utils.ObjectList` of function arguments."
                                    " Items are :class:`FunctionArgument` instances.")
    # Firebird 3.0
    engine_mame = LateBindingProperty(_get_engine_mame, doc="Engine name.")
    package = LateBindingProperty(_get_package,
                                  doc="Package this function belongs to. \nObject is"
                                  " :class:`Package` instance or None.")
    private_flag = LateBindingProperty(_get_private_flag, doc="Private flag.")
    source = LateBindingProperty(_get_source, doc="Function source.")
    id = LateBindingProperty(_get_id, doc="Function ID.")
    valid_blr = LateBindingProperty(_get_valid_blr, doc="BLR validity flag.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class.")
    owner_name = LateBindingProperty(_get_owner_name, doc="Owner name.")
    legacy_flag = LateBindingProperty(_get_legacy_flag, doc="Legacy flag.")
    deterministic_flag = LateBindingProperty(_get_deterministic_flag, doc="Deterministic flag.")
    #--- Public
    def isexternal(self):
        "Returns True if function is external UDF, False for PSQL functions."
        return True if self.module_name else False
    def has_arguments(self):
        "Returns True if function has input arguments."
        return bool(self.arguments)
    def has_return(self):
        "Returns True if function returns a value."
        return self.returns is not None
    def has_return_argument(self):
        "Returns True if function returns a value in input argument."
        return self.returns.position != 0 if self.returns is not None else False
    def ispackaged(self):
        "Returns True if function is defined in package."
        return bool(self._attributes.get('RDB$PACKAGE_NAME'))

class DatabaseFile(BaseSchemaItem):
    """Represents database extension file.

    Supported SQL actions:
        `create`
    """
    def __init__(self, schema, attributes):
        super(DatabaseFile, self).__init__(schema, attributes)
        self._type_code = []

        self._strip_attribute('RDB$FILE_NAME')

    #--- Protected
    def _get_name(self):
        return 'FILE_%d' % self.sequence
    def _get_filename(self):
        return self._attributes['RDB$FILE_NAME']
    def _get_sequence(self):
        return self._attributes['RDB$FILE_SEQUENCE']
    def _get_start(self):
        return self._attributes['RDB$FILE_START']
    def _get_length(self):
        return self._attributes['RDB$FILE_LENGTH']

    #--- Properties
    filename = LateBindingProperty(_get_filename, doc="File name.")
    sequence = LateBindingProperty(_get_sequence, doc="File sequence number.")
    start = LateBindingProperty(_get_start, doc="File start page number.")
    length = LateBindingProperty(_get_length, doc="File length in pages.")
    #--- Public
    def issystemobject(self):
        "Returns True."
        return True

class Shadow(BaseSchemaItem):
    """Represents database shadow.

    Supported SQL actions:
        `create`, `drop` (preserve=bool)
    """
    SHADOW_INACTIVE = 2
    SHADOW_MANUAL = 4
    SHADOW_CONDITIONAL = 16

    def __init__(self, schema, attributes):
        super(Shadow, self).__init__(schema, attributes)
        self._type_code = []

        self.__files = None
        self._actions = ['create', 'drop']

    def _get_create_sql(self, **params):
        self._check_params(params, [])
        result = 'CREATE SHADOW %d %s%s' % (self.id, 'MANUAL' if self.ismanual()
                                            else 'AUTO',
                                            ' CONDITIONAL' if self.isconditional()
                                            else '')
        if len(self.files) == 1:
            result += " '%s'" % self.files[0].filename
        else:
            f = self.files[0]
            result += " '%s'%s\n" % (f.filename,
                                     ' LENGTH %d' % f.length if f.length > 0 else '')
            for f in self.files[1:]:
                result += "  FILE '%s'%s%s" % (f.filename,
                                               ' STARTING AT %d' % f.start if f.start > 0 else '',
                                               ' LENGTH %d' % f.length if f.length > 0 else '')
                if f.sequence < len(self.files)-1:
                    result += '\n'
        return result
    def _get_drop_sql(self, **params):
        self._check_params(params, ['preserve'])
        preserve = params.get('preserve')
        return 'DROP SHADOW %d%s' % (self.id, ' PRESERVE FILE' if preserve else '')
    def _get_name(self):
        return 'SHADOW_%d' % self.id
    def _get_id(self):
        return self._attributes['RDB$SHADOW_NUMBER']
    def _get_flags(self):
        return self._attributes['RDB$FILE_FLAGS']
    def _get_files(self):
        if self.__files is None:
            self.__files = [DatabaseFile(self, row) for row
                            in self.schema._select("""
select RDB$FILE_NAME, RDB$FILE_SEQUENCE,
RDB$FILE_START, RDB$FILE_LENGTH from RDB$FILES
where RDB$SHADOW_NUMBER = ?
order by RDB$FILE_SEQUENCE""", (self._attributes['RDB$SHADOW_NUMBER'],))]
        return self.__files

    #--- Properties
    id = LateBindingProperty(_get_id, doc="Shadow ID number.")
    flags = LateBindingProperty(_get_flags, doc="Shadow flags.")
    files = LateBindingProperty(_get_files,
                                doc="List of shadow files. Items are :class:`DatabaseFile` instances.")
    #--- Public
    def issystemobject(self):
        "Returns False."
        return False
    def ismanual(self):
        "Returns True if it's MANUAL shadow."
        return bool(self.flags & self.SHADOW_MANUAL)
    def isinactive(self):
        "Returns True if it's INACTIVE shadow."
        return bool(self.flags & self.SHADOW_INACTIVE)
    def isconditional(self):
        "Returns True if it's CONDITIONAL shadow."
        return bool(self.flags & self.SHADOW_CONDITIONAL)

class Privilege(BaseSchemaItem):
    """Represents priviledge to database object.

    Supported SQL actions:
        `grant` (grantors), `revoke` (grantors, grant_option)
    """
    def __init__(self, schema, attributes):
        super(Privilege, self).__init__(schema, attributes)
        self._type_code = []

        self._actions = ['grant', 'revoke']

        self._strip_attribute('RDB$USER')
        self._strip_attribute('RDB$GRANTOR')
        self._strip_attribute('RDB$PRIVILEGE')
        self._strip_attribute('RDB$RELATION_NAME')
        self._strip_attribute('RDB$FIELD_NAME')
    def _get_grant_sql(self, **params):
        self._check_params(params, ['grantors'])
        grantors = params.get('grantors', ['SYSDBA'])
        privileges = {'S':'SELECT', 'I':'INSERT', 'U':'UPDATE', 'D':'DELETE', 'R':'REFERENCES'}
        admin_option = ' WITH GRANT OPTION' if self.has_grant() else ''
        if self.privilege in privileges:
            privilege = privileges[self.privilege]
            if self.field_name is not None:
                privilege += '(%s)' % self.field_name
            privilege += ' ON '
        elif self.privilege == 'X': # procedure
            privilege = 'EXECUTE ON PROCEDURE '
        else: # role membership
            privilege = ''
            admin_option = ' WITH ADMIN OPTION' if self.has_grant() else ''
        user = self.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        if (grantors is not None) and (self.grantor_name not in grantors):
            granted_by = ' GRANTED BY %s' % self.grantor_name
        else:
            granted_by = ''
        return 'GRANT %s%s TO %s%s%s%s' % (privilege, self.subject_name, utype,
                                           self.user_name, admin_option, granted_by)
    def _get_revoke_sql(self, **params):
        self._check_params(params, ['grant_option', 'grantors'])
        grantors = params.get('grantors', ['SYSDBA'])
        option_only = params.get('grant_option', False)
        if option_only and not self.has_grant():
            raise fdb.ProgrammingError("Can't revoke grant option that wasn't granted.")
        privileges = {'S':'SELECT', 'I':'INSERT', 'U':'UPDATE', 'D':'DELETE', 'R':'REFERENCES'}
        admin_option = 'GRANT OPTION FOR ' if self.has_grant() and option_only else ''
        if self.privilege in privileges:
            privilege = privileges[self.privilege]
            if self.field_name is not None:
                privilege += '(%s)' % self.field_name
            privilege += ' ON '
        elif self.privilege == 'X': # procedure
            privilege = 'EXECUTE ON PROCEDURE '
        else: # role membership
            privilege = ''
            admin_option = 'ADMIN OPTION FOR' if self.has_grant() and option_only else ''
        user = self.user
        if isinstance(user, Procedure):
            utype = 'PROCEDURE '
        elif isinstance(user, Trigger):
            utype = 'TRIGGER '
        elif isinstance(user, View):
            utype = 'VIEW '
        else:
            utype = ''
        if (grantors is not None) and (self.grantor_name not in grantors):
            granted_by = ' GRANTED BY %s' % self.grantor_name
        else:
            granted_by = ''
        return 'REVOKE %s%s%s FROM %s%s%s' % (admin_option, privilege, self.subject_name, utype,
                                              self.user_name, granted_by)
    def _get_user(self):
        return self.schema._get_item(self._attributes['RDB$USER'],
                                     self._attributes['RDB$USER_TYPE'])
    def _get_grantor(self):
        return fdb.services.User(self._attributes['RDB$GRANTOR'])
    def _get_privilege(self):
        return self._attributes['RDB$PRIVILEGE']
    def _get_subject(self):
        rname = self._attributes['RDB$RELATION_NAME']
        result = self.schema._get_item(rname, self.subject_type, self.field_name)
        if result is None and self.subject_type == 0:
            # Views are logged as tables, so try again for view code
            result = self.schema._get_item(rname, 1, self.field_name)
        return result
    def _get_user_name(self):
        return self._attributes['RDB$USER']
    def _get_user_type(self):
        return self._attributes['RDB$USER_TYPE']
    def _get_grantor_name(self):
        return self._attributes['RDB$GRANTOR']
    def _get_subject_name(self):
        return self._attributes['RDB$RELATION_NAME']
    def _get_subject_type(self):
        return self._attributes['RDB$OBJECT_TYPE']
    def _get_field_name(self):
        return self._attributes['RDB$FIELD_NAME']

    #--- Properties
    user = LateBindingProperty(_get_user,
                               doc="Grantee. Either :class:`~fdb.services.User`, :class:`Role`,"
                               " :class:`Procedure`, :class:`Trigger` or :class:`View` object.")
    grantor = LateBindingProperty(_get_grantor, doc="Grantor :class:`~fdb.services.User` object.")
    privilege = LateBindingProperty(_get_privilege, doc="Privilege code.")
    subject = LateBindingProperty(_get_subject,
                                  doc="Priviledge subject. Either :class:`Role`, :class:`Table`,"
                                  " :class:`View` or :class:`Procedure` object.")
    user_name = LateBindingProperty(_get_user_name, doc="User name.")
    user_type = LateBindingProperty(_get_user_type, doc="User type.")
    grantor_name = LateBindingProperty(_get_grantor_name, doc="Grantor name.")
    subject_name = LateBindingProperty(_get_subject_name, doc="Subject name.")
    subject_type = LateBindingProperty(_get_subject_type, doc="Subject type.")
    field_name = LateBindingProperty(_get_field_name, doc="Field name.")
    #--- Public
    def has_grant(self):
        "Returns True if privilege comes with GRANT OPTION."
        return bool(self._attributes['RDB$GRANT_OPTION'])
    def issystemobject(self):
        "Returns True."
        return True
    def isselect(self):
        "Returns True if this is SELECT privilege."
        return self.privilege == 'S'
    def isinsert(self):
        "Returns True if this is INSERT privilege."
        return self.privilege == 'I'
    def isupdate(self):
        "Returns True if this is UPDATE privilege."
        return self.privilege == 'U'
    def isdelete(self):
        "Returns True if this is DELETE privilege."
        return self.privilege == 'D'
    def isexecute(self):
        "Returns True if this is EXECUTE privilege."
        return self.privilege == 'X'
    def isreference(self):
        "Returns True if this is REFERENCE privilege."
        return self.privilege == 'R'
    def ismembership(self):
        "Returns True if this is ROLE membership privilege."
        return self.privilege == 'M'

class Package(BaseSchemaItem):
    """Represents PSQL package.

    Supported SQL actions:
        `create` (body=bool), `recreate` (body=bool), `create_or_alter` (body=bool),
        `alter` (header=string_or_list), `drop` (body=bool)
    """
    def __init__(self, schema, attributes):
        super(Package, self).__init__(schema, attributes)
        self._type_code = [18, 19]

        self._actions = ['create', 'recreate', 'create_or_alter', 'alter', 'drop']

        self._strip_attribute('RDB$PACKAGE_NAME')
        self._strip_attribute('RDB$SECURITY_CLASS')
        self._strip_attribute('RDB$OWNER_NAME')

    def _get_create_sql(self, **params):
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        result = 'CREATE PACKAGE %s%s' % (cbody, self.get_quoted_name())
        return result+'\nAS\n'+(self.body if body else self.header)
    def _get_alter_sql(self, **params):
        self._check_params(params, ['header'])
        header = params.get('header')
        if not header:
            hdr = ''
        else:
            hdr = '\n'.join(header) if isinstance(header, fdb.ListType) else header
        return 'ALTER PACKAGE %s\nAS\nBEGIN\n%s\nEND' % (self.get_quoted_name(), hdr)
    def _get_drop_sql(self, **params):
        self._check_params(params, ['body'])
        body = params.get('body')
        cbody = 'BODY ' if body else ''
        return 'DROP PACKAGE %s%s' % (cbody, self.get_quoted_name())
    def _get_name(self):
        return self._attributes['RDB$PACKAGE_NAME']
    def _get_security_class(self):
        return self._attributes['RDB$SECURITY_CLASS']
    def _get_owner_name(self):
        return self._attributes['RDB$OWNER_NAME']
    def _get_header(self):
        return self._attributes['RDB$PACKAGE_HEADER_SOURCE']
    def _get_body(self):
        return self._attributes['RDB$PACKAGE_BODY_SOURCE']
    def _get_functions(self):
        return self.schema.functions.filter(lambda fn:
                                            fn._attributes['RDB$PACKAGE_NAME'] == self.name)
    def _get_procedures(self):
        return self.schema.procedures.filter(lambda proc:
                                             proc._attributes['RDB$PACKAGE_NAME'] == self.name)

    #--- Properties
    header = LateBindingProperty(_get_header, doc="Package header source.")
    body = LateBindingProperty(_get_body, doc="Package body source.")
    security_class = LateBindingProperty(_get_security_class, doc="Security class name or None.")
    owner_name = LateBindingProperty(_get_owner_name, doc="User name of package creator.")
    functions = LateBindingProperty(_get_functions,
                                    doc=":class:`~fdb.utils.ObjectList` of package functions."
                                    " Items are :class:`Function` instances.")
    procedures = LateBindingProperty(_get_procedures,
                                     doc=":class:`~fdb.utils.ObjectList` of package procedures."
                                     " Items are :class:`Procedure` instances.")
    #--- Public
    def has_valid_body(self):
        result = self._attributes.get('RDB$VALID_BODY_FLAG')
        return bool(result) if result is not None else None

class BackupHistory(BaseSchemaItem):
    """Represents entry of history for backups performed using the nBackup utility.

    Supported SQL actions:
        `None`
    """
    def __init__(self, schema, attributes):
        super(BackupHistory, self).__init__(schema, attributes)
        self._type_code = []

        self._strip_attribute('RDB$FILE_NAME')

    #--- Protected
    def _get_name(self):
        return 'BCKP_%d' % self.sequence
    def _get_backup_id(self):
        return self._attributes['RDB$BACKUP_ID']
    def _get_filename(self):
        return self._attributes['RDB$FILE_NAME']
    def _get_created(self):
        return self._attributes['RDB$TIMESTAMP']
    def _get_level(self):
        return self._attributes['RDB$BACKUP_LEVEL']
    def _get_scn(self):
        return self._attributes['RDB$SCN']
    def _get_guid(self):
        return self._attributes['RDB$GUID']

    #--- Properties
    backup_id = LateBindingProperty(_get_backup_id, doc="The identifier assigned by the engine.")
    filename = LateBindingProperty(_get_filename, doc="Full path and file name of backup file.")
    created = LateBindingProperty(_get_created, doc="Backup date and time.")
    level = LateBindingProperty(_get_level, doc="Backup level.")
    scn = LateBindingProperty(_get_scn, doc="System (scan) number.")
    guid = LateBindingProperty(_get_guid, doc="Unique identifier.")
    #--- Public
    def issystemobject(self):
        "Returns True."
        return True


class Filter(BaseSchemaItem):
    """Represents userdefined BLOB filter.

    Supported SQL actions:
        - BLOB filter: `declare`, `drop`, `comment`
        - System UDF: `none`
    """
    def __init__(self, schema, attributes):
        super(Filter, self).__init__(schema, attributes)
        self._type_code = [16,]

        self._strip_attribute('RDB$FUNCTION_NAME')
        self._strip_attribute('RDB$MODULE_NAME')
        self._strip_attribute('RDB$ENTRYPOINT')

        self.__ods = schema._con.ods

        if not self.issystemobject():
            self._actions = ['comment', 'declare', 'drop']

    #--- Protected
    def _get_declare_sql(self, **params):
        self._check_params(params, [])
        fdef = 'DECLARE FILTER %s\nINPUT_TYPE %d OUTPUT_TYPE %d\n' % (self.get_quoted_name(),
                                                                      self.input_sub_type,
                                                                      self.output_sub_type)
        return "%sENTRY_POINT '%s' MODULE_NAME '%s'" % (fdef, self.entrypoint, self.module_name)
    def _get_drop_sql(self, **params):
        self._check_params(params, [])
        return 'DROP FILTER %s' % self.get_quoted_name()
    def _get_comment_sql(self, **params):
        return 'COMMENT ON FILTER %s IS %s' % (self.get_quoted_name(),
                                               'NULL' if self.description is None
                                               else "'%s'" % escape_single_quotes(self.description))
    def _get_name(self):
        return self._attributes['RDB$FUNCTION_NAME']
    def _get_module_name(self):
        return self._attributes['RDB$MODULE_NAME']
    def _get_entrypoint(self):
        return self._attributes['RDB$ENTRYPOINT']
    def _get_input_sub_type(self):
        return self._attributes.get('RDB$INPUT_SUB_TYPE')
    def _get_output_sub_type(self):
        return self._attributes.get('RDB$OUTPUT_SUB_TYPE')

    #--- Properties
    module_name = LateBindingProperty(_get_module_name, doc="The name of the dynamic library or shared object where the code of the BLOB filter is located.")
    entrypoint = LateBindingProperty(_get_entrypoint, doc="The exported name of the BLOB filter in the filter library.")
    input_sub_type = LateBindingProperty(_get_input_sub_type, doc="The BLOB subtype of the data to be converted by the function.")
    output_sub_type = LateBindingProperty(_get_output_sub_type, doc="The BLOB subtype of the converted data.")

