#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           services.py
#   DESCRIPTION:    Python driver for Firebird - Firebird services
#   CREATED:        19.11.2011
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

import fdb
import sys
import os
import fdb.ibase as ibase
import ctypes
import struct
import warnings
import datetime
import types

api = None

#: The following SHUT_* constants are to be passed as the `shutdown_mode` parameter to Connection.shutdown:
SHUT_LEGACY = -1
SHUT_NORMAL = ibase.isc_spb_prp_sm_normal
SHUT_MULTI = ibase.isc_spb_prp_sm_multi
SHUT_SINGLE = ibase.isc_spb_prp_sm_single
SHUT_FULL = ibase.isc_spb_prp_sm_full

#: The following SHUT_* constants are to be passed as the `shutdown_method` parameter to Connection.shutdown:
SHUT_FORCE = ibase.isc_spb_prp_shutdown_db
SHUT_DENY_NEW_TRANSACTIONS = ibase.isc_spb_prp_deny_new_transactions
SHUT_DENY_NEW_ATTACHMENTS = ibase.isc_spb_prp_deny_new_attachments

#: The following WRITE_* constants are to be passed as the `mode` parameter to Connection.set_write_mode:
WRITE_FORCED = ibase.isc_spb_prp_wm_sync
WRITE_BUFFERED = ibase.isc_spb_prp_wm_async

#: The following ACCESS_* constants are to be passed as the `mode` parameter to Connection.set_access_mode:
ACCESS_READ_WRITE = ibase.isc_spb_prp_am_readwrite
ACCESS_READ_ONLY = ibase.isc_spb_prp_am_readonly

#: The following CAPABILITY_* constants are return values of `get_server_capabilities`
CAPABILITY_MULTI_CLIENT = 0x2
CAPABILITY_REMOTE_HOP = 0x4
CAPABILITY_SERVER_CONFIG = 0x200
CAPABILITY_QUOTED_FILENAME = 0x400
CAPABILITY_NO_SERVER_SHUTDOWN = 0x100

#: The following STATS_* constants are options for backup/restore 'stats' parameter.
STATS_TOTAL_TIME = 'T'
STATS_TIME_DELTA = 'D'
STATS_PAGE_READS = 'R'
STATS_PAGE_WRITES = 'W'

def _check_string(st):
    if ibase.PYTHON_MAJOR_VER == 3:
        try:
            if isinstance(st, str):
               # In str instances, Python allows any character
               # Since Firebird's
               # Services API only works (properly) with ASCII, we need to make
               # sure there are no non-ASCII characters in s.
                st.encode('ASCII')
            else:
                if not isinstance(st, ibase.mybytes):
                    raise TypeError('String argument to Services API must be'
                                    ' of type %s, not %s.' % (type(ibase.mybytes), type(st)))
        except UnicodeEncodeError:
            raise TypeError("The database engine's Services API only works"
                            " properly with ASCII string parameters, so str instances that"
                            " contain non-ASCII characters are disallowed.")
    else:
        try:
            if isinstance(st, ibase.UnicodeType):
               # In str instances, Python allows any character in the "default
               # encoding", which is typically not ASCII.  Since Firebird's
               # Services API only works (properly) with ASCII, we need to make
               # sure there are no non-ASCII characters in s, even though we
               # already know s is a str instance.
                st.encode('ASCII')
            else:
                if not isinstance(st, ibase.mybytes):
                    raise TypeError('String argument to Services API must be'
                                    ' of type %s, not %s.' % (type(ibase.mybytes), type(st)))
        except UnicodeError:
            raise TypeError("The database engine's Services API only works"
                            " properly with ASCII string parameters, so str instances that"
                            " contain non-ASCII characters, and all unicode instances, are"
                            " disallowed.")

def _string2spb(spb, code, st):
    myslen = len(st)
    _numeric2spb(spb, code, myslen, numctype='H')
    myformat = str(myslen) + 's'  # The length, then 's'.
    spb.append(struct.pack(myformat, st))


def _numeric2spb(spb, code, num, numctype='I'):
    # numCType is one of the pack format characters specified by the Python
    # standard library module 'struct'.
    _code2spb(spb, code)
    (numeric_format, numeric_bytes) = _render_sized_integer_for_spb(num, numctype)
    spb.append(struct.pack(numeric_format, numeric_bytes))


def _code2spb(spb, code):
    (myformat, mybytes) = _render_sized_integer_for_spb(code, 'b')
    spb.append(struct.pack(myformat, mybytes))


def _vax_inverse(i, myformat):
    # Apply the inverse of _ksrv.isc_vax_integer to a Python integer; return
    # the raw bytes of the resulting value.
    iraw = struct.pack(myformat, i)
    iconv = api.isc_vax_integer(iraw, len(iraw))
    iconvraw = struct.pack(myformat, iconv)
    return iconvraw


def _render_sized_integer_for_spb(i, myformat):
    #   In order to prepare the Python integer i for inclusion in a Services
    # API action request buffer, the byte sequence of i must be reversed, which
    # will make i unrepresentible as a normal Python integer.
    #   Therefore, the rendered version of i must be stored in a raw byte
    # buffer.
    #   This function returns a 2-tuple containing:
    # 1. the calculated struct.pack-compatible format string for i
    # 2. the Python string containing the SPB-compatible raw binary rendering
    #    of i
    #
    # Example:
    # To prepare the Python integer 12345 for storage as an unsigned int in a
    # SPB, use code such as this:
    #   (iPackFormat, iRawBytes) = _renderSizedIntegerForSPB(12345, 'I')
    #   spbBytes = struct.pack(iPackFormat, iRawBytes)
    #
    dest_format = '%ds' % struct.calcsize(myformat)
    dest_val = _vax_inverse(i, myformat)
    return (dest_format, dest_val)


def connect(host='service_mgr', user=None, password=None):
    """Establishes a connection to the Services Manager.

    Args:
        host (str): (optional) Host machine specification. Local by default.
        user (str): (optional) Administrative user name. Defaults to content
            of environment variable `ISC_USER` or `SYSDBA`.
        password (str): Administrative user password. Default is content
            of environment variable `ISC_PASSWORD`.

    Note:
       By definition, a Services Manager connection is bound to a particular
       host.  Therefore, the database specified as a parameter to methods such as
       `getStatistics` MUST NOT include the host name of the database server.

    Hooks:
        Event `HOOK_SERVICE_ATTACHED`: Executed before :class:`Connection`
        instance is returned. Hook must have signature:
        hook_func(connection). Any value returned by hook is ignored.
    """
    setattr(sys.modules[__name__], 'api', fdb.load_api())
    if not user:
        user = os.environ.get('ISC_USER', 'SYSDBA')
    if not password:
        password = os.environ.get('ISC_PASSWORD', None)
    if password is None:
        raise fdb.ProgrammingError('A password is required to use'
                                   ' the Services Manager.')

    _check_string(host)
    _check_string(user)
    _check_string(password)

    # The database engine's Services API requires that connection strings
    # conform to one of the following formats:
    # 1. 'service_mgr' - Connects to the Services Manager on localhost.
    # 2. 'hostname:service_mgr' - Connects to the Services Manager on the
    #   server named hostname.
    #
    # This Python function glosses over the database engine's rules as follows:
    # - If the $host parameter is not supplied, the connection defaults to
    #   the local host.
    # - If the $host parameter is supplied, the ':service_mgr' suffix is
    #   optional (the suffix will be appended automatically if necessary).
    #
    # Of course, this scheme would collapse if someone actually had a host
    # named 'service_mgr', and supplied the connection string 'service_mgr'
    # with the intent of connecting to that host.  In that case, the connection
    # would be attempted to the local host, not to the host named
    # 'service_mgr'.  An easy workaround would be to supply the following
    # connection string:
    #   'service_mgr:service_mgr'.
    if not host.endswith('service_mgr'):
        if host and not host.endswith(':'):
            host += ':'
        host += 'service_mgr'

    con = Connection(host, user, password)
    for hook in fdb.get_hooks(fdb.HOOK_SERVICE_ATTACHED):
        hook(con)
    return con

class Connection(object):
    """
    Represents a sevice connection between the database client (the Python process)
    and the database server.

    .. important::

       DO NOT create instances of this class directly! Use only :func:`connect`
       to get Connection instances.

    .. tip::

       Connection supports the iterator protocol, yielding lines of result like
       :meth:`readline`.
    """
    QUERY_TYPE_PLAIN_INTEGER = 1
    QUERY_TYPE_PLAIN_STRING = 2
    QUERY_TYPE_RAW = 3

    def __init__(self, host, user, password, charset=None):
        self.__fetching = False
        self._svc_handle = ibase.isc_svc_handle(0)
        self._isc_status = ibase.ISC_STATUS_ARRAY()
        self._result_buffer = ctypes.create_string_buffer(ibase.USHRT_MAX)
        self._line_buffer = []
        self.__eof = False
        self.charset = charset
        self.host = ibase.b(host)
        self.user = ibase.b(user)
        self.password = ibase.b(password)

        if len(self.host) + len(self.user) + len(self.password) > 118:
            raise fdb.ProgrammingError("The combined length of host, user and"
                                       " password can't exceed 118 bytes.")
        # spb_length = 2 + 1 + 1 + len(self.user) + 1 + 1 + len(self.password)
        spb = fdb.bs([ibase.isc_spb_version, ibase.isc_spb_current_version,
                      ibase.isc_spb_user_name, len(self.user)]) + self.user + \
                      fdb.bs([ibase.isc_spb_password, len(self.password)]) + self.password
        api.isc_service_attach(self._isc_status, len(self.host), self.host,
                               self._svc_handle, len(spb), spb)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.DatabaseError,
                                            self._isc_status,
                                            "Services/isc_service_attach:")
        # Get Firebird engine version
        verstr = self.get_server_version()
        x = verstr.split()
        if x[0].find('V') > 0:
            (x, self.__version) = x[0].split('V')
        elif x[0].find('T') > 0:
            (x, self.__version) = x[0].split('T')
        else:
            # Unknown version
            self.__version = '0.0.0.0'
        x = self.__version.split('.')
        self.__engine_version = float('%s.%s' % (x[0], x[1]))
    def __del__(self):
        self.close()
    def next(self):
        """Return the next result line from service manager. Part of *iterator
        protocol*.

        Raises:
            StopIteration: If there are no further lines.
        """
        line = self.readline()
        if line is not None:
            return line
        else:
            self.__fetching = False
            raise StopIteration
    __next__ = next
    def __iter__(self):
        return self
    def __check_active(self):
        if not self._svc_handle:
            raise ProgrammingError("Connection object is detached from service manager")
    def __get_closed(self):
        return True if self._svc_handle else False
    def __get_fetching(self):
        return self.__fetching
    def __read_buffer(self, init=''):
        request = fdb.bs([ibase.isc_info_svc_to_eof])
        spb = ibase.b('')
        api.isc_service_query(self._isc_status, self._svc_handle, None, len(spb), spb,
                              len(request), request, ibase.USHRT_MAX, self._result_buffer)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.DatabaseError, self._isc_status,
                                            "Services/isc_service_query:")
        (result, _) = self._extract_string(self._result_buffer, 1)
        if ord(self._result_buffer[_]) == ibase.isc_info_end:
            self.__eof = True
        if init:
            result = init + result
        self._line_buffer = result.split('\n')

    def __fetchline(self):
        if self._line_buffer:
            if len(self._line_buffer) == 1 and not self.__eof:
                self.__read_buffer(self._line_buffer.pop(0))
            return self._line_buffer.pop(0)
        else:
            if not self.__eof:
                self.__read_buffer()
            if self._line_buffer:
                return self._line_buffer.pop(0)
            else:
                self.__fetching = False
                return None
    def __get_version(self):
        return self.__version
    def __get_engine_version(self):
        return self.__engine_version
    def _bytes_to_str(self, sb):
        ### Todo: verify handling of P version differences, refactor
        if ibase.PYTHON_MAJOR_VER == 3:
            return sb.decode(ibase.charset_map.get(self.charset, self.charset))
        else:
            return sb.encode(ibase.charset_map.get(self.charset, self.charset))
    def _str_to_bytes(self, st):
        ### Todo: verify handling of P version differences, refactor
        if ibase.PYTHON_MAJOR_VER == 3:
            return st.encode(ibase.charset_map.get(self.charset, self.charset))
        else:
            return st
    def _extract_int(self, raw, index):
        new_index = index + ctypes.sizeof(ctypes.c_ushort)
        return (fdb.bytes_to_uint(raw[index:new_index]), new_index)
    def _extract_longint(self, raw, index):
        new_index = index + ctypes.sizeof(ctypes.c_uint)
        return (fdb.bytes_to_int(raw[index:new_index]), new_index)
    def _extract_string(self, raw, index):
        (size, index) = self._extract_int(raw, index)
        new_index = index + size
        ### Todo: verify handling of P version differences, refactor
        if ibase.PYTHON_MAJOR_VER == 3:
            return (str(raw[index:new_index], ibase.charset_map.get(self.charset, self.charset)),
                    new_index)
        else:
            return (str(raw[index:new_index]), new_index)
    def _extract_bytes(self, raw, index):
        (size, index) = self._extract_int(raw, index)
        new_index = index + size
        return (bytes(raw[index:new_index]), new_index)
    def _Q(self, code, result_type, timeout=-1):
        if code < 0 or code > ibase.USHRT_MAX:
            raise fdb.ProgrammingError("The service query request_buf code must fall between 0 and %d, inclusive." % ibase.USHRT_MAX)
        result = None
        result_size = 1024
        request = fdb.bs([code])
        if timeout == -1:
            spb = ibase.b('')
        else:
            spb = ctypes.create_string_buffer(8)
            spb[0] = ibase.int2byte(ibase.isc_info_svc_timeout)
            spb[1:3] = fdb.uint_to_bytes(4, 2)
            spb[3:7] = fdb.uint_to_bytes(timeout, 4)
            spb[7] = ibase.int2byte(ibase.isc_info_end)
        while True:
            result_buffer = ctypes.create_string_buffer(result_size)
            api.isc_service_query(self._isc_status, self._svc_handle, None,
                                  len(spb), spb,
                                  len(request), request,
                                  result_size, result_buffer)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError, self._isc_status, "Services/isc_service_query:")
            if ord(result_buffer[0]) == ibase.isc_info_truncated:
                if result_size == ibase.USHRT_MAX:
                    raise fdb.InternalError("Database C API constraints maximum result buffer size to %d" % ibase.USHRT_MAX)
                else:
                    result_size = result_size * 4
                    if result_size > ibase.USHRT_MAX:
                        result_size = ibase.USHRT_MAX
                    continue
            break
        if result_type == self.QUERY_TYPE_PLAIN_INTEGER:
            (result, _) = self._extract_int(result_buffer, 1)
        elif result_type == self.QUERY_TYPE_PLAIN_STRING:
            (result, _) = self._extract_string(result_buffer, 1)
        elif result_type == self.QUERY_TYPE_RAW:
            size = result_size - 1
            while result_buffer[size] == '\0':
                size -= 1
            result = ibase.s(result_buffer[:size])
        return result
    def _get_isc_info_svc_svr_db_info(self):
        num_attachments = -1
        databases = []

        raw = self._QR(ibase.isc_info_svc_svr_db_info)
#        assert raw[-1] == api.int2byte(ibase.isc_info_flag_end)

        pos = 1  # Ignore raw[0].
        upper_limit = len(raw) - 1
        while pos < upper_limit:
            cluster = ibase.ord2(raw[pos])
            pos += 1

            if cluster == ibase.isc_spb_num_att:  # Number of attachments.
                (num_attachments, pos) = self._extract_int(raw, pos)
            elif cluster == ibase.isc_spb_num_db:  # Number of databases
                                                   # attached to.
                # Do nothing except to advance pos; the number of databases
                # can be had from len(databases).
                (_, pos) = self._extract_int(raw, pos)
            elif cluster == ibase.isc_spb_dbname:
                (db_name, pos) = self._extract_string(raw, pos)
                databases.append(db_name)

        return (num_attachments, databases)
    def _QI(self, code):
        return self._Q(code, self.QUERY_TYPE_PLAIN_INTEGER)
    def _QS(self, code):
        return self._Q(code, self.QUERY_TYPE_PLAIN_STRING)
    def _QR(self, code):
        return self._Q(code, self.QUERY_TYPE_RAW)
    def _action_thin(self, request_buffer):
        if len(request_buffer) > ibase.USHRT_MAX:
            raise fdb.ProgrammingError("The size of the request buffer must not exceed %d." % ibase.USHRT_MAX)
        api.isc_service_start(self._isc_status, self._svc_handle, None, len(request_buffer), request_buffer)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.OperationalError, self._isc_status, "Unable to perform the requested Service API action:")
        return None
    def _act(self, request_buffer):
        return self._action_thin(request_buffer.render())
    def _act_and_return_textual_results(self, request_buffer):
        self._act(request_buffer)
        return self._collect_unformatted_results()
    def _collect_unformatted_results(self, line_separator='\n'):
        # YYY: It might be desirable to replace this function with a more
        # performant version based on ibase.isc_info_svc_to_eof rather than
        # ibase.isc_info_svc_line; the function's interface is transparent
        # either way.
        #   This enhancement should be a very low priority; the Service Manager
        # API is not typically used for performance-intensive operations.
        result_lines = []
        while 1:
            try:
                line = self._QS(ibase.isc_info_svc_line)
            except fdb.OperationalError:
                # YYY: It is routine for actions such as RESTORE to raise an
                # exception at the end of their output.  We ignore any such
                # exception and assume that it was expected, which is somewhat
                # risky.  For example, suppose the network connection is broken
                # while the client is receiving the action's output...
                break
            if not line:
                break
            result_lines.append(line)
        return line_separator.join(result_lines)
    def _repair_action(self, database, partial_req_buf, line_separator='\n'):
        # Begin constructing the request buffer (incorporate the one passed as
        # param $partial_req_buf).
        full_req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_repair)
        # The filename of the database must be specified regardless of the
        # action sub-action being perfomed.
        full_req_buf.add_database_name(database)
        # Incorporate the caller's partial request buffer.
        full_req_buf.extend(partial_req_buf)
        self._action_thin(full_req_buf.render())
        # Return the results to the caller synchronously (in this case, there
        # won't be any textual results, but issuing this call will helpfully
        # cause the program to block until the Services Manager is finished
        # with the action).
        return self._collect_unformatted_results(line_separator=line_separator)
    def _validate_companion_string_numeric_sequences(self, strings, numbers,
                                                     string_caption,
                                                     number_caption):
        # The core constraint here is that len(numbers) must equal len(strings) - 1
        strings_count = len(strings)
        numbers_count = len(numbers)

        required_numbers_count = strings_count - 1

        if numbers_count != required_numbers_count:
            raise ValueError(
                'Since you passed %d %s, you must %s corresponding %s.'
                % (strings_count, string_caption,
                   ('pass %d' % required_numbers_count
                    if required_numbers_count > 0
                    else 'not pass any'),
                   number_caption)
            )
    def _exclude_elements_of_types(self, seq, types_to_exclude):
        if not isinstance(types_to_exclude, tuple):
            types_to_exclude = tuple(types_to_exclude)
        return [element for element in seq
                if not isinstance(element, types_to_exclude)]
    def _require_str_or_tuple_of_str(self, x):
        if isinstance(x, str):
            x = (x,)
        elif isinstance(x, ibase.myunicode):
          # We know the following call to _checkString will raise an exception,
          # but calling it anyway allows us to centralize the error message
          # generation:
            _check_string(x)
        ### Todo: verify handling of P version differences, refactor?
        for el in x:
            _check_string(el)
        return x
    def _property_action(self, database, partial_req_buf):
        # Begin constructing the request buffer (incorporate the one passed as
        # param $partialReqBuf).
        full_req_buf = _ServiceActionRequestBuilder(
            ibase.isc_action_svc_properties)
        # The filename of the database must be specified regardless of the
        # action sub-action being perfomed.
        full_req_buf.add_database_name(database)
        # Incorporate the caller's partial request buffer.
        full_req_buf.extend(partial_req_buf)
        self._action_thin(full_req_buf.render())
        # Return the results to the caller synchronously
        # because it blocks until there's been some resolution of the action.
        return self._collect_unformatted_results()
    def _property_action_with_one_num_code(self, database, code, num, num_ctype='I'):
        req_buf = _ServiceActionRequestBuilder()
        req_buf.add_numeric(code, num, numctype=num_ctype)
        return self._property_action(database, req_buf)
    def close(self):
        """Close the connection now (rather than whenever `__del__` is called).
        The connection will be unusable from this point forward; an :exc:`Error`
        (or subclass) exception will be raised if any operation is attempted
        with the connection.
        """
        if self._svc_handle:
            api.isc_service_detach(self._isc_status, self._svc_handle)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError,
                                                self._isc_status, "Services/isc_service_detach:")
            self._svc_handle = None
            self.__fetching = False
    def readline(self):
        """Get next line of textual output from last service query.

        Returns:
            str: Output line.
        """
        if self.__fetching:
            return self.__fetchline()
        else:
            return None
    def readlines(self):
        """Get list of remaining output lines from last service query.

        Returns:
            list: Service output.

        Raises:
            fdb.ProgrammingError: When service is not in :attr:`fetching` mode.
        """
        return [line for line in self]
    def isrunning(self):
        """Returns True if service is running.

        Note:
           Some services like :meth:`backup` or :meth:`sweep` may take time to
           comlete, so they're called asynchronously. Until they're finished,
           no other async service could be started.
        """
        return self._QI(ibase.isc_info_svc_running) > 0
    def wait(self):
        """Wait until running service completes, i.e. stops sending data.
        """
        while self.isrunning():
            for x in self:
                pass
    def get_service_manager_version(self):
        """Get Firebird Service Manager version number.

        Returns:
            int: Version number.
        """
        self.__check_active()
        return self._QI(ibase.isc_info_svc_version)
    def get_server_version(self):
        """Get Firebird version.

        Returns:
            str: Firebird version (example: 'LI-V2.5.2.26536 Firebird 2.5').
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_server_version)
    def get_architecture(self):
        """Get Firebird Server architecture.

        Returns:
            str: Architecture (example: 'Firebird/linux AMD64').
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_implementation)
    def get_home_directory(self):
        """Get Firebird Home (installation) Directory.

        Returns:
            str: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env)
    def get_security_database_path(self):
        """Get full path to Firebird security database.

        Returns:
            str: Path (path+filename) to security database.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_user_dbpath)
    def get_lock_file_directory(self):
        """Get directory location for Firebird lock files.

        Returns:
            str: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env_lock)
    def get_server_capabilities(self):
        """Get list of Firebird capabilities.

        Returns:
            tuple: Capability info codes for each capability reported by server.

        Next `fdb.services` constants define possible info codes returned::

            CAPABILITY_MULTI_CLIENT
            CAPABILITY_REMOTE_HOP
            CAPABILITY_SERVER_CONFIG
            CAPABILITY_QUOTED_FILENAME
            CAPABILITY_NO_SERVER_SHUTDOWN

        Example:
            .. code-block:: python

                >>>fdb.services.CAPABILITY_REMOTE_HOP in svc.get_server_capabilities()
                True
        """
        self.__check_active()
        capabilities = self._QI(ibase.isc_info_svc_capabilities)
        return tuple([x for x in (CAPABILITY_MULTI_CLIENT,
                                  CAPABILITY_REMOTE_HOP,
                                  CAPABILITY_SERVER_CONFIG,
                                  CAPABILITY_QUOTED_FILENAME,
                                  CAPABILITY_NO_SERVER_SHUTDOWN)
                      if capabilities & x])
    def get_message_file_directory(self):
        """Get directory with Firebird message file.

        Returns:
            str: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env_msg)
    def get_connection_count(self):
        """Get number of attachments to server.

        Returns:
            int: Number of attachments.
        """
        self.__check_active()
        return self._get_isc_info_svc_svr_db_info()[0]
    def get_attached_database_names(self):
        """Get list of attached databases.

        Returns:
            list: Filenames of attached databases.
        """
        self.__check_active()
        return self._get_isc_info_svc_svr_db_info()[1]
    def get_log(self, callback=None):
        """Request content of Firebird Server log. **(ASYNC service)**

        Keyword Args:
            callback (callable): Function to call back with each output line.
                Function must accept only one parameter: line of output.

        If `callback` is not specified, log content could be retrieved through
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.

        Note:
           Until log content is not fully fetched from service (or ignored via
           :meth:`wait`), any attempt to start another asynchronous service will
           fail with exception.
        """
        self.__check_active()
        self._act(_ServiceActionRequestBuilder(ibase.isc_action_svc_get_ib_log))
        self.__fetching = True
        self.__eof = False
        if callback:
            for line in self:
                callback(line)
    def get_limbo_transaction_ids(self, database):
        """Get list of transactions in limbo.

        Args:
            database (str): Database filename or alias.

        Returns:
            list: Transaction IDs.

        Raises:
            fdb.InternalError: When can't process the result buffer.
        """
        self.__check_active()
        _check_string(database)

        req_buf = _ServiceActionRequestBuilder()
        req_buf.add_option_mask(ibase.isc_spb_rpr_list_limbo_trans)
        raw = self._repair_action(database, req_buf, line_separator='')
        raw = ibase.b(raw)
        nbytes = len(raw)

        trans_ids = []
        i = 0
        while i < nbytes:
            byte = ibase.ord2(raw[i])
            if byte in (ibase.isc_spb_single_tra_id,
                        ibase.isc_spb_multi_tra_id):
                # The transaction ID is a 32-bit integer that begins
                # immediately after position i.
                trans_id = struct.unpack('i', raw[i + 1:i + 5])[0]
                i += 5  # Advance past the marker byte and the 32-bit integer.
                trans_ids.append(trans_id)
            else:
                raise fdb.InternalError('Unable to process buffer contents'
                                        ' beginning at position %d.' % i)
        return trans_ids
    def _resolve_limbo_transaction(self, resolution, database, transaction_id):
        _check_string(database)

        req_buf = _ServiceActionRequestBuilder()
        req_buf.add_numeric(resolution, transaction_id)
        self._repair_action(database, req_buf)
    def commit_limbo_transaction(self, database, transaction_id):
        """Resolve limbo transaction with commit.

        Args:
            database (str): Database filename or alias.
            transaction_id (int): ID of Transaction to resolve.
        """
        self.__check_active()
        self._resolve_limbo_transaction(ibase.isc_spb_rpr_commit_trans,
                                        database, transaction_id)
    def rollback_limbo_transaction(self, database, transaction_id):
        """Resolve limbo transaction with rollback.

        Args:
            database (str): Database filename or alias.
            transaction_id (int): ID of Transaction to resolve.
        """
        self.__check_active()
        self._resolve_limbo_transaction(ibase.isc_spb_rpr_rollback_trans,
                                        database, transaction_id)
    def get_statistics(self, database, show_only_db_log_pages=0,
                       show_only_db_header_pages=0, show_user_data_pages=1,
                       show_user_index_pages=1, show_system_tables_and_indexes=0,
                       show_record_versions=0, callback=None, tables=None):
        """Request database statisctics. **(ASYNC service)**

        Args:
            database (str): Database specification.

        Keyword Args:
            show_only_db_log_pages (int): `1` to analyze only log pages.
            show_only_db_header_pages (int): `1` to analyze only database
                header. When set, all other parameters are ignored.
            show_user_data_pages (int): `0` to skip user data analysis.
            show_user_index_pages (int): `0` to skip user index analysis.
            show_system_tables_and_indexes (int): `1` to analyze system
                tables and indices.
            show_record_versions (int): `1` to analyze record versions.
            callback (callable): Function to call back with each output line.
                Function must accept only one parameter: line of output.
            tables (str or list): table name or iterable of table names.

        If `callback` is not specified, statistical report could be retrieved
        through :meth:`readline`, :meth:`readlines`, iteration over `Connection`
        or ignored via call to :meth:`wait`.

        Note:
           Until report is not fully fetched from service (or ignored via
           :meth:`wait`), any attempt to start another asynchronous service will
           fail with exception.
        """
        self.__check_active()
        _check_string(database)

        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_db_stats)

        # Request for header negates all other options
        if show_only_db_header_pages:
            show_only_db_log_pages = show_user_data_pages = 0
            show_user_index_pages = show_system_tables_and_indexes = 0
            show_record_versions = 0
        option_mask = 0
        if show_user_data_pages:
            option_mask |= ibase.isc_spb_sts_data_pages
        if show_only_db_log_pages:
            option_mask |= ibase.isc_spb_sts_db_log
        if show_only_db_header_pages:
            option_mask |= ibase.isc_spb_sts_hdr_pages
        if show_user_index_pages:
            option_mask |= ibase.isc_spb_sts_idx_pages
        if show_system_tables_and_indexes:
            option_mask |= ibase.isc_spb_sts_sys_relations
        if show_record_versions:
            option_mask |= ibase.isc_spb_sts_record_versions

        req_buf.add_database_name(database)
        req_buf.add_option_mask(option_mask)
        if tables is not None:
            if isinstance(tables, ibase.StringTypes):
                tables = (tables,)
            cmdline = ['-t']
            cmdline.extend(tables)
            for item in cmdline:
                req_buf.add_string(ibase.isc_spb_command_line, item)
        self._act(req_buf)
        self.__fetching = True
        self.__eof = False
        if callback:
            for line in self:
                callback(line)
    # Backup and Restore methods:
    def backup(self,
               source_database,
               dest_filenames, dest_file_sizes=(),
               #factor=None, # YYY:What is this?

               # Backup operation optionMask:
               ignore_checksums=0,
               ignore_limbo_transactions=0,
               metadata_only=0,
               collect_garbage=1,
               transportable=1,
               convert_external_tables=0,
               compressed=1,
               no_db_triggers=0,
               callback=None,
               stats=None):
        """Request logical (GBAK) database backup. **(ASYNC service)**

        Args:
            source_database (str): Source database specification.
            dest_filenames (str or tuple(str)): Backup file(s) specification.

        Keyword Args:
            dest_file_sizes (tuple(int)): Specification of backup file max. sizes.
            ignore_checksums (int): `1` to ignore checksums.
            ignore_limbo_transactions (int): `1` to ignore limbo transactions.
            metadata_only (int): `1` to create only metadata backup.
            collect_garbage (int): `0` to skip garbage collection.
            transportable (int): `0` to do not create transportable backup.
            convert_external_tables (int): `1` to convert external table to internal ones.
            compressed (int): `0` to create uncompressed backup.
            no_db_triggers (int): `1` to disable database triggers temporarily.
            callback (callable): Function to call back with each output line.
                Function must accept only one parameter: line of output.
            stats (list): List of arguments for run-time statistics, see STATS_* constants.

        If `callback` is not specified, backup log could be retrieved through
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.

        Note:
           Until backup report is not fully fetched from service (or ignored via
           :meth:`wait`), any attempt to start another asynchronous service will
           fail with exception.
        """
        self.__check_active()
        # Begin parameter validation section.
        _check_string(source_database)
        dest_filenames = self._require_str_or_tuple_of_str(dest_filenames)

        dest_filenames_count = len(dest_filenames)
        # 2004.07.17: YYY: Temporary warning:
        # Current (1.5.1) versions of the database engine appear to hang the
        # Services API request when it contains more than 11 destFilenames
        if dest_filenames_count > 11:
            warnings.warn('Current versions of the database engine appear to hang when'
                          ' passed a request to generate a backup with more than 11'
                          ' constituents.', RuntimeWarning)

        if dest_filenames_count > 9999:
            raise fdb.ProgrammingError("The database engine cannot output a"
                                       " single source database to more than 9999 backup files.")
        self._validate_companion_string_numeric_sequences(dest_filenames, dest_file_sizes,
                                                          'destination filenames', 'destination file sizes')

        if len(self._exclude_elements_of_types(dest_file_sizes, (int, ibase.mylong))) > 0:
            raise TypeError("Every element of dest_file_sizes must be an int or long.")
        dest_file_sizes_count = len(dest_file_sizes)

        # The following should have already been checked by
        # _validateCompanionStringNumericSequences.
        assert dest_file_sizes_count == dest_filenames_count - 1
        # End parameter validation section.

        # Begin option bitmask setup section.
        option_mask = 0
        if ignore_checksums:
            option_mask |= ibase.isc_spb_bkp_ignore_checksums
        if ignore_limbo_transactions:
            option_mask |= ibase.isc_spb_bkp_ignore_limbo
        if metadata_only:
            option_mask |= ibase.isc_spb_bkp_metadata_only
        if not collect_garbage:
            option_mask |= ibase.isc_spb_bkp_no_garbage_collect
        if not transportable:
            option_mask |= ibase.isc_spb_bkp_non_transportable
        if convert_external_tables:
            option_mask |= ibase.isc_spb_bkp_convert
        if not compressed:
            option_mask |= ibase.isc_spb_bkp_expand
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.
        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_backup)
        # Source database filename:
        request.add_database_name(source_database)
        # Backup filenames and sizes:
        request.add_string_numeric_pairs_sequence(ibase.isc_spb_bkp_file, dest_filenames,
                                                  ibase.isc_spb_bkp_length, dest_file_sizes)
        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)
        # Tell the service to make its output available to us.
        request.add_code(ibase.isc_spb_verbose)
        # handle request for run-time statistics
        if stats:
            request.add_string(ibase.isc_spb_bkp_stat, ''.join(stats))
        # Done constructing the request buffer.
        self._act(request)
        self.__fetching = True
        self.__eof = False
        if callback:
            for line in self:
                callback(line)
    def local_backup(self,
                     source_database,
                     backup_stream,
                     # Backup operation optionMask:
                     ignore_checksums=0,
                     ignore_limbo_transactions=0,
                     metadata_only=0,
                     collect_garbage=1,
                     transportable=1,
                     convert_external_tables=0,
                     compressed=1,
                     no_db_triggers=0):
        """Request logical (GBAK) database backup into local byte stream. **(SYNC service)**

        Args:
            source_database (str): Source database specification.
            backup_stream (stream): Backup stream.

        Keyword Args:
            ignore_checksums (int): `1` to ignore checksums.
            ignore_limbo_transactions (int): `1` to ignore limbo transactions.
            metadata_only (int): `1` to create only metadata backup.
            collect_garbage (int): `0` to skip garbage collection.
            transportable (int): `0` to do not create transportable backup.
            convert_external_tables (int): `1` to convert external table to internal ones.
            compressed (int): `0` to create uncompressed backup.
            no_db_triggers (int): `1` to disable database triggers temporarily.
        """
        self.__check_active()
        # Begin parameter validation section.
        _check_string(source_database)

        # Begin option bitmask setup section.
        option_mask = 0
        if ignore_checksums:
            option_mask |= ibase.isc_spb_bkp_ignore_checksums
        if ignore_limbo_transactions:
            option_mask |= ibase.isc_spb_bkp_ignore_limbo
        if metadata_only:
            option_mask |= ibase.isc_spb_bkp_metadata_only
        if not collect_garbage:
            option_mask |= ibase.isc_spb_bkp_no_garbage_collect
        if not transportable:
            option_mask |= ibase.isc_spb_bkp_non_transportable
        if convert_external_tables:
            option_mask |= ibase.isc_spb_bkp_convert
        if not compressed:
            option_mask |= ibase.isc_spb_bkp_expand
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_backup)

        # Source database filename:
        request.add_database_name(source_database)

        # Backup file transported via stdout:
        request.add_string(ibase.isc_spb_bkp_file, 'stdout')

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)

        # Done constructing the request buffer.
        self._act(request)
        eof = False
        request = fdb.bs([ibase.isc_info_svc_to_eof])
        spb = ibase.b('')
        while not eof:
            api.isc_service_query(self._isc_status, self._svc_handle, None,
                                  len(spb), spb,
                                  len(request), request,
                                  ibase.USHRT_MAX, self._result_buffer)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError, self._isc_status,
                                                "Services/isc_service_query:")
            (result, _) = self._extract_bytes(self._result_buffer, 1)
            if ord(self._result_buffer[_]) == ibase.isc_info_end:
                eof = True
            backup_stream.write(result)
    def restore(self,
                source_filenames,
                dest_filenames, dest_file_pages=(),
                page_size=None,
                cache_buffers=None,
                access_mode_read_only=0,
                replace=0,
                deactivate_indexes=0,
                do_not_restore_shadows=0,
                do_not_enforce_constraints=0,
                commit_after_each_table=0,
                use_all_page_space=0,
                no_db_triggers=0,
                metadata_only=0,
                callback=None,
                stats=None):
        """Request database restore from logical (GBAK) backup. **(ASYNC service)**

        Args:
            source_filenames (str or tuple(str)): Backup file(s) specification.
            dest_filenames (str or tuple(str)): Database file(s) specification.

        Keyword Args:
            dest_file_pages (tuple(int)): Specification of database file max. # of pages.
            page_size (int): Page size.
            cache_buffers (int): Size of page-cache for this database.
            access_mode_read_only (int): `1` to create R/O database.
            replace (int): `1` to replace existing database.
            deactivate_indexes (int): `1` to do not activate indices.
            do_not_restore_shadows (int): `1` to do not restore shadows.
            do_not_enforce_constraints (int): `1` to do not enforce constraints during restore.
            commit_after_each_table (int): `1` to commit after each table is restored.
            use_all_page_space (int): `1` to use all space on data pages.
            no_db_triggers (int): `1` to disable database triggers temporarily.
            metadata_only (int): `1` to restore only database metadata.
            callback (callable): Function to call back with each output line.
                Function must accept only one parameter: line of output.
            stats (list): List of arguments for run-time statistics, see STATS_* constants.

        If `callback` is not specified, restore log could be retrieved through
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.

        Note:
           Until restore report is not fully fetched from service (or ignored via
           :meth:`wait`), any attempt to start another asynchronous service will
           fail with exception.
        """
        self.__check_active()
        # Begin parameter validation section.
        source_filenames = self._require_str_or_tuple_of_str(source_filenames)
        dest_filenames = self._require_str_or_tuple_of_str(dest_filenames)

        self._validate_companion_string_numeric_sequences(
            dest_filenames, dest_file_pages,
            'destination filenames', 'destination file page counts'
            )
        # End parameter validation section.

        # Begin option bitmask setup section.
        option_mask = 0
        if replace:
            option_mask |= ibase.isc_spb_res_replace
        else:
            option_mask |= ibase.isc_spb_res_create
        if deactivate_indexes:
            option_mask |= ibase.isc_spb_res_deactivate_idx
        if do_not_restore_shadows:
            option_mask |= ibase.isc_spb_res_no_shadow
        if do_not_enforce_constraints:
            option_mask |= ibase.isc_spb_res_no_validity
        if commit_after_each_table:
            option_mask |= ibase.isc_spb_res_one_at_a_time
        if use_all_page_space:
            option_mask |= ibase.isc_spb_res_use_all_space
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        if metadata_only:
            option_mask |= ibase.isc_spb_bkp_metadata_only
        # End option bitmask setup section.
        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_restore)
        # Backup filenames:
        request.add_string_sequence(ibase.isc_spb_bkp_file, source_filenames)
        # Database filenames:
        request.add_string_numeric_pairs_sequence(ibase.isc_spb_dbname, dest_filenames,
                                                  ibase.isc_spb_res_length, dest_file_pages)
        # Page size of the restored database:
        if page_size:
            request.add_numeric(ibase.isc_spb_res_page_size, page_size)
        # cacheBuffers is the number of default cache buffers to configure for
        # attachments to the restored database:
        if cache_buffers:
            request.add_numeric(ibase.isc_spb_res_buffers, cache_buffers)
        # accessModeReadOnly controls whether the restored database is
        # "mounted" in read only or read-write mode:
        if access_mode_read_only:
            access_mode = ibase.isc_spb_prp_am_readonly
        else:
            access_mode = ibase.isc_spb_prp_am_readwrite
        request.add_numeric(ibase.isc_spb_res_access_mode, access_mode, numctype='B')
        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)
        # Tell the service to make its output available to us.
        request.add_code(ibase.isc_spb_verbose)
        # handle request for run-time statistics
        if stats:
            request.add_string(ibase.isc_spb_res_stat, ''.join(stats))
        # Done constructing the request buffer.
        self._act(request)
        self.__fetching = True
        self.__eof = False
        if callback:
            for line in self:
                callback(line)
    def local_restore(self,
                      backup_stream,
                      dest_filenames, dest_file_pages=(),
                      page_size=None,
                      cache_buffers=None,
                      access_mode_read_only=0,
                      replace=0,
                      deactivate_indexes=0,
                      do_not_restore_shadows=0,
                      do_not_enforce_constraints=0,
                      commit_after_each_table=0,
                      use_all_page_space=0,
                      no_db_triggers=0,
                      metadata_only=0):
        """Request database restore from logical (GBAK) backup stored in local byte stream. **(SYNC service)**

        Args:
            backup_stream (stream): Backup stream.
            dest_filenames (str or tuple(str)): Database file(s) specification.

        Keyword Args:
            dest_file_pages (tuple(int)): Specification of database file max. # of pages.
            page_size (int): Page size.
            cache_buffers (int): Size of page-cache for this database.
            access_mode_read_only (int): `1` to create R/O database.
            replace (int): `1` to replace existing database.
            deactivate_indexes (int): `1` to do not activate indices.
            do_not_restore_shadows (int): `1` to do not restore shadows.
            do_not_enforce_constraints (int): `1` to do not enforce constraints during restore.
            commit_after_each_table (int): `1` to commit after each table is restored.
            use_all_page_space (int): `1` to use all space on data pages.
            no_db_triggers (int): `1` to disable database triggers temporarily.
            metadata_only (int): `1` to restore only database metadata.
        """
        self.__check_active()
        # Begin parameter validation section.
        dest_filenames = self._require_str_or_tuple_of_str(dest_filenames)

        self._validate_companion_string_numeric_sequences(
            dest_filenames, dest_file_pages,
            'destination filenames', 'destination file page counts'
            )
        # End parameter validation section.

        # Begin option bitmask setup section.
        option_mask = 0
        if replace:
            option_mask |= ibase.isc_spb_res_replace
        else:
            option_mask |= ibase.isc_spb_res_create
        if deactivate_indexes:
            option_mask |= ibase.isc_spb_res_deactivate_idx
        if do_not_restore_shadows:
            option_mask |= ibase.isc_spb_res_no_shadow
        if do_not_enforce_constraints:
            option_mask |= ibase.isc_spb_res_no_validity
        if commit_after_each_table:
            option_mask |= ibase.isc_spb_res_one_at_a_time
        if use_all_page_space:
            option_mask |= ibase.isc_spb_res_use_all_space
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        if metadata_only:
            option_mask |= ibase.isc_spb_bkp_metadata_only
        # End option bitmask setup section.
        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_restore)
        # Backup stream:
        request.add_string(ibase.isc_spb_bkp_file, 'stdin')
        # Database filenames:
        request.add_string_numeric_pairs_sequence(ibase.isc_spb_dbname, dest_filenames,
                                                  ibase.isc_spb_res_length, dest_file_pages)
        # Page size of the restored database:
        if page_size:
            request.add_numeric(ibase.isc_spb_res_page_size, page_size)
        # cacheBuffers is the number of default cache buffers to configure for
        # attachments to the restored database:
        if cache_buffers:
            request.add_numeric(ibase.isc_spb_res_buffers, cache_buffers)
        # accessModeReadOnly controls whether the restored database is
        # "mounted" in read only or read-write mode:
        if access_mode_read_only:
            access_mode = ibase.isc_spb_prp_am_readonly
        else:
            access_mode = ibase.isc_spb_prp_am_readwrite
        request.add_numeric(ibase.isc_spb_res_access_mode, access_mode, numctype='B')
        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)
        # Done constructing the request buffer.
        self._act(request)

        request_length = 0
        stop = False
        pos = backup_stream.tell()
        backup_stream.seek(0, 2)
        bytes_available = backup_stream.tell() - pos
        backup_stream.seek(pos)
        spb = ctypes.create_string_buffer(16)
        spb[0] = ibase.int2byte(ibase.isc_info_svc_timeout)
        spb[1:3] = fdb.uint_to_bytes(4, 2)
        spb[3:7] = fdb.uint_to_bytes(1, 4)
        spb[7] = ibase.int2byte(ibase.isc_info_end)
        wait = True
        while not stop:
            if request_length > 0:
                request_length = min([request_length, 65500])
                raw = backup_stream.read(request_length)
                if len(spb) < request_length+4:
                    spb = ctypes.create_string_buffer(request_length+4)
                spb[0] = ibase.int2byte(ibase.isc_info_svc_line)
                spb[1:3] = fdb.uint_to_bytes(len(raw), 2)
                spb[3:3+len(raw)] = raw
                spb[3+len(raw)] = ibase.int2byte(ibase.isc_info_end)
                bytes_available -= len(raw)
            req = fdb.bs([ibase.isc_info_svc_stdin, ibase.isc_info_svc_line])
            api.isc_service_query(self._isc_status, self._svc_handle, None,
                                  len(spb), spb,
                                  len(req), req,
                                  ibase.USHRT_MAX, self._result_buffer)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError, self._isc_status,
                                                "Services/isc_service_query:")
            i = 0
            request_length = 0
            while self._result_buffer[i] != ibase.int2byte(ibase.isc_info_end):
                code = ibase.ord2(self._result_buffer[i])
                i += 1
                if code == ibase.isc_info_svc_stdin:
                    (request_length, i) = self._extract_longint(self._result_buffer, i)
                elif code == ibase.isc_info_svc_line:
                    (line, i) = self._extract_string(self._result_buffer, i)
                else:
                    pass
            if not wait:
                stop = (request_length == 0) and (len(line) == 0)
            elif request_length != 0:
                wait = False
    # nbackup methods:
    def nbackup(self, source_database,
                dest_filename,
                nbackup_level=0,
                no_db_triggers=0):
        """Perform physical (NBACKUP) database backup.

        Args:
            source_database (str): Source database specification.
            dest_filename (str): Backup file specification.

        Keyword Args:
            nbackup_level (int): Incremental backup level.
            no_db_triggers (int): `1` to disable database triggers temporarily.

        Note:
            Method call will not return until action is finished.
        """
        self.__check_active()
        # Begin parameter validation section.
        _check_string(source_database)
        _check_string(dest_filename)
        dest_filename = ibase.b(dest_filename)

        # Begin option bitmask setup section.
        option_mask = 0
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_nbak)

        # Source database filename:
        request.add_database_name(source_database)

        # Backup filename:
        request.add_string(ibase.isc_spb_nbk_file, dest_filename)

        # backup level
        request.add_numeric(ibase.isc_spb_nbk_level, nbackup_level)

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)

        # Done constructing the request buffer.
        self._act(request)
        self.wait()
    def nrestore(self, source_filenames,
                 dest_filename,
                 no_db_triggers=0):
        """Perform restore from physical (NBACKUP) database backup.

        Args:
            source_filenames (str or tuple(str)): Backup file(s) specification.
            dest_filename (str): Database file specification.

        Keyword Args:
            no_db_triggers (int): `1` to disable database triggers temporarily.

        Note:
            Method call will not return until action is finished.
        """
        self.__check_active()
        # Begin parameter validation section.
        source_filenames = self._require_str_or_tuple_of_str(source_filenames)
        _check_string(dest_filename)
        dest_filename = ibase.b(dest_filename)

        # Begin option bitmask setup section.
        option_mask = 0
        if no_db_triggers:
            option_mask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_nrest)

        # Source database filename:
        request.add_string(ibase.isc_spb_dbname, dest_filename)

        # Backup filenames:
        request.add_string_sequence(ibase.isc_spb_nbk_file, source_filenames)

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, option_mask)

        # Done constructing the request buffer.
        self._act(request)
        self.wait()
    # Trace
    def trace_start(self, config, name=None):
        """Start new trace session. **(ASYNC service)**

        Args:
            config (str): Trace session configuration.

        Keyword Args:
            name (str): Trace session name.

        Returns:
            integer: Trace session ID.

        Raises:
            fdb.DatabaseError: When session ID is not returned on start.

        Trace session output could be retrieved through :meth:`readline`,
        :meth:`readlines`, iteration over `Connection` or ignored via call to
        :meth:`wait`.

        Note:
           Until session output is not fully fetched from service (or ignored
           via :meth:`wait`), any attempt to start another asynchronous service
           including call to any `trace_` method will fail with exception.
        """
        self.__check_active()
        if not name is None:
            _check_string(name)
        _check_string(config)
        # Construct the request buffer.
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_start)
        # trace name:
        if not name is None:
            req_buf.add_string(ibase.isc_spb_trc_name, name)
        # trace configuration:
        req_buf.add_string(ibase.isc_spb_trc_cfg, config)
        self._act(req_buf)
        self.__fetching = True
        self.__eof = False
        response = self._Q(ibase.isc_info_svc_line, self.QUERY_TYPE_PLAIN_STRING)
        if response.startswith('Trace session ID'):
            return int(response.split()[3])
        else:
            # response should contain the error message
            raise fdb.DatabaseError(response)
    def trace_stop(self, trace_id):
        """Stop trace session.

        Args:
            trace_id (int): Trace session ID.

        Returns:
            str: Text with confirmation that session was stopped.

        Raises:
            fdb.DatabaseError: When trace session is not stopped.
            fdb.OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_stop)
        req_buf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(req_buf)
        if not response.startswith("Trace session ID %i stopped" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_suspend(self, trace_id):
        """Suspend trace session.

        Args:
            trace_id (int): Trace session ID.

        Returns:
            str: Text with confirmation that session was paused.

        Raises:
            fdb.DatabaseError: When trace session is not paused.
            fdb.OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_suspend)
        req_buf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(req_buf)
        if not response.startswith("Trace session ID %i paused" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_resume(self, trace_id):
        """Resume trace session.

        Args:
            trace_id (int): Trace session ID.

        Returns:
            str: Text with confirmation that session was resumed.

        Raises:
            fdb.DatabaseError: When trace session is not resumed.
            fdb.OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_resume)
        req_buf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(req_buf)
        if not response.startswith("Trace session ID %i resumed" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_list(self):
        """Get information about existing trace sessions.

        Returns:
            dict: Mapping `SESSION_ID -> SESSION_PARAMS`

        Session parameters is another dictionary with next keys:

          :name:  (str) (optional) Session name if specified.
          :date:  (datetime.datetime) Session start date and time.
          :user:  (str) Trace user name.
          :flags: (list(str)) Session flags.

        Raises:
            fdb.OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_list)
        # Get and parse the returned list.
        session_list = self._act_and_return_textual_results(req_buf)
        result = {}
        session_id = None
        for line in session_list.split('\n'):
            if not line.strip():
                session_id = None
            elif line.startswith("Session ID:"):
                session_id = int(line.split(':')[1].strip())
                result[session_id] = dict()
            elif line.lstrip().startswith("name:"):
                result[session_id]["name"] = line.split(':')[1].strip()
            elif line.lstrip().startswith("user:"):
                result[session_id]["user"] = line.split(':')[1].strip()
            elif line.lstrip().startswith("date:"):
                result[session_id]["date"] = datetime.datetime.strptime(
                    line.split(':', 1)[1].strip(),
                    '%Y-%m-%d %H:%M:%S')
            elif line.lstrip().startswith("flags:"):
                result[session_id]["flags"] = line.split(':')[1].strip().split(',')
            else:
                raise fdb.OperationalError("Unexpected line in trace session list:`%s`" % line)
        return result
    # Database property alteration methods:
    def set_default_page_buffers(self, database, n):
        """Set individual page cache size for Database.

        Args:
            database (str): Database filename or alias.
            n (int): Number of pages.
        """
        self.__check_active()
        _check_string(database)
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_page_buffers,
                                                n)
    def set_sweep_interval(self, database, n):
        """Set treshold for automatic sweep.

        Args:
            database (str): Database filename or alias.
            n (int): Sweep treshold, or `0` to disable automatic sweep.
        """
        self.__check_active()
        _check_string(database)
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_sweep_interval,
                                                n)
    def set_reserve_page_space(self, database, reserve_space):
        """Set data page space reservation policy.

        Args:
            database (str): Database filename or alias.
            reserve_space (bool): `True` to reserve space, `False` to do not.
        """
        self.__check_active()
        _check_string(database)
        if reserve_space:
            reserve_code = ibase.isc_spb_prp_res
        else:
            reserve_code = ibase.isc_spb_prp_res_use_full
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_reserve_space,
                                                reserve_code, num_ctype='b')
    def set_write_mode(self, database, mode):
        """Set Disk Write Mode: Sync (forced writes) or Async (buffered).

        Args:
            database (str): Database filename or alias.
            mode (int): One from following constants:
                        :data:`~fdb.services.WRITE_FORCED` or
                        :data:`~fdb.services.WRITE_BUFFERED`
        """
        self.__check_active()
        _check_string(database)
        if mode not in (WRITE_FORCED, WRITE_BUFFERED):
            raise ValueError('mode must be one of the following constants:'
                             '  fdb.services.WRITE_FORCED, fdb.services.WRITE_BUFFERED.')
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_write_mode,
                                                mode, num_ctype='b')
    def set_access_mode(self, database, mode):
        """Set Database Access mode: Read Only or Read/Write

        Args:
            database (str): Database filename or alias.
            mode (int): One from following constants:
                        :data:`~fdb.services.ACCESS_READ_WRITE` or
                        :data:`~fdb.services.ACCESS_READ_ONLY`
        """
        self.__check_active()
        _check_string(database)
        if mode not in (ACCESS_READ_WRITE, ACCESS_READ_ONLY):
            raise ValueError('mode must be one of the following constants:'
                             ' fdb.services.ACCESS_READ_WRITE, fdb.services.ACCESS_READ_ONLY.')
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_access_mode,
                                                mode, num_ctype='b')
    def set_sql_dialect(self, database, dialect):
        """Set SQL Dialect for Database.

        Args:
            database (str): Database filename or alias.
            dialect (int): `1` or `3`.
        """
        self.__check_active()
        _check_string(database)
        # The IB 6 API Guide says that dialect "must be 1 or 3", but other
        # dialects may become valid in future versions, so don't require
        #   dialect in (1, 3)
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_set_sql_dialect,
                                                dialect)
    def activate_shadow(self, database):
        """Activate Database Shadow(s).

        Args:
            database (str): Database filename or alias.
        """
        self.__check_active()
        _check_string(database)
        req_buf = _ServiceActionRequestBuilder()
        req_buf.add_option_mask(ibase.isc_spb_prp_activate)
        self._property_action(database, req_buf)
    def no_linger(self, database):
        """Set one-off override for database linger.

        Args:
            database (str): Database filename or alias.
        """
        self.__check_active()
        _check_string(database)
        req_buf = _ServiceActionRequestBuilder()
        req_buf.add_option_mask(ibase.isc_spb_prp_nolinger)
        self._property_action(database, req_buf)
    # Database repair/maintenance methods:
    def shutdown(self, database, shutdown_mode, shutdown_method, timeout):
        """Database shutdown.

        Args:
            database (str): Database filename or alias.
            shutdown_mode (int): One from following constants:
                                 :data:`~fdb.services.SHUT_LEGACY`, :data:`~fdb.services.SHUT_SINGLE`,
                                 :data:`~fdb.services.SHUT_MULTI` or :data:`~fdb.services.SHUT_FULL`.
            shutdown_method (int): One from following constants:
                                   :data:`~fdb.services.SHUT_FORCE`,
                                   :data:`~fdb.services.SHUT_DENY_NEW_TRANSACTIONS`
                                   or :data:`~fdb.services.SHUT_DENY_NEW_ATTACHMENTS`.
            timeout (int): Time in seconds, that the shutdown must complete in.

        See also:
            Method :meth:`bring_online`
        """
        self.__check_active()
        _check_string(database)
        if shutdown_mode not in (SHUT_LEGACY, SHUT_SINGLE, SHUT_MULTI, SHUT_FULL):
            raise ValueError('shutdown_mode must be one of the following'
                             ' constants:  fdb.services.SHUT_LEGACY, fdb.services.SHUT_SINGLE,'
                             ' fdbfdb.services.SHUT_MULTI,'
                             ' fdb.services.SHUT_FULL.')
        if shutdown_method not in (SHUT_FORCE, SHUT_DENY_NEW_TRANSACTIONS,
                                   SHUT_DENY_NEW_ATTACHMENTS):
            raise ValueError('shutdown_method must be one of the following'
                             ' constants:  fdb.services.SHUT_FORCE,'
                             ' fdb.services.SHUT_DENY_NEW_TRANSACTIONS,'
                             ' fdb.services.SHUT_DENY_NEW_ATTACHMENTS.')
        req_buf = _ServiceActionRequestBuilder()
        if shutdown_mode != SHUT_LEGACY:
            req_buf.add_numeric(ibase.isc_spb_prp_shutdown_mode,
                                shutdown_mode, numctype='B')
        req_buf.add_numeric(shutdown_method, timeout, numctype='I')
        self._property_action(database, req_buf)
    def bring_online(self, database, online_mode=SHUT_NORMAL):
        """Bring previously shut down database back online.

        Args:
            database (str): Database filename or alias.

        Keyword Args:
            online_mode (int): One from following constants:
                :data:`~fdb.services.SHUT_LEGACY`, :data:`~fdb.services.SHUT_SINGLE`,
                :data:`~fdb.services.SHUT_MULTI` or (**Default**) :data:`~fdb.services.SHUT_NORMAL`.

        See also:
            Method :meth:`shutdown`
        """
        self.__check_active()
        _check_string(database)
        if online_mode not in (SHUT_LEGACY, SHUT_NORMAL, SHUT_SINGLE, SHUT_MULTI):
            raise ValueError('online_mode must be one of the following'
                             ' constants:  fdb.services.SHUT_LEGACY, fdb.services.SHUT_NORMAL,'
                             ' fdbfdb.services.SHUT_SINGLE,'
                             ' fdb.services.SHUT_MULTI.')
        req_buf = _ServiceActionRequestBuilder()
        if online_mode == SHUT_LEGACY:
            req_buf.add_option_mask(ibase.isc_spb_prp_db_online)
        else:
            req_buf.add_numeric(ibase.isc_spb_prp_online_mode,
                                online_mode, numctype='B')
        self._property_action(database, req_buf)
    def sweep(self, database):
        """Perform Database Sweep.

        Note:
            Method call will not return until sweep is finished.

        Args:
            database (str): Database filename or alias.
        """
        self.__check_active()
        _check_string(database)
        req_buf = _ServiceActionRequestBuilder()
        option_mask = ibase.isc_spb_rpr_sweep_db
        req_buf.add_option_mask(option_mask)
        return self._repair_action(database, req_buf)
    def repair(self, database,
               read_only_validation=0,
               ignore_checksums=0,
               kill_unavailable_shadows=0,
               mend_database=0,
               validate_database=1,
               validate_record_fragments=1):
        """Database Validation and Repair.

        Args:
            database (str): Database filename or alias.

        Keyword Args:
            read_only_validation (int): `1` to prevent any database changes.
            ignore_checksums (int): `1` to ignore page checksum errors.
            kill_unavailable_shadows (int): `1` to kill unavailable shadows.
            mend_database (int): `1` to fix database for backup.
            validate_database (int): `0` to skip database validation.
            validate_record_fragments (int): `0` to skip validation of record fragments.

        Note:
            Method call will not return until action is finished.
        """
        self.__check_active()
        _check_string(database)
        # YYY: With certain option combinations, this method raises errors
        # that may not be very comprehensible to a Python programmer who's not
        # well versed with IB/FB.  Should option combination filtering be
        # done right here instead of leaving that responsibility to the
        # database engine?
        #   I think not, since any filtering done in this method is liable to
        # become outdated, or to inadvertently enforce an unnecessary,
        # crippling constraint on a certain option combination that the
        # database engine would have allowed.
        req_buf = _ServiceActionRequestBuilder()
        option_mask = 0

        if read_only_validation:
            option_mask |= ibase.isc_spb_rpr_check_db
        if ignore_checksums:
            option_mask |= ibase.isc_spb_rpr_ignore_checksum
        if kill_unavailable_shadows:
            option_mask |= ibase.isc_spb_rpr_kill_shadows
        if mend_database:
            option_mask |= ibase.isc_spb_rpr_mend_db
        if validate_database:
            option_mask |= ibase.isc_spb_rpr_validate_db
        if validate_record_fragments:
            option_mask |= ibase.isc_spb_rpr_full
        req_buf.add_option_mask(option_mask)
        return self._repair_action(database, req_buf)

    # 2003.07.12:  Removed method resolveLimboTransactions (dropped plans to
    # support that operation from kinterbasdb since transactions IDs are not
    # exposed at the Python level and I don't consider limbo transaction
    # resolution compelling enough to warrant exposing transaction IDs).

    def validate(self, database,
                 include_tables=None, exclude_tables=None,
                 include_indices=None, exclude_indices=None,
                 lock_timeout=None, callback=None):
        """On-line database validation.

        Args:
            database (str): Database filename or alias.

        Keyword Args:
            include_tables (str): Pattern for table names to include in validation run.
            exclude_tables (str): Pattern for table names to exclude from validation run.
            include_indices (str): Pattern for index names to include in validation run.
            exclude_indices (str): Pattern for index names to exclude from validation run.
            lock_timeout (int): lock timeout, used to acquire locks for table to validate,
                in seconds, default is 10 secs. 0 is no-wait, -1 is infinite wait.
            callback (callable): Function to call back with each output line.
                Function must accept only one parameter: line of output.

        Note:
            Patterns are regular expressions, processed by the same rules as SIMILAR TO
            expressions. All patterns are case-sensitive, regardless of database dialect. If the
            pattern for tables is omitted then all user tables will be validated. If the pattern
            for indexes is omitted then all indexes of the appointed tables will be validated.
            System tables are not validated.

        If `callback` is not specified, validation log could be retrieved through
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.

        Note:
           Until validate report is not fully fetched from service (or ignored via
           :meth:`wait`), any attempt to start another asynchronous service will
           fail with exception.
        """
        self.__check_active()
        _check_string(database)
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_validate)
        request.add_database_name(database)
        if include_tables is not None:
            request.add_string(ibase.isc_spb_val_tab_incl, include_tables)
        if exclude_tables is not None:
            request.add_string(ibase.isc_spb_val_tab_excl, exclude_tables)
        if include_indices is not None:
            request.add_string(ibase.isc_spb_val_idx_incl, include_indices)
        if exclude_indices is not None:
            request.add_string(ibase.isc_spb_val_idx_excl, exclude_indices)
        if lock_timeout is not None:
            request.add_numeric(ibase.isc_spb_val_lock_timeout, lock_timeout, numctype='i')

        # Done constructing the request buffer.
        self._act(request)
        self.__fetching = True
        self.__eof = False
        if callback:
            for line in self:
                callback(line)

    # User management methods:
    def get_users(self, user_name=None):
        """Get information about user(s).

        Keyword Args:
            user_name (str): When specified, returns information
                only about user with specified user name.

        Returns:
            list: List of :class:`User` instances.
        """
        self.__check_active()
        if user_name is not None:
            if isinstance(user_name, ibase.myunicode):
                _check_string(user_name)
                user_name = ibase.b(user_name)
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_display_user)
        if user_name:
            user_name = user_name.upper()
            req_buf.add_string(ibase.isc_spb_sec_username, user_name)
        self._act(req_buf)
        raw = self._QR(ibase.isc_info_svc_get_users)
        users = []
        cur_user = None
        pos = 1  # Ignore raw[0].
        upper_limit = len(raw) - 1
        while pos < upper_limit:
            cluster = ibase.ord2(raw[pos])
            pos += 1
            if cluster == ibase.isc_spb_sec_username:
                if cur_user is not None:
                    users.append(cur_user)
                    cur_user = None
                (user_name, pos) = self._extract_string(raw, pos)
                cur_user = User(user_name)
            elif cluster == ibase.isc_spb_sec_password:
                (password, pos) = self._extract_string(raw, pos)
                cur_user.password = password
            elif cluster == ibase.isc_spb_sec_firstname:
                (first_name, pos) = self._extract_string(raw, pos)
                cur_user.first_name = first_name
            elif cluster == ibase.isc_spb_sec_middlename:
                (middle_name, pos) = self._extract_string(raw, pos)
                cur_user.middle_name = middle_name
            elif cluster == ibase.isc_spb_sec_lastname:
                (last_name, pos) = self._extract_string(raw, pos)
                cur_user.last_name = last_name
            elif cluster == ibase.isc_spb_sec_groupid:
                (group_id, pos) = self._extract_int(raw, pos)
                cur_user.group_id = group_id
            elif cluster == ibase.isc_spb_sec_userid:
                (user_id, pos) = self._extract_int(raw, pos)
                cur_user.user_id = user_id
        # Handle the last user:
        if cur_user is not None:
            users.append(cur_user)
            cur_user = None
        return users
    def add_user(self, user):
        """Add new user.

        Args:
            user (:class:`User`): Instance of :class:`User` with **at least** its
                :attr:`~User.name` and :attr:`~User.password` attributes specified
                as non-empty values. All other attributes are optional.

        Note:
           This method ignores the :attr:`~User.user_id` and :attr:`~User.group_id`
           attributes of :class:`~User` regardless of their values.
        """
        self.__check_active()
        if not user.name:
            raise fdb.ProgrammingError('You must specify a username.')
        else:
            _check_string(user.name)
            user.name = ibase.b(user.name)

        if not user.password:
            raise fdb.ProgrammingError('You must specify a password.')
        else:
            _check_string(user.password)
            user.password = ibase.b(user.password)

        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_add_user)

        req_buf.add_string(ibase.isc_spb_sec_username, user.name)
        req_buf.add_string(ibase.isc_spb_sec_password, user.password)

        if user.first_name:
            user.first_name = ibase.b(user.first_name)
            req_buf.add_string(ibase.isc_spb_sec_firstname, user.first_name)
        if user.middle_name:
            user.middle_name = ibase.b(user.middle_name)
            req_buf.add_string(ibase.isc_spb_sec_middlename, user.middle_name)
        if user.last_name:
            user.last_name = ibase.b(user.last_name)
            req_buf.add_string(ibase.isc_spb_sec_lastname, user.last_name)

        self._act_and_return_textual_results(req_buf)
    def modify_user(self, user):
        """Modify user information.

        Args:
            user (:class:`User`): Instance of :class:`User` with **at least** its
                :attr:`~User.name` attribute specified as non-empty value.

        Note:
           This method sets :attr:`~User.first_name`, :attr:`~User.middle_name`
           and :attr:`~User.last_name` to their actual values, and ignores
           the :attr:`~User.user_id` and :attr:`~User.group_id` attributes
           regardless of their values. :attr:`~User.password` is set **only**
           when it has value.
        """
        self.__check_active()
        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_modify_user)

        if isinstance(user.name, str):
            user.name = ibase.b(user.name)
        req_buf.add_string(ibase.isc_spb_sec_username, user.name)
        if isinstance(user.password, str):
            user.password = ibase.b(user.password)
        req_buf.add_string(ibase.isc_spb_sec_password, user.password)
        # Change the optional attributes whether they're empty or not.
        if isinstance(user.first_name, str):
            user.first_name = ibase.b(user.first_name)
        req_buf.add_string(ibase.isc_spb_sec_firstname, user.first_name)
        if isinstance(user.middle_name, str):
            user.middle_name = ibase.b(user.middle_name)
        req_buf.add_string(ibase.isc_spb_sec_middlename, user.middle_name)
        if isinstance(user.last_name, str):
            user.last_name = ibase.b(user.last_name)
        req_buf.add_string(ibase.isc_spb_sec_lastname, user.last_name)

        self._act_and_return_textual_results(req_buf)
    def remove_user(self, user):
        """Remove user.

        Args:
            user (:class:`User`): User name or Instance of :class:`User` with **at least** its
                :attr:`~User.name` attribute specified as non-empty value.
        """
        self.__check_active()
        if isinstance(user, User):
            username = user.name
        else:
            _check_string(user)
            user = ibase.b(user)
            username = user

        req_buf = _ServiceActionRequestBuilder(ibase.isc_action_svc_delete_user)
        req_buf.add_string(ibase.isc_spb_sec_username, username)
        self._act_and_return_textual_results(req_buf)
    def user_exists(self, user):
        """Check for user's existence.

        Args:
            user (:class:`User`): User name or Instance of :class:`User` with **at least** its
                :attr:`~User.name` attribute specified as non-empty value.

        Returns:
            bool: `True` when the specified user exists.
        """
        self.__check_active()
        if isinstance(user, User):
            username = user.name
        else:
            username = user
        return len(self.get_users(user_name=username)) > 0

    #: (Read Only) `True` if connection is closed.
    closed = property(__get_closed)
    #: (Read Only) `True` if connection is fetching result.
    fetching = property(__get_fetching)
    #: (Read Only) (string) Firebird version number string of connected server.
    #: Uses Firebird version numbers in form: major.minor.subrelease.build
    version = property(__get_version)
    #: (Read Only) (float) Firebird version number of connected server. Only major.minor version.
    engine_version = property(__get_engine_version)

class User(object):
    def __init__(self, name=None):
        if name:
            _check_string(name)
            self.name = name.upper()
        else:
            #: str: User `login` name (username).
            self.name = None
        #: str: Password. Not returned by user output methods, but must be
        #: specified to add a user.
        self.password = None

        #: str: First name.
        self.first_name = None
        #: str: Middle name
        self.middle_name = None
        #: str: Last name
        self.last_name = None

        # The user id and group id are not fully supported.  For details, see
        # the documentation of the "User Management Methods" of
        # services.Connection.
        #: int: User ID
        self.user_id = None
        #: int: User group ID
        self.group_id = None

    def __str__(self):
        return '<fdb.services.User %s>' % (
            (self.name is None and 'without a name')
            or 'named "%s"' % self.name)
    def load_information(self, svc):
        """Load information about user from server.

        Args:
           svc (:class:`Connection`): Open service connection.

        Returns:
            True if information was successfuly retrieved, False otherwise.

        Raises:
            fdb.ProgrammingError: If user name is not defined.
        """
        if self.name is None:
            raise fdb.ProgrammingError("Can't load information about user without name.")
        user = svc.get_users(self.name)
        if len(user) > 0:
            self.first_name = user.first_name
            self.middle_name = user.middle_name
            self.last_name = user.last_name
            self.user_id = user.user_id
            self.group_id = user.group_id
        return len(user) > 0

class _ServiceActionRequestBuilder(object):
    # This private class helps public facilities in this module to build
    # the binary action request buffers required by the database Services API
    # using high-level, easily comprehensible syntax.

    def __init__(self, clusterIdentifier=None):
        self.ci = clusterIdentifier
        self.clear()

    def __str__(self):
        return self.render()

    def clear(self):
        self._buffer = []
        if self.ci:
            self.add_code(self.ci)

    def extend(self, other_request_builder):
        self._buffer.append(other_request_builder.render())

    def add_code(self, code):
        _code2spb(self._buffer, code)

    def add_string(self, code, s):
        _check_string(s)
        _string2spb(self._buffer, code, ibase.b(s))

    def add_string_sequence(self, code, string_sequence):
        for s in string_sequence:
            self.add_string(code, s)

    def add_string_numeric_pairs_sequence(self, string_code, string_sequence,
                                          numeric_code, numeric_sequence):
        string_count = len(string_sequence)
        numeric_count = len(numeric_sequence)
        if numeric_count != string_count - 1:
            raise ValueError("Numeric sequence must contain exactly one less"
                             " element than its companion string sequence.")
        i = 0
        while i < string_count:
            self.add_string(string_code, string_sequence[i])
            if i < numeric_count:
                self.add_numeric(numeric_code, numeric_sequence[i])
            i += 1

    def add_numeric(self, code, n, numctype='I'):
        _numeric2spb(self._buffer, code, n, numctype=numctype)

    def add_option_mask(self, option_mask):
        self.add_numeric(ibase.isc_spb_options, option_mask)

    def add_database_name(self, database_name):
        # 2003.07.20: Issue a warning for a hostname-containing databaseName
        # because it will cause isc_service_start to raise an inscrutable error
        # message with Firebird 1.5 (though it would not have raised an error
        # at all with Firebird 1.0 and earlier).
        ### Todo: verify handling of P version differences, refactor
        database_name = ibase.b(database_name, fdb.fbcore._FS_ENCODING)
        if ibase.PYTHON_MAJOR_VER == 3:
            colon_index = (database_name.decode(fdb.fbcore._FS_ENCODING)).find(':')
        else:
            colon_index = database_name.find(':')
        if colon_index != -1:
            # This code makes no provision for platforms other than Windows
            # that allow colons in paths (such as MacOS).  Some of
            # kinterbasdb's current implementation (e.g., event handling) is
            # constrained to Windows or POSIX anyway.
            if not sys.platform.lower().startswith('win') or (
                # This client process is running on Windows.
                #
                # Files that don't exist might still be valid if the connection
                # is to a server other than the local machine.
                not os.path.exists(ibase.nativestr(database_name, fdb.fbcore._FS_ENCODING))
                # "Guess" that if the colon falls within the first two
                # characters of the string, the pre-colon portion refers to a
                # Windows drive letter rather than to a remote host.
                # This isn't guaranteed to be correct.
                and colon_index > 1):
                warnings.warn(' Unlike conventional DSNs, Services API database names'
                              ' must not include the host name; remove the "%s" from'
                              ' your database name.'
                              ' (Firebird 1.0 will accept this, but Firebird 1.5 will'
                              ' raise an error.)'
                              % database_name[:colon_index + 1], UserWarning)
        self.add_string(ibase.isc_spb_dbname, database_name)

    def render(self):
        return ibase.b('').join(self._buffer)
