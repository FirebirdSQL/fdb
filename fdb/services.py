#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           services.py
#   DESCRIPTION:    Python driver for Firebird
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

# The following SHUT_* constants are to be passed as the `shutdown_mode`
# parameter to Connection.shutdown:
SHUT_NORMAL = ibase.isc_spb_prp_sm_normal
SHUT_MULTI = ibase.isc_spb_prp_sm_multi
SHUT_SINGLE = ibase.isc_spb_prp_sm_single
SHUT_FULL = ibase.isc_spb_prp_sm_full

# The following SHUT_* constants are to be passed as the `shutdown_method`
# parameter to Connection.shutdown:
SHUT_FORCE = ibase.isc_spb_prp_shutdown_db
SHUT_DENY_NEW_TRANSACTIONS = ibase.isc_spb_prp_deny_new_transactions
SHUT_DENY_NEW_ATTACHMENTS = ibase.isc_spb_prp_deny_new_attachments

# The following WRITE_* constants are to be passed as the `mode` parameter
# to Connection.set_write_mode:
WRITE_FORCED = ibase.isc_spb_prp_wm_sync
WRITE_BUFFERED = ibase.isc_spb_prp_wm_async

# The following ACCESS_* constants are to be passed as the `mode` parameter
# to Connection.set_access_mode:
ACCESS_READ_WRITE = ibase.isc_spb_prp_am_readwrite
ACCESS_READ_ONLY = ibase.isc_spb_prp_am_readonly

# The following CAPABILITY_* constants are return values of `get_server_capabilities`
CAPABILITY_MULTI_CLIENT = 0x2
CAPABILITY_REMOTE_HOP = 0x4
CAPABILITY_SERVER_CONFIG = 0x200
CAPABILITY_QUOTED_FILENAME = 0x400
CAPABILITY_NO_SERVER_SHUTDOWN = 0x100


def _checkString(st):
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
                        ' of type %s, not %s.' % (type(ibase.mybytes),type(st))
                      )
        except UnicodeEncodeError:
            raise TypeError("The database engine's Services API only works"
                " properly with ASCII string parameters, so str instances that"
                " contain non-ASCII characters are disallowed."
                )
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
                        ' of type %s, not %s.' % (type(ibase.mybytes),type(st))
                      )
        except UnicodeError:
            raise TypeError("The database engine's Services API only works"
                " properly with ASCII string parameters, so str instances that"
                " contain non-ASCII characters, and all unicode instances, are"
                " disallowed."
              )


def _string2spb(spb, code, st):
    myslen = len(st)
    _numeric2spb(spb, code, myslen, numCType='H')
    myformat = str(myslen) + 's'  # The length, then 's'.
    spb.append(struct.pack(myformat, st))


def _numeric2spb(spb, code, num, numCType='I'):
    # numCType is one of the pack format characters specified by the Python
    # standard library module 'struct'.
    _code2spb(spb, code)
    (numericFormat, numericBytes) = _renderSizedIntegerForSPB(num, numCType)
    spb.append(struct.pack(numericFormat, numericBytes))


def _code2spb(spb, code):
    (myformat, mybytes) = _renderSizedIntegerForSPB(code, 'b')
    spb.append(struct.pack(myformat, mybytes))


def _vax_inverse(i, myformat):
    # Apply the inverse of _ksrv.isc_vax_integer to a Python integer; return
    # the raw bytes of the resulting value.
    iRaw = struct.pack(myformat, i)
    iConv = ibase.isc_vax_integer(iRaw, len(iRaw))
    iConvRaw = struct.pack(myformat, iConv)
    return iConvRaw


def _renderSizedIntegerForSPB(i, myformat):
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
    destFormat = '%ds' % struct.calcsize(myformat)
    destVal = _vax_inverse(i, myformat)
    return (destFormat, destVal)


def connect(host='service_mgr',
    user=os.environ.get('ISC_USER', 'sysdba'),
    password=os.environ.get('ISC_PASSWORD', None)
  ):
    """Establishes a connection to the Services Manager.

    :param string host: (optional) Host machine specification. Local by default.
    :param string user: (optional) Administrative user name. Defaults to content
       of environment variable `'ISC_USER'` or `'SYSDBA'`.
    :param string password: Administrative user password. Default is content 
       of environment variable `'ISC_PASSWORD'`.

    
    .. note::
    
       By definition, a Services Manager connection is bound to a particular
       host.  Therefore, the database specified as a parameter to methods such as
       `getStatistics` MUST NOT include the host name of the database server.
    """

    if password is None:
        raise fdb.ProgrammingError('A password is required to use'
                                   ' the Services Manager.')

    _checkString(host)
    _checkString(user)
    _checkString(password)

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
        if not host.endswith(':'):
            host += ':'
        host += 'service_mgr'

    return Connection(host, user, password)


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
                      fdb.bs([ibase.isc_spb_password,
                            len(self.password)]) + self.password
        ibase.isc_service_attach(self._isc_status, len(self.host), self.host,
                                 self._svc_handle, len(spb), spb)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.DatabaseError,
                                            self._isc_status,
                                            "Services/isc_service_attach:")
    def __del__(self):
        self.close()
    def next(self):
        """Return the next result line from service manager. Part of *iterator 
        protocol*.
        
        :raises StopIteration: If there are no further lines. 
        """
        line = self.readline()
        if line:
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
    def __fetchline(self):
        try:
            line = self._Q(ibase.isc_info_svc_line, self.QUERY_TYPE_PLAIN_STRING)
        except:
            # YYY: It is routine for actions such as RESTORE to raise an
            # exception at the end of their output.  We ignore any such
            # exception and assume that it was expected, which is somewhat
            # risky.  For example, suppose the network connection is broken
            # while the client is receiving the action's output...
            self.__fetching = False
            return None
        return line
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
        return (fdb.bytes_to_int(raw[index:new_index]), new_index)
    def _extract_string(self, raw, index):
        (size, index) = self._extract_int(raw, index)
        new_index = index + size
        ### Todo: verify handling of P version differences, refactor
        if ibase.PYTHON_MAJOR_VER == 3:
            return (str(raw[index:new_index],
                 ibase.charset_map.get(self.charset, self.charset)), new_index)
        else:
            return (str(raw[index:new_index]), new_index)
    def _Q(self, code, result_type, timeout=-1):
        if code < 0 or code > ibase.USHRT_MAX:
            raise fdb.ProgrammingError("The service query request_buf code"
                                       " must fall between 0 and %d,"
                                       " inclusive." % ibase.USHRT_MAX)
        result = None
        result_size = 1024
        request = fdb.bs([code])
        if timeout == -1:
            spb = ibase.b('')
        else:
            spb = fdb.bs(ibase.isc_info_svc_timeout, timeout)
        while True:
            if result_size > ibase.USHRT_MAX:
                raise fdb.InternalError("Database C API constraints maximum"
                                        "result buffer size to %d"
                                        % ibase.USHRT_MAX)
            result_buffer = ctypes.create_string_buffer(result_size)
            ibase.isc_service_query(self._isc_status, self._svc_handle, None,
                                    len(spb), spb,
                                    len(request), request,
                                    result_size, result_buffer)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError,
                                      self._isc_status,
                                      "Services/isc_service_query:")
            if result_buffer[0] == ibase.isc_info_truncated:
                result_size = result_size * 4
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
#        assert raw[-1] == ibase.int2byte(ibase.isc_info_flag_end)

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
            raise fdb.ProgrammingError("The size of the request buffer"
                                       " must not exceed %d."
                                       % ibase.USHRT_MAX)
        ibase.isc_service_start(self._isc_status, self._svc_handle, None,
                                len(request_buffer), request_buffer)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.OperationalError,
                         self._isc_status,
                         "Unable to perform the requested Service API action:")
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
        resultLines = []
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
            resultLines.append(line)
        return line_separator.join(resultLines)
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
    def _validate_companion_string_numeric_sequences(self,strings, numbers,
                                                     string_caption, 
                                                     number_caption):
        # The core constraint here is that len(numbers) must equal len(strings) - 1
        stringsCount = len(strings)
        numbersCount = len(numbers)

        requiredNumbersCount = stringsCount - 1

        if numbersCount != requiredNumbersCount:
            raise ValueError(
                'Since you passed %d %s, you must %s corresponding %s.'
                % (stringsCount, string_caption, 
                   ('pass %d' % requiredNumbersCount 
                    if requiredNumbersCount > 0 
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
            _checkString(x)
        ### Todo: verify handling of P version differences, refactor?
        for el in x:
            _checkString(el)
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
    def _property_action_with_one_num_code(self, database, code, num, 
                                                  num_ctype = 'I'):
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_numeric(code, num, numCType=num_ctype)
        return self._property_action(database, reqBuf)
    def close(self):
        """Close the connection now (rather than whenever `__del__` is called). 
        The connection will be unusable from this point forward; an :exc:`Error` 
        (or subclass) exception will be raised if any operation is attempted 
        with the connection.
        """
        if self._svc_handle:
            ibase.isc_service_detach(self._isc_status, self._svc_handle)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError,
                              self._isc_status, "Services/isc_service_detach:")
            self._svc_handle = None
            self.__fetching = False
    def readline(self):
        """Get next line of textual output from last service query.
        
        :returns string: Output line.
        """
        if self.__fetching:
            return self.__fetchline()
        else:
            return None
    def readlines(self):
        """Get list of remaining output lines from last service query.
        
        :returns list: Service output.
        :raises ProgrammingError: When service is not in :attr:`fetching` mode.
        """
        return [line for line in self]
    def isrunning(self):
        """Returns True if service is running.
        
        .. note::
        
           Some services like :meth:`backup` or :meth:`sweep` may take time to
           comlete, so they're called asynchronously. Until they're finished,
           no other async service could be started.
        """
        return self._QI(ibase.isc_info_svc_running) > 0
    def wait(self):
        """Wait until running service completes.
        """
        if self.isrunning:
            x = 1
            while x:
                x = self.__fetchline()
            self.__fetching = False
    def get_service_manager_version(self):
        """Get Firebird Service Manager version number.
        
        :returns integer: Version number.
        """
        self.__check_active()
        return self._QI(ibase.isc_info_svc_version)
    def get_server_version(self):
        """Get Firebird version.
        
        :returns string: Firebird version (example: 'LI-V2.5.2.26536 Firebird 2.5').
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_server_version)
    def get_architecture(self):
        """Get Firebird Server architecture.
        
        :returns string: Architecture (example: 'Firebird/linux AMD64').
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_implementation)
    def get_home_directory(self):
        """Get Firebird Home (installation) Directory.
        
        :returns string: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env)
    def get_security_database_path(self):
        """Get full path to Firebird security database.
        
        :returns string: Path (path+filename) to security database.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_user_dbpath)
    def get_lock_file_directory(self):
        """Get directory location for Firebird lock files.
        
        :returns string: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env_lock)
    def get_server_capabilities(self):
        """Get list of Firebird capabilities.
        
        :returns tuple: Capability info codes for each capability reported by
           server.
        
        Next fdb.services constants define possible info codes returned::
        
            :data:`CAPABILITY_MULTI_CLIENT`
            :data:`CAPABILITY_REMOTE_HOP`
            :data:`CAPABILITY_SERVER_CONFIG`
            :data:`CAPABILITY_QUOTED_FILENAME`
            :data:`CAPABILITY_NO_SERVER_SHUTDOWN`

        Example::
        
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
        
        :returns string: Directory path.
        """
        self.__check_active()
        return self._QS(ibase.isc_info_svc_get_env_msg)
    def get_connection_count(self):
        """Get number of attachments to server.
        
        :returns integer: Directory path.
        """
        self.__check_active()
        return self._get_isc_info_svc_svr_db_info()[0]
    def get_attached_database_names(self):
        """Get list of attached databases.
        
        :returns list: Filenames of attached databases.
        """
        self.__check_active()
        return self._get_isc_info_svc_svr_db_info()[1]
    def get_log(self, callback=None):
        """Request content of Firebird Server log. **(ASYNC service)**
        
        :param function callback: Function to call back with each output line.
          Function must accept only one parameter: line of output.
        
        If `callback` is not specified, log content could be retrieved through 
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.
        
        .. note::
        
           Until log content is not fully fetched from service (or ignored via 
           :meth:`wait`), any attempt to start another asynchronous service will 
           fail with exception.
        """
        self.__check_active()
        self._act(_ServiceActionRequestBuilder(ibase.isc_action_svc_get_ib_log))
        self.__fetching = True
        if callback:
            for line in self:
                callback(line)
    def get_limbo_transaction_ids(self, database):
        """Get list of transactions in limbo.
        
        :param string database: Database filename or alias.
        :returns list: Transaction IDs.
        :raises InternalError: When can't process the result buffer.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)

        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_option_mask(ibase.isc_spb_rpr_list_limbo_trans)
        raw = self._repair_action(database, reqBuf, line_separator='')
        raw = ibase.b(raw)
        nBytes = len(raw)

        transIDs = []
        i = 0
        while i < nBytes:
            byte = ibase.ord2(raw[i])
            if byte in (ibase.isc_spb_single_tra_id,
                        ibase.isc_spb_multi_tra_id):
                # The transaction ID is a 32-bit integer that begins
                # immediately after position i.
                transID = struct.unpack('i', raw[i + 1:i + 5])[0]
                i += 5  # Advance past the marker byte and the 32-bit integer.
                transIDs.append(transID)
            else:
                raise fdb.InternalError('Unable to process buffer contents'
                    ' beginning at position %d.' % i)
        return transIDs
    def _resolve_limbo_transaction(self, resolution, database, transaction_id):
        _checkString(database)
        database = ibase.b(database)

        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_numeric(resolution, transaction_id)
        self._repair_action(database, reqBuf)
    def commit_limbo_transaction(self, database, transaction_id):
        """Resolve limbo transaction with commit.
        
        :param string database: Database filename or alias.
        :param integer transaction_id: ID of Transaction to resolve.
        """
        self.__check_active()
        database = ibase.b(database)
        self._resolve_limbo_transaction(ibase.isc_spb_rpr_commit_trans,
                                        database, transaction_id)
    def rollback_limbo_transaction(self, database, transaction_id):
        """Resolve limbo transaction with rollback.
        
        :param string database: Database filename or alias.
        :param integer transaction_id: ID of Transaction to resolve.
        """
        self.__check_active()
        database = ibase.b(database)
        self._resolve_limbo_transaction(ibase.isc_spb_rpr_rollback_trans,
                                        database, transaction_id)
    def get_statistics(self, database, 
                       show_only_db_log_pages=0,
                       show_only_db_header_pages=0,
                       show_user_data_pages=1,
                       show_user_index_pages=1,
                       # 2004.06.06: False by default b/c gstat behaves that way:
                       show_system_tables_and_indexes=0,
                       show_record_versions=0,
                       callback=None
                       ):
        """Request database statisctics. **(ASYNC service)**

        :param string database: Database specification.
        :param integer show_only_db_log_pages: `1` to analyze only log pages.
        :param integer show_only_db_header_pages: `1` to analyze only database 
           header. When set, all other parameters are ignored.
        :param integer show_user_data_pages: `0` to skip user data analysis.
        :param integer show_user_index_pages: `0` to skip user index analysis.
        :param integer show_system_tables_and_indexes: `1` to analyze system 
           tables and indices.
        :param integer show_record_versions: `1` to analyze record versions.
        :param function callback: Function to call back with each output line.
          Function must accept only one parameter: line of output.
        
        If `callback` is not specified, statistical report could be retrieved 
        through :meth:`readline`, :meth:`readlines`, iteration over `Connection` 
        or ignored via call to :meth:`wait`.
        
        .. note::
        
           Until report is not fully fetched from service (or ignored via 
           :meth:`wait`), any attempt to start another asynchronous service will 
           fail with exception.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_db_stats)
        
        # Request for header negates all other options
        if show_only_db_header_pages:
            show_only_db_log_pages = show_user_data_pages = 0
            show_user_index_pages = show_system_tables_and_indexes =0
            show_record_versions = 0
        optionMask = 0
        if show_user_data_pages:
            optionMask |= ibase.isc_spb_sts_data_pages
        if show_only_db_log_pages:
            optionMask |= ibase.isc_spb_sts_db_log
        if show_only_db_header_pages:
            optionMask |= ibase.isc_spb_sts_hdr_pages
        if show_user_index_pages:
            optionMask |= ibase.isc_spb_sts_idx_pages
        if show_system_tables_and_indexes:
            optionMask |= ibase.isc_spb_sts_sys_relations
        if show_record_versions:
            optionMask |= ibase.isc_spb_sts_record_versions

        reqBuf.add_database_name(database)
        reqBuf.add_option_mask(optionMask)
        self._act(reqBuf)
        self.__fetching = True
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
               convert_external_tables_to_internal=0,
               compressed=1,  
               no_db_triggers=0,
               callback=None
               ):
        """Request logical (GBAK) database backup. **(ASYNC service)**
        
        :param string source_database: Source database specification.
        :param dest_filenames: Backup file(s) specification.
        :type dest_filenames: string or tuple of strings
        :param dest_file_sizes: (optional) specification of backup file max. sizes.
        :type dest_file_sizes: tuple of integers
        :param integer ignore_checksums: `1` to ignore checksums.
        :param integer ignore_limbo_transactions: `1` to ignore limbo transactions.
        :param integer metadata_only: `1` to create only metadata backup.
        :param integer collect_garbage: `0` to skip garbage collection.
        :param integer transportable: `0` to do not create transportable backup.
        :param integer convert_external_tables_to_internal: `1` to convert
           external table to internal ones.
        :param integer compressed: `0` to create uncompressed backup.
        :param integer no_db_triggers: `1` to disable database triggers temporarily.
        :param function callback: Function to call back with each output line.
          Function must accept only one parameter: line of output.
        
        If `callback` is not specified, backup log could be retrieved through 
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.
        
        .. note::
        
           Until backup report is not fully fetched from service (or ignored via 
           :meth:`wait`), any attempt to start another asynchronous service will 
           fail with exception.
        """
        self.__check_active()
        # Begin parameter validation section.
        _checkString(source_database)
        source_database = ibase.b(source_database)
        dest_filenames = self._require_str_or_tuple_of_str(dest_filenames)

        destFilenamesCount = len(dest_filenames)
        # 2004.07.17: YYY: Temporary warning:
        # Current (1.5.1) versions of the database engine appear to hang the
        # Services API request when it contains more than 11 destFilenames
        if destFilenamesCount > 11:
            warnings.warn(
                'Current versions of the database engine appear to hang when'
                ' passed a request to generate a backup with more than 11'
                ' constituents.',
                RuntimeWarning
              )

        if destFilenamesCount > 9999:
            raise fdb.ProgrammingError("The database engine cannot output a"
                " single source database to more than 9999 backup files."
              )
        self._validate_companion_string_numeric_sequences(
            dest_filenames, dest_file_sizes,
            'destination filenames', 'destination file sizes'
            )

        if len(self._exclude_elements_of_types(dest_file_sizes,
                                            (int, ibase.mylong))) > 0:
            raise TypeError("Every element of destFileSizes must be an int"
                " or long."
              )
        destFileSizesCount = len(dest_file_sizes)

        # The following should have already been checked by
        # _validateCompanionStringNumericSequences.
        assert destFileSizesCount == destFilenamesCount - 1
        # End parameter validation section.

        # Begin option bitmask setup section.
        optionMask = 0
        if ignore_checksums:
            optionMask |= ibase.isc_spb_bkp_ignore_checksums
        if ignore_limbo_transactions:
            optionMask |= ibase.isc_spb_bkp_ignore_limbo
        if metadata_only:
            optionMask |= ibase.isc_spb_bkp_metadata_only
        if not collect_garbage:
            optionMask |= ibase.isc_spb_bkp_no_garbage_collect
        if not transportable:
            optionMask |= ibase.isc_spb_bkp_non_transportable
        if convert_external_tables_to_internal:
            optionMask |= ibase.isc_spb_bkp_convert
        if not compressed:
            optionMask |= ibase.isc_spb_bkp_expand
        if no_db_triggers:
            optionMask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_backup)

        # Source database filename:
        request.add_database_name(source_database)

        # Backup filenames and sizes:
        request.add_string_numeric_pairs_sequence(
            ibase.isc_spb_bkp_file, dest_filenames,
            ibase.isc_spb_bkp_length, dest_file_sizes
          )

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, optionMask)

        # Tell the service to make its output available to us.
        request.add_code(ibase.isc_spb_verbose)

        # Done constructing the request buffer.
        self._act(request)
        self.__fetching = True
        if callback:
            for line in self:
                callback(line)
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
                callback=None):
        """Request database restore from logical (GBAK) backup. **(ASYNC service)**
        
        :param source_filenames: Backup file(s) specification.
        :type source_filenames: string or tuple of strings
        :param dest_filenames: Database file(s) specification.
        :type dest_filenames: string or tuple of strings
        :param dest_file_pages: (optional) specification of database file max. 
           # of pages.
        :type dest_file_pages: tuple of integers
        :param integer page_size: (optional) Page size.
        :param integer cache_buffers: (optional) Size of page-cache for this 
           database.
        :param integer access_mode_read_only: `1` to create R/O database.
        :param integer replace: `1` to replace existing database.
        :param integer deactivate_indexes: `1` to do not activate indices.
        :param integer do_not_restore_shadows: `1` to do not restore shadows.
        :param integer do_not_enforce_constraints: `1` to do not enforce
           constraints during restore.
        :param integer commit_after_each_table: `1` to commit after each table
           is restored.
        :param integer use_all_page_space: `1` to use all space on data pages.
        :param integer no_db_triggers: `1` to disable database triggers temporarily.
        :param integer metadata_only: `1` to restore only database metadata.
        :param function callback: Function to call back with each output line.
          Function must accept only one parameter: line of output.
        
        If `callback` is not specified, restore log could be retrieved through 
        :meth:`readline`, :meth:`readlines`, iteration over `Connection` or
        ignored via call to :meth:`wait`.
        
        .. note::
        
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
        optionMask = 0
        if replace:
            optionMask |= ibase.isc_spb_res_replace
        else:
            optionMask |= ibase.isc_spb_res_create
        if deactivate_indexes:
            optionMask |= ibase.isc_spb_res_deactivate_idx
        if do_not_restore_shadows:
            optionMask |= ibase.isc_spb_res_no_shadow
        if do_not_enforce_constraints:
            optionMask |= ibase.isc_spb_res_no_validity
        if commit_after_each_table:
            optionMask |= ibase.isc_spb_res_one_at_a_time
        if use_all_page_space:
            optionMask |= ibase.isc_spb_res_use_all_space
        if no_db_triggers:
            optionMask |= ibase.isc_spb_bkp_no_triggers
        if metadata_only:
            optionMask |= ibase.isc_spb_bkp_metadata_only
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_restore)

        # Backup filenames:
        request.add_string_sequence(ibase.isc_spb_bkp_file, source_filenames)

        # Database filenames:
        request.add_string_numeric_pairs_sequence(
            ibase.isc_spb_dbname, dest_filenames,
            ibase.isc_spb_res_length, dest_file_pages
          )

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
            accessMode = ibase.isc_spb_prp_am_readonly
        else:
            accessMode = ibase.isc_spb_prp_am_readwrite
        request.add_numeric(ibase.isc_spb_res_access_mode, accessMode,
            numCType='B'
          )

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, optionMask)

        # Tell the service to make its output available to us.
        request.add_code(ibase.isc_spb_verbose)

        # Done constructing the request buffer.
        self._act(request)
        self.__fetching = True
        if callback:
            for line in self:
                callback(line)
    # nbackup methods:
    def nbackup(self, source_database, 
                dest_filename, 
                nbackup_level=0,
                no_db_triggers=0):
        """Perform physical (NBACKUP) database backup.
        
        :param string source_database: Source database specification.
        :param dest_filename: Backup file specification.
        :param integer nbackup_level: Incremental backup level.
        :param integer no_db_triggers: `1` to disable database triggers temporarily.

        .. note:: Method call will not return until action is finished.
        """
        self.__check_active()
        # Begin parameter validation section.
        _checkString(source_database)
        source_database = ibase.b(source_database)
        _checkString(dest_filename)
        dest_filename = ibase.b(dest_filename)

        # Begin option bitmask setup section.
        optionMask = 0
        if no_db_triggers:
            optionMask |= ibase.isc_spb_bkp_no_triggers
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
        request.add_numeric(ibase.isc_spb_options, optionMask)

        # Done constructing the request buffer.
        self._act(request)
        self.wait()
    def nrestore(self, source_filenames, 
                 dest_filename, 
                 no_db_triggers=0):
        """Perform restore from physical (NBACKUP) database backup.
        
        :param source_filenames: Backup file(s) specification.
        :type source_filenames: string or tuple of strings
        :param dest_filename: Database file specification.
        :param integer no_db_triggers: `1` to disable database triggers temporarily.

        .. note:: Method call will not return until action is finished.
        """
        self.__check_active()
        # Begin parameter validation section.
        source_filenames = self._require_str_or_tuple_of_str(source_filenames)
        _checkString(dest_filename)
        dest_filename = ibase.b(dest_filename)

        # Begin option bitmask setup section.
        optionMask = 0
        if no_db_triggers:
            optionMask |= ibase.isc_spb_bkp_no_triggers
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_nrest)

        # Source database filename:
        request.add_string(ibase.isc_spb_dbname, dest_filename)

        # Backup filenames:
        request.add_string_sequence(ibase.isc_spb_nbk_file, source_filenames)

        # Options bitmask:
        request.add_numeric(ibase.isc_spb_options, optionMask)

        # Done constructing the request buffer.
        self._act(request)
        self.wait()
    # Trace
    def trace_start(self, config, name=None):
        """Start new trace session. **(ASYNC service)**
        
        :param string config: Trace session configuration.
        :param string name: (optional) Trace session name.
        :returns integer: Trace session ID.
        :raises DatabaseError: When session ID is not returned on start.

        Trace session output could be retrieved through :meth:`readline`, 
        :meth:`readlines`, iteration over `Connection` or ignored via call to 
        :meth:`wait`.
        
        .. note::
        
           Until session output is not fully fetched from service (or ignored 
           via :meth:`wait`), any attempt to start another asynchronous service 
           including call to any `trace_` method will fail with exception.
        """
        self.__check_active()
        if not name == None:
            _checkString(name)
        _checkString(config)
        # Construct the request buffer.
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_start)
        # trace name:
        if not name == None:
            reqBuf.add_string(ibase.isc_spb_trc_name, name)
        # trace configuration:
        reqBuf.add_string(ibase.isc_spb_trc_cfg, config)
        self._act(reqBuf)
        self.__fetching = True
        response = self.readline()
        if response.startswith('Trace session ID'):
            return int(response.split()[3])
        else:
            # response should contain the error message
            raise fdb.DatabaseError(response)
    def trace_stop(self, trace_id):
        """Stop trace session.
        
        :param integer trace_id: Trace session ID.
        :returns string: Text with confirmation that session was stopped.
        :raises DatabaseError: When trace session is not stopped.
        :raises OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_stop)
        reqBuf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(reqBuf)
        if not response.startswith("Trace session ID %i stopped" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_suspend(self, trace_id):
        """Suspend trace session.
        
        :param integer trace_id: Trace session ID.
        :returns string: Text with confirmation that session was paused.
        :raises DatabaseError: When trace session is not paused.
        :raises OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        reqBuf = _ServiceActionRequestBuilder(
                                            ibase.isc_action_svc_trace_suspend)
        reqBuf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(reqBuf)
        if not response.startswith("Trace session ID %i paused" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_resume(self, trace_id):
        """Resume trace session.
        
        :param integer trace_id: Trace session ID.
        :returns string: Text with confirmation that session was resumed.
        :raises DatabaseError: When trace session is not resumed.
        :raises OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        reqBuf = _ServiceActionRequestBuilder(
                                             ibase.isc_action_svc_trace_resume)
        reqBuf.add_numeric(ibase.isc_spb_trc_id, trace_id)

        response = self._act_and_return_textual_results(reqBuf)
        if not response.startswith("Trace session ID %i resumed" % trace_id):
            # response should contain the error message
            raise fdb.DatabaseError(response)
        return response
    def trace_list(self):
        """Get information about existing trace sessions.
        
        :returns dictionary: Mapping `SESSION_ID -> SESSION_PARAMS`
        
          Session parameters is another dictionary with next keys:
          
          :name:  (string) (optional) Session name if specified.
          :date:  (datetime.datetime) Session start date and time.
          :user:  (string) Trace user name.
          :flags: (list of strings) Session flags.
        
        :raises OperationalError: When server can't perform requested operation.
        """
        self.__check_active()
        # Construct the request buffer.
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_trace_list)
        # Get and parse the returned list.
        session_list = self._act_and_return_textual_results(reqBuf)
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
                    line.split(':',1)[1].strip(),
                    '%Y-%m-%d %H:%M:%S')
            elif line.lstrip().startswith("flags:"):
                result[session_id]["flags"] = line.split(':')[1].strip().split(',')
            else:
                raise fdb.OperationalError("Unexpected line in trace session list:`%s`" % line)
        return result
    # Database property alteration methods:
    def set_default_page_buffers(self, database, n):
        """Set individual page cache size for Database.
        
        :param string database: Database filename or alias.
        :param integer n: Number of pages.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        self._property_action_with_one_num_code(database, 
                                                ibase.isc_spb_prp_page_buffers, 
                                                n)
    def set_sweep_interval(self, database, n):
        """Set treshold for automatic sweep.
        
        :param string database: Database filename or alias.
        :param integer n: Sweep treshold, or `0` to disable automatic sweep.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_sweep_interval, 
                                                n)
    def set_reserve_page_space(self, database, reserve_space):
        """Set data page space reservation policy.
        
        :param string database: Database filename or alias.
        :param boolean reserve_space: `True` to reserve space, `False` to do not.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        if reserve_space:
            reserveCode = ibase.isc_spb_prp_res
        else:
            reserveCode = ibase.isc_spb_prp_res_use_full
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_reserve_space, 
                                                reserveCode, num_ctype='b')
    def set_write_mode(self, database, mode):
        """Set Disk Write Mode: Sync (forced writes) or Async (buffered).
        
        :param string database: Database filename or alias.
        :param integer mode: One from following constants:
           :data:`~fdb.services.WRITE_FORCED` or 
           :data:`~fdb.services.WRITE_BUFFERED`
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        if mode not in (WRITE_FORCED, WRITE_BUFFERED):
            raise ValueError('mode must be one of the following constants:'
                '  fdb.services.WRITE_FORCED, fdb.services.WRITE_BUFFERED.')
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_write_mode, 
                                                mode, num_ctype='b')
    def set_access_mode(self, database, mode):
        """Set Database Access mode: Read Only or Read/Write
        
        :param string database: Database filename or alias.
        :param integer mode: One from following constants:
           :data:`~fdb.services.ACCESS_READ_WRITE` or 
           :data:`~fdb.services.ACCESS_READ_ONLY`
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        if mode not in (ACCESS_READ_WRITE, ACCESS_READ_ONLY):
            raise ValueError('mode must be one of the following constants:'
                ' fdb.services.ACCESS_READ_WRITE, fdb.services.ACCESS_READ_ONLY.')
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_access_mode, 
                                                mode, num_ctype='b')
    def set_sql_dialect(self, database, dialect):
        """Set SQL Dialect for Database.
        
        :param string database: Database filename or alias.
        :param integer dialect: `1` or `3`.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        # The IB 6 API Guide says that dialect "must be 1 or 3", but other
        # dialects may become valid in future versions, so don't require
        #   dialect in (1, 3)
        self._property_action_with_one_num_code(database,
                                                ibase.isc_spb_prp_set_sql_dialect, 
                                                dialect)
    def activate_shadow(self, database):
        """Activate Database Shadow(s).
        
        :param string database: Database filename or alias.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_option_mask(ibase.isc_spb_prp_activate)
        self._property_action(database, reqBuf)
    # Database repair/maintenance methods:
    def shutdown(self, database, shutdown_mode, shutdown_method, timeout):
        """Database shutdown.
        
        :param string database: Database filename or alias.
        :param integer shutdown_mode: One from following constants:
           :data:`~fdb.services.SHUT_SINGLE`, :data:`~fdb.services.SHUT_MULTI` 
           or :data:`~fdb.services.SHUT_FULL`.
        :param integer shutdown_method: One from following constants:
           :data:`~fdb.services.SHUT_FORCE`, 
           :data:`~fdb.services.SHUT_DENY_NEW_TRANSACTIONS`
           or :data:`~fdb.services.SHUT_DENY_NEW_ATTACHMENTS`.
        :param integer timeout: Time in seconds, that the shutdown must complete in.
           
        .. seealso:: See also :meth:`~Connection.bring_online` method.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        if shutdown_mode not in (SHUT_SINGLE, SHUT_MULTI, SHUT_FULL):
            raise ValueError('shutdown_mode must be one of the following'
                ' constants:  fdb.services.SHUT_SINGLE,'
                ' fdbfdb.services.SHUT_MULTI,'
                ' fdb.services.SHUT_FULL.')
        if shutdown_method not in (SHUT_FORCE, SHUT_DENY_NEW_TRANSACTIONS, 
                                   SHUT_DENY_NEW_ATTACHMENTS):
            raise ValueError('shutdown_method must be one of the following'
                ' constants:  fdb.services.SHUT_FORCE,'
                ' fdb.services.SHUT_DENY_NEW_TRANSACTIONS,'
                ' fdb.services.SHUT_DENY_NEW_ATTACHMENTS.')
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_numeric(ibase.isc_spb_prp_shutdown_mode, 
                           shutdown_mode, numCType='B')
        reqBuf.add_numeric(shutdown_method, timeout, numCType='I')
        self._property_action(database, reqBuf)
    def bring_online(self, database, online_mode=SHUT_NORMAL):
        """Bring previously shut down database back online.
        
        :param string database: Database filename or alias.
        :param integer online_mode: (Optional) One from following constants:
           :data:`~fdb.services.SHUT_SINGLE`, :data:`~fdb.services.SHUT_MULTI` 
           or :data:`~fdb.services.SHUT_NORMAL` (**Default**).
           
        .. seealso:: See also :meth:`~Connection.shutdown` method.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        if online_mode not in (SHUT_NORMAL,SHUT_SINGLE, SHUT_MULTI):
            raise ValueError('online_mode must be one of the following'
                ' constants:  fdb.services.SHUT_NORMAL,'
                ' fdbfdb.services.SHUT_SINGLE,'
                ' fdb.services.SHUT_MULTI.')
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.add_numeric(ibase.isc_spb_prp_online_mode, 
                           online_mode, numCType='B')
        self._property_action(database, reqBuf)
    def sweep(self, database):
        """Perform Database Sweep.
        
        .. note:: Method call will not return until sweep is finished.
        
        :param string database: Database filename or alias.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        reqBuf = _ServiceActionRequestBuilder()
        optionMask = ibase.isc_spb_rpr_sweep_db
        reqBuf.add_option_mask(optionMask)
        return self._repair_action(database, reqBuf)
    def repair(self, database,
               read_only_validation=0,
               ignore_checksums=0,
               kill_unavailable_shadows=0,
               mend_database=0,
               validate_database=1,
               validate_record_fragments=1):
        """Database Validation and Repair.
        
        :param string database: Database filename or alias.
        :param integer read_only_validation: `1` to prevent any database changes.
        :param integer ignore_checksums: `1` to ignore page checksum errors.
        :param integer kill_unavailable_shadows: `1` to kill unavailable shadows.
        :param integer mend_database: `1` to fix database for backup.
        :param integer validate_database: `0` to skip database validation.
        :param integer validate_record_fragments: `0` to skip validation of 
           record fragments.

        .. note:: Method call will not return until action is finished.
        """
        self.__check_active()
        _checkString(database)
        database = ibase.b(database)
        # YYY: With certain option combinations, this method raises errors
        # that may not be very comprehensible to a Python programmer who's not
        # well versed with IB/FB.  Should option combination filtering be
        # done right here instead of leaving that responsibility to the
        # database engine?
        #   I think not, since any filtering done in this method is liable to
        # become outdated, or to inadvertently enforce an unnecessary,
        # crippling constraint on a certain option combination that the
        # database engine would have allowed.
        reqBuf = _ServiceActionRequestBuilder()
        optionMask = 0

        if read_only_validation:
            optionMask |= ibase.isc_spb_rpr_check_db
        if ignore_checksums:
            optionMask |= ibase.isc_spb_rpr_ignore_checksum
        if kill_unavailable_shadows:
            optionMask |= ibase.isc_spb_rpr_kill_shadows
        if mend_database:
            optionMask |= ibase.isc_spb_rpr_mend_db
        if validate_database:
            optionMask |= ibase.isc_spb_rpr_validate_db
        if validate_record_fragments:
            optionMask |= ibase.isc_spb_rpr_full
        reqBuf.add_option_mask(optionMask)
        return self._repair_action(database, reqBuf)

    # 2003.07.12:  Removed method resolveLimboTransactions (dropped plans to
    # support that operation from kinterbasdb since transactions IDs are not
    # exposed at the Python level and I don't consider limbo transaction
    # resolution compelling enough to warrant exposing transaction IDs).

    # User management methods:
    def get_users(self, user_name=None):
        """Get information about user(s).
        
        :param string user_name: (Optional) When specified, returns information 
           only about user with specified user name.
        :returns list: :class:`User` instances.
        """
        self.__check_active()
        if user_name is not None:
            if isinstance(user_name, ibase.myunicode):
                _checkString(user_name)
                user_name = ibase.b(user_name)
        reqBuf = _ServiceActionRequestBuilder(
                                              ibase.isc_action_svc_display_user
                                              )
        if user_name:
            user_name = user_name.upper()  # 2002.12.11
            reqBuf.add_string(ibase.isc_spb_sec_username, user_name)
        self._act(reqBuf)
        raw = self._QR(ibase.isc_info_svc_get_users)
        users = []
        curUser = None
        pos = 1  # Ignore raw[0].
        upper_limit = len(raw) - 1
        while pos < upper_limit:
            cluster = ibase.ord2(raw[pos])
            pos += 1
            if cluster == ibase.isc_spb_sec_username:
                if curUser is not None:
                    users.append(curUser)
                    curUser = None
                (user_name, pos) = self._extract_string(raw, pos)
                curUser = User(user_name)
            elif cluster == ibase.isc_spb_sec_password:
                (password, pos) = self._extract_string(raw, pos)
                curUser.password = password
            elif cluster == ibase.isc_spb_sec_firstname:
                (firstName, pos) = self._extract_string(raw, pos)
                curUser.first_name = firstName
            elif cluster == ibase.isc_spb_sec_middlename:
                (middleName, pos) = self._extract_string(raw, pos)
                curUser.middle_name = middleName
            elif cluster == ibase.isc_spb_sec_lastname:
                (lastName, pos) = self._extract_string(raw, pos)
                curUser.last_name = lastName
            elif cluster == ibase.isc_spb_sec_groupid:
                (groupId, pos) = self._extract_int(raw, pos)
                curUser.group_id = groupId
            elif cluster == ibase.isc_spb_sec_userid:
                (userId, pos) = self._extract_int(raw, pos)
                curUser.user_id = userId
        # Handle the last user:
        if curUser is not None:
            users.append(curUser)
            curUser = None
        return users
    def add_user(self, user):
        """Add new user.
        
        :param user: Instance of :class:`User` with **at least** its 
           :attr:`~User.name` and :attr:`~User.password` attributes specified 
           as non-empty values. All other attributes are optional.
        :type user: :class:`User`
        
        .. note::
        
           This method ignores the :attr:`~User.user_id` and :attr:`~User.group_id` 
           attributes of :class:`~User` regardless of their values.
        """
        self.__check_active()
        if not user.name:
            raise fdb.ProgrammingError('You must specify a username.')
        else:
            _checkString(user.name)
            user.name = ibase.b(user.name)

        if not user.password:
            raise fdb.ProgrammingError('You must specify a password.')
        else:
            _checkString(user.password)
            user.password = ibase.b(user.password)

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_add_user)

        reqBuf.add_string(ibase.isc_spb_sec_username, user.name)
        reqBuf.add_string(ibase.isc_spb_sec_password, user.password)

        if user.first_name:
            user.first_name = ibase.b(user.first_name)
            reqBuf.add_string(ibase.isc_spb_sec_firstname, user.first_name)
        if user.middle_name:
            user.middle_name = ibase.b(user.middle_name)
            reqBuf.add_string(ibase.isc_spb_sec_middlename, user.middle_name)
        if user.last_name:
            user.last_name = ibase.b(user.last_name)
            reqBuf.add_string(ibase.isc_spb_sec_lastname, user.last_name)

        self._act_and_return_textual_results(reqBuf)
    def modify_user(self, user):
        """Modify user information.
        
        :param user: Instance of :class:`User` with **at least** its 
           :attr:`~User.name` attribute specified as non-empty value.
        :type user: :class:`User`
        
        .. note::
        
           This method sets :attr:`~User.first_name`, :attr:`~User.middle_name`
           and :attr:`~User.last_name` to their actual values, and ignores
           the :attr:`~User.user_id` and :attr:`~User.group_id` attributes 
           regardless of their values. :attr:`~User.password` is set **only** 
           when it has value.
        """
        self.__check_active()
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_modify_user)

        if isinstance(user.name, str):
            user.name = ibase.b(user.name)
        reqBuf.add_string(ibase.isc_spb_sec_username, user.name)
        if isinstance(user.password, str):
            user.password = ibase.b(user.password)
        reqBuf.add_string(ibase.isc_spb_sec_password, user.password)
        # Change the optional attributes whether they're empty or not.
        if isinstance(user.first_name, str):
            user.first_name = ibase.b(user.first_name)
        reqBuf.add_string(ibase.isc_spb_sec_firstname, user.first_name)
        if isinstance(user.middle_name, str):
            user.middle_name = ibase.b(user.middle_name)
        reqBuf.add_string(ibase.isc_spb_sec_middlename, user.middle_name)
        if isinstance(user.last_name, str):
            user.last_name = ibase.b(user.last_name)
        reqBuf.add_string(ibase.isc_spb_sec_lastname, user.last_name)

        self._act_and_return_textual_results(reqBuf)
    def remove_user(self, user):
        """Remove user.

        :param user: User name or Instance of :class:`User` with **at least** its 
           :attr:`~User.name` attribute specified as non-empty value.
        :type user: string or :class:`User`
        """
        self.__check_active()
        if isinstance(user, User):
            username = user.name
        else:
            _checkString(user)
            user = ibase.b(user)
            username = user

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_delete_user)
        reqBuf.add_string(ibase.isc_spb_sec_username, username)
        self._act_and_return_textual_results(reqBuf)
    def user_exists(self, user):
        """Check for user's existence.

        :param user: User name or Instance of :class:`User` with **at least** its 
           :attr:`~User.name` attribute specified as non-empty value.
        :type user: string or :class:`User`
        :returns boolean: `True` when the specified user exists.
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

class User(object):
    def __init__(self, name=None):
        if name:
            _checkString(name)
            self.name = name.upper()
        else:
            #: User `login` name (username).
            self.name = None
        #: Password. Not returned by user output methods, but must be
        #: specified to add a user.
        self.password = None

        #: First name.
        self.first_name = None
        #: Middle name
        self.middle_name = None
        #: Last name
        self.last_name = None

        # The user id and group id are not fully supported.  For details, see
        # the documentation of the "User Management Methods" of
        # services.Connection.
        #: User ID
        self.user_id = None
        #: User group ID
        self.group_id = None

    def __str__(self):
        return '<fdb.services.User %s>' % (
            (self.name is None and 'without a name')
            or 'named "%s"' % self.name)


class _ServiceActionRequestBuilder(object):
    # This private class helps public facilities in this module to build
    # the binary action request buffers required by the database Services API
    # using high-level, easily comprehensible syntax.

    def __init__(self, clusterIdentifier=None):
        self._buffer = []
        if clusterIdentifier:
            self.add_code(clusterIdentifier)

    def __str__(self):
        return self.render()

    def extend(self, otherRequestBuilder):
        self._buffer.append(otherRequestBuilder.render())

    def add_code(self, code):
        _code2spb(self._buffer, code)

    def add_string(self, code, s):
#        _checkString(s)
        _string2spb(self._buffer, code, ibase.b(s))

    def add_string_sequence(self, code, stringSequence):
        for s in stringSequence:
            self.add_string(code, s)

    def add_string_numeric_pairs_sequence(self, stringCode, stringSequence,
                                            numericCode, numericSequence):
        stringCount = len(stringSequence)
        numericCount = len(numericSequence)
        if numericCount != stringCount - 1:
            raise ValueError("Numeric sequence must contain exactly one less"
                " element than its companion string sequence."
              )
        i = 0
        while i < stringCount:
            self.add_string(stringCode, stringSequence[i])
            if i < numericCount:
                self.add_numeric(numericCode, numericSequence[i])
            i += 1

    def add_numeric(self, code, n, numCType='I'):
        _numeric2spb(self._buffer, code, n, numCType=numCType)

    def add_option_mask(self, optionMask):
        self.add_numeric(ibase.isc_spb_options, optionMask)

    def add_database_name(self, databaseName):
        # 2003.07.20: Issue a warning for a hostname-containing databaseName
        # because it will cause isc_service_start to raise an inscrutable error
        # message with Firebird 1.5 (though it would not have raised an error
        # at all with Firebird 1.0 and earlier).
        ### Todo: verify handling of P version differences, refactor
        if ibase.PYTHON_MAJOR_VER == 3:
            colonIndex = (databaseName.decode(fdb.fbcore._FS_ENCODING)).find(':')
        else:
            colonIndex = databaseName.find(':')
        if colonIndex != -1:
            # This code makes no provision for platforms other than Windows
            # that allow colons in paths (such as MacOS).  Some of
            # kinterbasdb's current implementation (e.g., event handling) is
            # constrained to Windows or POSIX anyway.
            if not sys.platform.lower().startswith('win') or (
                # This client process is running on Windows.
                #
                # Files that don't exist might still be valid if the connection
                # is to a server other than the local machine.
                not os.path.exists(databaseName)
                # "Guess" that if the colon falls within the first two
                # characters of the string, the pre-colon portion refers to a
                # Windows drive letter rather than to a remote host.
                # This isn't guaranteed to be correct.
                and colonIndex > 1
              ):
                warnings.warn(
                    ' Unlike conventional DSNs, Services API database names'
                    ' must not include the host name; remove the "%s" from'
                    ' your database name.'
                    ' (Firebird 1.0 will accept this, but Firebird 1.5 will'
                    ' raise an error.)'
                    % databaseName[:colonIndex + 1],
                    UserWarning
                  )
        self.add_string(ibase.isc_spb_dbname, databaseName)

    def render(self):
        return ibase.b('').join(self._buffer)