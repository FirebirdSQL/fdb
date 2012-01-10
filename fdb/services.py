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
#  Contributor(s): ______________________________________.
#
# See LICENSE.TXT for details.

import fdb
import sys
import os
import types
import ibase
import ctypes, struct
import warnings

# The following SHUT_* constants are to be passed as the $shutdownMethod
# parameter to Connection.shutdown:
SHUT_FORCE = ibase.isc_spb_prp_shutdown_db
SHUT_DENY_NEW_TRANSACTIONS = ibase.isc_spb_prp_deny_new_transactions
SHUT_DENY_NEW_ATTACHMENTS = ibase.isc_spb_prp_deny_new_attachments

# The following WRITE_* constants are to be passed as the $mode parameter
# to Connection.setWriteMode:
WRITE_FORCED = ibase.isc_spb_prp_wm_sync
WRITE_BUFFERED = ibase.isc_spb_prp_wm_async

# The following ACCESS_* constants are to be passed as the $mode parameter
# to Connection.setAccessMode:
ACCESS_READ_WRITE = ibase.isc_spb_prp_am_readwrite
ACCESS_READ_ONLY = ibase.isc_spb_prp_am_readonly

def _checkString(s):
    try:
        if isinstance(s, str):
            # In str instances, Python allows any character in the "default
            # encoding", which is typically not ASCII.  Since Firebird's
            # Services API only works (properly) with ASCII, we need to make
            # sure there are no non-ASCII characters in s, even though we
            # already know s is a str instance.
            s.encode('ASCII')
        else:
            if isinstance(s, unicode):
                # Raise a more specific error message than the general case.
                raise UnicodeError
            else:
                raise TypeError('String argument to Services API must be'
                    ' of type str, not %s.' % type(s)
                  )
    except UnicodeError:
        raise TypeError("The database engine's Services API only works"
            " properly with ASCII string parameters, so str instances that"
            " contain non-ASCII characters, and all unicode instances, are"
            " disallowed."
          )

def _string2spb(spb, code, s):
    sLen = len(s)
    _numeric2spb(spb, code, sLen, numCType='H')
    format = str(sLen) + 's' # The length, then 's'.
    spb.append( struct.pack(format, s) )

def _numeric2spb(spb, code, num, numCType='I'):
    # numCType is one of the pack format characters specified by the Python
    # standard library module 'struct'.
    _code2spb(spb, code)
    (numericFormat, numericBytes) = _renderSizedIntegerForSPB(num, numCType)
    spb.append( struct.pack(numericFormat, numericBytes) )

def _code2spb(spb, code):
    (format, bytes) = _renderSizedIntegerForSPB(code, 'b')
    spb.append( struct.pack(format, bytes) )

def _vax_inverse(i, format):
    # Apply the inverse of _ksrv.isc_vax_integer to a Python integer; return
    # the raw bytes of the resulting value.
    iRaw = struct.pack(format, i)
    iConv = ibase.isc_vax_integer(iRaw,len(iRaw))
    iConvRaw = struct.pack(format, iConv)
    return iConvRaw

def _renderSizedIntegerForSPB(i, format):
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
    destFormat = '%ds' % struct.calcsize(format)
    destVal = _vax_inverse(i,  format)
    return (destFormat, destVal)

def connect(host='service_mgr',
    user=os.environ.get('ISC_USER', 'sysdba'),
    password=os.environ.get('ISC_PASSWORD', None)
  ):
    """
      Establishes a connection to the Services Manager.

      The $user and $password parameters must refer to an administrative user
    such as sysdba.  In fact, $user can be left blank, in which case it will
    default to sysdba.  The $password parameter is required.
      If the $host parameter is not supplied, the connection will default to
    the local host.

    NOTE:
      By definition, a Services Manager connection is bound to a particular
    host.  Therefore, the database specified as a parameter to methods such as
    getStatistics MUST NOT include the host name of the database server.
    """
    
    if password is None:
        raise ProgrammingError('A password is required to use the Services Manager.')

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
    QUERY_TYPE_PLAIN_INTEGER = 1
    QUERY_TYPE_PLAIN_STRING = 2
    QUERY_TYPE_RAW = 3
    
    def __init__(self, host, user, password):
        self._svc_handle = ibase.isc_svc_handle(0)
        self._isc_status = ibase.ISC_STATUS_ARRAY()
        self.host = host
        self.user = user
        self.password = password
        
        if len(host) + len(user) + len(password) > 118:
            raise fdb.ProgrammingError("The combined length of host,user and password can't exceed 118 bytes.")
        spb_length = 2 + 1 + 1 + len(user) + 1 + 1+ len(password)
        spb = fdb.bs([ibase.isc_spb_version,ibase.isc_spb_current_version,ibase.isc_spb_user_name,len(user)]) + \
            user + fdb.bs([ibase.isc_spb_password,len(password)]) + password
        ibase.isc_service_attach(self._isc_status,len(host),host,self._svc_handle,len(spb),spb)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.DatabaseError,self._isc_status,
                                  "Services/isc_service_attach:")
    def close(self):
        if self._svc_handle:
            ibase.isc_service_detach(self._isc_status,self._svc_handle)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError,self._isc_status,
                                      "Services/isc_service_detach:")
            self._svc_handle = None
    
    def _extract_int(self,raw,index):
        new_index = index+ctypes.sizeof(ctypes.c_ushort)
        return (fdb.bytes_to_int(raw[index:new_index]),new_index)
    def _extract_string(self,raw,index):
        (size,index) = self._extract_int(raw,index)
        new_index = index+size
        return (str(raw[index:new_index]),new_index)
    def _Q(self, code, resultType, timeout = -1):
        if code < 0 or code > ibase.USHRT_MAX:
            raise fdb.ProgrammingError("The service query request_buf code must fall between 0 and %d, inclusive." % ibase.USHRT_MAX)
        result = None
        result_size = 1024
        request = fdb.bs([code])
        if timeout == -1:
            spb = ''
        else:
            spb = fdb.bs(ibase.isc_info_svc_timeout,timeout)
        while True:
            if result_size > ibase.USHRT_MAX:
                raise fdb.InternalError("Database C API constraints maximum result buffer size to %d" % ibase.USHRT_MAX)
            result_buffer = ctypes.create_string_buffer(result_size)
            ibase.isc_service_query(self._isc_status,self._svc_handle,None,
                                    len(spb),spb,
                                    len(request),request,
                                    result_size,result_buffer)
            if fdb.db_api_error(self._isc_status):
                raise fdb.exception_from_status(fdb.DatabaseError,self._isc_status,
                                      "Services/isc_service_query:")
            if result_buffer[0] == ibase.isc_info_truncated:
                result_size = result_size * 4
                continue
            break
        if resultType == self.QUERY_TYPE_PLAIN_INTEGER:
            (result,_) = self._extract_int(result_buffer,1)
        elif resultType == self.QUERY_TYPE_PLAIN_STRING:
            (result,_) = self._extract_string(result_buffer,1)
        elif resultType == self.QUERY_TYPE_RAW:
            size = result_size - 1
            while result_buffer[size] == '\0':
                size -= 1
            result = str(result_buffer[:size])
            
        return result
    def _get_isc_info_svc_svr_db_info(self):
        num_attachments = -1
        databases = []

        raw = self._QR(ibase.isc_info_svc_svr_db_info)
        assert raw[-1] == chr(ibase.isc_info_flag_end)

        pos = 1 # Ignore raw[0].
        upper_limit = len(raw) - 1
        while pos < upper_limit:
            cluster = ord(raw[pos])
            pos += 1

            if cluster == ibase.isc_spb_num_att: # Number of attachments.
                (num_attachments, pos) = self._extract_int(raw, pos)
            elif cluster == ibase.isc_spb_num_db: # Number of databases
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
    def _action_thin(self,request_buffer):
        if len(request_buffer) > ibase.USHRT_MAX:
            raise fdb.ProgrammingError("The size of the request buffer must not exceed %d." % ibase.USHRT_MAX)
        ibase.isc_service_start(self._isc_status,self._svc_handle,None,
                                len(request_buffer),request_buffer)
        if fdb.db_api_error(self._isc_status):
            raise fdb.exception_from_status(fdb.OperationalError,self._isc_status,
                                  "Unable to perform the requested Service API action:")
        return None
    def _act(self, requestBuffer):
        return self._action_thin(requestBuffer.render())
    def _actAndReturnTextualResults(self, requestBuffer):
        self._act(requestBuffer)
        return self._collectUnformattedResults()
    def _collectUnformattedResults(self, lineSep='\n'):
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
        return lineSep.join(resultLines)
    def _repairAction(self, database, partialReqBuf):
        # Begin constructing the request buffer (incorporate the one passed as
        # param $partialReqBuf).
        fullReqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_repair)
        # The filename of the database must be specified regardless of the
        # action sub-action being perfomed.
        fullReqBuf.addDatabaseName(database)
        # Incorporate the caller's partial request buffer.
        fullReqBuf.extend(partialReqBuf)
        self._action_thin(fullReqBuf.render())
        # Return the results to the caller synchronously (in this case, there
        # won't be any textual results, but issuing this call will helpfully
        # cause the program to block until the Services Manager is finished
        # with the action).
        return self._collectUnformattedResults()
    def _validateCompanionStringNumericSequences(self,
        strings, numbers,
        stringCaption, numberCaption
      ):
        # The core constraint here is that len(numbers) must equal len(strings) - 1
        stringsCount = len(strings)
        numbersCount = len(numbers)
    
        requiredNumbersCount = stringsCount - 1
    
        if numbersCount != requiredNumbersCount:
            raise ValueError(
                'Since you passed %d %s, you must %s corresponding %s.'
                % (stringsCount,
                   stringCaption,
                   (requiredNumbersCount > 0
                    and 'pass %d' % requiredNumbersCount
                   ) or 'not pass any',
                   numberCaption
                  )
              )
    def _excludeElementsOfTypes(self, seq, theTypesToExclude):
        if not isinstance(theTypesToExclude, tuple):
            theTypesToExclude = tuple(theTypesToExclude)
        return [element for element in seq 
                if not isinstance(element, theTypesToExclude)]
    def _requireStrOrTupleOfStr(self,x):
        if isinstance(x, str):
            x = (x,)
        elif isinstance(x, unicode):
            # We know the following call to _checkString will raise an exception,
            # but calling it anyway allows us to centralize the error message
            # generation:
            _checkString(x)
        for el in x:
            _checkString(el)
        return x
    def _propertyAction(self, database, partialReqBuf):
        # Begin constructing the request buffer (incorporate the one passed as
        # param $partialReqBuf).
        fullReqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_properties)
        # The filename of the database must be specified regardless of the
        # action sub-action being perfomed.
        fullReqBuf.addDatabaseName(database)
        # Incorporate the caller's partial request buffer.
        fullReqBuf.extend(partialReqBuf)
        self._action_thin(fullReqBuf.render())
        # Return the results to the caller synchronously.
        # Since they don't produce output, is the following useful?
        # LATER: Yes, because it blocks until there's been some resolution of
        # the action.
        return self._collectUnformattedResults()
    def _propertyActionWithSingleNumericCode(self, database, code, num, numCType='I'):
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.addNumeric(code, num, numCType=numCType)
        return self._propertyAction(database, reqBuf)

    def getServiceManagerVersion(self):
        return self._QI(ibase.isc_info_svc_version)
    def getServerVersion(self):
        return self._QS(ibase.isc_info_svc_server_version)
    def getArchitecture(self):
        return self._QS(ibase.isc_info_svc_implementation)
    def getHomeDir(self):
        return self._QS(ibase.isc_info_svc_get_env)
    def getSecurityDatabasePath(self):
        return self._QS(ibase.isc_info_svc_user_dbpath)
    def getLockFileDir(self):
        return self._QS(ibase.isc_info_svc_get_env_lock)
    def getCapabilityMask(self):
        return self._QI(ibase.isc_info_svc_capabilities)
    def getMessageFileDir(self):
        return self._QS(ibase.isc_info_svc_get_env_msg)
    def getConnectionCount(self):
        return self._get_isc_info_svc_svr_db_info()[0]
    def getAttachedDatabaseNames(self):
        return self._get_isc_info_svc_svr_db_info()[1]
    def getLog(self):
        """
          Note:  Current versions of the database server do not rotate the log
        file, so it can become VERY large, and take a long time to retrieve.
        """
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_get_ib_log)
        return self._actAndReturnTextualResults(reqBuf)
    def getLimboTransactionIDs(self, database):
        _checkString(database)

        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.addOptionMask(ibase.isc_spb_rpr_list_limbo_trans)
        raw = self._repairAction(database, reqBuf)
        nBytes = len(raw)

        transIDs = []
        i = 0
        while i < nBytes:
            byte = ord(raw[i])
            if byte in (ibase.isc_spb_single_tra_id, ibase.isc_spb_multi_tra_id):
                # The transaction ID is a 32-bit integer that begins
                # immediately after position i.
                transID = struct.unpack('i', raw[i+1:i+5])[0]
                i += 5 # Advance past the marker byte and the 32-bit integer.
                transIDs.append(transID)
            else:
                raise InternalError('Unable to process buffer contents'
                    ' beginning at position %d.' % i
                  )

        return transIDs
    def _resolveLimboTransaction(self, resolution, database, transactionID):
        _checkString(database)

        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.addNumeric(resolution, transactionID)
        self._repairAction(database, reqBuf)
    def commitLimboTransaction(self, database, transactionID):
        return self._resolveLimboTransaction(ibase.isc_spb_rpr_commit_trans,
                                             database, transactionID)
    def rollbackLimboTransaction(self, database, transactionID):
        return self._resolveLimboTransaction(ibase.isc_spb_rpr_rollback_trans,
                                             database, transactionID)
    def getStatistics(self, database,
        showOnlyDatabaseLogPages=0,
        showOnlyDatabaseHeaderPages=0,
        showUserDataPages=1,
        showUserIndexPages=1,
        # 2004.06.06: False by default b/c gstat behaves that way:
        showSystemTablesAndIndexes=0,
        showRecordVersions=0
      ):
        _checkString(database)

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_db_stats)

        optionMask = 0
        if showUserDataPages:
            optionMask |= ibase.isc_spb_sts_data_pages
        if showOnlyDatabaseLogPages:
            optionMask |= ibase.isc_spb_sts_db_log
        if showOnlyDatabaseHeaderPages:
            optionMask |= ibase.isc_spb_sts_hdr_pages
        if showUserIndexPages:
            optionMask |= ibase.isc_spb_sts_idx_pages
        if showSystemTablesAndIndexes:
            optionMask |= ibase.isc_spb_sts_sys_relations
        if showRecordVersions:
            optionMask |= ibase.isc_spb_sts_record_versions

        reqBuf.addDatabaseName(database)
        reqBuf.addOptionMask(optionMask)
        return self._actAndReturnTextualResults(reqBuf)
    # Backup and Restore methods:
    def backup(self,
        sourceDatabase,
        destFilenames, destFileSizes=(),
        #factor=None, # YYY:What is this?

        # Backup operation optionMask:
        ignoreChecksums=0,
        ignoreLimboTransactions=0,
        metadataOnly=0,
        garbageCollect=1,
        transportable=1,
        convertExternalTablesToInternalTables=1,
        expand=1 # YYY:What is this?
      ):
        # Begin parameter validation section.
        _checkString(sourceDatabase)
        destFilenames = self._requireStrOrTupleOfStr(destFilenames)

        destFilenamesCount = len(destFilenames)
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
            raise ProgrammingError("The database engine cannot output a"
                " single source database to more than 9999 backup files."
              )
        self._validateCompanionStringNumericSequences(destFilenames, destFileSizes,
            'destination filenames', 'destination file sizes'
          )

        if len(self._excludeElementsOfTypes(destFileSizes, (int, long))) > 0:
            raise TypeError("Every element of destFileSizes must be an int"
                " or long."
              )
        destFileSizesCount = len(destFileSizes)

        # The following should have already been checked by
        # _validateCompanionStringNumericSequences.
        assert destFileSizesCount == destFilenamesCount - 1
        # End parameter validation section.

        # Begin option bitmask setup section.
        optionMask = 0
        if ignoreChecksums:
            optionMask |= ibase.isc_spb_bkp_ignore_checksums
        if ignoreLimboTransactions:
            optionMask |= ibase.isc_spb_bkp_ignore_limbo
        if metadataOnly:
            optionMask |= ibase.isc_spb_bkp_metadata_only
        if not garbageCollect:
            optionMask |= ibase.isc_spb_bkp_no_garbage_collect
        if not transportable:
            optionMask |= ibase.isc_spb_bkp_non_transportable
        if convertExternalTablesToInternalTables:
            optionMask |= ibase.isc_spb_bkp_convert
        if expand:
            optionMask |= ibase.isc_spb_bkp_expand
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_backup)

        # Source database filename:
        request.addDatabaseName(sourceDatabase)

        # Backup filenames and sizes:
        request.addSequenceOfStringNumericPairs(
            ibase.isc_spb_bkp_file,   destFilenames,
            ibase.isc_spb_bkp_length, destFileSizes
          )

        # Options bitmask:
        request.addNumeric(ibase.isc_spb_options, optionMask)

        # Tell the service to make its output available to us.
        request.addCode(ibase.isc_spb_verbose)

        # Done constructing the request buffer.
        return self._actAndReturnTextualResults(request)
    def restore(self,
        sourceFilenames,
        destFilenames, destFilePages=(),

        pageSize=None,
        cacheBuffers=None,
        accessModeReadOnly=0,

        replace=0,
        create=1,
        deactivateIndexes=0,
        doNotRestoreShadows=0,
        doNotEnforceConstraints=0,
        commitAfterEachTable=0,
        useAllPageSpace=0
      ):
        # Begin parameter validation section.
        sourceFilenames = self._requireStrOrTupleOfStr(sourceFilenames)
        destFilenames = self._requireStrOrTupleOfStr(destFilenames)

        self._validateCompanionStringNumericSequences(destFilenames, destFilePages,
            'destination filenames', 'destination file page counts'
          )
        # End parameter validation section.

        # Begin option bitmask setup section.
        optionMask = 0
        if replace:
            optionMask |= ibase.isc_spb_res_replace
        if create:
            optionMask |= ibase.isc_spb_res_create
        if deactivateIndexes:
            optionMask |= ibase.isc_spb_res_deactivate_idx
        if doNotRestoreShadows:
            optionMask |= ibase.isc_spb_res_no_shadow
        if doNotEnforceConstraints:
            optionMask |= ibase.isc_spb_res_no_validity
        if commitAfterEachTable:
            optionMask |= ibase.isc_spb_res_one_at_a_time
        if useAllPageSpace:
            optionMask |= ibase.isc_spb_res_use_all_space
        # End option bitmask setup section.

        # Construct the request buffer.
        request = _ServiceActionRequestBuilder(ibase.isc_action_svc_restore)

        # Backup filenames:
        request.addSequenceOfStrings(ibase.isc_spb_bkp_file, sourceFilenames)

        # Database filenames:
        request.addSequenceOfStringNumericPairs(
            ibase.isc_spb_dbname,     destFilenames,
            ibase.isc_spb_res_length, destFilePages
          )

        # Page size of the restored database:
        if pageSize:
            request.addNumeric(ibase.isc_spb_res_page_size, pageSize)

        # cacheBuffers is the number of default cache buffers to configure for
        # attachments to the restored database:
        if cacheBuffers:
            request.addNumeric(ibase.isc_spb_res_buffers, cacheBuffers)

        # accessModeReadOnly controls whether the restored database is
        # "mounted" in read only or read-write mode:
        if accessModeReadOnly:
            accessMode = ibase.isc_spb_prp_am_readonly
        else:
            accessMode = ibase.isc_spb_prp_am_readwrite
        request.addNumeric(ibase.isc_spb_res_access_mode, accessMode,
            numCType='B'
          )

        # Options bitmask:
        request.addNumeric(ibase.isc_spb_options, optionMask)

        # Tell the service to make its output available to us.
        request.addCode(ibase.isc_spb_verbose)

        # Done constructing the request buffer.
        self._action_thin(request.render())

        # Return the results to the caller synchronously.
        return self._collectUnformattedResults()
    # Database property alteration methods:
    def setDefaultPageBuffers(self, database, n):
        _checkString(database)
        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_page_buffers, n)
    def setSweepInterval(self, database, n):
        _checkString(database)
        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_sweep_interval, n)
    def shutdown(self, database, shutdownMethod, timeout):
        _checkString(database)
        if shutdownMethod not in (
            SHUT_FORCE, SHUT_DENY_NEW_TRANSACTIONS, SHUT_DENY_NEW_ATTACHMENTS
          ):
            raise ValueError('shutdownMethod must be one of the following'
                ' constants:  services.SHUT_FORCE,'
                ' services.SHUT_DENY_NEW_TRANSACTIONS,'
                ' services.SHUT_DENY_NEW_ATTACHMENTS.'
              )
        return self._propertyActionWithSingleNumericCode(database,
            shutdownMethod, timeout)
    def bringOnline(self, database):
        _checkString(database)
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.addOptionMask(ibase.isc_spb_prp_db_online)
        return self._propertyAction(database, reqBuf)
    def setShouldReservePageSpace(self, database, shouldReserve):
        _checkString(database)
        if shouldReserve:
            reserveCode = ibase.isc_spb_prp_res
        else:
            reserveCode = ibase.isc_spb_prp_res_use_full
        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_reserve_space, reserveCode, numCType='b')
    def setWriteMode(self, database, mode):
        _checkString(database)
        if mode not in (WRITE_FORCED, WRITE_BUFFERED):
            raise ValueError('mode must be one of the following constants:'
                '  services.WRITE_FORCED, services.WRITE_BUFFERED.'
              )

        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_write_mode, mode, numCType='b')
    def setAccessMode(self, database, mode):
        _checkString(database)
        if mode not in (ACCESS_READ_WRITE, ACCESS_READ_ONLY):
            raise ValueError('mode must be one of the following constants:'
                '  services.ACCESS_READ_WRITE, services.ACCESS_READ_ONLY.'
              )
        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_access_mode, mode, numCType='b')
    def setSQLDialect(self, database, dialect):
        _checkString(database)
        # The IB 6 API Guide says that dialect "must be 1 or 3", but other
        # dialects may become valid in future versions, so don't require
        #   dialect in (1, 3)
        return self._propertyActionWithSingleNumericCode(database,
            ibase.isc_spb_prp_set_sql_dialect, dialect)
    def activateShadowFile(self, database):
        _checkString(database)
        reqBuf = _ServiceActionRequestBuilder()
        reqBuf.addOptionMask(ibase.isc_spb_prp_activate)
        return self._propertyAction(database, reqBuf)
    # Database repair/maintenance methods:
    def sweep(self, database, markOutdatedRecordsAsFreeSpace=1):
        _checkString(database)
        reqBuf = _ServiceActionRequestBuilder()
        optionMask = 0
        if markOutdatedRecordsAsFreeSpace:
            optionMask |= ibase.isc_spb_rpr_sweep_db
        reqBuf.addOptionMask(optionMask)
        return self._repairAction(database, reqBuf)
    def repair(self, database,
               readOnlyValidation=0,
               ignoreChecksums=0,
               removeReferencesToUnavailableShadowFiles=0,
               markCorruptedRecordsAsUnavailable=0,
               releaseUnassignedPages=1,
               releaseUnassignedRecordFragments=1):
        _checkString(database)
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

        if readOnlyValidation:
            optionMask |= ibase.isc_spb_rpr_check_db
        if ignoreChecksums:
            optionMask |= ibase.isc_spb_rpr_ignore_checksum
        if removeReferencesToUnavailableShadowFiles:
            optionMask |= ibase.isc_spb_rpr_kill_shadows
        if markCorruptedRecordsAsUnavailable:
            optionMask |= ibase.isc_spb_rpr_mend_db
        if releaseUnassignedPages:
            optionMask |= ibase.isc_spb_rpr_validate_db
        if releaseUnassignedRecordFragments:
            optionMask |= ibase.isc_spb_rpr_full
        reqBuf.addOptionMask(optionMask)
        return self._repairAction(database, reqBuf)

    # 2003.07.12:  Removed method resolveLimboTransactions (dropped plans to
    # support that operation from kinterbasdb since transactions IDs are not
    # exposed at the Python level and I don't consider limbo transaction
    # resolution compelling enough to warrant exposing transaction IDs).

    # User management methods:
    def getUsers(self, username=None):
        """
          By default, lists all users.  Specify parameter $username to list
        only the user with that username.
        """
        if username is not None:
            _checkString(username)
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_display_user)
        if username:
            username = username.upper() # 2002.12.11
            reqBuf.addString(ibase.isc_spb_sec_username, username)
        self._act(reqBuf)
        raw = self._QR(ibase.isc_info_svc_get_users)
        users = []
        curUser = None
        pos = 1 # Ignore raw[0].
        upper_limit = len(raw) - 1
        while pos < upper_limit:
            cluster = ord(raw[pos])
            pos += 1
            if cluster == ibase.isc_spb_sec_username:
                if curUser is not None:
                    users.append(curUser)
                    curUser = None
                (username, pos) = self._extract_string(raw, pos)
                curUser = User(username)
            elif cluster == ibase.isc_spb_sec_password:
                (password, pos) = self._extract_string(raw, pos)
                curUser.password = password
            elif cluster == ibase.isc_spb_sec_firstname:
                (firstName, pos) = self._extract_string(raw, pos)
                curUser.firstName = firstName
            elif cluster == ibase.isc_spb_sec_middlename:
                (middleName, pos) = self._extract_string(raw, pos)
                curUser.middleName = middleName
            elif cluster == ibase.isc_spb_sec_lastname:
                (lastName, pos) = self._extract_string(raw, pos)
                curUser.lastName = lastName
            elif cluster == ibase.isc_spb_sec_groupid:
                (groupId, pos) = self._extract_int(raw, pos)
                curUser.groupId = groupId
            elif cluster == ibase.isc_spb_sec_userid:
                (userId, pos) = self._extract_int(raw, pos)
                curUser.userId = userId
        # Handle the last user:
        if curUser is not None:
            users.append(curUser)
            curUser = None
        return users
    def addUser(self, user):
        """
          Parameter $user must be an instance of services.User with *at least*
        its username and password attributes specified as non-empty values.
        All other $user attributes are optional.
          This method ignores the userId and groupId attributes of $user
        regardless of their values.
        """
        if not user.username:
            raise ProgrammingError('You must specify a username.')
        else:
            _checkString(user.username)

        if not user.password:
            raise ProgrammingError('You must specify a password.')
        else:
            _checkString(user.password)

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_add_user)

        reqBuf.addString(ibase.isc_spb_sec_username, user.username)
        reqBuf.addString(ibase.isc_spb_sec_password, user.password)

        if user.firstName:
            reqBuf.addString(ibase.isc_spb_sec_firstname, user.firstName)
        if user.middleName:
            reqBuf.addString(ibase.isc_spb_sec_middlename, user.middleName)
        if user.lastName:
            reqBuf.addString(ibase.isc_spb_sec_lastname, user.lastName)

        self._actAndReturnTextualResults(reqBuf)
    def modifyUser(self, user):
        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_modify_user)

        reqBuf.addString(ibase.isc_spb_sec_username, user.username)
        reqBuf.addString(ibase.isc_spb_sec_password, user.password)
        # Change the optional attributes whether they're empty or not.
        reqBuf.addString(ibase.isc_spb_sec_firstname, user.firstName)
        reqBuf.addString(ibase.isc_spb_sec_middlename, user.middleName)
        reqBuf.addString(ibase.isc_spb_sec_lastname, user.lastName)

        self._actAndReturnTextualResults(reqBuf)
    def removeUser(self, user):
        """
          Accepts either an instance of services.User or a string username, and
        deletes the specified user.
        """
        if isinstance(user, User):
            username = user.username
        else:
            _checkString(user)
            username = user

        reqBuf = _ServiceActionRequestBuilder(ibase.isc_action_svc_delete_user)
        reqBuf.addString(ibase.isc_spb_sec_username, username)
        self._actAndReturnTextualResults(reqBuf)
    def userExists(self, user):
        """
          Returns a boolean that indicates whether the specified user exists.
        The $user parameter can be an instance of services.User or a string
        username.
        """
        if isinstance(user, User):
            username = user.username
        else:
            _checkString(user)
            username = user
        return len(self.getUsers(username=username)) > 0

class User(object):
    def __init__(self, username=None):
        if username:
            _checkString(username)
            self.username = username.upper()
        else:
            self.username = None
        # The password is not returned by user output methods, but must be
        # specified to add a user.
        self.password = None

        self.firstName = None
        self.middleName = None
        self.lastName = None

        # The user id and group id are not fully supported.  For details, see
        # the documentation of the "User Management Methods" of
        # services.Connection.
        self.userId = None
        self.groupId = None
    def __str__(self):
        return '<kinterbasdb.services.User %s>' % (
            (self.username is None and 'without a name')
            or 'named "%s"' % self.username)

class _ServiceActionRequestBuilder(object):
    # This private class helps public facilities in this module to build
    # the binary action request buffers required by the database Services API
    # using high-level, easily comprehensible syntax.

    def __init__(self, clusterIdentifier=None):
        self._buffer = []
        if clusterIdentifier:
            self.addCode(clusterIdentifier)
    def __str__(self):
        return self.render()
    def extend(self, otherRequestBuilder):
        self._buffer.append(otherRequestBuilder.render())
    def addCode(self, code):
        _code2spb(self._buffer, code)
    def addString(self, code, s):
        _checkString(s)
        _string2spb(self._buffer, code, s)
    def addSequenceOfStrings(self, code, stringSequence):
        for s in stringSequence:
            self.addString(code, s)
    def addSequenceOfStringNumericPairs(self, stringCode, stringSequence,
                                            numericCode, numericSequence):
        stringCount = len(stringSequence)
        numericCount = len(numericSequence)
        if numericCount != stringCount - 1:
            raise ValueError("Numeric sequence must contain exactly one less"
                " element than its companion string sequence."
              )
        i = 0
        while i < stringCount:
            self.addString(stringCode, stringSequence[i])
            if i < numericCount:
                self.addNumeric(numericCode, numericSequence[i])
            i += 1
    def addNumeric(self, code, n, numCType='I'):
        _numeric2spb(self._buffer, code, n, numCType=numCType)
    def addOptionMask(self, optionMask):
        self.addNumeric(ibase.isc_spb_options, optionMask)
    def addDatabaseName(self, databaseName):
        # 2003.07.20: Issue a warning for a hostname-containing databaseName
        # because it will cause isc_service_start to raise an inscrutable error
        # message with Firebird 1.5 (though it would not have raised an error
        # at all with Firebird 1.0 and earlier).
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
                    % databaseName[:colonIndex+1],
                    UserWarning
                  )
        self.addString(ibase.isc_spb_dbname, databaseName)
    def render(self):
        return ''.join(self._buffer)

