#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           testfdb.py
#   DESCRIPTION:    Python driver for Firebird
#   CREATED:        12.10.2011
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

import unittest
import datetime, decimal, types
#import kinterbasdb as fdb
import fdb
import fdb.ibase as ibase
import sys, os

class TestCreateDrop(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'droptest.fdb')
    def test_create_drop(self):
        con = fdb.create_database("create database '"+self.dbfile+"' user 'sysdba' password 'masterkey'")
        con.drop_database()

class TestConnection(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
    def tearDown(self):
        pass
    def test_connect(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        assert con._db_handle != None
        assert con._dpb == '\x01\x1c\x06sysdba\x1d\tmasterkey?\x01\x03'
        con.close()
    def test_connect_role(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey',role='role')
        assert con._db_handle != None
        #assert con._dpb == '\x01\x1c\x06sysdba\x1d\tmasterkey?\x01\x03'
        con.close()
    def test_transaction(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        assert con.main_transaction != None
        assert con.main_transaction.closed
        assert con.main_transaction.default_action == 'commit'
        assert len(con.main_transaction._connections) == 1
        assert con.main_transaction._connections[0]() == con
        con.begin()
        assert not con.main_transaction.closed
        con.commit()
        assert con.main_transaction.closed
        con.begin()
        con.rollback()
        assert con.main_transaction.closed
        con.begin()
        con.commit(retaining=True)
        assert con.main_transaction.closed
        con.begin()
        con.rollback(retaining=True)
        assert con.main_transaction.closed
        tr = con.trans()
        assert isinstance(tr,fdb.Transaction)
        assert con.main_transaction.closed
        assert len(con.transactions) == 2
        tr.begin()
        assert not tr.closed
        con.begin()
        con.close()
        assert con.main_transaction.closed
        assert tr.closed
    def test_execute_immediate(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        con.execute_immediate("recreate table t (c1 integer)")
        con.commit()
        con.execute_immediate("delete from t")
        con.commit()
        con.close()
    def test_database_info(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        assert con.database_info(fdb.isc_info_db_read_only,'i') == 0
        assert con.database_info(fdb.isc_info_page_size,'i') == 4096
        assert con.database_info(fdb.isc_info_db_sql_dialect,'i') == 3
        con.close()
    def test_db_info(self):
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        res = con.db_info([fdb.isc_info_page_size, fdb.isc_info_db_read_only,
                           fdb.isc_info_db_sql_dialect,fdb.isc_info_user_names])
        assert repr(res) == "{53: {'SYSDBA': 1}, 62: 3, 14: 4096, 63: 0}"
        con.close()

class TestTransaction(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
    def test_cursor(self):
        tr = self.con.main_transaction
        tr.begin()
        cur = tr.cursor()
        cur.execute("insert into t (c1) values (1)")
        tr.commit()
        cur.execute("select * from t")
        rows = cur.fetchall()
        assert repr(rows) == "[(1,)]"
        cur.execute("delete from t")
        tr.commit()
    def test_savepoint(self):
        self.con.begin()
        tr = self.con.main_transaction
        self.con.execute_immediate("insert into t (c1) values (1)")
        tr.savepoint('test')
        self.con.execute_immediate("insert into t (c1) values (2)")
        tr.rollback(savepoint='test')
        tr.commit()
        cur = tr.cursor()
        cur.execute("select * from t")
        rows = cur.fetchall()
        assert repr(rows) == "[(1,)]"
    def test_tpb(self):
        tpb = fdb.TPB()
        tpb.access_mode = fdb.isc_tpb_write
        tpb.isolation_level = fdb.isc_tpb_read_committed
        tpb.isolation_level = (fdb.isc_tpb_read_committed,fdb.isc_tpb_rec_version)
        tpb.lock_resolution = fdb.isc_tpb_wait
        tpb.lock_timeout = 10
        tpb.table_reservation['COUNTRY'] = (fdb.isc_tpb_protected,fdb.isc_tpb_lock_write)
        tr = self.con.trans(tpb)
        tr.begin()
        tr.commit()
    def test_transaction_info(self):
        self.con.begin()
        tr = self.con.main_transaction
        info = tr.transaction_info(ibase.isc_info_tra_isolation,'s')
        assert info == '\x08\x02\x00\x03\x01'
        tr.commit()

class TestCursor(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
    def test_description(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        assert len(cur.description) == 2
        assert repr(cur.description) == "(('COUNTRY', <type 'str'>, 15, 15, 0, 0, False), ('CURRENCY', <type 'str'>, 10, 10, 0, 0, False))"
        cur.execute('select country as CT, currency as CUR from country')
        assert len(cur.description) == 2
        cur.execute('select * from customer')
        assert repr(cur.description) == "(('CUST_NO', <type 'int'>, 11, 4, 0, 0, False), ('CUSTOMER', <type 'str'>, 25, 25, 0, 0, False), ('CONTACT_FIRST', <type 'str'>, 15, 15, 0, 0, True), ('CONTACT_LAST', <type 'str'>, 20, 20, 0, 0, True), ('PHONE_NO', <type 'str'>, 20, 20, 0, 0, True), ('ADDRESS_LINE1', <type 'str'>, 30, 30, 0, 0, True), ('ADDRESS_LINE2', <type 'str'>, 30, 30, 0, 0, True), ('CITY', <type 'str'>, 25, 25, 0, 0, True), ('STATE_PROVINCE', <type 'str'>, 15, 15, 0, 0, True), ('COUNTRY', <type 'str'>, 15, 15, 0, 0, True), ('POSTAL_CODE', <type 'str'>, 12, 12, 0, 0, True), ('ON_HOLD', <type 'str'>, 1, 1, 0, 0, True))"
        cur.execute('select * from job')
        assert repr(cur.description) == "(('JOB_CODE', <type 'str'>, 5, 5, 0, 0, False), ('JOB_GRADE', <type 'int'>, 6, 2, 0, 0, False), ('JOB_COUNTRY', <type 'str'>, 15, 15, 0, 0, False), ('JOB_TITLE', <type 'str'>, 25, 25, 0, 0, False), ('MIN_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), ('MAX_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), ('JOB_REQUIREMENT', <type 'str'>, 0, 8, 0, 1, True), ('LANGUAGE_REQ', <type 'list'>, -1, 8, 0, 0, True))"
    def test_fetchone(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchone()
        assert len(row) == 2
        assert repr(row) == "('USA', 'Dollar')"
    def test_fetchall(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchall()
        assert len(rows) == 14
        assert repr(rows) == "[('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'), ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'), ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'), ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'), ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]"
    def test_fetchmany(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchmany(10)
        assert len(rows) == 10
        assert repr(rows) == "[('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'), ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'), ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'), ('Hong Kong', 'HKDollar')]"
        rows = cur.fetchmany(10)
        assert len(rows) == 4
        assert repr(rows) == "[('Netherlands', 'Guilder'), ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]"
        rows = cur.fetchmany(10)
        assert len(rows) == 0
    def test_rowcount(self):
        cur = self.con.cursor()
        assert cur.rowcount == -1
        cur.execute('select * from project')
        assert cur.rowcount == 0
        cur.fetchone()
        assert cur.rowcount == 6
    def test_name(self):
        def assign_name():
            cur.name = 'test'
        cur = self.con.cursor()
        assert cur.name == None
        self.assertRaises(fdb.ProgrammingError,assign_name)
        cur.execute('select * from country')
        cur.name = 'test'
        assert cur.name == 'test'
        self.assertRaises(fdb.ProgrammingError,assign_name)

class TestPreparedStatement(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
    def test_basic(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from country')
        assert ps.in_sqlda.sqln == 10
        assert ps.in_sqlda.sqld == 0
        assert ps.out_sqlda.sqln == 10
        assert ps.out_sqlda.sqld == 2
        assert ps.statement_type == 1
        assert ps.sql == 'select * from country'
    def test_get_plan(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from country')
        assert ps.plan == "PLAN (COUNTRY NATURAL)"
        
class TestCursor2(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
        #self.con.execute_immediate("RECREATE TABLE T2 (C1 Smallint,C2 Integer,C3 Bigint,C4 Char(5),C5 Varchar(10),C6 Date,C7 Time,C8 Timestamp,C9 Blob sub_type 1,C10 Numeric(18,2),C11 Decimal(18,2),C12 Float,C13 Double precision,C14 Numeric(8,4),C15 Decimal(8,4))")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.execute_immediate("delete from t2")
        self.con.commit()
        self.con.close()
    def test_insert_integers(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C2,C3) values (?,?,?)',[1,1,1])
        self.con.commit()
        cur.execute('select C1,C2,C3 from T2 where C1 = 1')
        rows = cur.fetchall()
        assert repr(rows) == "[(1, 1, 1)]"
    def test_insert_char_varchar(self):    
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C4,C5) values (?,?,?)',[2,'AA','AA'])
        self.con.commit()
        cur.execute('select C1,C4,C5 from T2 where C1 = 2')
        rows = cur.fetchall()
        assert repr(rows) == "[(2, 'AA   ', 'AA')]"
    def test_insert_datetime(self):
        cur = self.con.cursor()
        now = datetime.datetime(2011,11,13,15,00,1,200)
        cur.execute('insert into T2 (C1,C6,C7,C8) values (?,?,?,?)',[3,now.date(),now.time(),now])
        self.con.commit()
        cur.execute('select C1,C6,C7,C8 from T2 where C1 = 3')
        rows = cur.fetchall()
        assert repr(rows) == "[(3, datetime.date(2011, 11, 13), datetime.time(15, 0, 1, 200), datetime.datetime(2011, 11, 13, 15, 0, 1, 200))]"
    def test_insert_blob(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C9) values (?,?)',[4,'This is a BLOB!'])
        self.con.commit()
        cur.execute('select C1,C9 from T2 where C1 = 4')
        rows = cur.fetchall()
        assert repr(rows) == "[(4, 'This is a BLOB!')]"
    def test_insert_float_double(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C12,C13) values (?,?,?)',[5,1.0,1.0])
        self.con.commit()
        cur.execute('select C1,C12,C13 from T2 where C1 = 5')
        rows = cur.fetchall()
        assert repr(rows) == "[(5, 1.0, 1.0)]"
    def test_insert_numeric_decimal(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?)',[6,1.1,1.1])
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?)',[6,decimal.Decimal('100.11'),decimal.Decimal('100.11')])
        self.con.commit()
        cur.execute('select C1,C10,C11 from T2 where C1 = 6')
        rows = cur.fetchall()
        assert repr(rows) == "[(6, Decimal('1.1'), Decimal('1.1')), (6, Decimal('100.11'), Decimal('100.11'))]"

class TestStoredProc(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
    def tearDown(self):
        self.con.close()
    def test_callproc(self):
        cur = self.con.cursor()
        result = cur.callproc('sub_tot_budget',['100'])
        assert result == ['100']
        row = cur.fetchone()
        assert repr(row) == "(Decimal('3800000'), Decimal('760000'), Decimal('500000'), Decimal('1500000'))"
        result = cur.callproc('sub_tot_budget',[100])
        assert result == [100]
        row = cur.fetchone()
        assert repr(row) == "(Decimal('3800000'), Decimal('760000'), Decimal('500000'), Decimal('1500000'))"

class TestServices(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
    def test_attach(self):
        svc = fdb.services.connect(password='masterkey')
        svc.close()
    def test_query(self):
        svc = fdb.services.connect(password='masterkey')
        x = svc.getServiceManagerVersion()
        assert x == 2
        x = svc.getServerVersion()
        assert 'Firebird' in 'LI-V2.5.0.26074 Firebird 2.5'
        x = svc.getArchitecture()
        assert 'Firebird' in 'Firebird/linux AMD64'
        x = svc.getHomeDir()
        #assert x == '/opt/firebird/'
        x = svc.getSecurityDatabasePath()
        assert 'security2.fdb' in '/opt/firebird/security2.fdb'
        x = svc.getLockFileDir()
        #assert x == '/tmp/firebird/'
        x = svc.getCapabilityMask()
        assert x == 774
        x = svc.getMessageFileDir()
        #assert x == '/opt/firebird/'
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        con2 = fdb.connect(dsn='employee',user='sysdba',password='masterkey')
        x = svc.getConnectionCount()
        print 'getConnectionCount',x
        assert x == 3
        x = svc.getAttachedDatabaseNames()
        assert len(x) == 2
        assert self.dbfile in x
        #assert '/opt/firebird/examples/empbuild/employee.fdb' in x
        svc.close()

class TestServices2(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.svc = fdb.services.connect(password='masterkey')
    def tearDown(self):
        self.svc.close()
    def test_log(self):
        log = self.svc.getLog()
        assert log
        assert isinstance(log,types.StringType)
    def test_getLimboTransactionIDs(self):
        ids = self.svc.getLimboTransactionIDs('employee')
        assert isinstance(ids,types.ListType)
    def test_getStatistics(self):
        stat = self.svc.getStatistics('employee')
        assert stat
        assert isinstance(stat,types.StringType)
    def test_backup(self):
        log = self.svc.backup('employee','test_employee.fbk')
        assert log
        assert isinstance(log,types.StringType)
    def test_restore(self):
        log = self.svc.restore('test_employee.fbk','test_employee.fdb',replace=1)
        assert log
        assert isinstance(log,types.StringType)
    def test_setDefaultPageBuffers(self):
        result = self.svc.setDefaultPageBuffers('test_employee.fdb',100)
        assert not result
    def test_setSweepInterval(self):
        result = self.svc.setSweepInterval('test_employee.fdb',10000)
        assert not result
    def test_shutdown_bringOnline(self):
        result = self.svc.shutdown('test_employee.fdb',fdb.services.SHUT_FORCE,0)
        assert not result
        result = self.svc.bringOnline('test_employee.fdb')
        assert not result
    def test_setShouldReservePageSpace(self):
        result = self.svc.setShouldReservePageSpace('test_employee.fdb',False)
        assert not result
    def test_setWriteMode(self):
        result = self.svc.setWriteMode('test_employee.fdb',fdb.services.WRITE_BUFFERED)
        assert not result
    def test_setAccessMode(self):
        result = self.svc.setAccessMode('test_employee.fdb',fdb.services.ACCESS_READ_ONLY)
        assert not result
        result = self.svc.setAccessMode('test_employee.fdb',fdb.services.ACCESS_READ_WRITE)
        assert not result
    def test_setSQLDialect(self):
        result = self.svc.setSQLDialect('test_employee.fdb',1)
        assert not result
        #result = self.svc.setSQLDialect('test_employee.fdb',3)
        #assert not result
    def test_activateShadowFile(self):
        result = self.svc.activateShadowFile('test_employee.fdb')
        assert not result
    def test_sweep(self):
        result = self.svc.sweep('test_employee.fdb')
        assert not result
    def test_repair(self):
        result = self.svc.repair('test_employee.fdb')
        assert not result
    def test_getUsers(self):
        users = self.svc.getUsers()
        assert isinstance(users,types.ListType)
        assert isinstance(users[0],fdb.services.User)
        assert users[0].username == 'SYSDBA'
    def test_manage_user(self):
        user = fdb.services.User('FDB_TEST')
        user.password = 'FDB_TEST'
        user.firstName = 'FDB'
        user.middleName = 'X.'
        user.lastName = 'TEST'
        result = self.svc.addUser(user)
        assert not result
        exists = self.svc.userExists(user)
        assert exists
        exists = self.svc.userExists('FDB_TEST')
        assert exists
        users = [user for user in self.svc.getUsers() if user.username == 'FDB_TEST']
        assert users
        assert len(users) == 1
        #assert users[0].password == 'FDB_TEST'
        assert users[0].firstName == 'FDB'
        assert users[0].middleName == 'X.'
        assert users[0].lastName == 'TEST'
        user.password = 'XFDB_TEST'
        user.firstName = 'XFDB'
        user.middleName = 'XX.'
        user.lastName = 'XTEST'
        result = self.svc.modifyUser(user)
        users = [user for user in self.svc.getUsers() if user.username == 'FDB_TEST']
        assert users
        assert len(users) == 1
        #assert users[0].password == 'XFDB_TEST'
        assert users[0].firstName == 'XFDB'
        assert users[0].middleName == 'XX.'
        assert users[0].lastName == 'XTEST'
        result = self.svc.removeUser(user)
        assert not result
        
        
if __name__ == '__main__':
    unittest.main()
    
#unittest.main()
#import datetime as dt
#con = fdb.connect(dsn='employee',user='sysdba',password='masterkey')
#con.execute_immediate("recreate table t (c1 integer)")
#c = con.cursor()
#ts = dt.datetime.now()
#c.execute('insert into T2 (C1,C9) values (?,?)',[5,"This is a BLOB!"])
#c.execute('select * from project')
#print c.rowcount
#print c.fetchone()
#print c.rowcount
#c.execute('insert into t (c1) values(1)')
#con.commit()
#con = fdb.create_database("create database 'test.fdb' user 'sysdba' password 'masterkey'")
#con.execute_immediate("recreate table t (c1 integer)")
#con.commit()
