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
#  Contributor(s): Philippe Makowski <pmakowski@ibphoenix.fr>
#                  ______________________________________.
#
# See LICENSE.TXT for details.

import unittest
import datetime, decimal, types
#import kinterbasdb as fdb
import fdb
import fdb.ibase as ibase
import sys, os
import threading
import time

if ibase.PYTHON_MAJOR_VER == 3:
    from io import StringIO
else:
    from StringIO import StringIO

#def printData(cur):
#    """Print data from open cursor to stdout."""
#    # Print a header.
#    for fieldDesc in cur.description:
#        print fieldDesc[fdb.DESCRIPTION_NAME].ljust(fieldDesc[fdb.DESCRIPTION_DISPLAY_SIZE]) ,
#    print
#    for fieldDesc in cur.description:
#        print "-" * max((len(fieldDesc[fdb.DESCRIPTION_NAME]),fieldDesc[fdb.DESCRIPTION_DISPLAY_SIZE])),
#    print
#    # For each row, print the value of each field left-justified within
#    # the maximum possible width of that field.
#    fieldIndices = range(len(cur.description))
#    for row in cur:
#        for fieldIndex in fieldIndices:
#            fieldValue = str(row[fieldIndex])
#            fieldMaxWidth = max((len(cur.description[fieldIndex][fdb.DESCRIPTION_NAME]),cur.description[fieldIndex][fdb.DESCRIPTION_DISPLAY_SIZE]))
#            print fieldValue.ljust(fieldMaxWidth) ,
#        print


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
        #print 'con._dpb:',repr(con._dpb)
        assert con._dpb == ibase.b('\x01\x1c\x06sysdba\x1d\tmasterkey?\x01\x03')
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
        assert not con.main_transaction.closed
        con.rollback(retaining=True)
        assert not con.main_transaction.closed
        tr = con.trans()
        assert isinstance(tr,fdb.Transaction)
        assert not con.main_transaction.closed
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

class TestDistributedTransaction(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.db1 = os.path.join(self.dbpath,'fbtest-1.fdb')
        self.db2 = os.path.join(self.dbpath,'fbtest-2.fdb')
        if not os.path.exists(self.db1):
            self.con1 = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.db1)
        else:
            self.con1 = fdb.connect(dsn=self.db1,user='SYSDBA',password='masterkey')
        self.con1.execute_immediate("recreate table T (PK integer, C1 integer)")
        self.con1.commit()
        if not os.path.exists(self.db2):
            self.con2 = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.db2)
        else:
            self.con2 = fdb.connect(dsn=self.db2,user='SYSDBA',password='masterkey')
        self.con2.execute_immediate("recreate table T (PK integer, C1 integer)")
        self.con2.commit()
    def tearDown(self):
        if self.con1.group:
            # We can't drop database via connection in group
            self.con1.group.disband()
        self.con1.drop_database()
        self.con1.close()
        self.con2.drop_database()
        self.con2.close()
    def test_simple_dt(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))
        assert self.con1.group == cg
        assert self.con2.group == cg
        
        c1 = cg.cursor(self.con1)
        cc1 = self.con1.cursor()
        p1 = cc1.prep('select * from T order by pk')
        
        c2 = cg.cursor(self.con2)
        cc2 = self.con2.cursor()
        p2 = cc2.prep('select * from T order by pk')
        
        c1.execute('insert into t (pk) values (1)')
        c2.execute('insert into t (pk) values (1)')
        cg.commit()
        
        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        #print 'db1:',result
        assert repr(result) == '[(1, None)]'
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        #print 'db2:',result
        assert repr(result) == '[(1, None)]'
        
        c1.execute('insert into t (pk) values (2)')
        c2.execute('insert into t (pk) values (2)')
        cg.prepare()
        cg.commit()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        #print 'db1:',result
        assert repr(result) == '[(1, None), (2, None)]'
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        #print 'db2:',result
        assert repr(result) == '[(1, None), (2, None)]'
        
        c1.execute('insert into t (pk) values (3)')
        c2.execute('insert into t (pk) values (3)')
        cg.rollback()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        #print 'db1:',result
        assert repr(result) == '[(1, None), (2, None)]'
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        #print 'db2:',result
        assert repr(result) == '[(1, None), (2, None)]'
        
        cg.disband()
        assert self.con1.group == None
        assert self.con2.group == None

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
    def test_iteration(self):
        data = [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'), 
                ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'), 
                ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'), 
                ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'), 
                ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = [row for row in cur]
        assert len(rows) == 14
        assert repr(rows) == "[('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'), ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'), ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'), ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'), ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]"
        cur.execute('select * from country')
        rows = []
        for row in cur:
            rows.append(row)
        assert len(rows) == 14
        assert repr(rows) == "[('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'), ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'), ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'), ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'), ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]"
        cur.execute('select * from country')
        i = 0
        for row in cur:
            i += 1
            assert row in data
        assert i == 14
    def test_description(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        assert len(cur.description) == 2
        if ibase.PYTHON_MAJOR_VER==3:
            assert repr(cur.description) == "(('COUNTRY', <class 'str'>, 15, 15, 0, 0, False), ('CURRENCY', <class 'str'>, 10, 10, 0, 0, False))"
        else:
            assert repr(cur.description) == "(('COUNTRY', <type 'str'>, 15, 15, 0, 0, False), ('CURRENCY', <type 'str'>, 10, 10, 0, 0, False))"
        cur.execute('select country as CT, currency as CUR from country')
        assert len(cur.description) == 2
        cur.execute('select * from customer')
        if ibase.PYTHON_MAJOR_VER==3:
            assert repr(cur.description) == "(('CUST_NO', <class 'int'>, 11, 4, 0, 0, False), ('CUSTOMER', <class 'str'>, 25, 25, 0, 0, False), ('CONTACT_FIRST', <class 'str'>, 15, 15, 0, 0, True), ('CONTACT_LAST', <class 'str'>, 20, 20, 0, 0, True), ('PHONE_NO', <class 'str'>, 20, 20, 0, 0, True), ('ADDRESS_LINE1', <class 'str'>, 30, 30, 0, 0, True), ('ADDRESS_LINE2', <class 'str'>, 30, 30, 0, 0, True), ('CITY', <class 'str'>, 25, 25, 0, 0, True), ('STATE_PROVINCE', <class 'str'>, 15, 15, 0, 0, True), ('COUNTRY', <class 'str'>, 15, 15, 0, 0, True), ('POSTAL_CODE', <class 'str'>, 12, 12, 0, 0, True), ('ON_HOLD', <class 'str'>, 1, 1, 0, 0, True))"
        else:
            assert repr(cur.description) == "(('CUST_NO', <type 'int'>, 11, 4, 0, 0, False), ('CUSTOMER', <type 'str'>, 25, 25, 0, 0, False), ('CONTACT_FIRST', <type 'str'>, 15, 15, 0, 0, True), ('CONTACT_LAST', <type 'str'>, 20, 20, 0, 0, True), ('PHONE_NO', <type 'str'>, 20, 20, 0, 0, True), ('ADDRESS_LINE1', <type 'str'>, 30, 30, 0, 0, True), ('ADDRESS_LINE2', <type 'str'>, 30, 30, 0, 0, True), ('CITY', <type 'str'>, 25, 25, 0, 0, True), ('STATE_PROVINCE', <type 'str'>, 15, 15, 0, 0, True), ('COUNTRY', <type 'str'>, 15, 15, 0, 0, True), ('POSTAL_CODE', <type 'str'>, 12, 12, 0, 0, True), ('ON_HOLD', <type 'str'>, 1, 1, 0, 0, True))"
        cur.execute('select * from job')
        if ibase.PYTHON_MAJOR_VER==3:
            assert repr(cur.description) == "(('JOB_CODE', <class 'str'>, 5, 5, 0, 0, False), ('JOB_GRADE', <class 'int'>, 6, 2, 0, 0, False), ('JOB_COUNTRY', <class 'str'>, 15, 15, 0, 0, False), ('JOB_TITLE', <class 'str'>, 25, 25, 0, 0, False), ('MIN_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), ('MAX_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), ('JOB_REQUIREMENT', <class 'str'>, 0, 8, 0, 1, True), ('LANGUAGE_REQ', <class 'list'>, -1, 8, 0, 0, True))"
        else:
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
    def test_fetchonemap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchonemap()
        assert len(row) == 2
        assert repr(row.items()) == "[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')]"
    def test_fetchallmap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchallmap()
        assert len(rows) == 14
        assert repr([row.items() for row in rows]) == "[[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')], [('COUNTRY', 'England'), ('CURRENCY', 'Pound')], [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')], [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')], [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')], [('COUNTRY', 'Italy'), ('CURRENCY', 'Lira')], [('COUNTRY', 'France'), ('CURRENCY', 'FFranc')], [('COUNTRY', 'Germany'), ('CURRENCY', 'D-Mark')], [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')], [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')], [('COUNTRY', 'Netherlands'), ('CURRENCY', 'Guilder')], [('COUNTRY', 'Belgium'), ('CURRENCY', 'BFranc')], [('COUNTRY', 'Austria'), ('CURRENCY', 'Schilling')], [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')]]"
    def test_fetchmanymap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchmanymap(10)
        assert len(rows) == 10
        assert repr([row.items() for row in rows]) == "[[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')], [('COUNTRY', 'England'), ('CURRENCY', 'Pound')], [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')], [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')], [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')], [('COUNTRY', 'Italy'), ('CURRENCY', 'Lira')], [('COUNTRY', 'France'), ('CURRENCY', 'FFranc')], [('COUNTRY', 'Germany'), ('CURRENCY', 'D-Mark')], [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')], [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')]]"
        rows = cur.fetchmanymap(10)
        assert len(rows) == 4
        assert repr([row.items() for row in rows]) == "[[('COUNTRY', 'Netherlands'), ('CURRENCY', 'Guilder')], [('COUNTRY', 'Belgium'), ('CURRENCY', 'BFranc')], [('COUNTRY', 'Austria'), ('CURRENCY', 'Schilling')], [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')]]"
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

class TestCursorInsert(unittest.TestCase):
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
        # Too long values
        try:
            cur.execute('insert into T2 (C1,C4) values (?,?)',[3,'123456'])
            self.con.commit()
        except Exception as e:
            assert e.args == ('Value of parameter (1) is too long, expected 5, found 6',)
        else:
            raise ProgrammingError('Exception expected')
        try:
            cur.execute('insert into T2 (C1,C5) values (?,?)',[3,'12345678901'])
            self.con.commit()
        except Exception as e:
            assert e.args == ('Value of parameter (1) is too long, expected 10, found 11',)
        else:
            raise ProgrammingError('Exception expected')
    def test_insert_datetime(self):
        cur = self.con.cursor()
        now = datetime.datetime(2011,11,13,15,00,1,200)
        cur.execute('insert into T2 (C1,C6,C7,C8) values (?,?,?,?)',[3,now.date(),now.time(),now])
        self.con.commit()
        cur.execute('select C1,C6,C7,C8 from T2 where C1 = 3')
        rows = cur.fetchall()
        assert repr(rows) == "[(3, datetime.date(2011, 11, 13), datetime.time(15, 0, 1, 200), datetime.datetime(2011, 11, 13, 15, 0, 1, 200))]"

        cur.execute('insert into T2 (C1,C6,C7,C8) values (?,?,?,?)',[4,'2011-11-13','15:0:1:200','2011-11-13 15:0:1:200'])
        self.con.commit()
        cur.execute('select C1,C6,C7,C8 from T2 where C1 = 4')
        rows = cur.fetchall()
        assert repr(rows) == "[(4, datetime.date(2011, 11, 13), datetime.time(15, 0, 1, 200000), datetime.datetime(2011, 11, 13, 15, 0, 1, 200000))]"
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
    def test_insert_returning(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?) returning C1',[6,1.1,1.1])
        result = cur.fetchall()
        assert repr(result) == '[(6,)]'

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
        assert 'Firebird' in x
        x = svc.getArchitecture()
        assert 'Firebird' in x
        x = svc.getHomeDir()
        #assert x == '/opt/firebird/'
        x = svc.getSecurityDatabasePath()
        assert 'security2.fdb' in x
        x = svc.getLockFileDir()
        #assert x == '/tmp/firebird/'
        x = svc.getCapabilityMask()
        #assert x == 774  # value is server dependent
        x = svc.getMessageFileDir()
        #assert x == '/opt/firebird/'
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        con2 = fdb.connect(dsn='employee',user='sysdba',password='masterkey')
        x = svc.getAttachedDatabaseNames()
        assert len(x) == 2
        assert self.dbfile in x
        #assert '/opt/firebird/examples/empbuild/employee.fdb' in x
        x = svc.getConnectionCount()
#        print 'getConnectionCount',x
        assert x == 2
        svc.close()

class TestServices2(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.fbk = os.path.join(self.dbpath,'test_employee.fbk')
        self.rfdb = os.path.join(self.dbpath,'test_employee.fdb')
        self.svc = fdb.services.connect(password='masterkey')
        c = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.rfdb)
        c.close()
    def tearDown(self):
        self.svc.close()
        if os.path.exists(self.rfdb):
            os.remove(self.rfdb)
        if os.path.exists(self.fbk):
            os.remove(self.fbk)
    def test_log(self):
        log = self.svc.getLog()
        assert log
        assert isinstance(log,str)
    def test_getLimboTransactionIDs(self):
        ids = self.svc.getLimboTransactionIDs('employee')
        assert isinstance(ids,list)
    def test_getStatistics(self):
        stat = self.svc.getStatistics('employee')
        assert stat
        assert isinstance(stat,str)
    def test_backup(self):
        log = self.svc.backup('employee',self.fbk)
        assert log
        assert isinstance(log,str)
    def test_restore(self):
        self.test_backup()
        log = self.svc.restore(self.fbk,self.rfdb,replace=1)
        assert log
        assert isinstance(log,str)
    def test_setDefaultPageBuffers(self):
        result = self.svc.setDefaultPageBuffers(self.rfdb,100)
        assert not result
    def test_setSweepInterval(self):
        result = self.svc.setSweepInterval(self.rfdb,10000)
        assert not result
    def test_shutdown_bringOnline(self):
        result = self.svc.shutdown(self.rfdb,fdb.services.SHUT_FORCE,0)
        assert not result
        result = self.svc.bringOnline(self.rfdb)
        assert not result
    def test_setShouldReservePageSpace(self):
        result = self.svc.setShouldReservePageSpace(self.rfdb,False)
        assert not result
    def test_setWriteMode(self):
        result = self.svc.setWriteMode(self.rfdb,fdb.services.WRITE_BUFFERED)
        assert not result
    def test_setAccessMode(self):
        result = self.svc.setAccessMode(self.rfdb,fdb.services.ACCESS_READ_ONLY)
        assert not result
        result = self.svc.setAccessMode(self.rfdb,fdb.services.ACCESS_READ_WRITE)
        assert not result
    def test_setSQLDialect(self):
        result = self.svc.setSQLDialect(self.rfdb,1)
        assert not result
        result = self.svc.setSQLDialect(self.rfdb,3)
        assert not result
    def test_activateShadowFile(self):
        result = self.svc.activateShadowFile(self.rfdb)
        assert not result
    def test_sweep(self):
        result = self.svc.sweep(self.rfdb)
        assert not result
    def test_repair(self):
        result = self.svc.repair(self.rfdb)
        assert not result
    def test_getUsers(self):
        users = self.svc.getUsers()
        assert isinstance(users,list)
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


class TestEvents(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbevents.fdb')
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.con = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.dbfile)
        c = self.con.cursor()
        c.execute("CREATE TABLE T (PK Integer, C1 Integer)")
        c.execute("""CREATE TRIGGER EVENTS_AU FOR T ACTIVE
BEFORE UPDATE POSITION 0
AS 
BEGIN 
    if (old.C1 <> new.C1) then
        post_event 'c1_updated' ;
END""")
        c.execute("""CREATE TRIGGER EVENTS_AI FOR T ACTIVE
AFTER INSERT POSITION 0
AS 
BEGIN 
    if (new.c1 = 1) then
        post_event 'insert_1' ;
    else if (new.c1 = 2) then
        post_event 'insert_2' ;
    else if (new.c1 = 3) then
        post_event 'insert_3' ;
    else
        post_event 'insert_other' ;
END""")
        self.con.commit()
    def tearDown(self):
        self.con.drop_database()
        self.con.close()
    def test_one_event(self):
        def send_events(command_list):
            c = self.con.cursor()
            for cmd in command_list:
                c.execute(cmd)
            self.con.commit()
        
        timed_event = threading.Timer(3.0,send_events,args=[["insert into T (PK,C1) values (1,1)",]])
        events = self.con.event_conduit(['insert_1'])
        timed_event.start()
        e = events.wait()
        events.close()
        assert repr(e) == "{'insert_1': 1}"
    def test_multiple_events(self):
        def send_events(command_list):
            c = self.con.cursor()
            for cmd in command_list:
                c.execute(cmd)
            self.con.commit()
        cmds = ["insert into T (PK,C1) values (1,1)",
                "insert into T (PK,C1) values (1,2)",
                "insert into T (PK,C1) values (1,3)",
                "insert into T (PK,C1) values (1,1)",
                "insert into T (PK,C1) values (1,2)",]
        timed_event = threading.Timer(3.0,send_events,args=[cmds])
        events = self.con.event_conduit(['insert_1','insert_3'])
        timed_event.start()
        e = events.wait()
        events.close()
        assert repr(e) == "{'insert_3': 1, 'insert_1': 2}"
    def test_20_events(self):
        def send_events(command_list):
            c = self.con.cursor()
            for cmd in command_list:
                c.execute(cmd)
            self.con.commit()
        cmds = ["insert into T (PK,C1) values (1,1)",
                "insert into T (PK,C1) values (1,2)",
                "insert into T (PK,C1) values (1,3)",
                "insert into T (PK,C1) values (1,1)",
                "insert into T (PK,C1) values (1,2)",]
        self.e = {}
        timed_event = threading.Timer(1.0,send_events,args=[cmds])
        events = self.con.event_conduit(['insert_1','A','B','C','D',
                                         'E','F','G','H','I','J','K','L','M',
                                         'N','O','P','Q','R','insert_3'])
        timed_event.start()
        time.sleep(3)
        e = events.wait()
        events.close()
        assert repr(e) == "{'A': 0, 'C': 0, 'B': 0, 'E': 0, 'D': 0, 'G': 0, 'insert_1': 2, 'I': 0, 'H': 0, 'K': 0, 'J': 0, 'M': 0, 'L': 0, 'O': 0, 'N': 0, 'Q': 0, 'P': 0, 'R': 0, 'insert_3': 1, 'F': 0}"
    def test_flush_events(self):
        def send_events(command_list):
            c = self.con.cursor()
            for cmd in command_list:
                c.execute(cmd)
            self.con.commit()
        
        timed_event = threading.Timer(3.0,send_events,args=[["insert into T (PK,C1) values (1,1)",]])
        events = self.con.event_conduit(['insert_1'])
        send_events(["insert into T (PK,C1) values (1,1)",
                     "insert into T (PK,C1) values (1,1)"])
        time.sleep(2)
        events.flush()
        timed_event.start()
        e = events.wait()
        events.close()
        assert repr(e) == "{'insert_1': 1}"

class TestStreamBLOBs(unittest.TestCase):
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
    def testBlobBasic(self):
        blob = """Firebird supports two types of blobs, stream and segmented.
The database stores segmented blobs in chunks.
Each chunk starts with a two byte length indicator followed by however many bytes of data were passed as a segment.
Stream blobs are stored as a continuous array of data bytes with no length indicators included."""
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C9) values (?,?)',[4,StringIO(blob)])
        self.con.commit()
        p = cur.prep('select C1,C9 from T2 where C1 = 4')
        p.set_stream_blob('C9')
        cur.execute(p)
        row = cur.fetchone()
        blob_reader = row[1]
        try:
            assert blob_reader.read(20) == 'Firebird supports tw'
            assert blob_reader.read(20) == 'o types of blobs, st'
            assert blob_reader.read(400) == 'ream and segmented.\nThe database stores segmented blobs in chunks.\nEach chunk starts with a two byte length indicator followed by however many bytes of data were passed as a segment.\nStream blobs are stored as a continuous array of data bytes with no length indicators included.'
            assert blob_reader.read(20) == ''
            assert blob_reader.tell() == 318
            blob_reader.seek(20)
            assert blob_reader.tell() == 20
            assert blob_reader.read(20) == 'o types of blobs, st'
            blob_reader.seek(0)
            assert blob_reader.tell() == 0
            assert blob_reader.readlines() == StringIO(blob).readlines()
            blob_reader.seek(0)
            for line in blob_reader:
                assert line.rstrip('\n') in blob.split('\n')
            #blob_reader.seek(50)
            #print blob_reader.tell()
            #print repr(blob_reader.readline())
            blob_reader.seek(0)
            assert blob_reader.read() == blob
            blob_reader.seek(-9,os.SEEK_END)
            assert blob_reader.read() == 'included.'
            blob_reader.seek(-20,os.SEEK_END)
            blob_reader.seek(11,os.SEEK_CUR)
            assert blob_reader.read() == 'included.'
            blob_reader.seek(60)
            assert blob_reader.readline() == 'The database stores segmented blobs in chunks.\n'
        finally:
            # Necessary to avoid bad BLOB handle on BlobReader.close in tearDown
            # because BLOB handle is no longer valid after table purge
            p.close()
    def testBlobExtended(self):
        blob = """Firebird supports two types of blobs, stream and segmented.
The database stores segmented blobs in chunks.
Each chunk starts with a two byte length indicator followed by however many bytes of data were passed as a segment.
Stream blobs are stored as a continuous array of data bytes with no length indicators included."""
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C9) values (?,?)',[1,StringIO(blob)])
        cur.execute('insert into T2 (C1,C9) values (?,?)',[2,StringIO(blob)])
        self.con.commit()
        p = cur.prep('select C1,C9 from T2')
        p.set_stream_blob('C9')
        cur.execute(p)
        #rows = [row for row in cur]
        try:
            for row in cur:
                blob_reader = row[1]
                assert blob_reader.read(20) == 'Firebird supports tw'
                assert blob_reader.read(20) == 'o types of blobs, st'
                assert blob_reader.read(400) == 'ream and segmented.\nThe database stores segmented blobs in chunks.\nEach chunk starts with a two byte length indicator followed by however many bytes of data were passed as a segment.\nStream blobs are stored as a continuous array of data bytes with no length indicators included.'
                assert blob_reader.read(20) == ''
                assert blob_reader.tell() == 318
                blob_reader.seek(20)
                assert blob_reader.tell() == 20
                assert blob_reader.read(20) == 'o types of blobs, st'
                blob_reader.seek(0)
                assert blob_reader.tell() == 0
                assert blob_reader.readlines() == StringIO(blob).readlines()
                blob_reader.seek(0)
                for line in blob_reader:
                    assert line.rstrip('\n') in blob.split('\n')
                #blob_reader.seek(50)
                #print blob_reader.tell()
                #print repr(blob_reader.readline())
                blob_reader.seek(0)
                assert blob_reader.read() == blob
                blob_reader.seek(-9,os.SEEK_END)
                assert blob_reader.read() == 'included.'
                blob_reader.seek(-20,os.SEEK_END)
                blob_reader.seek(11,os.SEEK_CUR)
                assert blob_reader.read() == 'included.'
                blob_reader.seek(60)
                assert blob_reader.readline() == 'The database stores segmented blobs in chunks.\n'
        finally:
            # Necessary to avoid bad BLOB handle on BlobReader.close in tearDown
            # because BLOB handle is no longer valid after table purge
            p.close()

class TestCharsetConversion(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey',charset='utf8')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
        #self.con.execute_immediate("RECREATE TABLE T2 (C1 Smallint,C2 Integer,C3 Bigint,C4 Char(5),C5 Varchar(10),C6 Date,C7 Time,C8 Timestamp,C9 Blob sub_type 1,C10 Numeric(18,2),C11 Decimal(18,2),C12 Float,C13 Double precision,C14 Numeric(8,4),C15 Decimal(8,4))")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t3")
        self.con.commit()
        self.con.close()
    def testCharVarchar(self):
        if ibase.PYTHON_MAJOR_VER == 3:
            s = 'Introdução'
        else:
            s = u'Introdução'
        assert len(s) == 10
        data = tuple([1,s,s])
        cur = self.con.cursor()
        cur.execute('insert into T3 (C1,C2,C3) values (?,?,?)',data)
        self.con.commit()
        cur.execute('select C1,C2,C3 from T3 where C1 = 1')
        row = cur.fetchone()
        assert row == data
    def testBlob(self):
        if ibase.PYTHON_MAJOR_VER == 3:
            s = """Introdução

Este artigo descreve como você pode fazer o InterBase e o Firebird 1.5 
coehabitarem pacificamente seu computador Windows. Por favor, note que esta 
solução não permitirá que o Interbase e o Firebird rodem ao mesmo tempo. 
Porém você poderá trocar entre ambos com um mínimo de luta. """
        else:
            s = u"""Introdução

Este artigo descreve como você pode fazer o InterBase e o Firebird 1.5 
coehabitarem pacificamente seu computador Windows. Por favor, note que esta 
solução não permitirá que o Interbase e o Firebird rodem ao mesmo tempo. 
Porém você poderá trocar entre ambos com um mínimo de luta. """
        data = tuple([2,s])
        b_data = tuple([3,ibase.b('bytestring')])
        cur = self.con.cursor()
        # Text BLOB
        cur.execute('insert into T3 (C1,C4) values (?,?)',data)
        self.con.commit()
        cur.execute('select C1,C4 from T3 where C1 = 2')
        row = cur.fetchone()
        assert row == data
        # Insert Unicode into non-textual BLOB
        try:
            cur.execute('insert into T3 (C1,C5) values (?,?)',data)
            self.con.commit()
        except Exception as e:
            assert e.args == ('Unicode strings are not acceptable input for a non-textual BLOB column.',)
        else:
            raise ProgrammingError('Exception expected')
        # Read binary from non-textual BLOB
        cur.execute('insert into T3 (C1,C5) values (?,?)',b_data)
        self.con.commit()
        cur.execute('select C1,C5 from T3 where C1 = 3')
        row = cur.fetchone()
        assert row == b_data

class TestBugs(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbbugs.fdb')
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.con = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.dbfile)
    def tearDown(self):
        self.con.drop_database()
        self.con.close()
    def test_pyfb_17(self):
        create_table = """
        Create Table table1  (
            ID Integer,
            sort Integer NOT Null
        );
        """
        
        create_trigger = """CREATE TRIGGER BIU_Trigger FOR table1
        ACTIVE BEFORE INSERT OR UPDATE POSITION 0
        as
        begin
          if (new.sort IS NULL) then
          begin
            new.sort = 1;
          end
        end
        """
        
        cur = self.con.cursor()
        cur.execute(create_table)
        cur.execute(create_trigger)
        self.con.commit()
        # PYFB-17: fails with fdb, passes with kinterbasdb
        cur.execute('insert into table1 (ID, sort) values(1, ?)', (None, ))

    
if __name__ == '__main__':
    unittest.main()

