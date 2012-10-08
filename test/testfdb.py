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
        con.close()

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
        assert not con.main_transaction.active
        assert not con.main_transaction.closed
        assert con.main_transaction.default_action == 'commit'
        assert len(con.main_transaction._connections) == 1
        assert con.main_transaction._connections[0]() == con
        con.begin()
        assert not con.main_transaction.closed
        con.commit()
        assert not con.main_transaction.active
        con.begin()
        con.rollback()
        assert not con.main_transaction.active
        con.begin()
        con.commit(retaining=True)
        assert con.main_transaction.active
        con.rollback(retaining=True)
        assert con.main_transaction.active
        tr = con.trans()
        assert isinstance(tr,fdb.Transaction)
        assert not con.main_transaction.closed
        assert len(con.transactions) == 2
        tr.begin()
        assert not tr.closed
        con.begin()
        con.close()
        assert not con.main_transaction.active
        assert con.main_transaction.closed
        assert not tr.active
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
        assert len(tr.cursors) == 1
        assert tr.cursors[0] is cur
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
    def test_fetch_after_commit(self):
        self.con.execute_immediate("insert into t (c1) values (1)")
        self.con.commit()
        cur = self.con.cursor()
        cur.execute("select * from t")
        self.con.commit()
        try:
            rows = cur.fetchall()
        except Exception as e:
            assert e.args == ('Cannot fetch from this cursor because it has not executed a statement.',)
        else:
            raise Exception('Exception expected')
    def test_fetch_after_rollback(self):
        self.con.execute_immediate("insert into t (c1) values (1)")
        self.con.rollback()
        cur = self.con.cursor()
        cur.execute("select * from t")
        self.con.commit()
        try:
            rows = cur.fetchall()
        except Exception as e:
            assert e.args == ('Cannot fetch from this cursor because it has not executed a statement.',)
        else:
            raise Exception('Exception expected')
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
        if self.con1 and self.con1.group:
            # We can't drop database via connection in group
            self.con1.group.disband()
        if not self.con1:
            self.con1 = fdb.connect(dsn=self.db1,user='SYSDBA',password='masterkey')
        self.con1.drop_database()
        self.con1.close()
        if not self.con2:
            self.con2 = fdb.connect(dsn=self.db2,user='SYSDBA',password='masterkey')
        self.con2.drop_database()
        self.con2.close()
    def test_simple_dt(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))
        assert self.con1.group == cg
        assert self.con2.group == cg
        
        q = 'select * from T order by pk'
        c1 = cg.cursor(self.con1)
        cc1 = self.con1.cursor()
        p1 = cc1.prep(q)
        
        c2 = cg.cursor(self.con2)
        cc2 = self.con2.cursor()
        p2 = cc2.prep(q)
        
        # Distributed transaction: COMMIT
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
        
        # Distributed transaction: PREPARE+COMMIT
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
        
        # Distributed transaction: SAVEPOINT+ROLLBACK to it
        c1.execute('insert into t (pk) values (3)')
        cg.savepoint('CG_SAVEPOINT')
        c2.execute('insert into t (pk) values (3)')
        cg.rollback(savepoint='CG_SAVEPOINT')

        c1.execute(q)
        result = c1.fetchall()
        #print 'db1:',result
        assert repr(result) == '[(1, None), (2, None), (3, None)]'
        c2.execute(q)
        result = c2.fetchall()
        #print 'db2:',result
        assert repr(result) == '[(1, None), (2, None)]'
        
        # Distributed transaction: ROLLBACK
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
        
        # Distributed transaction: EXECUTE_IMMEDIATE
        cg.execute_immediate('insert into t (pk) values (3)')
        cg.commit()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        #print 'db1:',result
        assert repr(result) == '[(1, None), (2, None), (3, None)]'
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        #print 'db2:',result
        assert repr(result) == '[(1, None), (2, None), (3, None)]'
        
        cg.disband()
        assert self.con1.group == None
        assert self.con2.group == None
    def test_limbo_transactions(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))
        svc = fdb.services.connect(password='masterkey')

        ids1 = svc.get_limbo_transaction_ids(self.db1)
        assert ids1 == []
        ids2 = svc.get_limbo_transaction_ids(self.db2)
        assert ids2 == []
        
        cg.execute_immediate('insert into t (pk) values (3)')
        cg.prepare()
        
        # Force out both connections
        self.con1._set_group(None)
        cg._cons.remove(self.con1)
        del self.con1
        self.con1 = None

        self.con2._set_group(None)
        cg._cons.remove(self.con2)
        del self.con2
        self.con2 = None
        
        # Disband will raise an error
        try:
            cg.disband()
        except Exception as e:
            assert isinstance(e,fdb.DatabaseError)
            assert e.args == ('Error while rolling back transaction:\n- SQLCODE: -901\n- invalid transaction handle (expecting explicit transaction start)', -901, 335544332)
            
        ids1 = svc.get_limbo_transaction_ids(self.db1)
        #print ids1
        assert ids1 == [5]
        id1 = ids1[0]
        ids2 = svc.get_limbo_transaction_ids(self.db2)
        #print ids1
        assert ids2 == [5]
        id2 = ids2[0]
        
        # Data chould be blocked by limbo transaction
        if not self.con1:
            self.con1 = fdb.connect(dsn=self.db1,user='SYSDBA',password='masterkey')
        if not self.con2:
            self.con2 = fdb.connect(dsn=self.db2,user='SYSDBA',password='masterkey')
        c1 = self.con1.cursor()
        c1.execute('select * from t')
        try:
            row = c1.fetchall()
        except Exception as e:
            assert isinstance(e,fdb.DatabaseError)
            assert e.args == ('Cursor.fetchone:\n- SQLCODE: -911\n- record from transaction %i is stuck in limbo' % id1, -911, 335544459)
        else:
            raise Exception('Exception expected')
        c2 = self.con2.cursor()
        c2.execute('select * from t')
        try:
            row = c2.fetchall()
        except Exception as e:
            assert isinstance(e,fdb.DatabaseError)
            assert e.args == ('Cursor.fetchone:\n- SQLCODE: -911\n- record from transaction %i is stuck in limbo' % id2, -911, 335544459)
        else:
            raise Exception('Exception expected')

        # resolve via service
        svc = fdb.services.connect(password='masterkey')
        svc.commit_limbo_transaction(self.db1,id1)
        svc.rollback_limbo_transaction(self.db2,id2)

        # check the resolution
        c1 = self.con1.cursor()
        c1.execute('select * from t')
        row = c1.fetchall()
        assert repr(row) == "[(3, None)]"
        c2 = self.con2.cursor()
        c2.execute('select * from t')
        row = c2.fetchall()
        assert repr(row) == "[]"
        
        svc.close()

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
    def test_exec_after_close(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchone()
        assert len(row) == 2
        assert repr(row) == "('USA', 'Dollar')"
        cur.close()
        cur.execute('select * from country')
        row = cur.fetchone()
        assert len(row) == 2
        assert repr(row) == "('USA', 'Dollar')"
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
    def test_use_after_close(self):
        cmd = 'select * from country'
        cur = self.con.cursor()
        cur.execute(cmd)
        assert list(cur._prepared_statements.keys()) == [cmd]
        assert not cur._prepared_statements[cmd].closed
        cur.close()
        assert list(cur._prepared_statements.keys()) == [cmd]
        assert cur._prepared_statements[cmd].closed
        cur.execute(cmd)
        row = cur.fetchone()
        assert len(row) == 2
        assert repr(row) == "('USA', 'Dollar')"
    def test_pscache(self):
        cmds = ['select * from country',
                'select country from country',
                'select currency from country']
        cur = self.con.cursor()
        assert list(cur._prepared_statements.keys()) == []
        cur.execute(cmds[0])
        assert list(cur._prepared_statements.keys()) == [cmds[0]]
        for cmd in cmds:
            cur.execute(cmd)
            assert cmd in cur._prepared_statements.keys()
        assert len(cur._prepared_statements.keys()) == 3
        
        # Are cached ps closed? But active one should be still open
        for cmd in cmds:
            ps = cur._prepared_statements[cmd]
            assert ps.closed == (cur._ps != ps)
        
        cur.close()
        for cmd in cmds:
            assert cur._prepared_statements[cmd].closed
        
        cur.clear_cache()
        assert cur._prepared_statements == {}

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
    def test_execution(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from country')
        cur.execute(ps)
        row = cur.fetchone()
        assert len(row) == 2
        assert repr(row) == "('USA', 'Dollar')"
    def test_wrong_cursor(self):
        cur = self.con.cursor()
        cur2 = self.con.cursor()
        ps = cur.prep('select * from country')
        try:
            cur2.execute(ps)
        except Exception as e:
            assert e.args == ('PreparedStatement was created by different Cursor.',)
        else:
            raise ProgrammingError('Exception expected')
        

class TestInsertData(unittest.TestCase):
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
        cur.execute('insert into T2 (C1,C12,C13) values (?,?,?)',[6,1,1])
        self.con.commit()
        cur.execute('select C1,C12,C13 from T2 where C1 = 6')
        rows = cur.fetchall()
        assert repr(rows) == "[(6, 1.0, 1.0)]"
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
        x = svc.get_service_manager_version()
        assert x == 2
        x = svc.get_server_version()
        assert 'Firebird' in x
        x = svc.get_architecture()
        assert 'Firebird' in x
        x = svc.get_home_directory()
        #assert x == '/opt/firebird/'
        x = svc.get_security_database_path()
        assert 'security2.fdb' in x
        x = svc.get_lock_file_directory()
        #assert x == '/tmp/firebird/'
        x = svc.get_server_capabilities()
        assert isinstance(x,type(tuple()))
        x = svc.get_message_file_directory()
        #assert x == '/opt/firebird/'
        con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        con2 = fdb.connect(dsn='employee',user='sysdba',password='masterkey')
        x = svc.get_attached_database_names()
        #print repr(x)
        assert len(x) == 2
        assert self.dbfile.upper() in [s.upper() for s in x]
            
        #assert '/opt/firebird/examples/empbuild/employee.fdb' in x
        x = svc.get_connection_count()
        #print 'getConnectionCount',x
        assert x == 2
        svc.close()
    def test_running(self):
        svc = fdb.services.connect(password='masterkey')
        assert not svc.isrunning()
        svc.get_log()
        assert svc.isrunning()
        assert svc.fetching
        # fetch materialized
        log = svc.readlines()
        assert not svc.isrunning()
        svc.close()
    def test_wait(self):
        svc = fdb.services.connect(password='masterkey')
        assert not svc.isrunning()
        svc.get_log()
        assert svc.isrunning()
        assert svc.fetching
        svc.wait()
        assert not svc.isrunning()
        assert not svc.fetching
        svc.close()

class TestServices2(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbtest.fdb')
        self.fbk = os.path.join(self.dbpath,'test_employee.fbk')
        self.fbk2 = os.path.join(self.dbpath,'test_employee.fbk2')
        self.rfdb = os.path.join(self.dbpath,'test_employee.fdb')
        self.svc = fdb.services.connect(password='masterkey')
        self.con = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey')
        if not os.path.exists(self.rfdb):
            c = fdb.create_database("CREATE DATABASE '%s' USER 'SYSDBA' PASSWORD 'masterkey'" % self.rfdb)
            c.close()
    def tearDown(self):
        self.svc.close()
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
        if os.path.exists(self.rfdb):
            os.remove(self.rfdb)
        if os.path.exists(self.fbk):
            os.remove(self.fbk)
        if os.path.exists(self.fbk2):
            os.remove(self.fbk2)
    def test_log(self):
        def fetchline(line):
            output.append(line)
        self.svc.get_log()
        assert self.svc.fetching
        # fetch materialized
        log = self.svc.readlines()
        assert not self.svc.fetching
        assert log
        assert isinstance(log,type(list()))
        # iterate over result
        self.svc.get_log()
        for line in self.svc:
            assert line
            assert isinstance(line,fdb.StringType)
        assert not self.svc.fetching
        # callback
        output = []
        self.svc.get_log(callback=fetchline)
        assert len(output) > 0
    def test_getLimboTransactionIDs(self):
        ids = self.svc.get_limbo_transaction_ids('employee')
        assert isinstance(ids,type(list()))
    def test_getStatistics(self):
        def fetchline(line):
            output.append(line)
        self.svc.get_statistics('employee')
        assert self.svc.fetching
        assert self.svc.isrunning()
        # fetch materialized
        stats = self.svc.readlines()
        assert not self.svc.fetching
        assert not self.svc.isrunning()
        assert stats
        assert isinstance(stats,type(list()))
        # iterate over result
        self.svc.get_statistics('employee',
                                show_system_tables_and_indexes=True,
                                show_record_versions=True)
        for line in self.svc:
            assert line
            assert isinstance(line,fdb.StringType)
        assert not self.svc.fetching
        # callback
        output = []
        self.svc.get_statistics('employee', callback=fetchline)
        assert len(output) > 0
    def test_backup(self):
        def fetchline(line):
            output.append(line)
        self.svc.backup('employee', self.fbk)
        assert self.svc.fetching
        assert self.svc.isrunning()
        # fetch materialized
        report = self.svc.readlines()
        assert not self.svc.fetching
        assert not self.svc.isrunning()
        assert os.path.exists(self.fbk)
        assert report
        assert isinstance(report,type(list()))
        # iterate over result
        self.svc.backup('employee', self.fbk,
                        ignore_checksums=1,
                        ignore_limbo_transactions=1,
                        metadata_only=1,
                        collect_garbage=0,
                        transportable=0,
                        convert_external_tables_to_internal=1,
                        compressed=0,  
                        no_db_triggers=1)
        for line in self.svc:
            assert line
            assert isinstance(line,fdb.StringType)
        assert not self.svc.fetching
        # callback
        output = []
        self.svc.backup('employee', self.fbk, callback=fetchline)
        assert len(output) > 0
    def test_restore(self):
        def fetchline(line):
            output.append(line)
        self.test_backup()
        assert os.path.exists(self.fbk)
        self.svc.restore(self.fbk, self.rfdb, replace=1)
        assert self.svc.fetching
        assert self.svc.isrunning()
        # fetch materialized
        report = self.svc.readlines()
        assert not self.svc.fetching
        assert not self.svc.isrunning()
        assert report
        assert isinstance(report,type(list()))
        # iterate over result
        self.svc.restore(self.fbk, self.rfdb, replace=1)
        for line in self.svc:
            assert line
            assert isinstance(line,fdb.StringType)
        assert not self.svc.fetching
        # callback
        output = []
        self.svc.restore(self.fbk, self.rfdb, replace=1, callback=fetchline)
        assert len(output) > 0
    def test_nbackup(self):
        self.svc.nbackup('employee', self.fbk)
        assert os.path.exists(self.fbk)
    def test_nrestore(self):
        self.test_nbackup()
        assert os.path.exists(self.fbk)
        if os.path.exists(self.rfdb):
            os.remove(self.rfdb)
        self.svc.nrestore(self.fbk, self.rfdb)
        assert os.path.exists(self.rfdb)
    def test_trace(self):
        trace_config = """<database %s>
          enabled true
          log_statement_finish true
          print_plan true
          include_filter %%SELECT%%
          exclude_filter %%RDB$%%
          time_threshold 0
          max_sql_length 2048
        </database>
        """ % self.dbfile
        svc2 = fdb.services.connect(password='masterkey')
        svcx = fdb.services.connect(password='masterkey')
        # Start trace sessions
        trace1_id = self.svc.trace_start(trace_config,'test_trace_1')
        trace2_id = svc2.trace_start(trace_config)
        # check sessions
        sessions = svcx.trace_list()
        assert trace1_id in sessions
        assert repr(list(sessions[trace1_id].keys())) == "['date', 'flags', 'name', 'user']"
        assert trace2_id in sessions
        assert repr(list(sessions[trace2_id].keys())) == "['date', 'flags', 'user']"
        assert repr(sessions[trace1_id]['flags']) == "['active', ' admin', ' trace']"
        assert repr(sessions[trace2_id]['flags']) == "['active', ' admin', ' trace']"
        # Pause session
        svcx.trace_suspend(trace2_id)
        assert 'suspend' in svcx.trace_list()[trace2_id]['flags']
        # Resume session
        svcx.trace_resume(trace2_id)
        assert 'active' in svcx.trace_list()[trace2_id]['flags']
        # Stop session
        svcx.trace_stop(trace2_id)
        assert trace2_id not in svcx.trace_list()
        # Finalize
        svcx.trace_stop(trace1_id)
        svc2.close()
        svcx.close()
    def test_setDefaultPageBuffers(self):
        self.svc.set_default_page_buffers(self.rfdb,100)
    def test_setSweepInterval(self):
        self.svc.set_sweep_interval(self.rfdb,10000)
    def test_shutdown_bringOnline(self):
        # Shutdown database to single-user maintenance mode
        self.svc.shutdown(self.rfdb,
                          fdb.services.SHUT_SINGLE,
                          fdb.services.SHUT_FORCE,0)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'single-user maintenance' in ''.join(self.svc.readlines())
        # Enable multi-user maintenance
        self.svc.bring_online(self.rfdb,fdb.services.SHUT_MULTI)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'multi-user maintenance' in ''.join(self.svc.readlines())
        # Go to full shutdown mode, disabling new attachments during 5 seconds
        self.svc.shutdown(self.rfdb,
                          fdb.services.SHUT_FULL,
                          fdb.services.SHUT_DENY_NEW_ATTACHMENTS,5)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'full shutdown' in ''.join(self.svc.readlines())
        # Enable single-user maintenance
        self.svc.bring_online(self.rfdb,fdb.services.SHUT_SINGLE)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'single-user maintenance' in ''.join(self.svc.readlines())
        # Return to normal state
        self.svc.bring_online(self.rfdb)
    def test_setShouldReservePageSpace(self):
        self.svc.set_reserve_page_space(self.rfdb,False)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'no reserve' in ''.join(self.svc.readlines())
        self.svc.set_reserve_page_space(self.rfdb,True)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'no reserve' not in ''.join(self.svc.readlines())
    def test_setWriteMode(self):
        # Forced writes
        self.svc.set_write_mode(self.rfdb,fdb.services.WRITE_FORCED)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'force write' in ''.join(self.svc.readlines())
        # No Forced writes
        self.svc.set_write_mode(self.rfdb,fdb.services.WRITE_BUFFERED)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'force write' not in ''.join(self.svc.readlines())
    def test_setAccessMode(self):
        # Read Only
        self.svc.set_access_mode(self.rfdb,fdb.services.ACCESS_READ_ONLY)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'read only' in ''.join(self.svc.readlines())
        # Read/Write
        self.svc.set_access_mode(self.rfdb,fdb.services.ACCESS_READ_WRITE)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'read only' not in ''.join(self.svc.readlines())
    def test_setSQLDialect(self):
        self.svc.set_sql_dialect(self.rfdb,1)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'Database dialect\t1' in ''.join(self.svc.readlines())
        self.svc.set_sql_dialect(self.rfdb,3)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        assert 'Database dialect\t3' in ''.join(self.svc.readlines())
    def test_activateShadowFile(self):
        self.svc.activate_shadow(self.rfdb)
    def test_sweep(self):
        self.svc.sweep(self.rfdb)
    def test_repair(self):
        result = self.svc.repair(self.rfdb)
        assert not result
    def test_getUsers(self):
        users = self.svc.get_users()
        assert isinstance(users,type(list()))
        assert isinstance(users[0],fdb.services.User)
        assert users[0].name == 'SYSDBA'
    def test_manage_user(self):
        user = fdb.services.User('FDB_TEST')
        user.password = 'FDB_TEST'
        user.first_name = 'FDB'
        user.middle_name = 'X.'
        user.last_name = 'TEST'
        self.svc.add_user(user)
        assert self.svc.user_exists(user)
        assert self.svc.user_exists('FDB_TEST')
        users = [user for user in self.svc.get_users() if user.name == 'FDB_TEST']
        assert users
        assert len(users) == 1
        #assert users[0].password == 'FDB_TEST'
        assert users[0].first_name == 'FDB'
        assert users[0].middle_name == 'X.'
        assert users[0].last_name == 'TEST'
        user.password = 'XFDB_TEST'
        user.first_name = 'XFDB'
        user.middle_name = 'XX.'
        user.last_name = 'XTEST'
        self.svc.modify_user(user)
        users = [user for user in self.svc.get_users() if user.name == 'FDB_TEST']
        assert users
        assert len(users) == 1
        #assert users[0].password == 'XFDB_TEST'
        assert users[0].first_name == 'XFDB'
        assert users[0].middle_name == 'XX.'
        assert users[0].last_name == 'XTEST'
        self.svc.remove_user(user)
        assert not self.svc.user_exists('FDB_TEST')


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
        timed_event.join()
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
        timed_event.join()
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
        timed_event.join()
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
        timed_event.join()
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
        self.con.execute_immediate("delete from t4")
        self.con.commit()
        self.con.close()
    def test_octets(self):
        bytestring = fdb.fbcore.bs([1,2,3,4,5])
        cur = self.con.cursor()
        cur.execute("insert into T4 (C1, C_OCTETS, V_OCTETS) values (?,?,?)",
                    (1, bytestring,bytestring))
        self.con.commit()
        cur.execute("select C1, C_OCTETS, V_OCTETS from T4 where C1 = 1")
        row = cur.fetchone()
        if ibase.PYTHON_MAJOR_VER == 3:
            assert row == (1, b'\x01\x02\x03\x04\x05', b'\x01\x02\x03\x04\x05')
        else:
            assert row == (1, '\x01\x02\x03\x04\x05', '\x01\x02\x03\x04\x05')
    def test_utf82win1250(self):
        s5 = 'ěščřž'
        s30 = 'ěščřžýáíéúůďťňóĚŠČŘŽÝÁÍÉÚŮĎŤŇÓ'
        if ibase.PYTHON_MAJOR_VER != 3:
            s5 = s5.decode('utf8')
            s30 = s30.decode('utf8')
        
        con1250 = fdb.connect(dsn=self.dbfile,user='sysdba',password='masterkey',
                              charset='win1250')
        c_utf8 = self.con.cursor()
        c_win1250 = con1250.cursor()
        
        # Insert unicode data
        c_utf8.execute("insert into T4 (C1, C_WIN1250, V_WIN1250, C_UTF8, V_UTF8)"
                       "values (?,?,?,?,?)",
                       (1,s5,s30,s5,s30))
        self.con.commit()
        
        # Should return the same unicode content when read from win1250 or utf8 connection
        c_win1250.execute("select C1, C_WIN1250, V_WIN1250,"
                          "C_UTF8, V_UTF8 from T4 where C1 = 1")
        row = c_win1250.fetchone()
        assert row == (1,s5,s30,s5,s30)
        c_utf8.execute("select C1, C_WIN1250, V_WIN1250,"
                       "C_UTF8, V_UTF8 from T4 where C1 = 1")
        row = c_utf8.fetchone()
        assert row == (1,s5,s30,s5,s30)
        
    def testCharVarchar(self):
        s = 'Introdução'
        if ibase.PYTHON_MAJOR_VER != 3:
            s = s.decode('utf8')
        assert len(s) == 10
        data = tuple([1,s,s])
        cur = self.con.cursor()
        cur.execute('insert into T3 (C1,C2,C3) values (?,?,?)',data)
        self.con.commit()
        cur.execute('select C1,C2,C3 from T3 where C1 = 1')
        row = cur.fetchone()
        assert row == data
    def testBlob(self):
        s = """Introdução

Este artigo descreve como você pode fazer o InterBase e o Firebird 1.5 
coehabitarem pacificamente seu computador Windows. Por favor, note que esta 
solução não permitirá que o Interbase e o Firebird rodem ao mesmo tempo. 
Porém você poderá trocar entre ambos com um mínimo de luta. """
        if ibase.PYTHON_MAJOR_VER != 3:
            s = s.decode('utf8')
        assert len(s) == 295
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

