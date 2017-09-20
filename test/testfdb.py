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
import fdb.schema as sm
import sys, os
import threading
import time
import cStringIO
import struct
from decimal import Decimal
from contextlib import closing

if ibase.PYTHON_MAJOR_VER == 3:
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO
    BytesIO = StringIO

FB20 = '2.0'
FB21 = '2.1'
FB25 = '2.5'
FB30 = '3.0'

# Default server host
#FBTEST_HOST = ''
FBTEST_HOST = 'localhost'
# Default user
FBTEST_USER = 'SYSDBA'
# Default user password
FBTEST_PASSWORD = 'masterkey'

class SchemaVisitor(fdb.schema.SchemaVisitor):
    def __init__(self,test,action,follow='dependencies'):
        self.test = test
        self.seen = []
        self.action = action
        self.follow = follow
    def default_action(self,obj):
        if not obj.issystemobject() and self.action in obj.actions:
            if self.follow == 'dependencies':
                for dependency in obj.get_dependencies():
                    d = dependency.depended_on
                    if d and d not in self.seen:
                        d.accept_visitor(self)
            elif self.follow == 'dependents':
                for dependency in obj.get_dependents():
                    d = dependency.dependent
                    if d and d not in self.seen:
                        d.accept_visitor(self)
            if obj not in self.seen:
                self.test.printout(obj.get_sql_for(self.action))
                self.seen.append(obj)
    def visitSchema(self,schema):
        pass
    def visitMetadataItem(self,item):
        pass
    def visitTableColumn(self,column):
        column.table.accept_visitor(self)
    def visitViewColumn(self,column):
        column.view.accept_visitor(self)
    def visitDependency(self,dependency):
        pass
    def visitConstraint(self,constraint):
        pass
    def visitProcedureParameter(self,param):
        param.procedure.accept_visitor(self)
    def visitFunctionArgument(self,arg):
        arg.function.accept_visitor(self)
    def visitDatabaseFile(self,dbfile):
        pass
    def visitShadow(self,shadow):
        pass

class FDBTestBase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(FDBTestBase,self).__init__(methodName)
        self.output = StringIO()
    def setUp(self):
        with closing(fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)) as svc:
            self.version = svc.version
        if self.version.startswith('2.0'):
            self.FBTEST_DB = 'fbtest20.fdb'
            self.version = FB20
        elif self.version.startswith('2.1'):
            self.FBTEST_DB = 'fbtest21.fdb'
            self.version = FB21
        elif self.version.startswith('2.5'):
            self.FBTEST_DB = 'fbtest25.fdb'
            self.version = FB25
        elif self.version.startswith('3.0'):
            self.FBTEST_DB = 'fbtest30.fdb'
            self.version = FB30
        else:
            raise Exception("Unsupported Firebird version (%s)" % self.version)
    def clear_output(self):
        self.output.close()
        self.output = StringIO()
    def show_output(self):
        sys.stdout.write(self.output.getvalue())
        sys.stdout.flush()
    def printout(self,text='',newline=True):
        self.output.write(text)
        if newline:
            self.output.write('\n')
        self.output.flush()
    def printData(self,cur):
        """Print data from open cursor to stdout."""
        # Print a header.
        for fieldDesc in cur.description:
            self.printout(fieldDesc[fdb.DESCRIPTION_NAME].ljust(fieldDesc[fdb.DESCRIPTION_DISPLAY_SIZE]),newline=False)
        self.printout()
        for fieldDesc in cur.description:
            self.printout("-" * max((len(fieldDesc[fdb.DESCRIPTION_NAME]),fieldDesc[fdb.DESCRIPTION_DISPLAY_SIZE])),newline=False)
        self.printout()
        # For each row, print the value of each field left-justified within
        # the maximum possible width of that field.
        fieldIndices = range(len(cur.description))
        for row in cur:
            for fieldIndex in fieldIndices:
                fieldValue = str(row[fieldIndex])
                fieldMaxWidth = max((len(cur.description[fieldIndex][fdb.DESCRIPTION_NAME]),cur.description[fieldIndex][fdb.DESCRIPTION_DISPLAY_SIZE]))
                self.printout(fieldValue.ljust(fieldMaxWidth),newline=False)
            self.printout()


class TestCreateDrop(FDBTestBase):
    def setUp(self):
        super(TestCreateDrop,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'droptest.fdb')
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
    def test_create_drop(self):
        with closing(fdb.create_database(host=FBTEST_HOST,database=self.dbfile,
                                         user=FBTEST_USER,password=FBTEST_PASSWORD)) as con:
            self.assertEqual(con.sql_dialect,3)
            self.assertEqual(con.charset,None)
            con.drop_database()
        #
        with closing(fdb.create_database(host=FBTEST_HOST,port=3050,database=self.dbfile,
                                         user=FBTEST_USER,password=FBTEST_PASSWORD)) as con:
            self.assertEqual(con.sql_dialect,3)
            self.assertEqual(con.charset,None)
            con.drop_database()
        #
        with closing(fdb.create_database(host=FBTEST_HOST,database=self.dbfile,
                                         user=FBTEST_USER,password=FBTEST_PASSWORD,
                                         sql_dialect=1,charset='UTF8')) as con:
            self.assertEqual(con.sql_dialect,1)
            self.assertEqual(con.charset,'UTF8')
            con.drop_database()

class TestConnection(FDBTestBase):
    def setUp(self):
        super(TestConnection,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
    def tearDown(self):
        pass
    def test_connect(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertIsNotNone(con._db_handle)
            dpb = [1,0x1c,len(FBTEST_USER)]
            dpb.extend(ord(x) for x in FBTEST_USER)
            dpb.extend((0x1d,len(FBTEST_PASSWORD)))
            dpb.extend(ord(x) for x in FBTEST_PASSWORD)
            dpb.extend((ord('?'),1,3))
            self.assertEqual(con._dpb,fdb.bs(dpb))
        with fdb.connect(database=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertIsNotNone(con._db_handle)
        with fdb.connect(port=3050,database=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertIsNotNone(con._db_handle)
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD,
                         no_gc=1,no_db_triggers=1) as con:
            dpb.extend([ibase.isc_dpb_no_garbage_collect,1,1])
            dpb.extend([ibase.isc_dpb_no_db_triggers,1,1])
            self.assertEqual(con._dpb,fdb.bs(dpb))
    def test_properties(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertIn('Firebird',con.server_version)
            self.assertIn('Firebird',con.firebird_version)
            self.assertIsInstance(con.version,str)
            self.assertGreaterEqual(con.engine_version,2.0)
            self.assertGreaterEqual(con.ods,11.0)
            self.assertIsNone(con.group)
            self.assertIsNone(con.charset)
            self.assertEqual(len(con.transactions),2)
            self.assertIn(con.main_transaction,con.transactions)
            self.assertIn(con.query_transaction,con.transactions)
            self.assertEqual(con.default_tpb,fdb.ISOLATION_LEVEL_READ_COMMITED)
            self.assertIsInstance(con.schema,sm.Schema)
            self.assertFalse(con.closed)
    def test_connect_role(self):
        rolename = 'role'
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,
                         password=FBTEST_PASSWORD,role=rolename) as con:
            self.assertIsNotNone(con._db_handle)
            dpb = [1,0x1c,len(FBTEST_USER)]
            dpb.extend(ord(x) for x in FBTEST_USER)
            dpb.extend((0x1d,len(FBTEST_PASSWORD)))
            dpb.extend(ord(x) for x in FBTEST_PASSWORD)
            dpb.extend((ord('<'),len(rolename)))
            dpb.extend(ord(x) for x in rolename)
            dpb.extend((ord('?'),1,3))
            self.assertEqual(con._dpb,fdb.bs(dpb))
    def test_transaction(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertIsNotNone(con.main_transaction)
            self.assertFalse(con.main_transaction.active)
            self.assertFalse(con.main_transaction.closed)
            self.assertEqual(con.main_transaction.default_action,'commit')
            self.assertEqual(len(con.main_transaction._connections),1)
            self.assertEqual(con.main_transaction._connections[0](),con)
            con.begin()
            self.assertFalse(con.main_transaction.closed)
            con.commit()
            self.assertFalse(con.main_transaction.active)
            con.begin()
            con.rollback()
            self.assertFalse(con.main_transaction.active)
            con.begin()
            con.commit(retaining=True)
            self.assertTrue(con.main_transaction.active)
            con.rollback(retaining=True)
            self.assertTrue(con.main_transaction.active)
            tr = con.trans()
            self.assertIsInstance(tr,fdb.Transaction)
            self.assertFalse(con.main_transaction.closed)
            self.assertEqual(len(con.transactions),3)
            tr.begin()
            self.assertFalse(tr.closed)
            con.begin()
            con.close()
            self.assertFalse(con.main_transaction.active)
            self.assertTrue(con.main_transaction.closed)
            self.assertFalse(tr.active)
            self.assertTrue(tr.closed)
    def test_execute_immediate(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            con.execute_immediate("recreate table t (c1 integer)")
            con.commit()
            con.execute_immediate("delete from t")
            con.commit()
    def test_database_info(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            #x = con.database_info(fdb.fb_info_page_contents,'s',0)
            self.assertEqual(con.database_info(fdb.isc_info_db_read_only,'i'),0)
            if con.ods < fdb.ODS_FB_30:
                self.assertEqual(con.database_info(fdb.isc_info_page_size,'i'),4096)
            else:
                self.assertEqual(con.database_info(fdb.isc_info_page_size,'i'),8192)
            self.assertEqual(con.database_info(fdb.isc_info_db_sql_dialect,'i'),3)
    def test_db_info(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            with con.trans() as t1, con.trans() as t2:
                self.assertListEqual(con.db_info(fdb.isc_info_active_transactions),[])
                t1.begin()
                t2.begin()
                self.assertListEqual(con.db_info(fdb.isc_info_active_transactions),
                                     [t1.transaction_id,t2.transaction_id])
            #
            self.assertEqual(len(con.get_page_contents(0)),con.page_size)
            res = con.db_info([fdb.isc_info_page_size, fdb.isc_info_db_read_only,
                               fdb.isc_info_db_sql_dialect,fdb.isc_info_user_names])
            if con.ods < fdb.ODS_FB_30:
                self.assertDictEqual(res,{53: {'SYSDBA': 1}, 62: 3, 14: 4096, 63: 0})
            else:
                self.assertDictEqual(res,{53: {'SYSDBA': 1}, 62: 3, 14: 8192, 63: 0})
            res = con.db_info(fdb.isc_info_read_seq_count)
            if con.ods < fdb.ODS_FB_30:
                self.assertDictEqual(res,{0: 98, 1: 1})
            else:
                self.assertDictEqual(res,{0: 106, 1: 2})
    def test_info_attributes(self):
        with fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD) as con:
            self.assertGreater(con.attachment_id,0)
            self.assertEqual(con.sql_dialect,3)
            self.assertEqual(con.database_sql_dialect,3)
            self.assertEqual(con.database_name,self.dbfile)
            self.assertIsInstance(con.site_name,str)
            self.assertIn(con.implementation_id,fdb.IMPLEMENTATION_NAMES.keys())
            self.assertIn(con.provider_id,fdb.PROVIDER_NAMES.keys())
            self.assertIn(con.db_class_id,fdb.DB_CLASS_NAMES.keys())
            self.assertIsInstance(con.creation_date,datetime.datetime)
            self.assertIn(con.page_size,[4096,8192,16384])
            self.assertEqual(con.sweep_interval,20000)
            self.assertTrue(con.space_reservation)
            self.assertTrue(con.forced_writes)
            self.assertGreater(con.current_memory,0)
            self.assertGreater(con.max_memory,0)
            self.assertGreater(con.oit,0)
            self.assertGreater(con.oat,0)
            self.assertGreater(con.ost,0)
            self.assertGreater(con.next_transaction,0)
            self.assertFalse(con.isreadonly())
            #
            io = con.io_stats
            self.assertEqual(len(io),4)
            self.assertIsInstance(io,dict)
            s = con.get_table_access_stats()
            self.assertEqual(len(s),6)
            self.assertIsInstance(s[0],fdb.fbcore._TableAccessStats)
            #
            with con.trans() as t1, con.trans() as t2:
                self.assertListEqual(con.get_active_transaction_ids(),[])
                t1.begin()
                t2.begin()
                self.assertListEqual(con.get_active_transaction_ids(),
                                     [t1.transaction_id,t2.transaction_id])
                self.assertEqual(con.get_active_transaction_count(), 2)
class TestTransaction(FDBTestBase):
    def setUp(self):
        super(TestTransaction,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
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
        self.assertListEqual(rows,[(1,)])
        cur.execute("delete from t")
        tr.commit()
        self.assertEqual(len(tr.cursors),1)
        self.assertIs(tr.cursors[0],cur)
    def test_context_manager(self):
        with fdb.TransactionContext(self.con) as tr:
            cur = tr.cursor()
            cur.execute("insert into t (c1) values (1)")

        cur.execute("select * from t")
        rows = cur.fetchall()
        self.assertListEqual(rows,[(1,)])

        try:
            with fdb.TransactionContext(self.con) as tr:
                cur.execute("delete from t")
                raise Exception()
        except Exception as e:
            pass

        cur.execute("select * from t")
        rows = cur.fetchall()
        self.assertListEqual(rows,[(1,)])

        with fdb.TransactionContext(self.con) as tr:
            cur.execute("delete from t")

        cur.execute("select * from t")
        rows = cur.fetchall()
        self.assertListEqual(rows,[])
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
        self.assertListEqual(rows,[(1,)])
    def test_fetch_after_commit(self):
        self.con.execute_immediate("insert into t (c1) values (1)")
        self.con.commit()
        cur = self.con.cursor()
        cur.execute("select * from t")
        self.con.commit()
        with self.assertRaises(fdb.DatabaseError) as cm:
            rows = cur.fetchall()
        self.assertTupleEqual(cm.exception.args,('Cannot fetch from this cursor because it has not executed a statement.',))
    def test_fetch_after_rollback(self):
        self.con.execute_immediate("insert into t (c1) values (1)")
        self.con.rollback()
        cur = self.con.cursor()
        cur.execute("select * from t")
        self.con.commit()
        with self.assertRaises(fdb.DatabaseError) as cm:
            rows = cur.fetchall()
        self.assertTupleEqual(cm.exception.args,('Cannot fetch from this cursor because it has not executed a statement.',))
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
        self.assertEqual(info,'\x08\x02\x00\x03\x01')
        #
        self.assertGreater(tr.transaction_id,0)
        self.assertGreater(tr.oit,0)
        self.assertGreater(tr.oat,0)
        self.assertGreater(tr.ost,0)
        self.assertEqual(tr.lock_timeout,-1)
        self.assertTupleEqual(tr.isolation,(3,1))
        tr.commit()

class TestDistributedTransaction(FDBTestBase):
    def setUp(self):
        super(TestDistributedTransaction,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.db1 = os.path.join(self.dbpath,'fbtest-1.fdb')
        self.db2 = os.path.join(self.dbpath,'fbtest-2.fdb')
        if not os.path.exists(self.db1):
            self.con1 = fdb.create_database(host=FBTEST_HOST,database=self.db1,
                                            user=FBTEST_USER,
                                            password=FBTEST_PASSWORD)
        else:
            self.con1 = fdb.connect(host=FBTEST_HOST,database=self.db1,
                                    user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con1.execute_immediate("recreate table T (PK integer, C1 integer)")
        self.con1.commit()
        if not os.path.exists(self.db2):
            self.con2 = fdb.create_database(host=FBTEST_HOST,database=self.db2,
                                            user=FBTEST_USER,
                                            password=FBTEST_PASSWORD)
        else:
            self.con2 = fdb.connect(host=FBTEST_HOST,database=self.db2,
                                    user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con2.execute_immediate("recreate table T (PK integer, C1 integer)")
        self.con2.commit()
    def tearDown(self):
        if self.con1 and self.con1.group:
            # We can't drop database via connection in group
            self.con1.group.disband()
        if not self.con1:
            self.con1 = fdb.connect(host=FBTEST_HOST,database=self.db1,
                                    user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con1.drop_database()
        self.con1.close()
        if not self.con2:
            self.con2 = fdb.connect(host=FBTEST_HOST,database=self.db2,
                                    user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con2.drop_database()
        self.con2.close()
    def test_context_manager(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))

        q = 'select * from T order by pk'
        c1 = cg.cursor(self.con1)
        cc1 = self.con1.cursor()
        p1 = cc1.prep(q)

        c2 = cg.cursor(self.con2)
        cc2 = self.con2.cursor()
        p2 = cc2.prep(q)

        # Distributed transaction: COMMIT
        with fdb.TransactionContext(cg):
            c1.execute('insert into t (pk) values (1)')
            c2.execute('insert into t (pk) values (1)')

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        self.assertListEqual(result,[(1, None)])
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        self.assertListEqual(result,[(1, None)])

        # Distributed transaction: ROLLBACK
        try:
            with fdb.TransactionContext(cg):
                c1.execute('insert into t (pk) values (2)')
                c2.execute('insert into t (pk) values (2)')
                raise Exception()
        except Exception as e:
            pass

        c1.execute(q)
        result = c1.fetchall()
        self.assertListEqual(result,[(1, None)])
        c2.execute(q)
        result = c2.fetchall()
        self.assertListEqual(result,[(1, None)])

        cg.disband()

    def test_simple_dt(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))
        self.assertEqual(self.con1.group,cg)
        self.assertEqual(self.con2.group,cg)

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
        self.assertListEqual(result,[(1, None)])
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        self.assertListEqual(result,[(1, None)])

        # Distributed transaction: PREPARE+COMMIT
        c1.execute('insert into t (pk) values (2)')
        c2.execute('insert into t (pk) values (2)')
        cg.prepare()
        cg.commit()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        self.assertListEqual(result,[(1, None), (2, None)])
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        self.assertListEqual(result,[(1, None), (2, None)])

        # Distributed transaction: SAVEPOINT+ROLLBACK to it
        c1.execute('insert into t (pk) values (3)')
        cg.savepoint('CG_SAVEPOINT')
        c2.execute('insert into t (pk) values (3)')
        cg.rollback(savepoint='CG_SAVEPOINT')

        c1.execute(q)
        result = c1.fetchall()
        self.assertListEqual(result,[(1, None), (2, None), (3, None)])
        c2.execute(q)
        result = c2.fetchall()
        self.assertListEqual(result,[(1, None), (2, None)])

        # Distributed transaction: ROLLBACK
        cg.rollback()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        self.assertListEqual(result,[(1, None), (2, None)])
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        self.assertListEqual(result,[(1, None), (2, None)])

        # Distributed transaction: EXECUTE_IMMEDIATE
        cg.execute_immediate('insert into t (pk) values (3)')
        cg.commit()

        self.con1.commit()
        cc1.execute(p1)
        result = cc1.fetchall()
        self.assertListEqual(result,[(1, None), (2, None), (3, None)])
        self.con2.commit()
        cc2.execute(p2)
        result = cc2.fetchall()
        self.assertListEqual(result,[(1, None), (2, None), (3, None)])

        cg.disband()
        self.assertIsNone(self.con1.group)
        self.assertIsNone(self.con2.group)
    def test_limbo_transactions(self):
        cg = fdb.ConnectionGroup((self.con1,self.con2))
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)

        ids1 = svc.get_limbo_transaction_ids(self.db1)
        self.assertEqual(ids1,[])
        ids2 = svc.get_limbo_transaction_ids(self.db2)
        self.assertEqual(ids2,[])

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
        with self.assertRaises(fdb.DatabaseError) as cm:
            cg.disband()
        self.assertTupleEqual(cm.exception.args,
            ('Error while rolling back transaction:\n- SQLCODE: -901\n- invalid transaction handle (expecting explicit transaction start)', -901, 335544332))

        ids1 = svc.get_limbo_transaction_ids(self.db1)
        id1 = ids1[0]
        ids2 = svc.get_limbo_transaction_ids(self.db2)
        id2 = ids2[0]

        # Data chould be blocked by limbo transaction
        if not self.con1:
            self.con1 = fdb.connect(dsn=self.db1,user=FBTEST_USER,
                                    password=FBTEST_PASSWORD)
        if not self.con2:
            self.con2 = fdb.connect(dsn=self.db2,user=FBTEST_USER,
                                    password=FBTEST_PASSWORD)
        c1 = self.con1.cursor()
        c1.execute('select * from t')
        with self.assertRaises(fdb.DatabaseError) as cm:
            row = c1.fetchall()
        self.assertTupleEqual(cm.exception.args,
            ('Cursor.fetchone:\n- SQLCODE: -911\n- record from transaction %i is stuck in limbo' % id1, -911, 335544459))
        c2 = self.con2.cursor()
        c2.execute('select * from t')
        with self.assertRaises(fdb.DatabaseError) as cm:
            row = c2.fetchall()
        self.assertTupleEqual(cm.exception.args,
                ('Cursor.fetchone:\n- SQLCODE: -911\n- record from transaction %i is stuck in limbo' % id2, -911, 335544459))

        # resolve via service
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        svc.commit_limbo_transaction(self.db1,id1)
        svc.rollback_limbo_transaction(self.db2,id2)

        # check the resolution
        c1 = self.con1.cursor()
        c1.execute('select * from t')
        row = c1.fetchall()
        self.assertListEqual(row,[(3, None)])
        c2 = self.con2.cursor()
        c2.execute('select * from t')
        row = c2.fetchall()
        self.assertListEqual(row,[])

        svc.close()

class TestCursor(FDBTestBase):
    def setUp(self):
        super(TestCursor,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con.execute_immediate("recreate table t (c1 integer primary key)")
        self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
    def test_executemany(self):
        cur = self.con.cursor()
        cur.executemany("insert into t values(?)",[(1,),(2,)])
        cur.executemany("insert into t values(?)",[(3,)])
        cur.executemany("insert into t values(?)",[(4,),(5,),(6,)])
        self.con.commit()
        p = cur.prep("insert into t values(?)")
        cur.executemany(p,[(7,),(8,)])
        cur.executemany(p,[(9,)])
        cur.executemany(p,[(10,),(11,),(12,)])
        self.con.commit()
        cur.execute("select * from T order by c1")
        rows = cur.fetchall()
        self.assertListEqual(rows,[(1,),(2,),(3,),(4,),
                                   (5,),(6,),(7,),(8,),
                                   (9,),(10,),(11,),(12,)])
    def test_iteration(self):
        if self.con.ods < fdb.ODS_FB_30:
            data = [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                    ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'),
                    ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'),
                    ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'),
                    ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')]
        else:
            data = [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                    ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Euro'),
                    ('France', 'Euro'), ('Germany', 'Euro'), ('Australia', 'ADollar'),
                    ('Hong Kong', 'HKDollar'), ('Netherlands', 'Euro'), ('Belgium', 'Euro'),
                    ('Austria', 'Euro'), ('Fiji', 'FDollar'), ('Russia', 'Ruble'),
                    ('Romania', 'RLeu')]
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = [row for row in cur]
        self.assertEqual(len(rows),len(data))
        self.assertListEqual(rows,data)
        cur.execute('select * from country')
        rows = []
        for row in cur:
            rows.append(row)
        self.assertEqual(len(rows),len(data))
        self.assertListEqual(rows,data)
        cur.execute('select * from country')
        i = 0
        for row in cur:
            i += 1
            self.assertIn(row,data)
        self.assertEqual(i,len(data))
    def test_description(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        self.assertEqual(len(cur.description),2)
        if ibase.PYTHON_MAJOR_VER==3:
            self.assertEqual(repr(cur.description),
                "(('COUNTRY', <class 'str'>, 15, 15, 0, 0, False), " \
                "('CURRENCY', <class 'str'>, 10, 10, 0, 0, False))")
        else:
            self.assertEqual(repr(cur.description),
                "(('COUNTRY', <type 'str'>, 15, 15, 0, 0, False), " \
                "('CURRENCY', <type 'str'>, 10, 10, 0, 0, False))")
        cur.execute('select country as CT, currency as CUR from country')
        self.assertEqual(len(cur.description),2)
        cur.execute('select * from customer')
        if ibase.PYTHON_MAJOR_VER==3:
            self.assertEqual(repr(cur.description),
                "(('CUST_NO', <class 'int'>, 11, 4, 0, 0, False), " \
                "('CUSTOMER', <class 'str'>, 25, 25, 0, 0, False), " \
                "('CONTACT_FIRST', <class 'str'>, 15, 15, 0, 0, True), " \
                "('CONTACT_LAST', <class 'str'>, 20, 20, 0, 0, True), " \
                "('PHONE_NO', <class 'str'>, 20, 20, 0, 0, True), " \
                "('ADDRESS_LINE1', <class 'str'>, 30, 30, 0, 0, True), " \
                "('ADDRESS_LINE2', <class 'str'>, 30, 30, 0, 0, True), " \
                "('CITY', <class 'str'>, 25, 25, 0, 0, True), " \
                "('STATE_PROVINCE', <class 'str'>, 15, 15, 0, 0, True), " \
                "('COUNTRY', <class 'str'>, 15, 15, 0, 0, True), " \
                "('POSTAL_CODE', <class 'str'>, 12, 12, 0, 0, True), " \
                "('ON_HOLD', <class 'str'>, 1, 1, 0, 0, True))")
        else:
            self.assertEqual(repr(cur.description),
                "(('CUST_NO', <type 'int'>, 11, 4, 0, 0, False), " \
                "('CUSTOMER', <type 'str'>, 25, 25, 0, 0, False), " \
                "('CONTACT_FIRST', <type 'str'>, 15, 15, 0, 0, True), " \
                "('CONTACT_LAST', <type 'str'>, 20, 20, 0, 0, True), " \
                "('PHONE_NO', <type 'str'>, 20, 20, 0, 0, True), " \
                "('ADDRESS_LINE1', <type 'str'>, 30, 30, 0, 0, True), " \
                "('ADDRESS_LINE2', <type 'str'>, 30, 30, 0, 0, True), " \
                "('CITY', <type 'str'>, 25, 25, 0, 0, True), " \
                "('STATE_PROVINCE', <type 'str'>, 15, 15, 0, 0, True), " \
                "('COUNTRY', <type 'str'>, 15, 15, 0, 0, True), " \
                "('POSTAL_CODE', <type 'str'>, 12, 12, 0, 0, True), " \
                "('ON_HOLD', <type 'str'>, 1, 1, 0, 0, True))")
        cur.execute('select * from job')
        if ibase.PYTHON_MAJOR_VER==3:
            self.assertEqual(repr(cur.description),
                "(('JOB_CODE', <class 'str'>, 5, 5, 0, 0, False), " \
                "('JOB_GRADE', <class 'int'>, 6, 2, 0, 0, False), " \
                "('JOB_COUNTRY', <class 'str'>, 15, 15, 0, 0, False), " \
                "('JOB_TITLE', <class 'str'>, 25, 25, 0, 0, False), " \
                "('MIN_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), " \
                "('MAX_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), " \
                "('JOB_REQUIREMENT', <class 'str'>, 0, 8, 0, 1, True), " \
                "('LANGUAGE_REQ', <class 'list'>, -1, 8, 0, 0, True))")
        else:
            self.assertEqual(repr(cur.description),
                "(('JOB_CODE', <type 'str'>, 5, 5, 0, 0, False), " \
                "('JOB_GRADE', <type 'int'>, 6, 2, 0, 0, False), " \
                "('JOB_COUNTRY', <type 'str'>, 15, 15, 0, 0, False), " \
                "('JOB_TITLE', <type 'str'>, 25, 25, 0, 0, False), " \
                "('MIN_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), " \
                "('MAX_SALARY', <class 'decimal.Decimal'>, 20, 8, 10, -2, False), " \
                "('JOB_REQUIREMENT', <type 'str'>, 0, 8, 0, 1, True), " \
                "('LANGUAGE_REQ', <type 'list'>, -1, 8, 0, 0, True))")
        cur.execute('select * from proj_dept_budget')
        if ibase.PYTHON_MAJOR_VER==3:
            self.assertEqual(repr(cur.description),
                "(('FISCAL_YEAR', <class 'int'>, 11, 4, 0, 0, False), " \
                "('PROJ_ID', <class 'str'>, 5, 5, 0, 0, False), " \
                "('DEPT_NO', <class 'str'>, 3, 3, 0, 0, False), " \
                "('QUART_HEAD_CNT', <class 'list'>, -1, 8, 0, 0, True), " \
                "('PROJECTED_BUDGET', <class 'decimal.Decimal'>, 20, 8, 12, -2, True))")
        else:
            self.assertEqual(repr(cur.description),
                "(('FISCAL_YEAR', <type 'int'>, 11, 4, 0, 0, False), " \
                "('PROJ_ID', <type 'str'>, 5, 5, 0, 0, False), " \
                "('DEPT_NO', <type 'str'>, 3, 3, 0, 0, False), " \
                "('QUART_HEAD_CNT', <type 'list'>, -1, 8, 0, 0, True), " \
                "('PROJECTED_BUDGET', <class 'decimal.Decimal'>, 20, 8, 12, -2, True))")
        # Check for precision cache
        cur2 = self.con.cursor()
        cur2.execute('select * from proj_dept_budget')
        if ibase.PYTHON_MAJOR_VER==3:
            self.assertEqual(repr(cur2.description),
                "(('FISCAL_YEAR', <class 'int'>, 11, 4, 0, 0, False), " \
                "('PROJ_ID', <class 'str'>, 5, 5, 0, 0, False), " \
                "('DEPT_NO', <class 'str'>, 3, 3, 0, 0, False), " \
                "('QUART_HEAD_CNT', <class 'list'>, -1, 8, 0, 0, True), " \
                "('PROJECTED_BUDGET', <class 'decimal.Decimal'>, 20, 8, 12, -2, True))")
        else:
            self.assertEqual(repr(cur2.description),
                "(('FISCAL_YEAR', <type 'int'>, 11, 4, 0, 0, False), " \
                "('PROJ_ID', <type 'str'>, 5, 5, 0, 0, False), " \
                "('DEPT_NO', <type 'str'>, 3, 3, 0, 0, False), " \
                "('QUART_HEAD_CNT', <type 'list'>, -1, 8, 0, 0, True), " \
                "('PROJECTED_BUDGET', <class 'decimal.Decimal'>, 20, 8, 12, -2, True))")
    def test_exec_after_close(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchone()
        self.assertTupleEqual(row,('USA', 'Dollar'))
        cur.close()
        cur.execute('select * from country')
        row = cur.fetchone()
        self.assertTupleEqual(row,('USA', 'Dollar'))
    def test_fetchone(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchone()
        self.assertTupleEqual(row,('USA', 'Dollar'))
    def test_fetchall(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchall()
        if self.con.ods < fdb.ODS_FB_30:
            self.assertListEqual(rows,
                [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                 ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'),
                 ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'),
                 ('Hong Kong', 'HKDollar'), ('Netherlands', 'Guilder'),
                 ('Belgium', 'BFranc'), ('Austria', 'Schilling'), ('Fiji', 'FDollar')])
        else:
            self.assertListEqual(rows,
                [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                 ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Euro'),
                 ('France', 'Euro'), ('Germany', 'Euro'), ('Australia', 'ADollar'),
                 ('Hong Kong', 'HKDollar'), ('Netherlands', 'Euro'),
                 ('Belgium', 'Euro'), ('Austria', 'Euro'), ('Fiji', 'FDollar'),
                 ('Russia', 'Ruble'), ('Romania', 'RLeu')])
    def test_fetchmany(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchmany(10)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertListEqual(rows,
                [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                 ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Lira'),
                 ('France', 'FFranc'), ('Germany', 'D-Mark'), ('Australia', 'ADollar'),
                 ('Hong Kong', 'HKDollar')])
            rows = cur.fetchmany(10)
            self.assertListEqual(rows,
                [('Netherlands', 'Guilder'), ('Belgium', 'BFranc'),
                 ('Austria', 'Schilling'), ('Fiji', 'FDollar')])
            rows = cur.fetchmany(10)
            self.assertEqual(len(rows),0)
        else:
            self.assertListEqual(rows,
                [('USA', 'Dollar'), ('England', 'Pound'), ('Canada', 'CdnDlr'),
                 ('Switzerland', 'SFranc'), ('Japan', 'Yen'), ('Italy', 'Euro'),
                 ('France', 'Euro'), ('Germany', 'Euro'), ('Australia', 'ADollar'),
                 ('Hong Kong', 'HKDollar')])
            rows = cur.fetchmany(10)
            self.assertListEqual(rows,
                [('Netherlands', 'Euro'), ('Belgium', 'Euro'), ('Austria', 'Euro'),
                 ('Fiji', 'FDollar'), ('Russia', 'Ruble'), ('Romania', 'RLeu')])
            rows = cur.fetchmany(10)
            self.assertEqual(len(rows),0)
    def test_fetchonemap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        row = cur.fetchonemap()
        self.assertListEqual(row.items(),[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')])
    def test_fetchallmap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchallmap()
        if self.con.ods < fdb.ODS_FB_30:
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')],
                 [('COUNTRY', 'England'), ('CURRENCY', 'Pound')],
                 [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')],
                 [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')],
                 [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')],
                 [('COUNTRY', 'Italy'), ('CURRENCY', 'Lira')],
                 [('COUNTRY', 'France'), ('CURRENCY', 'FFranc')],
                 [('COUNTRY', 'Germany'), ('CURRENCY', 'D-Mark')],
                 [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')],
                 [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')],
                 [('COUNTRY', 'Netherlands'), ('CURRENCY', 'Guilder')],
                 [('COUNTRY', 'Belgium'), ('CURRENCY', 'BFranc')],
                 [('COUNTRY', 'Austria'), ('CURRENCY', 'Schilling')],
                 [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')]])
        else:
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')],
                 [('COUNTRY', 'England'), ('CURRENCY', 'Pound')],
                 [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')],
                 [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')],
                 [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')],
                 [('COUNTRY', 'Italy'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'France'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Germany'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')],
                 [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')],
                 [('COUNTRY', 'Netherlands'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Belgium'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Austria'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')],
                 [('COUNTRY', 'Russia'), ('CURRENCY', 'Ruble')],
                 [('COUNTRY', 'Romania'), ('CURRENCY', 'RLeu')]])
    def test_fetchmanymap(self):
        cur = self.con.cursor()
        cur.execute('select * from country')
        rows = cur.fetchmanymap(10)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')],
                 [('COUNTRY', 'England'), ('CURRENCY', 'Pound')],
                 [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')],
                 [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')],
                 [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')],
                 [('COUNTRY', 'Italy'), ('CURRENCY', 'Lira')],
                 [('COUNTRY', 'France'), ('CURRENCY', 'FFranc')],
                 [('COUNTRY', 'Germany'), ('CURRENCY', 'D-Mark')],
                 [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')],
                 [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')]])
            rows = cur.fetchmanymap(10)
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'Netherlands'), ('CURRENCY', 'Guilder')],
                 [('COUNTRY', 'Belgium'), ('CURRENCY', 'BFranc')],
                 [('COUNTRY', 'Austria'), ('CURRENCY', 'Schilling')],
                 [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')]])
            rows = cur.fetchmany(10)
            self.assertEqual(len(rows),0)
        else:
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'USA'), ('CURRENCY', 'Dollar')],
                 [('COUNTRY', 'England'), ('CURRENCY', 'Pound')],
                 [('COUNTRY', 'Canada'), ('CURRENCY', 'CdnDlr')],
                 [('COUNTRY', 'Switzerland'), ('CURRENCY', 'SFranc')],
                 [('COUNTRY', 'Japan'), ('CURRENCY', 'Yen')],
                 [('COUNTRY', 'Italy'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'France'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Germany'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Australia'), ('CURRENCY', 'ADollar')],
                 [('COUNTRY', 'Hong Kong'), ('CURRENCY', 'HKDollar')]])
            rows = cur.fetchmanymap(10)
            self.assertListEqual([row.items() for row in rows],
                [[('COUNTRY', 'Netherlands'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Belgium'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Austria'), ('CURRENCY', 'Euro')],
                 [('COUNTRY', 'Fiji'), ('CURRENCY', 'FDollar')],
                 [('COUNTRY', 'Russia'), ('CURRENCY', 'Ruble')],
                 [('COUNTRY', 'Romania'), ('CURRENCY', 'RLeu')]])
            rows = cur.fetchmany(10)
            self.assertEqual(len(rows),0)
    def test_rowcount(self):
        cur = self.con.cursor()
        self.assertEqual(cur.rowcount,-1)
        cur.execute('select * from project')
        self.assertEqual(cur.rowcount,0)
        cur.fetchone()
        rcount = 1 if FBTEST_HOST == '' and self.con.engine_version >= 3.0 else 6
        self.assertEqual(cur.rowcount,rcount)
    def test_name(self):
        def assign_name():
            cur.name = 'testx'
        cur = self.con.cursor()
        self.assertIsNone(cur.name)
        self.assertRaises(fdb.ProgrammingError,assign_name)
        cur.execute('select * from country')
        cur.name = 'test'
        self.assertEqual(cur.name,'test')
        self.assertRaises(fdb.ProgrammingError,assign_name)
    def test_use_after_close(self):
        cmd = 'select * from country'
        cur = self.con.cursor()
        cur.execute(cmd)
        cur.close()
        cur.execute(cmd)
        row = cur.fetchone()
        self.assertTupleEqual(row,('USA', 'Dollar'))

class TestPreparedStatement(FDBTestBase):
    def setUp(self):
        super(TestPreparedStatement,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from t")
        self.con.commit()
        self.con.close()
    def test_basic(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from country')
        self.assertEqual(ps._in_sqlda.sqln,10)
        self.assertEqual(ps._in_sqlda.sqld,0)
        self.assertEqual(ps._out_sqlda.sqln,10)
        self.assertEqual(ps._out_sqlda.sqld,2)
        self.assertEqual(ps.statement_type,1)
        self.assertEqual(ps.sql,'select * from country')
    def test_get_plan(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from job')
        self.assertEqual(ps.plan,"PLAN (JOB NATURAL)")
    def test_execution(self):
        cur = self.con.cursor()
        ps = cur.prep('select * from country')
        cur.execute(ps)
        row = cur.fetchone()
        self.assertTupleEqual(row,('USA', 'Dollar'))
    def test_wrong_cursor(self):
        cur = self.con.cursor()
        cur2 = self.con.cursor()
        ps = cur.prep('select * from country')
        with self.assertRaises(ValueError) as cm:
            cur2.execute(ps)
        self.assertTupleEqual(cm.exception.args,
            ('PreparedStatement was created by different Cursor.',))


class TestArrays(FDBTestBase):
    def setUp(self):
        super(TestArrays,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
        tbl = """recreate table AR (c1 integer,
                                    c2 integer[1:4,0:3,1:2],
                                    c3 varchar(15)[0:5,1:2],
                                    c4 char(5)[5],
                                    c5 timestamp[2],
                                    c6 time[2],
                                    c7 decimal(10,2)[2],
                                    c8 numeric(10,2)[2],
                                    c9 smallint[2],
                                    c10 bigint[2],
                                    c11 float[2],
                                    c12 double precision[2],
                                    c13 decimal(10,1)[2],
                                    c14 decimal(10,5)[2],
                                    c15 decimal(18,5)[2]
                                    )
"""
        #
        self.c2 = [[[1, 1], [2, 2], [3, 3], [4, 4]], [[5, 5], [6, 6], [7, 7], [8, 8]], [[9, 9], [10, 10], [11, 11], [12, 12]], [[13, 13], [14, 14], [15, 15], [16, 16]]]
        self.c3 = [['a', 'a'], ['bb', 'bb'], ['ccc', 'ccc'], ['dddd', 'dddd'], ['eeeee', 'eeeee'], ['fffffff78901234', 'fffffff78901234']]
        self.c4 = ['a    ', 'bb   ', 'ccc  ', 'dddd ', 'eeeee']
        self.c5 = [datetime.datetime(2012, 11, 22, 12, 8, 24, 474800), datetime.datetime(2012, 11, 22, 12, 8, 24, 474800)]
        self.c6 = [datetime.time(12, 8, 24, 474800), datetime.time(12, 8, 24, 474800)]
        self.c7 = [decimal.Decimal('10.22'), decimal.Decimal('100000.33')]
        self.c8 = [decimal.Decimal('10.22'), decimal.Decimal('100000.33')]
        self.c9 = [1, 0]
        self.c10 = [5555555, 7777777]
        self.c11 = [3.140000104904175, 3.140000104904175]
        self.c12 = [3.14, 3.14]
        self.c13 = [decimal.Decimal('10.2'), decimal.Decimal('100000.3')]
        self.c14 = [decimal.Decimal('10.22222'), decimal.Decimal('100000.333')]
        self.c15 = [decimal.Decimal('1000000000000.22222'), decimal.Decimal('1000000000000.333')]
        #self.con.execute_immediate(tbl)
        #self.con.commit()
        #cur = self.con.cursor()
        #cur.execute("insert into ar (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12) values (1,?,?,?,?,?,?,?,?,?,?,?)",
                    #[self.c2,self.c3,self.c4,self.c5,self.c6,self.c7,self.c8,self.c9,
                     #self.c10,self.c11,self.c12])
        #cur.execute("insert into ar (c1,c2) values (2,?)",[self.c2])
        #cur.execute("insert into ar (c1,c3) values (3,?)",[self.c3])
        #cur.execute("insert into ar (c1,c4) values (4,?)",[self.c4])
        #cur.execute("insert into ar (c1,c5) values (5,?)",[self.c5])
        #cur.execute("insert into ar (c1,c6) values (6,?)",[self.c6])
        #cur.execute("insert into ar (c1,c7) values (7,?)",[self.c7])
        #cur.execute("insert into ar (c1,c8) values (8,?)",[self.c8])
        #cur.execute("insert into ar (c1,c9) values (9,?)",[self.c9])
        #cur.execute("insert into ar (c1,c10) values (10,?)",[self.c10])
        #cur.execute("insert into ar (c1,c11) values (11,?)",[self.c11])
        #cur.execute("insert into ar (c1,c12) values (12,?)",[self.c12])
        #cur.execute("insert into ar (c1,c13) values (13,?)",[self.c13])
        #cur.execute("insert into ar (c1,c14) values (14,?)",[self.c14])
        #cur.execute("insert into ar (c1,c15) values (15,?)",[self.c15])
        #self.con.commit()
    def tearDown(self):
        self.con.execute_immediate("delete from AR where c1>=100")
        self.con.commit()
        self.con.close()
    def test_basic(self):
        cur = self.con.cursor()
        cur.execute("select LANGUAGE_REQ from job "\
                    "where job_code='Eng' and job_grade=3 and job_country='Japan'")
        row = cur.fetchone()
        self.assertTupleEqual(row,
            (['Japanese\n', 'Mandarin\n', 'English\n', '\n', '\n'],))
        cur.execute('select QUART_HEAD_CNT from proj_dept_budget')
        row = cur.fetchall()
        self.assertListEqual(row,
            [([1, 1, 1, 0],), ([3, 2, 1, 0],), ([0, 0, 0, 1],), ([2, 1, 0, 0],),
             ([1, 1, 0, 0],), ([1, 1, 0, 0],), ([1, 1, 1, 1],), ([2, 3, 2, 1],),
             ([1, 1, 2, 2],), ([1, 1, 1, 2],), ([1, 1, 1, 2],), ([4, 5, 6, 6],),
             ([2, 2, 0, 3],), ([1, 1, 2, 2],), ([7, 7, 4, 4],), ([2, 3, 3, 3],),
             ([4, 5, 6, 6],), ([1, 1, 1, 1],), ([4, 5, 5, 3],), ([4, 3, 2, 2],),
             ([2, 2, 2, 1],), ([1, 1, 2, 3],), ([3, 3, 1, 1],), ([1, 1, 0, 0],)])
    def test_read_full(self):
        cur = self.con.cursor()
        cur.execute("select c1,c2 from ar where c1=2")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c2)
        cur.execute("select c1,c3 from ar where c1=3")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c3)
        cur.execute("select c1,c4 from ar where c1=4")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c4)
        cur.execute("select c1,c5 from ar where c1=5")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c5)
        cur.execute("select c1,c6 from ar where c1=6")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c6)
        cur.execute("select c1,c7 from ar where c1=7")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c7)
        cur.execute("select c1,c8 from ar where c1=8")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c8)
        cur.execute("select c1,c9 from ar where c1=9")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c9)
        cur.execute("select c1,c10 from ar where c1=10")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c10)
        cur.execute("select c1,c11 from ar where c1=11")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c11)
        cur.execute("select c1,c12 from ar where c1=12")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c12)
        cur.execute("select c1,c13 from ar where c1=13")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c13)
        cur.execute("select c1,c14 from ar where c1=14")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c14)
        cur.execute("select c1,c15 from ar where c1=15")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c15)
    def test_write_full(self):
        cur = self.con.cursor()
        # INTEGER
        cur.execute("insert into ar (c1,c2) values (102,?)",[self.c2])
        self.con.commit()
        cur.execute("select c1,c2 from ar where c1=102")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c2)

        # VARCHAR
        cur.execute("insert into ar (c1,c3) values (103,?)",[self.c3])
        self.con.commit()
        cur.execute("select c1,c3 from ar where c1=103")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c3)

        cur.execute("insert into ar (c1,c3) values (103,?)",[tuple(self.c3)])
        self.con.commit()
        cur.execute("select c1,c3 from ar where c1=103")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c3)

        # CHAR
        cur.execute("insert into ar (c1,c4) values (104,?)",[self.c4])
        self.con.commit()
        cur.execute("select c1,c4 from ar where c1=104")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c4)

        # TIMESTAMP
        cur.execute("insert into ar (c1,c5) values (105,?)",[self.c5])
        self.con.commit()
        cur.execute("select c1,c5 from ar where c1=105")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c5)

        # TIME OK
        cur.execute("insert into ar (c1,c6) values (106,?)",[self.c6])
        self.con.commit()
        cur.execute("select c1,c6 from ar where c1=106")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c6)

        # DECIMAL(10,2)
        cur.execute("insert into ar (c1,c7) values (107,?)",[self.c7])
        self.con.commit()
        cur.execute("select c1,c7 from ar where c1=107")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c7)

        # NUMERIC(10,2)
        cur.execute("insert into ar (c1,c8) values (108,?)",[self.c8])
        self.con.commit()
        cur.execute("select c1,c8 from ar where c1=108")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c8)

        # SMALLINT
        cur.execute("insert into ar (c1,c9) values (109,?)",[self.c9])
        self.con.commit()
        cur.execute("select c1,c9 from ar where c1=109")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c9)

        # BIGINT
        cur.execute("insert into ar (c1,c10) values (110,?)",[self.c10])
        self.con.commit()
        cur.execute("select c1,c10 from ar where c1=110")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c10)

        # FLOAT
        cur.execute("insert into ar (c1,c11) values (111,?)",[self.c11])
        self.con.commit()
        cur.execute("select c1,c11 from ar where c1=111")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c11)

        # DOUBLE PRECISION
        cur.execute("insert into ar (c1,c12) values (112,?)",[self.c12])
        self.con.commit()
        cur.execute("select c1,c12 from ar where c1=112")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c12)

        # DECIMAL(10,1) OK
        cur.execute("insert into ar (c1,c13) values (113,?)",[self.c13])
        self.con.commit()
        cur.execute("select c1,c13 from ar where c1=113")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c13)

        # DECIMAL(10,5)
        cur.execute("insert into ar (c1,c14) values (114,?)",[self.c14])
        self.con.commit()
        cur.execute("select c1,c14 from ar where c1=114")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c14)

        # DECIMAL(18,5)
        cur.execute("insert into ar (c1,c15) values (115,?)",[self.c15])
        self.con.commit()
        cur.execute("select c1,c15 from ar where c1=115")
        row = cur.fetchone()
        self.assertListEqual(row[1],self.c15)
    def test_write_wrong(self):
        cur = self.con.cursor()

        with self.assertRaises(ValueError) as cm:
            cur.execute("insert into ar (c1,c2) values (102,?)",[self.c3])
        self.assertTupleEqual(cm.exception.args,('Incorrect ARRAY field value.',))
        with self.assertRaises(ValueError) as cm:
            cur.execute("insert into ar (c1,c2) values (102,?)",[self.c2[:-1]])
        self.assertTupleEqual(cm.exception.args,('Incorrect ARRAY field value.',))

class TestInsertData(FDBTestBase):
    def setUp(self):
        super(TestInsertData,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.con2 = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                                user=FBTEST_USER,password=FBTEST_PASSWORD,
                                charset='utf-8')
        #self.con.execute_immediate("recreate table t (c1 integer)")
        #self.con.commit()
        #self.con.execute_immediate("RECREATE TABLE T2 (C1 Smallint,C2 Integer,C3 Bigint,C4 Char(5),C5 Varchar(10),C6 Date,C7 Time,C8 Timestamp,C9 Blob sub_type 1,C10 Numeric(18,2),C11 Decimal(18,2),C12 Float,C13 Double precision,C14 Numeric(8,4),C15 Decimal(8,4))")
        #self.con.commit()
    def tearDown(self):
        self.con2.close()
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
        self.assertListEqual(rows,[(1, 1, 1)])
        cur.execute('insert into T2 (C1,C2,C3) values (?,?,?)',
                    [2,1,9223372036854775807])
        cur.execute('insert into T2 (C1,C2,C3) values (?,?,?)',
                    [2,1,-9223372036854775807-1])
        self.con.commit()
        cur.execute('select C1,C2,C3 from T2 where C1 = 2')
        rows = cur.fetchall()
        self.assertListEqual(rows,
            [(2, 1, 9223372036854775807), (2, 1, -9223372036854775808)])
    def test_insert_char_varchar(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C4,C5) values (?,?,?)',[2,'AA','AA'])
        self.con.commit()
        cur.execute('select C1,C4,C5 from T2 where C1 = 2')
        rows = cur.fetchall()
        self.assertListEqual(rows,[(2, 'AA   ', 'AA')])
        # Too long values
        with self.assertRaises(ValueError) as cm:
            cur.execute('insert into T2 (C1,C4) values (?,?)',[3,'123456'])
            self.con.commit()
        self.assertTupleEqual(cm.exception.args,
            ('Value of parameter (1) is too long, expected 5, found 6',))
        with self.assertRaises(ValueError) as cm:
            cur.execute('insert into T2 (C1,C5) values (?,?)',[3,'12345678901'])
            self.con.commit()
        self.assertTupleEqual(cm.exception.args,
            ('Value of parameter (1) is too long, expected 10, found 11',))
    def test_insert_datetime(self):
        cur = self.con.cursor()
        now = datetime.datetime(2011,11,13,15,00,1,200)
        cur.execute('insert into T2 (C1,C6,C7,C8) values (?,?,?,?)',[3,now.date(),now.time(),now])
        self.con.commit()
        cur.execute('select C1,C6,C7,C8 from T2 where C1 = 3')
        rows = cur.fetchall()
        self.assertListEqual(rows,
            [(3, datetime.date(2011, 11, 13), datetime.time(15, 0, 1, 200),
              datetime.datetime(2011, 11, 13, 15, 0, 1, 200))])

        cur.execute('insert into T2 (C1,C6,C7,C8) values (?,?,?,?)',[4,'2011-11-13','15:0:1:200','2011-11-13 15:0:1:200'])
        self.con.commit()
        cur.execute('select C1,C6,C7,C8 from T2 where C1 = 4')
        rows = cur.fetchall()
        self.assertListEqual(rows,
            [(4, datetime.date(2011, 11, 13), datetime.time(15, 0, 1, 200000),
              datetime.datetime(2011, 11, 13, 15, 0, 1, 200000))])
    def test_insert_blob(self):
        cur = self.con.cursor()
        cur2 = self.con2.cursor()
        cur.execute('insert into T2 (C1,C9) values (?,?)',[4,'This is a BLOB!'])
        cur.transaction.commit()
        cur.execute('select C1,C9 from T2 where C1 = 4')
        rows = cur.fetchall()
        self.assertListEqual(rows,[(4, 'This is a BLOB!')])
        # Non-textual BLOB
        blob_data = fdb.bs([0,1,2,3,4,5,6,7,8,9,10])
        cur.execute('insert into T2 (C1,C16) values (?,?)',[8,blob_data])
        cur.transaction.commit()
        cur.execute('select C1,C16 from T2 where C1 = 8')
        rows = cur.fetchall()
        self.assertListEqual(rows,[(8, blob_data)])
        # BLOB bigger than max. segment size
        big_blob = '123456789' * 10000
        cur.execute('insert into T2 (C1,C9) values (?,?)',[5,big_blob])
        cur.transaction.commit()
        cur.execute('select C1,C9 from T2 where C1 = 5')
        row = cur.fetchone()
        self.assertIsInstance(row[1],fdb.BlobReader)
        self.assertEqual(row[1].read(),big_blob)
        # Unicode in BLOB
        blob_text = 'This is a BLOB!'
        if not isinstance(blob_text,ibase.myunicode):
            blob_text = blob_text.decode('utf-8')
        cur2.execute('insert into T2 (C1,C9) values (?,?)',[6,blob_text])
        cur2.transaction.commit()
        cur2.execute('select C1,C9 from T2 where C1 = 6')
        rows = cur2.fetchall()
        self.assertListEqual(rows,[(6, blob_text)])
        # Unicode non-textual BLOB
        with self.assertRaises(TypeError) as cm:
            cur2.execute('insert into T2 (C1,C16) values (?,?)',[7,blob_text])
        self.assertTupleEqual(cm.exception.args,
            ("Unicode strings are not acceptable input for a non-textual BLOB column.",))
    def test_insert_float_double(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C12,C13) values (?,?,?)',[5,1.0,1.0])
        self.con.commit()
        cur.execute('select C1,C12,C13 from T2 where C1 = 5')
        rows = cur.fetchall()
        self.assertListEqual(rows,[(5, 1.0, 1.0)])
        cur.execute('insert into T2 (C1,C12,C13) values (?,?,?)',[6,1,1])
        self.con.commit()
        cur.execute('select C1,C12,C13 from T2 where C1 = 6')
        rows = cur.fetchall()
        self.assertListEqual(rows,[(6, 1.0, 1.0)])
    def test_insert_numeric_decimal(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?)',[6,1.1,1.1])
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?)',[6,decimal.Decimal('100.11'),decimal.Decimal('100.11')])
        self.con.commit()
        cur.execute('select C1,C10,C11 from T2 where C1 = 6')
        rows = cur.fetchall()
        self.assertListEqual(rows,
                             [(6, Decimal('1.1'), Decimal('1.1')),
                              (6, Decimal('100.11'), Decimal('100.11'))])
    def test_insert_returning(self):
        cur = self.con.cursor()
        cur.execute('insert into T2 (C1,C10,C11) values (?,?,?) returning C1',[6,1.1,1.1])
        result = cur.fetchall()
        self.assertListEqual(result,[(6,)])

class TestStoredProc(FDBTestBase):
    def setUp(self):
        super(TestStoredProc,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
    def tearDown(self):
        self.con.close()
    def test_callproc(self):
        cur = self.con.cursor()
        result = cur.callproc('sub_tot_budget',['100'])
        self.assertListEqual(result,['100'])
        row = cur.fetchone()
        self.assertTupleEqual(row,(Decimal('3800000'), Decimal('760000'),
                                   Decimal('500000'), Decimal('1500000')))
        result = cur.callproc('sub_tot_budget',[100])
        self.assertListEqual(result,[100])
        row = cur.fetchone()
        self.assertTupleEqual(row,(Decimal('3800000'), Decimal('760000'),
                                   Decimal('500000'), Decimal('1500000')))

class TestServices(FDBTestBase):
    def setUp(self):
        super(TestServices,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
    def test_attach(self):
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        svc.close()
    def test_query(self):
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        self.assertEqual(svc.get_service_manager_version(),2)
        self.assertIn('Firebird',svc.get_server_version())
        self.assertIn('Firebird',svc.get_architecture())
        x = svc.get_home_directory()
        #self.assertEqual(x,'/opt/firebird/')
        if svc.engine_version < 3.0:
            self.assertIn('security2.fdb',svc.get_security_database_path())
        else:
            self.assertIn('security3.fdb',svc.get_security_database_path())
        x = svc.get_lock_file_directory()
        #self.assertEqual(x,'/tmp/firebird/')
        x = svc.get_server_capabilities()
        self.assertIsInstance(x,type(tuple()))
        x = svc.get_message_file_directory()
        #self.assertEqual(x,'/opt/firebird/')
        con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                          user=FBTEST_USER,password=FBTEST_PASSWORD)
        con2 = fdb.connect(host=FBTEST_HOST,database='employee',
                           user=FBTEST_USER,password=FBTEST_PASSWORD)
        self.assertGreaterEqual(len(svc.get_attached_database_names()),2)
        self.assertIn(self.dbfile.upper(),
                      [s.upper() for s in svc.get_attached_database_names()])

        #self.assertIn('/opt/firebird/examples/empbuild/employee.fdb',x)
        self.assertGreaterEqual(svc.get_connection_count(),2)
        svc.close()
    def test_running(self):
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        self.assertFalse(svc.isrunning())
        svc.get_log()
        #self.assertTrue(svc.isrunning())
        self.assertTrue(svc.fetching)
        # fetch materialized
        log = svc.readlines()
        self.assertFalse(svc.isrunning())
        svc.close()
    def test_wait(self):
        svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        self.assertFalse(svc.isrunning())
        svc.get_log()
        #self.assertTrue(svc.isrunning())
        self.assertTrue(svc.fetching)
        svc.wait()
        self.assertFalse(svc.isrunning())
        self.assertFalse(svc.fetching)
        svc.close()

class TestServices2(FDBTestBase):
    def setUp(self):
        super(TestServices2,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.fbk = os.path.join(self.dbpath,'test_employee.fbk')
        self.fbk2 = os.path.join(self.dbpath,'test_employee.fbk2')
        self.rfdb = os.path.join(self.dbpath,'test_employee.fdb')
        self.svc = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
        if not os.path.exists(self.rfdb):
            c = fdb.create_database(host=FBTEST_HOST,database=self.rfdb,
                                    user=FBTEST_USER,password=FBTEST_PASSWORD)
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
        self.assertTrue(self.svc.fetching)
        # fetch materialized
        log = self.svc.readlines()
        self.assertFalse(self.svc.fetching)
        self.assertTrue(log)
        self.assertIsInstance(log,type(list()))
        # iterate over result
        self.svc.get_log()
        for line in self.svc:
            self.assertIsNotNone(line)
            self.assertIsInstance(line,fdb.StringType)
        self.assertFalse(self.svc.fetching)
        # callback
        output = []
        self.svc.get_log(callback=fetchline)
        self.assertGreater(len(output),0)
    def test_getLimboTransactionIDs(self):
        ids = self.svc.get_limbo_transaction_ids('employee')
        self.assertIsInstance(ids,type(list()))
    def test_getStatistics(self):
        def fetchline(line):
            output.append(line)
        self.svc.get_statistics('employee')
        self.assertTrue(self.svc.fetching)
        self.assertTrue(self.svc.isrunning())
        # fetch materialized
        stats = self.svc.readlines()
        self.assertFalse(self.svc.fetching)
        self.assertFalse(self.svc.isrunning())
        self.assertIsInstance(stats,type(list()))
        # iterate over result
        self.svc.get_statistics('employee',
                                show_system_tables_and_indexes=True,
                                show_record_versions=True)
        for line in self.svc:
            self.assertIsInstance(line,fdb.StringType)
        self.assertFalse(self.svc.fetching)
        # callback
        output = []
        self.svc.get_statistics('employee', callback=fetchline)
        self.assertGreater(len(output),0)
        # fetch only selected tables
        stats = self.svc.get_statistics('employee',
                                        show_user_data_pages=True,
                                        tables='COUNTRY')
        stats = '\n'.join(self.svc.readlines())
        self.assertIn('COUNTRY',stats)
        self.assertNotIn('JOB',stats)
        #
        stats = self.svc.get_statistics('employee',
                                        show_user_data_pages=True,
                                        tables=('COUNTRY','PROJECT'))
        stats = '\n'.join(self.svc.readlines())
        self.assertIn('COUNTRY',stats)
        self.assertIn('PROJECT',stats)
        self.assertNotIn('JOB',stats)
    def test_backup(self):
        def fetchline(line):
            output.append(line)
        self.svc.backup('employee', self.fbk)
        self.assertTrue(self.svc.fetching)
        self.assertTrue(self.svc.isrunning())
        # fetch materialized
        report = self.svc.readlines()
        self.assertFalse(self.svc.fetching)
        self.assertFalse(self.svc.isrunning())
        self.assertTrue(os.path.exists(self.fbk))
        self.assertIsInstance(report,type(list()))
        # iterate over result
        self.svc.backup('employee', self.fbk,
                        ignore_checksums=1,
                        ignore_limbo_transactions=1,
                        metadata_only=1,
                        collect_garbage=0,
                        transportable=0,
                        convert_external_tables_to_internal=1,
                        compressed=0,
                        no_db_triggers=0)
        for line in self.svc:
            self.assertIsNotNone(line)
            self.assertIsInstance(line,fdb.StringType)
        self.assertFalse(self.svc.fetching)
        # callback
        output = []
        self.svc.backup('employee', self.fbk, callback=fetchline)
        self.assertGreater(len(output),0)
        # Firebird 3.0 stats
        if self.con.ods >= fdb.ODS_FB_30:
            output = []
            self.svc.backup('employee', self.fbk, callback=fetchline,
                            stats=[fdb.services.STATS_TOTAL_TIME,fdb.services.STATS_TIME_DELTA,
                                   fdb.services.STATS_PAGE_READS,fdb.services.STATS_PAGE_WRITES])
            self.assertGreater(len(output),0)
            self.assertIn('gbak: time     delta  reads  writes ', output)
    def test_restore(self):
        def fetchline(line):
            output.append(line)
        output = []
        self.svc.backup('employee', self.fbk, callback=fetchline)
        self.assertTrue(os.path.exists(self.fbk))
        self.svc.restore(self.fbk, self.rfdb, replace=1)
        self.assertTrue(self.svc.fetching)
        self.assertTrue(self.svc.isrunning())
        # fetch materialized
        report = self.svc.readlines()
        self.assertFalse(self.svc.fetching)
        self.assertFalse(self.svc.isrunning())
        self.assertIsInstance(report,type(list()))
        # iterate over result
        self.svc.restore(self.fbk, self.rfdb, replace=1)
        for line in self.svc:
            self.assertIsNotNone(line)
            self.assertIsInstance(line,fdb.StringType)
        self.assertFalse(self.svc.fetching)
        # callback
        output = []
        self.svc.restore(self.fbk, self.rfdb, replace=1, callback=fetchline)
        self.assertGreater(len(output),0)
        # Firebird 3.0 stats
        if self.con.ods >= fdb.ODS_FB_30:
            output = []
            self.svc.restore(self.fbk, self.rfdb, replace=1, callback=fetchline,
                             stats=[fdb.services.STATS_TOTAL_TIME,fdb.services.STATS_TIME_DELTA,
                                    fdb.services.STATS_PAGE_READS,fdb.services.STATS_PAGE_WRITES])
            self.assertGreater(len(output),0)
            self.assertIn('gbak: time     delta  reads  writes ', output)
    def test_nbackup(self):
        if self.con.engine_version < 2.5:
            return
        self.svc.nbackup('employee', self.fbk)
        self.assertTrue(os.path.exists(self.fbk))
    def test_nrestore(self):
        if self.con.engine_version < 2.5:
            return
        self.test_nbackup()
        self.assertTrue(os.path.exists(self.fbk))
        if os.path.exists(self.rfdb):
            os.remove(self.rfdb)
        self.svc.nrestore(self.fbk, self.rfdb)
        self.assertTrue(os.path.exists(self.rfdb))
    def test_trace(self):
        if self.con.engine_version < 2.5:
            return
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
        svc2 = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        svcx = fdb.services.connect(host=FBTEST_HOST,password=FBTEST_PASSWORD)
        # Start trace sessions
        trace1_id = self.svc.trace_start(trace_config,'test_trace_1')
        trace2_id = svc2.trace_start(trace_config)
        # check sessions
        sessions = svcx.trace_list()
        self.assertIn(trace1_id,sessions)
        self.assertListEqual(list(sessions[trace1_id].keys()),
                             ['date', 'flags', 'name', 'user'])
        self.assertIn(trace2_id,sessions)
        self.assertListEqual(list(sessions[trace2_id].keys()),
                             ['date', 'flags', 'user'])
        if self.con.engine_version < 3.0:
            self.assertListEqual(sessions[trace1_id]['flags'],['active', ' admin', ' trace'])
            self.assertListEqual(sessions[trace2_id]['flags'],['active', ' admin', ' trace'])
        else:
            self.assertListEqual(sessions[trace1_id]['flags'],['active', ' trace'])
            self.assertListEqual(sessions[trace2_id]['flags'],['active', ' trace'])
        # Pause session
        svcx.trace_suspend(trace2_id)
        self.assertIn('suspend',svcx.trace_list()[trace2_id]['flags'])
        # Resume session
        svcx.trace_resume(trace2_id)
        self.assertIn('active',svcx.trace_list()[trace2_id]['flags'])
        # Stop session
        svcx.trace_stop(trace2_id)
        self.assertNotIn(trace2_id,svcx.trace_list())
        # Finalize
        svcx.trace_stop(trace1_id)
        svc2.close()
        svcx.close()
    def test_setDefaultPageBuffers(self):
        self.svc.set_default_page_buffers(self.rfdb,100)
    def test_setSweepInterval(self):
        self.svc.set_sweep_interval(self.rfdb,10000)
    def test_shutdown_bringOnline(self):
        if self.con.engine_version < 2.5:
            # Basic shutdown/online
            self.svc.shutdown(self.rfdb,
                              fdb.services.SHUT_LEGACY,
                              fdb.services.SHUT_FORCE,0)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertIn('multi-user maintenance',''.join(self.svc.readlines()))
            # Return to normal state
            self.svc.bring_online(self.rfdb,fdb.services.SHUT_LEGACY)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertNotIn('multi-user maintenance',''.join(self.svc.readlines()))
        else:
            # Shutdown database to single-user maintenance mode
            self.svc.shutdown(self.rfdb,
                              fdb.services.SHUT_SINGLE,
                              fdb.services.SHUT_FORCE,0)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertIn('single-user maintenance',''.join(self.svc.readlines()))
            # Enable multi-user maintenance
            self.svc.bring_online(self.rfdb,fdb.services.SHUT_MULTI)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertIn('multi-user maintenance',''.join(self.svc.readlines()))

            # Go to full shutdown mode, disabling new attachments during 5 seconds
            self.svc.shutdown(self.rfdb,
                              fdb.services.SHUT_FULL,
                              fdb.services.SHUT_DENY_NEW_ATTACHMENTS,5)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertIn('full shutdown',''.join(self.svc.readlines()))
            # Enable single-user maintenance
            self.svc.bring_online(self.rfdb,fdb.services.SHUT_SINGLE)
            self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
            self.assertIn('single-user maintenance',''.join(self.svc.readlines()))
            # Return to normal state
            self.svc.bring_online(self.rfdb)
    def test_setShouldReservePageSpace(self):
        self.svc.set_reserve_page_space(self.rfdb,False)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertIn('no reserve',''.join(self.svc.readlines()))
        self.svc.set_reserve_page_space(self.rfdb,True)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertNotIn('no reserve',''.join(self.svc.readlines()))
    def test_setWriteMode(self):
        # Forced writes
        self.svc.set_write_mode(self.rfdb,fdb.services.WRITE_FORCED)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertIn('force write',''.join(self.svc.readlines()))
        # No Forced writes
        self.svc.set_write_mode(self.rfdb,fdb.services.WRITE_BUFFERED)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertNotIn('force write',''.join(self.svc.readlines()))
    def test_setAccessMode(self):
        # Read Only
        self.svc.set_access_mode(self.rfdb,fdb.services.ACCESS_READ_ONLY)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertIn('read only',''.join(self.svc.readlines()))
        # Read/Write
        self.svc.set_access_mode(self.rfdb,fdb.services.ACCESS_READ_WRITE)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertNotIn('read only',''.join(self.svc.readlines()))
    def test_setSQLDialect(self):
        self.svc.set_sql_dialect(self.rfdb,1)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertIn('Database dialect\t1',''.join(self.svc.readlines()))
        self.svc.set_sql_dialect(self.rfdb,3)
        self.svc.get_statistics(self.rfdb,show_only_db_header_pages=1)
        self.assertIn('Database dialect\t3',''.join(self.svc.readlines()))
    def test_activateShadowFile(self):
        self.svc.activate_shadow(self.rfdb)
    def test_nolinger(self):
        if self.con.ods >= fdb.ODS_FB_30:
            self.svc.no_linger(self.rfdb)
    def test_sweep(self):
        self.svc.sweep(self.rfdb)
    def test_repair(self):
        result = self.svc.repair(self.rfdb)
        self.assertFalse(result)
    def test_validate(self):
        def fetchline(line):
            output.append(line)
        output = []
        self.svc.validate(self.dbfile)
        # fetch materialized
        report = self.svc.readlines()
        self.assertFalse(self.svc.fetching)
        self.assertFalse(self.svc.isrunning())
        self.assertIsInstance(report,type(list()))
        self.assertIn('Validation started','/n'.join(report))
        self.assertIn('Validation finished','/n'.join(report))
        # iterate over result
        self.svc.validate(self.dbfile)
        for line in self.svc:
            self.assertIsNotNone(line)
            self.assertIsInstance(line,fdb.StringType)
        self.assertFalse(self.svc.fetching)
        # callback
        output = []
        self.svc.validate(self.dbfile, callback=fetchline)
        self.assertGreater(len(output),0)
        # Parameters
        self.svc.validate(self.dbfile,include_tables='COUNTRY|SALES',
                          include_indices='SALESTATX',lock_timeout=-1)
        report = '/n'.join(self.svc.readlines())
        self.assertIn('(COUNTRY)', report)
        self.assertIn('(SALES)', report)
        self.assertIn('(SALESTATX)', report)
    def test_getUsers(self):
        users = self.svc.get_users()
        self.assertIsInstance(users,type(list()))
        self.assertIsInstance(users[0],fdb.services.User)
        self.assertEqual(users[0].name,'SYSDBA')
    def test_manage_user(self):
        user = fdb.services.User('FDB_TEST')
        user.password = 'FDB_TEST'
        user.first_name = 'FDB'
        user.middle_name = 'X.'
        user.last_name = 'TEST'
        try:
            self.svc.remove_user(user)
        except:
            pass
        self.svc.add_user(user)
        self.assertTrue(self.svc.user_exists(user))
        self.assertTrue(self.svc.user_exists('FDB_TEST'))
        users = [u for u in self.svc.get_users() if u.name == 'FDB_TEST']
        self.assertTrue(users)
        self.assertEqual(len(users),1)
        #self.assertEqual(users[0].password,'FDB_TEST')
        self.assertEqual(users[0].first_name,'FDB')
        self.assertEqual(users[0].middle_name,'X.')
        self.assertEqual(users[0].last_name,'TEST')
        user.password = 'XFDB_TEST'
        user.first_name = 'XFDB'
        user.middle_name = 'XX.'
        user.last_name = 'XTEST'
        self.svc.modify_user(user)
        users = [u for u in self.svc.get_users() if u.name == 'FDB_TEST']
        self.assertTrue(users)
        self.assertEqual(len(users),1)
        #self.assertEqual(users[0].password,'XFDB_TEST')
        self.assertEqual(users[0].first_name,'XFDB')
        self.assertEqual(users[0].middle_name,'XX.')
        self.assertEqual(users[0].last_name,'XTEST')
        self.svc.remove_user(user)
        self.assertFalse(self.svc.user_exists('FDB_TEST'))


class TestEvents(FDBTestBase):
    def setUp(self):
        super(TestEvents,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbevents.fdb')
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.con = fdb.create_database(host=FBTEST_HOST,database=self.dbfile,
                                       user=FBTEST_USER,password=FBTEST_PASSWORD)
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
        with self.con.event_conduit(['insert_1']) as events:
            timed_event.start()
            e = events.wait()
        timed_event.join()
        self.assertDictEqual(e,{'insert_1': 1})
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
        with self.con.event_conduit(['insert_1','insert_3']) as events:
            timed_event.start()
            e = events.wait()
        timed_event.join()
        self.assertDictEqual(e,{'insert_3': 1, 'insert_1': 2})
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
        with self.con.event_conduit(['insert_1','A','B','C','D',
                                         'E','F','G','H','I','J','K','L','M',
                                         'N','O','P','Q','R','insert_3']) as events:
            timed_event.start()
            time.sleep(3)
            e = events.wait()
        timed_event.join()
        self.assertDictEqual(e,
            {'A': 0, 'C': 0, 'B': 0, 'E': 0, 'D': 0, 'G': 0, 'insert_1': 2,
             'I': 0, 'H': 0, 'K': 0, 'J': 0, 'M': 0, 'L': 0, 'O': 0, 'N': 0,
             'Q': 0, 'P': 0, 'R': 0, 'insert_3': 1, 'F': 0})
    def test_flush_events(self):
        def send_events(command_list):
            c = self.con.cursor()
            for cmd in command_list:
                c.execute(cmd)
            self.con.commit()

        timed_event = threading.Timer(3.0,send_events,args=[["insert into T (PK,C1) values (1,1)",]])
        with self.con.event_conduit(['insert_1']) as events:
            send_events(["insert into T (PK,C1) values (1,1)",
                         "insert into T (PK,C1) values (1,1)"])
            time.sleep(2)
            events.flush()
            timed_event.start()
            e = events.wait()
        timed_event.join()
        self.assertDictEqual(e,{'insert_1': 1})

class TestStreamBLOBs(FDBTestBase):
    def setUp(self):
        super(TestStreamBLOBs,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
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
        ## Necessary to avoid bad BLOB handle on BlobReader.close in tearDown
        ## because BLOB handle is no longer valid after table purge
        with closing(p):
            self.assertEqual(blob_reader.read(20),'Firebird supports tw')
            self.assertEqual(blob_reader.read(20),'o types of blobs, st')
            self.assertEqual(blob_reader.read(400),
                'ream and segmented.\nThe database stores segmented blobs in ' \
                'chunks.\nEach chunk starts with a two byte length indicator ' \
                'followed by however many bytes of data were passed as ' \
                'a segment.\nStream blobs are stored as a continuous array ' \
                'of data bytes with no length indicators included.')
            self.assertEqual(blob_reader.read(20),'')
            self.assertEqual(blob_reader.tell(),318)
            blob_reader.seek(20)
            self.assertEqual(blob_reader.tell(),20)
            self.assertEqual(blob_reader.read(20),'o types of blobs, st')
            blob_reader.seek(0)
            self.assertEqual(blob_reader.tell(),0)
            self.assertListEqual(blob_reader.readlines(),StringIO(blob).readlines())
            blob_reader.seek(0)
            for line in blob_reader:
                self.assertIn(line.rstrip('\n'),blob.split('\n'))
            blob_reader.seek(0)
            self.assertEqual(blob_reader.read(),blob)
            blob_reader.seek(-9,os.SEEK_END)
            self.assertEqual(blob_reader.read(),'included.')
            blob_reader.seek(-20,os.SEEK_END)
            blob_reader.seek(11,os.SEEK_CUR)
            self.assertEqual(blob_reader.read(),'included.')
            blob_reader.seek(60)
            self.assertEqual(blob_reader.readline(),
                             'The database stores segmented blobs in chunks.\n')
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
        # Necessary to avoid bad BLOB handle on BlobReader.close in tearDown
        # because BLOB handle is no longer valid after table purge
        with closing(p):
            for row in cur:
                blob_reader = row[1]
                self.assertEqual(blob_reader.read(20),'Firebird supports tw')
                self.assertEqual(blob_reader.read(20),'o types of blobs, st')
                self.assertEqual(blob_reader.read(400),
                    'ream and segmented.\nThe database stores segmented blobs ' \
                    'in chunks.\nEach chunk starts with a two byte length ' \
                    'indicator followed by however many bytes of data were ' \
                    'passed as a segment.\nStream blobs are stored as a ' \
                    'continuous array of data bytes with no length indicators ' \
                    'included.')
                self.assertEqual(blob_reader.read(20),'')
                self.assertEqual(blob_reader.tell(),318)
                blob_reader.seek(20)
                self.assertEqual(blob_reader.tell(),20)
                self.assertEqual(blob_reader.read(20),'o types of blobs, st')
                blob_reader.seek(0)
                self.assertEqual(blob_reader.tell(),0)
                self.assertListEqual(blob_reader.readlines(),
                                     StringIO(blob).readlines())
                blob_reader.seek(0)
                for line in blob_reader:
                    self.assertIn(line.rstrip('\n'),blob.split('\n'))
                blob_reader.seek(0)
                self.assertEqual(blob_reader.read(),blob)
                blob_reader.seek(-9,os.SEEK_END)
                self.assertEqual(blob_reader.read(),'included.')
                blob_reader.seek(-20,os.SEEK_END)
                blob_reader.seek(11,os.SEEK_CUR)
                self.assertEqual(blob_reader.read(),'included.')
                blob_reader.seek(60)
                self.assertEqual(blob_reader.readline(),
                    'The database stores segmented blobs in chunks.\n')

class TestCharsetConversion(FDBTestBase):
    def setUp(self):
        super(TestCharsetConversion,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD,
                               charset='utf8')
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
            self.assertTupleEqual(row,
                        (1, b'\x01\x02\x03\x04\x05', b'\x01\x02\x03\x04\x05'))
        else:
            self.assertTupleEqual(row,
                        (1, '\x01\x02\x03\x04\x05', '\x01\x02\x03\x04\x05'))
    def test_utf82win1250(self):
        s5 = 'ěščřž'
        s30 = 'ěščřžýáíéúůďťňóĚŠČŘŽÝÁÍÉÚŮĎŤŇÓ'
        if ibase.PYTHON_MAJOR_VER != 3:
            s5 = s5.decode('utf8')
            s30 = s30.decode('utf8')

        con1250 = fdb.connect(host=FBTEST_HOST,database=self.dbfile,user=FBTEST_USER,
                              password=FBTEST_PASSWORD,charset='win1250')
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
        self.assertTupleEqual(row,(1,s5,s30,s5,s30))
        c_utf8.execute("select C1, C_WIN1250, V_WIN1250,"
                       "C_UTF8, V_UTF8 from T4 where C1 = 1")
        row = c_utf8.fetchone()
        self.assertTupleEqual(row,(1,s5,s30,s5,s30))

    def testCharVarchar(self):
        s = 'Introdução'
        if ibase.PYTHON_MAJOR_VER != 3:
            s = s.decode('utf8')
        self.assertEqual(len(s),10)
        data = tuple([1,s,s])
        cur = self.con.cursor()
        cur.execute('insert into T3 (C1,C2,C3) values (?,?,?)',data)
        self.con.commit()
        cur.execute('select C1,C2,C3 from T3 where C1 = 1')
        row = cur.fetchone()
        self.assertEqual(row,data)
    def testBlob(self):
        s = """Introdução

Este artigo descreve como você pode fazer o InterBase e o Firebird 1.5
coehabitarem pacificamente seu computador Windows. Por favor, note que esta
solução não permitirá que o Interbase e o Firebird rodem ao mesmo tempo.
Porém você poderá trocar entre ambos com um mínimo de luta. """
        if ibase.PYTHON_MAJOR_VER != 3:
            s = s.decode('utf8')
        self.assertEqual(len(s),292)
        data = tuple([2,s])
        b_data = tuple([3,ibase.b('bytestring')])
        cur = self.con.cursor()
        # Text BLOB
        cur.execute('insert into T3 (C1,C4) values (?,?)',data)
        self.con.commit()
        cur.execute('select C1,C4 from T3 where C1 = 2')
        row = cur.fetchone()
        self.assertEqual(row,data)
        # Insert Unicode into non-textual BLOB
        with self.assertRaises(TypeError) as cm:
            cur.execute('insert into T3 (C1,C5) values (?,?)',data)
            self.con.commit()
        self.assertTupleEqual(cm.exception.args,
            ('Unicode strings are not acceptable input for a non-textual BLOB column.',))
        # Read binary from non-textual BLOB
        cur.execute('insert into T3 (C1,C5) values (?,?)',b_data)
        self.con.commit()
        cur.execute('select C1,C5 from T3 where C1 = 3')
        row = cur.fetchone()
        self.assertEqual(row,b_data)

class TestSchema(FDBTestBase):
    def setUp(self):
        super(TestSchema,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        #self.dbfile = '/home/data/db/employee30.fdb'
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
    def tearDown(self):
        self.con.close()
    def testSchemaBindClose(self):
        s = sm.Schema()
        self.assertTrue(s.closed)
        s.bind(self.con)
        # properties
        self.assertIsNone(s.description)
        self.assertIsNone(s.linger)
        self.assertEqual(s.owner_name,'SYSDBA')
        self.assertEqual(s.default_character_set.name,'NONE')
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(s.security_class)
        else:
            self.assertEqual(s.security_class,'SQL$363')
        self.assertFalse(s.closed)
        #
        s.close()
        self.assertTrue(s.closed)
        s.bind(self.con)
        self.assertFalse(s.closed)
        #
        s.bind(self.con)
        self.assertFalse(s.closed)
        #
        del s
    def testSchemaFromConnection(self):
        s = self.con.schema
        # enum_* disctionaries
        self.assertDictEqual(s.enum_param_type_from,
            {0: 'DATATYPE', 1: 'DOMAIN', 2: 'TYPE OF DOMAIN', 3: 'TYPE OF COLUMN'})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_object_types,
                {0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
                 4: 'VALIDATION', 5: 'PROCEDURE', 6: 'EXPRESSION_INDEX',
                 7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
                 11: 'DEPENDENT_COUNT', 12: 'USER_GROUP', 13: 'ROLE',
                 14: 'GENERATOR', 15: 'UDF', 16: 'BLOB_FILTER'})
            self.assertDictEqual(s.enum_object_type_codes,
                {'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'UDF': 15,
                 'EXPRESSION_INDEX': 6, 'FIELD': 9, 'COMPUTED_FIELD': 3,
                 'TRIGGER': 2, 'RELATION': 0, 'USER': 8, 'DEPENDENT_COUNT': 11,
                 'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
                 'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1})
        elif self.con.ods > fdb.ODS_FB_20 and self.con.ods < fdb.ODS_FB_30:
            self.assertDictEqual(s.enum_object_types,
                {0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
                 4: 'VALIDATION', 5: 'PROCEDURE', 6: 'EXPRESSION_INDEX',
                 7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
                 12: 'USER_GROUP', 13: 'ROLE', 14: 'GENERATOR', 15: 'UDF',
                 16: 'BLOB_FILTER', 17: 'COLLATION'})
            self.assertDictEqual(s.enum_object_type_codes,
                {'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'COLLATION': 17,
                 'UDF': 15, 'EXPRESSION_INDEX': 6, 'FIELD': 9,
                 'COMPUTED_FIELD': 3, 'TRIGGER': 2, 'RELATION': 0, 'USER': 8,
                 'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
                 'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1})
        else:
            self.assertDictEqual(s.enum_object_types,
                {0: 'RELATION', 1: 'VIEW', 2: 'TRIGGER', 3: 'COMPUTED_FIELD',
                 4: 'VALIDATION', 5: 'PROCEDURE', 6: 'EXPRESSION_INDEX',
                 7: 'EXCEPTION', 8: 'USER', 9: 'FIELD', 10: 'INDEX',
                 11: 'CHARACTER_SET',12: 'USER_GROUP', 13: 'ROLE',
                 14: 'GENERATOR', 15: 'UDF', 16: 'BLOB_FILTER', 17: 'COLLATION',
                 18:'PACKAGE',19:'PACKAGE BODY'})
            self.assertDictEqual(s.enum_object_type_codes,
                {'INDEX': 10, 'EXCEPTION': 7, 'GENERATOR': 14, 'COLLATION': 17,
                 'UDF': 15, 'EXPRESSION_INDEX': 6, 'FIELD': 9,
                 'COMPUTED_FIELD': 3, 'TRIGGER': 2, 'RELATION': 0, 'USER': 8,
                 'USER_GROUP': 12, 'BLOB_FILTER': 16, 'ROLE': 13,
                 'VALIDATION': 4, 'PROCEDURE': 5, 'VIEW': 1, 'CHARACTER_SET':11,
                 'PACKAGE':18, 'PACKAGE BODY':19})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_character_set_names,
                {0: 'NONE', 1: 'BINARY', 2: 'ASCII7', 3: 'SQL_TEXT', 4: 'UTF-8',
                 5: 'SJIS', 6: 'EUCJ', 9: 'DOS_737', 10: 'DOS_437',
                 11: 'DOS_850', 12: 'DOS_865', 13: 'DOS_860', 14: 'DOS_863',
                 15: 'DOS_775', 16: 'DOS_858', 17: 'DOS_862', 18: 'DOS_864',
                 19: 'NEXT', 21: 'ANSI', 22: 'ISO-8859-2', 23: 'ISO-8859-3',
                 34: 'ISO-8859-4', 35: 'ISO-8859-5', 36: 'ISO-8859-6',
                 37: 'ISO-8859-7', 38: 'ISO-8859-8', 39: 'ISO-8859-9',
                 40: 'ISO-8859-13', 44: 'WIN_949', 45: 'DOS_852', 46: 'DOS_857',
                 47: 'DOS_861', 48: 'DOS_866', 49: 'DOS_869', 50: 'CYRL',
                 51: 'WIN_1250', 52: 'WIN_1251', 53: 'WIN_1252', 54: 'WIN_1253',
                 55: 'WIN_1254', 56: 'WIN_950', 57: 'WIN_936', 58: 'WIN_1255',
                 59: 'WIN_1256', 60: 'WIN_1257', 63: 'KOI8R', 64: 'KOI8U',
                 65: 'WIN1258'})
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertDictEqual(s.enum_character_set_names,
                {0: 'NONE', 1: 'BINARY', 2: 'ASCII7', 3: 'SQL_TEXT',
                 4: 'UTF-8', 5: 'SJIS', 6: 'EUCJ', 9: 'DOS_737', 10: 'DOS_437',
                 11: 'DOS_850', 12: 'DOS_865', 13: 'DOS_860', 14: 'DOS_863',
                 15: 'DOS_775', 16: 'DOS_858', 17: 'DOS_862', 18: 'DOS_864',
                 19: 'NEXT', 21: 'ANSI', 22: 'ISO-8859-2', 23: 'ISO-8859-3',
                 34: 'ISO-8859-4', 35: 'ISO-8859-5', 36: 'ISO-8859-6',
                 37: 'ISO-8859-7', 38: 'ISO-8859-8', 39: 'ISO-8859-9',
                 40: 'ISO-8859-13', 44: 'WIN_949', 45: 'DOS_852', 46: 'DOS_857',
                 47: 'DOS_861', 48: 'DOS_866', 49: 'DOS_869', 50: 'CYRL',
                 51: 'WIN_1250', 52: 'WIN_1251', 53: 'WIN_1252', 54: 'WIN_1253',
                 55: 'WIN_1254', 56: 'WIN_950', 57: 'WIN_936', 58: 'WIN_1255',
                 59: 'WIN_1256', 60: 'WIN_1257', 63: 'KOI8R', 64: 'KOI8U',
                 65: 'WIN1258', 66: 'TIS620', 67: 'GBK', 68: 'CP943C'})
        elif self.con.ods >= fdb.ODS_FB_25:
            self.assertDictEqual(s.enum_character_set_names,
                {0: 'NONE', 1: 'BINARY', 2: 'ASCII7', 3: 'SQL_TEXT', 4: 'UTF-8',
                 5: 'SJIS', 6: 'EUCJ', 9: 'DOS_737', 10: 'DOS_437', 11: 'DOS_850',
                 12: 'DOS_865', 13: 'DOS_860', 14: 'DOS_863', 15: 'DOS_775',
                 16: 'DOS_858', 17: 'DOS_862', 18: 'DOS_864', 19: 'NEXT',
                 21: 'ANSI', 22: 'ISO-8859-2', 23: 'ISO-8859-3', 34: 'ISO-8859-4',
                 35: 'ISO-8859-5', 36: 'ISO-8859-6', 37: 'ISO-8859-7',
                 38: 'ISO-8859-8', 39: 'ISO-8859-9', 40: 'ISO-8859-13',
                 44: 'WIN_949', 45: 'DOS_852', 46: 'DOS_857', 47: 'DOS_861',
                 48: 'DOS_866', 49: 'DOS_869', 50: 'CYRL', 51: 'WIN_1250',
                 52: 'WIN_1251', 53: 'WIN_1252', 54: 'WIN_1253', 55: 'WIN_1254',
                 56: 'WIN_950', 57: 'WIN_936', 58: 'WIN_1255', 59: 'WIN_1256',
                 60: 'WIN_1257', 63: 'KOI8R', 64: 'KOI8U', 65: 'WIN_1258',
                 66: 'TIS620', 67: 'GBK', 68: 'CP943C', 69: 'GB18030'})
        if self.con.ods < fdb.ODS_FB_30:
            self.assertDictEqual(s.enum_field_types,
                    {35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
                     9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
                     13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
                     261: 'BLOB'})
        else:
            self.assertDictEqual(s.enum_field_types,
                    {35: 'TIMESTAMP', 37: 'VARYING', 7: 'SHORT', 8: 'LONG',
                     9: 'QUAD', 10: 'FLOAT', 12: 'DATE', 45: 'BLOB_ID', 14: 'TEXT',
                     13: 'TIME', 16: 'INT64', 40: 'CSTRING', 27: 'DOUBLE',
                     261: 'BLOB', 23:'BOOLEAN'})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_field_subtypes,
                {0: 'BINARY', 1: 'TEXT', 2: 'BLR', 3: 'ACL', 4: 'RANGES',
                 5: 'SUMMARY', 6: 'FORMAT', 7: 'TRANSACTION_DESCRIPTION',
                 8: 'EXTERNAL_FILE_DESCRIPTION'})
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_field_subtypes,
                {0: 'BINARY', 1: 'TEXT', 2: 'BLR', 3: 'ACL', 4: 'RANGES',
                 5: 'SUMMARY', 6: 'FORMAT', 7: 'TRANSACTION_DESCRIPTION',
                 8: 'EXTERNAL_FILE_DESCRIPTION', 9: 'DEBUG_INFORMATION'})
        self.assertDictEqual(s.enum_function_types,{0: 'VALUE', 1: 'BOOLEAN'})
        self.assertDictEqual(s.enum_mechanism_types,
                             {0: 'BY_VALUE', 1: 'BY_REFERENCE',
                              2: 'BY_VMS_DESCRIPTOR', 3: 'BY_ISC_DESCRIPTOR',
                              4: 'BY_SCALAR_ARRAY_DESCRIPTOR',
                              5: 'BY_REFERENCE_WITH_NULL'})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_parameter_mechanism_types,{})
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_parameter_mechanism_types,
                                 {0: 'NORMAL', 1: 'TYPE OF'})
        self.assertDictEqual(s.enum_procedure_types,
                             {0: 'LEGACY', 1: 'SELECTABLE', 2: 'EXECUTABLE'})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_relation_types,{})
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_relation_types,
                {0: 'PERSISTENT', 1: 'VIEW', 2: 'EXTERNAL', 3: 'VIRTUAL',
                 4: 'GLOBAL_TEMPORARY_PRESERVE', 5: 'GLOBAL_TEMPORARY_DELETE'})
        if self.con.ods < fdb.ODS_FB_30:
            self.assertDictEqual(s.enum_system_flag_types,
                {0: 'USER', 1: 'SYSTEM', 2: 'QLI', 3: 'CHECK_CONSTRAINT',
                 4: 'REFERENTIAL_CONSTRAINT', 5: 'VIEW_CHECK'})
        else:
            self.assertDictEqual(s.enum_system_flag_types,
                {0: 'USER', 1: 'SYSTEM', 2: 'QLI', 3: 'CHECK_CONSTRAINT',
                 4: 'REFERENTIAL_CONSTRAINT', 5: 'VIEW_CHECK', 6: 'IDENTITY_GENERATOR'})
        self.assertDictEqual(s.enum_transaction_state_types,
                             {1: 'LIMBO', 2: 'COMMITTED', 3: 'ROLLED_BACK'})
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_trigger_types,
                {1: 'PRE_STORE', 2: 'POST_STORE', 3: 'PRE_MODIFY',
                 4: 'POST_MODIFY', 5: 'PRE_ERASE', 6: 'POST_ERASE'})
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertDictEqual(s.enum_trigger_types,
                {8192: 'CONNECT', 1: 'PRE_STORE', 2: 'POST_STORE',
                 3: 'PRE_MODIFY', 4: 'POST_MODIFY', 5: 'PRE_ERASE',
                 6: 'POST_ERASE', 8193: 'DISCONNECT', 8194: 'TRANSACTION_START',
                 8195: 'TRANSACTION_COMMIT', 8196: 'TRANSACTION_ROLLBACK'})
        if self.con.ods >= fdb.ODS_FB_30:
            self.assertDictEqual(s.enum_parameter_types,
                {0: 'INPUT', 1: 'OUTPUT'})
            self.assertDictEqual(s.enum_index_activity_flags,
                {0: 'ACTIVE', 1: 'INACTIVE'})
            self.assertDictEqual(s.enum_index_unique_flags,
                {0: 'NON_UNIQUE', 1: 'UNIQUE'})
            self.assertDictEqual(s.enum_trigger_activity_flags,
                {0: 'ACTIVE', 1: 'INACTIVE'})
            self.assertDictEqual(s.enum_grant_options,
                {0: 'NONE', 1: 'GRANT_OPTION', 2: 'ADMIN_OPTION'})
            self.assertDictEqual(s.enum_page_types,
                {1: 'HEADER', 2: 'PAGE_INVENTORY', 3: 'TRANSACTION_INVENTORY',
                 4: 'POINTER', 5: 'DATA', 6: 'INDEX_ROOT', 7: 'INDEX_BUCKET',
                 8: 'BLOB', 9: 'GENERATOR', 10: 'SCN_INVENTORY'})
            self.assertDictEqual(s.enum_privacy_flags,
                {0: 'PUBLIC', 1: 'PRIVATE'})
            self.assertDictEqual(s.enum_legacy_flags,
                {0: 'NEW_STYLE', 1: 'LEGACY_STYLE'})
            self.assertDictEqual(s.enum_determinism_flags,
                {0: 'NON_DETERMINISTIC', 1: 'DETERMINISTIC'})

        # properties
        self.assertIsNone(s.description)
        self.assertEqual(s.owner_name,'SYSDBA')
        self.assertEqual(s.default_character_set.name,'NONE')
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(s.security_class)
        else:
            self.assertEqual(s.security_class,'SQL$363')
        # Lists of db objects
        self.assertIsInstance(s.collations,list)
        self.assertIsInstance(s.character_sets,list)
        self.assertIsInstance(s.exceptions,list)
        self.assertIsInstance(s.generators,list)
        self.assertIsInstance(s.sysgenerators,list)
        self.assertIsInstance(s.sequences,list)
        self.assertIsInstance(s.syssequences,list)
        self.assertIsInstance(s.domains,list)
        self.assertIsInstance(s.sysdomains,list)
        self.assertIsInstance(s.indices,list)
        self.assertIsInstance(s.sysindices,list)
        self.assertIsInstance(s.tables,list)
        self.assertIsInstance(s.systables,list)
        self.assertIsInstance(s.views,list)
        self.assertIsInstance(s.sysviews,list)
        self.assertIsInstance(s.triggers,list)
        self.assertIsInstance(s.systriggers,list)
        self.assertIsInstance(s.procedures,list)
        self.assertIsInstance(s.sysprocedures,list)
        self.assertIsInstance(s.constraints,list)
        self.assertIsInstance(s.roles,list)
        self.assertIsInstance(s.dependencies,list)
        self.assertIsInstance(s.functions,list)
        self.assertIsInstance(s.files,list)
        s.reload()
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(s.collations),138)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(s.collations),146)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(len(s.collations),149)
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(len(s.collations),150)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(s.character_sets),48)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(s.character_sets),51)
        elif self.con.ods >= fdb.ODS_FB_25:
            self.assertEqual(len(s.character_sets),52)
        self.assertEqual(len(s.exceptions),5)
        self.assertEqual(len(s.generators),2)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.sysgenerators),9)
            self.assertEqual(len(s.syssequences),9)
        else:
            self.assertEqual(len(s.sysgenerators),13)
            self.assertEqual(len(s.syssequences),13)
        self.assertEqual(len(s.sequences),2)
        self.assertEqual(len(s.domains),15)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(s.sysdomains),203)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(s.sysdomains),227)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(len(s.sysdomains),230)
        elif self.con.ods == fdb.ODS_FB_30:
            self.assertEqual(len(s.sysdomains),275)
        else:
            self.assertEqual(len(s.sysdomains),247)
        self.assertEqual(len(s.indices),12)
        if self.con.ods <= fdb.ODS_FB_21:
            self.assertEqual(len(s.sysindices),72)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(len(s.sysindices),76)
        elif self.con.ods == fdb.ODS_FB_30:
            self.assertEqual(len(s.sysindices),81)
        else:
            self.assertEqual(len(s.sysindices),78)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.tables),15)
        else:
            self.assertEqual(len(s.tables),16)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(s.systables),33)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(s.systables),40)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(len(s.systables),42)
        elif self.con.ods == fdb.ODS_FB_30:
            self.assertEqual(len(s.systables),50)
        else:
            self.assertEqual(len(s.systables),44)
        self.assertEqual(len(s.views),1)
        self.assertEqual(len(s.sysviews),0)
        self.assertEqual(len(s.triggers),6)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.systriggers),63)
        else:
            self.assertEqual(len(s.systriggers),57)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.procedures),10)
        else:
            self.assertEqual(len(s.procedures),11)
        self.assertEqual(len(s.sysprocedures),0)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.constraints),82)
        else:
            self.assertEqual(len(s.constraints),108)
        if self.con.ods <= fdb.ODS_FB_21:
            self.assertEqual(len(s.roles),1)
        elif self.con.ods >= fdb.ODS_FB_25:
            self.assertEqual(len(s.roles),2)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(s.dependencies),163)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(s.dependencies),157)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(len(s.dependencies),163)
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(len(s.dependencies),168)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.functions),0)
        else:
            self.assertEqual(len(s.functions),6)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.sysfunctions),2)
        else:
            self.assertEqual(len(s.sysfunctions),0)
        self.assertEqual(len(s.files),0)
        #
        self.assertIsInstance(s.collations[0],sm.Collation)
        self.assertIsInstance(s.character_sets[0],sm.CharacterSet)
        self.assertIsInstance(s.exceptions[0],sm.DatabaseException)
        self.assertIsInstance(s.generators[0],sm.Sequence)
        self.assertIsInstance(s.sysgenerators[0],sm.Sequence)
        self.assertIsInstance(s.sequences[0],sm.Sequence)
        self.assertIsInstance(s.syssequences[0],sm.Sequence)
        self.assertIsInstance(s.domains[0],sm.Domain)
        self.assertIsInstance(s.sysdomains[0],sm.Domain)
        self.assertIsInstance(s.indices[0],sm.Index)
        self.assertIsInstance(s.sysindices[0],sm.Index)
        self.assertIsInstance(s.tables[0],sm.Table)
        self.assertIsInstance(s.systables[0],sm.Table)
        self.assertIsInstance(s.views[0],sm.View)
        #self.assertIsInstance(s.sysviews[0],sm.View)
        self.assertIsInstance(s.triggers[0],sm.Trigger)
        self.assertIsInstance(s.systriggers[0],sm.Trigger)
        self.assertIsInstance(s.procedures[0],sm.Procedure)
        #self.assertIsInstance(s.sysprocedures[0],sm.Procedure)
        self.assertIsInstance(s.constraints[0],sm.Constraint)
        #self.assertIsInstance(s.roles[0],sm.Role)
        self.assertIsInstance(s.dependencies[0],sm.Dependency)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsInstance(s.sysfunctions[0],sm.Function)
        #self.assertIsInstance(s.files[0],sm.DatabaseFile)
        #
        self.assertEqual(s.get_collation('OCTETS').name,'OCTETS')
        self.assertEqual(s.get_character_set('WIN1250').name,'WIN1250')
        self.assertEqual(s.get_exception('UNKNOWN_EMP_ID').name,'UNKNOWN_EMP_ID')
        self.assertEqual(s.get_generator('EMP_NO_GEN').name,'EMP_NO_GEN')
        self.assertEqual(s.get_sequence('EMP_NO_GEN').name,'EMP_NO_GEN')
        self.assertEqual(s.get_index('MINSALX').name,'MINSALX')
        self.assertEqual(s.get_domain('FIRSTNAME').name,'FIRSTNAME')
        self.assertEqual(s.get_table('COUNTRY').name,'COUNTRY')
        self.assertEqual(s.get_view('PHONE_LIST').name,'PHONE_LIST')
        self.assertEqual(s.get_trigger('SET_EMP_NO').name,'SET_EMP_NO')
        self.assertEqual(s.get_procedure('GET_EMP_PROJ').name,'GET_EMP_PROJ')
        self.assertEqual(s.get_constraint('INTEG_1').name,'INTEG_1')
        #self.assertEqual(s.get_role('X').name,'X')
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(s.get_function('RDB$GET_CONTEXT').name,'RDB$GET_CONTEXT')
        self.assertEqual(s.get_collation_by_id(0,0).name,'NONE')
        self.assertEqual(s.get_character_set_by_id(0).name,'NONE')
        self.assertFalse(s.ismultifile())
        #
        self.assertFalse(s.closed)
        #
        with self.assertRaises(fdb.ProgrammingError) as cm:
            s.close()
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'close' not allowed for embedded Schema.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            s.bind(self.con)
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'bind' not allowed for embedded Schema.",))
    def testCollation(self):
        # System collation
        c = self.con.schema.get_collation('ES_ES')
        # common properties
        self.assertEqual(c.name,'ES_ES')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'ES_ES')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(c.security_class)
            self.assertIsNone(c.owner_name)
        else:
            self.assertEqual(c.security_class,'SQL$263')
            self.assertEqual(c.owner_name,'SYSDBA')
        #
        self.assertEqual(c.id,10)
        self.assertEqual(c.character_set.name,'ISO8859_1')
        self.assertIsNone(c.base_collation)
        self.assertEqual(c.attributes,1)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertIsNone(c.specific_attributes)
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertEqual(c.specific_attributes,
                             'DISABLE-COMPRESSIONS=1;SPECIALS-FIRST=1')
        self.assertIsNone(c.function_name)
        # User defined collation
        # create collation TEST_COLLATE
        # for win1250
        # from WIN_CZ no pad case insensitive accent insensitive
        # 'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'
        c = self.con.schema.get_collation('TEST_COLLATE')
        # common properties
        self.assertEqual(c.name,'TEST_COLLATE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment', 'create', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'TEST_COLLATE')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.id,126)
        self.assertEqual(c.character_set.name,'WIN1250')
        self.assertEqual(c.base_collation.name,'WIN_CZ')
        self.assertEqual(c.attributes,6)
        self.assertEqual(c.specific_attributes,
                         'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0')
        self.assertIsNone(c.function_name)
        self.assertEqual(c.get_sql_for('create'),
"""CREATE COLLATION TEST_COLLATE
   FOR WIN1250
   FROM WIN_CZ
   NO PAD
   CASE INSENSITIVE
   ACCENT INSENSITIVE
   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'""")
        self.assertEqual(c.get_sql_for('drop'),"DROP COLLATION TEST_COLLATE")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('drop',badparam='')
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON COLLATION TEST_COLLATE IS NULL")

    def testCharacterSet(self):
        c = self.con.schema.get_character_set('UTF8')
        # common properties
        self.assertEqual(c.name,'UTF8')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['alter','comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'UTF8')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(c.security_class)
            self.assertIsNone(c.owner_name)
        else:
            self.assertEqual(c.security_class,'SQL$166')
            self.assertEqual(c.owner_name,'SYSDBA')
        #
        self.assertEqual(c.id,4)
        self.assertEqual(c.bytes_per_character,4)
        self.assertEqual(c.default_collate.name,'UTF8')
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertListEqual([x.name for x in c.collations],
                                 ['UTF8', 'UCS_BASIC', 'UNICODE'])
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertListEqual([x.name for x in c.collations],
                                 ['UTF8', 'UCS_BASIC', 'UNICODE', 'UNICODE_CI'])
        elif self.con.ods >= fdb.ODS_FB_25:
            self.assertListEqual([x.name for x in c.collations],
                ['UTF8', 'UCS_BASIC', 'UNICODE', 'UNICODE_CI', 'UNICODE_CI_AI'])
        #
        self.assertEqual(c.get_sql_for('alter',collation='UCS_BASIC'),
            "ALTER CHARACTER SET UTF8 SET DEFAULT COLLATION UCS_BASIC")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam='UCS_BASIC')
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
            ("Missing required parameter: 'collation'.",))
        #
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON CHARACTER SET UTF8 IS NULL')
        #
        self.assertEqual(c.get_collation('UCS_BASIC').name,'UCS_BASIC')
        self.assertEqual(c.get_collation_by_id(c.get_collation('UCS_BASIC').id).name,
                         'UCS_BASIC')
    def testException(self):
        c = self.con.schema.get_exception('UNKNOWN_EMP_ID')
        # common properties
        self.assertEqual(c.name,'UNKNOWN_EMP_ID')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,
                ['comment', 'create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'UNKNOWN_EMP_ID')
        d = c.get_dependents()
        self.assertEqual(len(d),1)
        d = d[0]
        self.assertEqual(d.dependent_name,'ADD_EMP_PROJ')
        self.assertEqual(d.dependent_type,5)
        self.assertIsInstance(d.dependent,sm.Procedure)
        self.assertEqual(d.depended_on_name,'UNKNOWN_EMP_ID')
        self.assertEqual(d.depended_on_type,7)
        self.assertIsInstance(d.depended_on,sm.DatabaseException)
        self.assertListEqual(c.get_dependencies(),[])
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(c.security_class)
            self.assertIsNone(c.owner_name)
        else:
            self.assertEqual(c.security_class,'SQL$476')
            self.assertEqual(c.owner_name,'SYSDBA')
        #
        self.assertEqual(c.id,1)
        self.assertEqual(c.message,"Invalid employee number or project id.")
        #
        self.assertEqual(c.get_sql_for('create'),
            "CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('recreate'),
            "RECREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('drop'),
            "DROP EXCEPTION UNKNOWN_EMP_ID")
        self.assertEqual(c.get_sql_for('alter',message="New message."),
                         "ALTER EXCEPTION UNKNOWN_EMP_ID 'New message.'")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam="New message.")
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
            ("Missing required parameter: 'message'.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
            "CREATE OR ALTER EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'")
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON EXCEPTION UNKNOWN_EMP_ID IS NULL")
    def testSequence(self):
        # System generator
        c = self.con.schema.get_sequence('RDB$FIELD_NAME')
        # common properties
        self.assertEqual(c.name,'RDB$FIELD_NAME')
        self.assertEqual(c.description,"Implicit domain name")
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$FIELD_NAME')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.id,6)
        # User generator
        c = self.con.schema.get_generator('EMP_NO_GEN')
        # common properties
        self.assertEqual(c.name,'EMP_NO_GEN')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment', 'create',
                                        'alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'EMP_NO_GEN')
        d = c.get_dependents()
        self.assertEqual(len(d),1)
        d = d[0]
        self.assertEqual(d.dependent_name,'SET_EMP_NO')
        self.assertEqual(d.dependent_type,2)
        self.assertIsInstance(d.dependent,sm.Trigger)
        self.assertEqual(d.depended_on_name,'EMP_NO_GEN')
        self.assertEqual(d.depended_on_type,14)
        self.assertIsInstance(d.depended_on,sm.Sequence)
        self.assertListEqual(c.get_dependencies(),[])
        #
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(c.id,10)
            self.assertIsNone(c.security_class)
            self.assertIsNone(c.owner_name)
            self.assertIsNone(c.inital_value)
            self.assertIsNone(c.increment)
        else:
            self.assertEqual(c.id,12)
            self.assertEqual(c.security_class,'SQL$429')
            self.assertEqual(c.owner_name,'SYSDBA')
            self.assertEqual(c.inital_value,0)
            self.assertEqual(c.increment,1)
        self.assertEqual(c.value,145)
        #
        self.assertEqual(c.get_sql_for('create'),"CREATE SEQUENCE EMP_NO_GEN")
        self.assertEqual(c.get_sql_for('drop'),"DROP SEQUENCE EMP_NO_GEN")
        self.assertEqual(c.get_sql_for('alter',value=10),
                         "ALTER SEQUENCE EMP_NO_GEN RESTART WITH 10")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam=10)
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
            ("Missing required parameter: 'value'.",))
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON SEQUENCE EMP_NO_GEN IS NULL")
        c.schema.opt_generator_keyword = 'GENERATOR'
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON GENERATOR EMP_NO_GEN IS NULL")
    def testTableColumn(self):
        # System column
        c = self.con.schema.get_table('RDB$PAGES').get_column('RDB$PAGE_NUMBER')
        # common properties
        self.assertEqual(c.name,'RDB$PAGE_NUMBER')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$PAGE_NUMBER')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        self.assertFalse(c.isidentity())
        self.assertIsNone(c.generator)
        # User column
        c = self.con.schema.get_table('DEPARTMENT').get_column('PHONE_NO')
        # common properties
        self.assertEqual(c.name,'PHONE_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment', 'alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'PHONE_NO')
        d = c.get_dependents()
        self.assertEqual(len(d),1)
        d = d[0]
        self.assertEqual(d.dependent_name,'PHONE_LIST')
        self.assertEqual(d.dependent_type,1)
        self.assertIsInstance(d.dependent,sm.View)
        self.assertEqual(d.depended_on_name,'DEPARTMENT')
        self.assertEqual(d.depended_on_type,0)
        self.assertIsInstance(d.depended_on,sm.TableColumn)
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.table.name,'DEPARTMENT')
        self.assertEqual(c.domain.name,'PHONENUMBER')
        self.assertEqual(c.position,6)
        self.assertIsNone(c.security_class)
        self.assertEqual(c.default,"'555-1234'")
        self.assertIsNone(c.collation)
        self.assertEqual(c.datatype,'VARCHAR(20)')
        #
        self.assertTrue(c.isnullable())
        self.assertFalse(c.iscomputed())
        self.assertTrue(c.isdomainbased())
        self.assertTrue(c.has_default())
        self.assertIsNone(c.get_computedby())
        #
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON COLUMN DEPARTMENT.PHONE_NO IS NULL")
        self.assertEqual(c.get_sql_for('drop'),
                         "ALTER TABLE DEPARTMENT DROP PHONE_NO")
        self.assertEqual(c.get_sql_for('alter',name='NewName'),
            'ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TO "NewName"')
        self.assertEqual(c.get_sql_for('alter',position=2),
            "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO POSITION 2")
        self.assertEqual(c.get_sql_for('alter',datatype='VARCHAR(25)'),
            "ALTER TABLE DEPARTMENT ALTER COLUMN PHONE_NO TYPE VARCHAR(25)")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam=10)
        self.assertTupleEqual(cm.exception.args,("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,("Parameter required.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',expression='(1+1)')
        self.assertTupleEqual(cm.exception.args,
            ("Change from persistent column to computed is not allowed.",))
        # Computed column
        c = self.con.schema.get_table('EMPLOYEE').get_column('FULL_NAME')
        self.assertTrue(c.isnullable())
        self.assertTrue(c.iscomputed())
        self.assertFalse(c.isdomainbased())
        self.assertFalse(c.has_default())
        self.assertEqual(c.get_computedby(),"(last_name || ', ' || first_name)")
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(c.datatype,'VARCHAR(0)')
        else:
            self.assertEqual(c.datatype,'VARCHAR(37)')
        #
        self.assertEqual(c.get_sql_for('alter',datatype='VARCHAR(50)',
                                       expression="(first_name || ', ' || last_name)"),
            "ALTER TABLE EMPLOYEE ALTER COLUMN FULL_NAME TYPE VARCHAR(50) " \
            "COMPUTED BY (first_name || ', ' || last_name)")

        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',datatype='VARCHAR(50)')
        self.assertTupleEqual(cm.exception.args,
            ("Change from computed column to persistent is not allowed.",))
        # Array column
        c = self.con.schema.get_table('AR').get_column('C2')
        self.assertEqual(c.datatype,'INTEGER[4, 0:3, 2]')
        # Identity column
        if self.con.ods >= fdb.ODS_FB_30:
            c = self.con.schema.get_table('T5').get_column('ID')
            self.assertTrue(c.isidentity())
            self.assertTrue(c.generator.isidentity())
            self.assertEqual(c.identity_type,1)
            #
            self.assertEqual(c.get_sql_for('alter',restart=None),
                "ALTER TABLE T5 ALTER COLUMN ID RESTART")
            self.assertEqual(c.get_sql_for('alter',restart=100),
                "ALTER TABLE T5 ALTER COLUMN ID RESTART WITH 100")
    def testIndex(self):
        # System index
        c = self.con.schema.get_index('RDB$INDEX_0')
        # common properties
        self.assertEqual(c.name,'RDB$INDEX_0')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['recompute','comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$INDEX_0')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.table.name,'RDB$RELATIONS')
        self.assertListEqual(c.segment_names,['RDB$RELATION_NAME'])
        # user index
        c = self.con.schema.get_index('MAXSALX')
        # common properties
        self.assertEqual(c.name,'MAXSALX')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['recompute', 'comment',
                                        'create', 'activate',
                                        'deactivate', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'MAXSALX')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.id,3)
        self.assertEqual(c.table.name,'JOB')
        self.assertEqual(c.index_type,'DESCENDING')
        self.assertIsNone(c.partner_index)
        self.assertIsNone(c.expression)
        # startswith() is necessary, because Python 3 returns more precise value.
        self.assertTrue(str(c.statistics).startswith('0.0384615398943'))
        self.assertListEqual(c.segment_names,['JOB_COUNTRY', 'MAX_SALARY'])
        self.assertEqual(len(c.segments),2)
        for segment in c.segments:
            self.assertIsInstance(segment,sm.TableColumn)
        self.assertEqual(c.segments[0].name,'JOB_COUNTRY')
        self.assertEqual(c.segments[1].name,'MAX_SALARY')

        if self.con.ods <= fdb.ODS_FB_20:
            self.assertListEqual(c.segment_statistics,[None, None])
        elif self.con.ods > fdb.ODS_FB_20:
            self.assertListEqual(c.segment_statistics,
                                 [0.1428571492433548, 0.03846153989434242])
        self.assertIsNone(c.constraint)
        #
        self.assertFalse(c.isexpression())
        self.assertFalse(c.isunique())
        self.assertFalse(c.isinactive())
        self.assertFalse(c.isenforcer())
        #
        self.assertEqual(c.get_sql_for('create'),
          """CREATE DESCENDING INDEX MAXSALX ON JOB (JOB_COUNTRY,MAX_SALARY)""")
        self.assertEqual(c.get_sql_for('activate'),"ALTER INDEX MAXSALX ACTIVE")
        self.assertEqual(c.get_sql_for('deactivate'),"ALTER INDEX MAXSALX INACTIVE")
        self.assertEqual(c.get_sql_for('recompute'),"SET STATISTICS INDEX MAXSALX")
        self.assertEqual(c.get_sql_for('drop'),"DROP INDEX MAXSALX")
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON INDEX MAXSALX IS NULL")
        # Constraint index
        c = self.con.schema.get_index('RDB$FOREIGN6')
        # common properties
        self.assertEqual(c.name,'RDB$FOREIGN6')
        self.assertTrue(c.issystemobject())
        self.assertTrue(c.isenforcer())
        self.assertEqual(c.partner_index.name,'RDB$PRIMARY5')
        self.assertEqual(c.constraint.name,'INTEG_17')
    def testViewColumn(self):
        c = self.con.schema.get_view('PHONE_LIST').get_column('LAST_NAME')
        # common properties
        self.assertEqual(c.name,'LAST_NAME')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'LAST_NAME')
        self.assertListEqual(c.get_dependents(),[])
        d = c.get_dependencies()
        self.assertEqual(len(d),1)
        d = d[0]
        self.assertEqual(d.dependent_name,'PHONE_LIST')
        self.assertEqual(d.dependent_type,1)
        self.assertIsInstance(d.dependent,sm.View)
        self.assertEqual(d.field_name,'LAST_NAME')
        self.assertEqual(d.depended_on_name,'EMPLOYEE')
        self.assertEqual(d.depended_on_type,0)
        self.assertIsInstance(d.depended_on,sm.TableColumn)
        self.assertEqual(d.depended_on.name,'LAST_NAME')
        self.assertEqual(d.depended_on.table.name,'EMPLOYEE')
        #
        self.assertEqual(c.view.name,'PHONE_LIST')
        self.assertEqual(c.base_field.name,'LAST_NAME')
        self.assertEqual(c.base_field.table.name,'EMPLOYEE')
        self.assertEqual(c.domain.name,'LASTNAME')
        self.assertEqual(c.position,2)
        self.assertIsNone(c.security_class)
        self.assertEqual(c.collation.name,'NONE')
        self.assertEqual(c.datatype,'VARCHAR(20)')
        #
        self.assertTrue(c.isnullable())
        #
        self.assertEqual(c.get_sql_for('comment'),
            "COMMENT ON COLUMN PHONE_LIST.LAST_NAME IS NULL")
    def testDomain(self):
        # System domain
        c = self.con.schema.get_domain('RDB$6')
        # common properties
        self.assertEqual(c.name,'RDB$6')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$6')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(c.security_class)
            self.assertIsNone(c.owner_name)
        else:
            self.assertEqual(c.security_class,'SQL$439')
            self.assertEqual(c.owner_name,'SYSDBA')
        # User domain
        c = self.con.schema.get_domain('PRODTYPE')
        # common properties
        self.assertEqual(c.name,'PRODTYPE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment', 'create',
                                        'alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'PRODTYPE')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertIsNone(c.expression)
        self.assertEqual(c.validation,
            "CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))")
        self.assertEqual(c.default,"'software'")
        self.assertEqual(c.length,12)
        self.assertEqual(c.scale,0)
        self.assertEqual(c.field_type,37)
        self.assertEqual(c.sub_type,0)
        self.assertIsNone(c.segment_length)
        self.assertIsNone(c.external_length)
        self.assertIsNone(c.external_scale)
        self.assertIsNone(c.external_type)
        self.assertListEqual(c.dimensions,[])
        self.assertEqual(c.character_length,12)
        self.assertEqual(c.collation.name,'NONE')
        self.assertEqual(c.character_set.name,'NONE')
        self.assertIsNone(c.precision)
        self.assertEqual(c.datatype,'VARCHAR(12)')
        #
        self.assertFalse(c.isnullable())
        self.assertFalse(c.iscomputed())
        self.assertTrue(c.isvalidated())
        self.assertFalse(c.isarray())
        self.assertTrue(c.has_default())
        #
        self.assertEqual(c.get_sql_for('create'),
            "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' " \
            "NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))")
        self.assertEqual(c.get_sql_for('drop'),"DROP DOMAIN PRODTYPE")
        self.assertEqual(c.get_sql_for('alter',name='New_name'),
            'ALTER DOMAIN PRODTYPE TO "New_name"')
        self.assertEqual(c.get_sql_for('alter',default="'New_default'"),
            "ALTER DOMAIN PRODTYPE SET DEFAULT 'New_default'")
        self.assertEqual(c.get_sql_for('alter',check="VALUE STARTS WITH 'X'"),
            "ALTER DOMAIN PRODTYPE ADD CHECK (VALUE STARTS WITH 'X')")
        self.assertEqual(c.get_sql_for('alter',datatype='VARCHAR(30)'),
            "ALTER DOMAIN PRODTYPE TYPE VARCHAR(30)")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam=10)
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,("Parameter required.",))
        # Domain with quoted name
        c = self.con.schema.get_domain('FIRSTNAME')
        self.assertEqual(c.name,'FIRSTNAME')
        self.assertEqual(c.get_quoted_name(),'"FIRSTNAME"')
        self.assertEqual(c.get_sql_for('create'),
            'CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)')
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON DOMAIN "FIRSTNAME" IS NULL')
    def testDependency(self):
        l = self.con.schema.get_table('DEPARTMENT').get_dependents()
        self.assertEqual(len(l),18)
        c = l[0] if self.con.ods < fdb.ODS_FB_30 else l[3]
        # common properties
        self.assertIsNone(c.name)
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,[])
        self.assertTrue(c.issystemobject())
        self.assertIsNone(c.get_quoted_name())
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        self.assertIsNone(c.package)
        self.assertFalse(c.ispackaged())
        #
        self.assertEqual(c.dependent_name,'PHONE_LIST')
        self.assertEqual(c.dependent_type,1)
        self.assertIsInstance(c.dependent,sm.View)
        self.assertEqual(c.dependent.name,'PHONE_LIST')
        self.assertEqual(c.field_name,'DEPT_NO')
        self.assertEqual(c.depended_on_name,'DEPARTMENT')
        self.assertEqual(c.depended_on_type,0)
        self.assertIsInstance(c.depended_on,sm.TableColumn)
        self.assertEqual(c.depended_on.name,'DEPT_NO')
        #
        if self.con.engine_version >= 3.0:
            self.assertListEqual(c.get_dependents(),[])
            l = self.con.schema.get_package('TEST2').get_dependencies()
            self.assertEqual(len(l),2)
            x = l[0]
            self.assertEqual(x.depended_on.name,'FN')
            self.assertFalse(x.depended_on.ispackaged())
            x = l[1]
            self.assertEqual(x.depended_on.name,'F')
            self.assertTrue(x.depended_on.ispackaged())
            self.assertIsInstance(x.package,sm.Package)
    def testConstraint(self):
        # Common / PRIMARY KEY
        c = self.con.schema.get_table('CUSTOMER').primary_key
        # common properties
        self.assertEqual(c.name,'INTEG_60')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['create', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'INTEG_60')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.constraint_type,'PRIMARY KEY')
        self.assertEqual(c.table.name,'CUSTOMER')
        self.assertEqual(c.index.name,'RDB$PRIMARY22')
        self.assertListEqual(c.trigger_names,[])
        self.assertListEqual(c.triggers,[])
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.isnotnull())
        self.assertTrue(c.ispkey())
        self.assertFalse(c.isfkey())
        self.assertFalse(c.isunique())
        self.assertFalse(c.ischeck())
        self.assertFalse(c.isdeferrable())
        self.assertFalse(c.isdeferred())
        #
        self.assertEqual(c.get_sql_for('create'),
            "ALTER TABLE CUSTOMER ADD PRIMARY KEY (CUST_NO)")
        self.assertEqual(c.get_sql_for('drop'),
            "ALTER TABLE CUSTOMER DROP CONSTRAINT INTEG_60")
        # FOREIGN KEY
        c = self.con.schema.get_table('CUSTOMER').foreign_keys[0]
        #
        self.assertListEqual(c.actions,['create', 'drop'])
        self.assertEqual(c.constraint_type,'FOREIGN KEY')
        self.assertEqual(c.table.name,'CUSTOMER')
        self.assertEqual(c.index.name,'RDB$FOREIGN23')
        self.assertListEqual(c.trigger_names,[])
        self.assertListEqual(c.triggers,[])
        self.assertIsNone(c.column_name)
        self.assertEqual(c.partner_constraint.name,'INTEG_2')
        self.assertEqual(c.match_option,'FULL')
        self.assertEqual(c.update_rule,'RESTRICT')
        self.assertEqual(c.delete_rule,'RESTRICT')
        #
        self.assertFalse(c.isnotnull())
        self.assertFalse(c.ispkey())
        self.assertTrue(c.isfkey())
        self.assertFalse(c.isunique())
        self.assertFalse(c.ischeck())
        #
        self.assertEqual(c.get_sql_for('create'),
                         """ALTER TABLE CUSTOMER ADD FOREIGN KEY (COUNTRY)
  REFERENCES COUNTRY (COUNTRY)""")
        # CHECK
        c = self.con.schema.get_constraint('INTEG_59')
        #
        self.assertListEqual(c.actions,['create', 'drop'])
        self.assertEqual(c.constraint_type,'CHECK')
        self.assertEqual(c.table.name,'CUSTOMER')
        self.assertIsNone(c.index)
        self.assertListEqual(c.trigger_names,['CHECK_9', 'CHECK_10'])
        self.assertEqual(c.triggers[0].name,'CHECK_9')
        self.assertEqual(c.triggers[1].name,'CHECK_10')
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.isnotnull())
        self.assertFalse(c.ispkey())
        self.assertFalse(c.isfkey())
        self.assertFalse(c.isunique())
        self.assertTrue(c.ischeck())
        #
        self.assertEqual(c.get_sql_for('create'),
            "ALTER TABLE CUSTOMER ADD CHECK (on_hold IS NULL OR on_hold = '*')")
        # UNIQUE
        c = self.con.schema.get_constraint('INTEG_15')
        #
        self.assertListEqual(c.actions,['create', 'drop'])
        self.assertEqual(c.constraint_type,'UNIQUE')
        self.assertEqual(c.table.name,'DEPARTMENT')
        self.assertEqual(c.index.name,'RDB$4')
        self.assertListEqual(c.trigger_names,[])
        self.assertListEqual(c.triggers,[])
        self.assertIsNone(c.column_name)
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertFalse(c.isnotnull())
        self.assertFalse(c.ispkey())
        self.assertFalse(c.isfkey())
        self.assertTrue(c.isunique())
        self.assertFalse(c.ischeck())
        #
        self.assertEqual(c.get_sql_for('create'),
            "ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)")
        # NOT NULL
        c = self.con.schema.get_constraint('INTEG_13')
        #
        self.assertListEqual(c.actions,[])
        self.assertEqual(c.constraint_type,'NOT NULL')
        self.assertEqual(c.table.name,'DEPARTMENT')
        self.assertIsNone(c.index)
        self.assertListEqual(c.trigger_names,[])
        self.assertListEqual(c.triggers,[])
        self.assertEqual(c.column_name,'DEPT_NO')
        self.assertIsNone(c.partner_constraint)
        self.assertIsNone(c.match_option)
        self.assertIsNone(c.update_rule)
        self.assertIsNone(c.delete_rule)
        #
        self.assertTrue(c.isnotnull())
        self.assertFalse(c.ispkey())
        self.assertFalse(c.isfkey())
        self.assertFalse(c.isunique())
        self.assertFalse(c.ischeck())
    def testTable(self):
        # System table
        c = self.con.schema.get_table('RDB$PAGES')
        # common properties
        self.assertEqual(c.name,'RDB$PAGES')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$PAGES')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        # User table
        c = self.con.schema.get_table('EMPLOYEE')
        # common properties
        self.assertEqual(c.name,'EMPLOYEE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment', 'create',
                                        'recreate', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'EMPLOYEE')
        d = c.get_dependents()
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertListEqual([(x.dependent_name,x.dependent_type) for x in d],
                   [('RDB$9', 3), ('RDB$9', 3), ('PHONE_LIST', 1),
                    ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                    ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('CHECK_3', 2),
                    ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2),
                    ('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2),
                    ('CHECK_4', 2), ('SET_EMP_NO', 2), ('SAVE_SALARY_CHANGE', 2),
                    ('SAVE_SALARY_CHANGE', 2), ('DELETE_EMPLOYEE', 5),
                    ('DELETE_EMPLOYEE', 5), ('ORG_CHART', 5), ('ORG_CHART', 5),
                    ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5)])
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertListEqual([(x.dependent_name,x.dependent_type) for x in d],
                   [('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                    ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('CHECK_3', 2),
                    ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2),
                    ('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2),
                    ('CHECK_4', 2), ('SET_EMP_NO', 2), ('SAVE_SALARY_CHANGE', 2),
                    ('SAVE_SALARY_CHANGE', 2), ('PHONE_LIST', 1),
                    ('DELETE_EMPLOYEE', 5), ('DELETE_EMPLOYEE', 5),
                    ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5),
                    ('ORG_CHART', 5), ('ORG_CHART', 5)])
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertListEqual([(x.dependent_name,x.dependent_type) for x in d],
                    [('RDB$9', 3), ('RDB$9', 3), ('CHECK_3', 2),
                     ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2),
                     ('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2),
                     ('CHECK_4', 2), ('SET_EMP_NO', 2),
                     ('SAVE_SALARY_CHANGE', 2), ('PHONE_LIST', 1),
                     ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                     ('SAVE_SALARY_CHANGE', 2), ('PHONE_LIST', 1),
                     ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                     ('DELETE_EMPLOYEE', 5), ('DELETE_EMPLOYEE', 5),
                     ('ORG_CHART', 5), ('ORG_CHART', 5),
                     ('ORG_CHART', 5), ('ORG_CHART', 5),
                     ('ORG_CHART', 5)])
                   #[('RDB$9', 3), ('RDB$9', 3), ('PHONE_LIST', 1),
                    #('PHONE_LIST', 1), ('PHONE_LIST', 1), ('CHECK_3', 2),
                    #('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2),
                    #('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2),
                    #('CHECK_4', 2), ('SET_EMP_NO', 2), ('SAVE_SALARY_CHANGE', 2),
                    #('PHONE_LIST', 1), ('PHONE_LIST', 1), ('SAVE_SALARY_CHANGE', 2),
                    #('PHONE_LIST', 1), ('ORG_CHART', 5), ('ORG_CHART', 5),
                    #('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5),
                    #('DELETE_EMPLOYEE', 5), ('DELETE_EMPLOYEE', 5)])
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertListEqual([(x.dependent_name,x.dependent_type) for x in d],
                   [('SAVE_SALARY_CHANGE', 2), ('SAVE_SALARY_CHANGE', 2), ('CHECK_3', 2),
                    ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_3', 2), ('CHECK_4', 2),
                    ('CHECK_4', 2), ('CHECK_4', 2), ('CHECK_4', 2), ('PHONE_LIST', 1),
                    ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1), ('PHONE_LIST', 1),
                    ('PHONE_LIST', 1), ('DELETE_EMPLOYEE', 5), ('DELETE_EMPLOYEE', 5),
                    ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5), ('ORG_CHART', 5),
                    ('ORG_CHART', 5), ('RDB$9', 3), ('RDB$9', 3), ('SET_EMP_NO', 2)]
                   )
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.id,131)
        self.assertEqual(c.dbkey_length,8)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(c.format,1)
        elif (self.con.ods > fdb.ODS_FB_20) and (self.con.ods < fdb.ODS_FB_30):
            self.assertEqual(c.format,2)
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(c.format,1)
        self.assertEqual(c.table_type,'PERSISTENT')
        if self.con.ods <= fdb.ODS_FB_21:
            self.assertEqual(c.security_class,'SQL$EMPLOYEE')
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(c.security_class,'SQL$7')
        elif self.con.ods == fdb.ODS_FB_30:
            self.assertEqual(c.security_class,'SQL$440')
        else:
            self.assertEqual(c.security_class,'SQL$482')
        self.assertIsNone(c.external_file)
        self.assertEqual(c.owner_name,'SYSDBA')
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(c.default_class,'SQL$DEFAULT5')
        elif (self.con.ods > fdb.ODS_FB_20) and (self.con.ods < fdb.ODS_FB_30):
            self.assertEqual(c.default_class,'SQL$DEFAULT7')
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(c.default_class,'SQL$DEFAULT54')
        self.assertEqual(c.flags,1)
        self.assertEqual(c.primary_key.name,'INTEG_27')
        self.assertListEqual([x.name for x in c.foreign_keys],
                             ['INTEG_28', 'INTEG_29'])
        self.assertListEqual([x.name for x in c.columns],
                             ['EMP_NO', 'FIRST_NAME', 'LAST_NAME', 'PHONE_EXT',
                              'HIRE_DATE', 'DEPT_NO', 'JOB_CODE', 'JOB_GRADE',
                              'JOB_COUNTRY', 'SALARY', 'FULL_NAME'])
        self.assertListEqual([x.name for x in c.constraints],
                             ['INTEG_18', 'INTEG_19', 'INTEG_20', 'INTEG_21',
                              'INTEG_22', 'INTEG_23', 'INTEG_24', 'INTEG_25',
                              'INTEG_26', 'INTEG_27', 'INTEG_28', 'INTEG_29',
                              'INTEG_30'])
        self.assertListEqual([x.name for x in c.indices],
            ['RDB$PRIMARY7', 'RDB$FOREIGN8', 'RDB$FOREIGN9', 'NAMEX'])
        self.assertListEqual([x.name for x in c.triggers],
                             ['SET_EMP_NO', 'SAVE_SALARY_CHANGE'])
        #
        self.assertEqual(c.get_column('EMP_NO').name,'EMP_NO')
        self.assertFalse(c.isgtt())
        self.assertTrue(c.ispersistent())
        self.assertFalse(c.isexternal())
        self.assertTrue(c.has_pkey())
        self.assertTrue(c.has_fkey())
        #
        self.assertEqual(c.get_sql_for('create'),"""CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
        self.assertEqual(c.get_sql_for('create',no_pk=True),"""CREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name)
)""")
        self.assertEqual(c.get_sql_for('recreate'),"""RECREATE TABLE EMPLOYEE (
  EMP_NO EMPNO NOT NULL,
  FIRST_NAME "FIRSTNAME" NOT NULL,
  LAST_NAME "LASTNAME" NOT NULL,
  PHONE_EXT VARCHAR(4),
  HIRE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,
  DEPT_NO DEPTNO NOT NULL,
  JOB_CODE JOBCODE NOT NULL,
  JOB_GRADE JOBGRADE NOT NULL,
  JOB_COUNTRY COUNTRYNAME NOT NULL,
  SALARY SALARY NOT NULL,
  FULL_NAME COMPUTED BY (last_name || ', ' || first_name),
  PRIMARY KEY (EMP_NO)
)""")
        self.assertEqual(c.get_sql_for('drop'),"DROP TABLE EMPLOYEE")
        # Identity colums
        if self.con.ods >= fdb.ODS_FB_30:
            c = self.con.schema.get_table('T5')
            self.assertEqual(c.get_sql_for('create'), """CREATE TABLE T5
(
  ID NUMERIC(10, 0) GENERATED BY DEFAULT AS IDENTITY,
  C1 VARCHAR(15),
  UQ BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100),
  PRIMARY KEY (ID)
)""")
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON TABLE EMPLOYEE IS NULL')

    def testView(self):
        # User view
        c = self.con.schema.get_view('PHONE_LIST')
        # common properties
        self.assertEqual(c.name,'PHONE_LIST')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment','create',
                                        'recreate', 'alter',
                                        'create_or_alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'PHONE_LIST')
        self.assertListEqual(c.get_dependents(),[])
        d = c.get_dependencies()
        if self.con.ods < fdb.ODS_FB_30:
            self.assertListEqual([(x.depended_on_name,x.field_name,x.depended_on_type) for x in d],
                   [('DEPARTMENT', 'DEPT_NO', 0), ('EMPLOYEE', 'DEPT_NO', 0),
                    ('DEPARTMENT', None, 0), ('EMPLOYEE', None, 0),
                    ('DEPARTMENT', 'PHONE_NO', 0), ('EMPLOYEE', 'PHONE_EXT', 0),
                    ('EMPLOYEE', 'LAST_NAME', 0), ('EMPLOYEE', 'EMP_NO', 0),
                    ('DEPARTMENT', 'LOCATION', 0), ('EMPLOYEE', 'FIRST_NAME', 0)])
        else:
            self.assertListEqual([(x.depended_on_name,x.field_name,x.depended_on_type) for x in d],
                   [('DEPARTMENT', 'DEPT_NO', 0), ('EMPLOYEE', 'DEPT_NO', 0),
                    ('DEPARTMENT', None, 0), ('EMPLOYEE', None, 0), ('EMPLOYEE', 'EMP_NO', 0),
                    ('EMPLOYEE', 'FIRST_NAME', 0), ('EMPLOYEE', 'LAST_NAME', 0),
                    ('EMPLOYEE', 'PHONE_EXT', 0), ('DEPARTMENT', 'LOCATION', 0),
                    ('DEPARTMENT', 'PHONE_NO', 0)]
                   )
        #
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(c.id,143)
        else:
            self.assertEqual(c.id,132)
        self.assertEqual(c.sql,"""SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.dbkey_length,16)
        self.assertEqual(c.format,1)
        if self.con.ods <= fdb.ODS_FB_21:
            self.assertEqual(c.security_class,'SQL$PHONE_LIST')
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(c.security_class,'SQL$8')
        elif self.con.ods == fdb.ODS_FB_30:
            self.assertEqual(c.security_class,'SQL$444')
        else:
            self.assertEqual(c.security_class,'SQL$483')
        self.assertEqual(c.owner_name,'SYSDBA')
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(c.default_class,'SQL$DEFAULT17')
        elif (self.con.ods > fdb.ODS_FB_20) and (self.con.ods < fdb.ODS_FB_30):
            self.assertEqual(c.default_class,'SQL$DEFAULT19')
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(c.default_class,'SQL$DEFAULT55')
        self.assertEqual(c.flags,1)
        self.assertListEqual([x.name for x in c.columns],['EMP_NO', 'FIRST_NAME',
                            'LAST_NAME', 'PHONE_EXT', 'LOCATION', 'PHONE_NO'])
        self.assertListEqual(c.triggers,[])
        #
        self.assertEqual(c.get_column('LAST_NAME').name,'LAST_NAME')
        self.assertFalse(c.has_checkoption())
        #
        self.assertEqual(c.get_sql_for('create'),
"""CREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('recreate'),
"""RECREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('drop'),"DROP VIEW PHONE_LIST")
        self.assertEqual(c.get_sql_for('alter',query='select * from country'),
            "ALTER VIEW PHONE_LIST \n   AS\n     select * from country")
        self.assertEqual(c.get_sql_for('alter',columns='country,currency',
                                       query='select * from country'),
            "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country")
        self.assertEqual(c.get_sql_for('alter',columns='country,currency',
                                       query='select * from country',check=True),
            "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country\n     WITH CHECK OPTION")
        self.assertEqual(c.get_sql_for('alter',columns=('country','currency'),
                                       query='select * from country',check=True),
            "ALTER VIEW PHONE_LIST (country,currency)\n   AS\n     select * from country\n     WITH CHECK OPTION")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',badparam='select * from country')
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,("Missing required parameter: 'query'.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
"""CREATE OR ALTER VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)
   AS
     SELECT
    emp_no, first_name, last_name, phone_ext, location, phone_no
    FROM employee, department
    WHERE employee.dept_no = department.dept_no""")
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON VIEW PHONE_LIST IS NULL')

    def testTrigger(self):
        # System trigger
        c = self.con.schema.get_trigger('RDB$TRIGGER_1')
        # common properties
        self.assertEqual(c.name,'RDB$TRIGGER_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'RDB$TRIGGER_1')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        # User trigger
        c = self.con.schema.get_trigger('SET_EMP_NO')
        # common properties
        self.assertEqual(c.name,'SET_EMP_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,
            ['comment','create', 'recreate', 'alter', 'create_or_alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'SET_EMP_NO')
        self.assertListEqual(c.get_dependents(),[])
        d = c.get_dependencies()
        self.assertListEqual([(x.depended_on_name,x.field_name,x.depended_on_type) for x in d],
               [('EMPLOYEE', 'EMP_NO', 0), ('EMP_NO_GEN', None, 14)])
        #
        self.assertEqual(c.relation.name,'EMPLOYEE')
        self.assertEqual(c.sequence,0)
        self.assertEqual(c.trigger_type,1)
        self.assertEqual(c.source,
            "AS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND")
        self.assertEqual(c.flags,1)
        #
        self.assertTrue(c.isactive())
        self.assertTrue(c.isbefore())
        self.assertFalse(c.isafter())
        self.assertFalse(c.isdbtrigger())
        self.assertTrue(c.isinsert())
        self.assertFalse(c.isupdate())
        self.assertFalse(c.isdelete())
        self.assertEqual(c.get_type_as_string(),'BEFORE INSERT')
        #
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(c.valid_blr)
            self.assertIsNone(c.engine_name)
            self.assertIsNone(c.entrypoint)
        else:
            self.assertEqual(c.valid_blr,1)
            self.assertIsNone(c.engine_name)
            self.assertIsNone(c.entrypoint)
        #
        self.assertEqual(c.get_sql_for('create'),
"""CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        self.assertEqual(c.get_sql_for('recreate'),
"""RECREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter')
        self.assertTupleEqual(cm.exception.args,
            ("Header or body definition required.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',declare="DECLARE VARIABLE i integer;")
        self.assertTupleEqual(cm.exception.args,
            ("Header or body definition required.",))
        self.assertEqual(c.get_sql_for('alter',fire_on='AFTER INSERT',
                active=False,sequence=0,
                declare='  DECLARE VARIABLE i integer;\n  DECLARE VARIABLE x integer;',
                code='  i = 1;\n  x = 2;'),
"""ALTER TRIGGER SET_EMP_NO INACTIVE
  AFTER INSERT
  POSITION 0
AS
  DECLARE VARIABLE i integer;
  DECLARE VARIABLE x integer;
BEGIN
  i = 1;
  x = 2;
END""")
        self.assertEqual(c.get_sql_for('alter',
                             declare=['DECLARE VARIABLE i integer;',
                                      'DECLARE VARIABLE x integer;'],
                             code=['i = 1;','x = 2;']),
"""ALTER TRIGGER SET_EMP_NO
AS
  DECLARE VARIABLE i integer;
  DECLARE VARIABLE x integer;
BEGIN
  i = 1;
  x = 2;
END""")
        self.assertEqual(c.get_sql_for('alter',active=False),
            "ALTER TRIGGER SET_EMP_NO INACTIVE")
        self.assertEqual(c.get_sql_for('alter',sequence=10,
                                       code=('i = 1;','x = 2;')),
"""ALTER TRIGGER SET_EMP_NO
  POSITION 10
AS
BEGIN
  i = 1;
  x = 2;
END""")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',fire_on='ON CONNECT')
        self.assertTupleEqual(cm.exception.args,
            ("Trigger type change is not allowed.",))
        self.assertEqual(c.get_sql_for('create_or_alter'),
"""CREATE OR ALTER TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE
BEFORE INSERT POSITION 0
AS
BEGIN
    if (new.emp_no is null) then
    new.emp_no = gen_id(emp_no_gen, 1);
END""")
        self.assertEqual(c.get_sql_for('drop'),"DROP TRIGGER SET_EMP_NO")
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON TRIGGER SET_EMP_NO IS NULL')
        # Multi-trigger
        c = self.con.schema.get_trigger('TR_MULTI')
        #
        self.assertTrue(c.isinsert())
        self.assertTrue(c.isupdate())
        self.assertTrue(c.isdelete())
        self.assertEqual(c.get_type_as_string(),
                         'AFTER INSERT OR UPDATE OR DELETE')
        # DB trigger
        c = self.con.schema.get_trigger('TR_CONNECT')
        #
        self.assertTrue(c.isdbtrigger())
        self.assertFalse(c.isinsert())
        self.assertFalse(c.isupdate())
        self.assertFalse(c.isdelete())
        self.assertEqual(c.get_type_as_string(),'ON CONNECT')

    def testProcedureParameter(self):
        # Input parameter
        c = self.con.schema.get_procedure('GET_EMP_PROJ').input_params[0]
        # common properties
        self.assertEqual(c.name,'EMP_NO')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'EMP_NO')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.procedure.name,'GET_EMP_PROJ')
        self.assertEqual(c.sequence,0)
        self.assertEqual(c.domain.name,'RDB$32')
        self.assertEqual(c.datatype,'SMALLINT')
        self.assertEqual(c.type_from,sm.PROCPAR_DATATYPE)
        self.assertIsNone(c.default)
        self.assertIsNone(c.collation)
        if self.con.ods <= fdb.ODS_FB_25:
            self.assertIsNone(c.mechanism)
        elif self.con.ods > fdb.ODS_FB_25:
            self.assertEqual(c.mechanism,0)
        self.assertIsNone(c.column)
        #
        self.assertTrue(c.isinput())
        self.assertTrue(c.isnullable())
        self.assertFalse(c.has_default())
        self.assertEqual(c.get_sql_definition(),'EMP_NO SMALLINT')
        # Output parameter
        c = self.con.schema.get_procedure('GET_EMP_PROJ').output_params[0]
        # common properties
        self.assertEqual(c.name,'PROJ_ID')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'PROJ_ID')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON PARAMETER GET_EMP_PROJ.PROJ_ID IS NULL')
        #
        self.assertFalse(c.isinput())
        self.assertEqual(c.get_sql_definition(),'PROJ_ID CHAR(5)')
    def testProcedure(self):
        c = self.con.schema.get_procedure('GET_EMP_PROJ')
        # common properties
        self.assertEqual(c.name,'GET_EMP_PROJ')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment','create',
                                        'recreate', 'alter',
                                        'create_or_alter', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'GET_EMP_PROJ')
        self.assertListEqual(c.get_dependents(),[])
        d = c.get_dependencies()
        self.assertListEqual([(x.depended_on_name,x.field_name,x.depended_on_type) for x in d],
               [('EMPLOYEE_PROJECT', 'PROJ_ID', 0), ('EMPLOYEE_PROJECT', 'EMP_NO', 0),
                ('EMPLOYEE_PROJECT', None, 0)])
        #
        self.assertEqual(c.id,1)
        self.assertEqual(c.source,"""BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        if self.con.ods < fdb.ODS_FB_25:
            self.assertEqual(c.security_class,'SQL$GET_EMP_PROJ')
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(c.security_class,'SQL$20')
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(c.security_class,'SQL$473')
        self.assertEqual(c.owner_name,'SYSDBA')
        self.assertListEqual([x.name for x in c.input_params],['EMP_NO'])
        self.assertListEqual([x.name for x in c.output_params],['PROJ_ID'])
        if self.con.engine_version >= 3.0:
            self.assertTrue(c.valid_blr)
            self.assertEqual(c.proc_type,1)
            self.assertIsNone(c.engine_name)
            self.assertIsNone(c.entrypoint)
            self.assertIsNone(c.package)
            self.assertIsNone(c.privacy)
        else:
            self.assertTrue(c.valid_blr)
            self.assertEqual(c.proc_type,1)
        #
        self.assertEqual(c.get_param('EMP_NO').name,'EMP_NO')
        self.assertEqual(c.get_param('PROJ_ID').name,'PROJ_ID')
        #
        self.assertEqual(c.get_sql_for('create'),
"""CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('create',no_code=True),
"""CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('recreate'),
"""RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('recreate',no_code=True),
"""RECREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('create_or_alter'),
"""CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
	FOR SELECT proj_id
		FROM employee_project
		WHERE emp_no = :emp_no
		INTO :proj_id
	DO
		SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('create_or_alter',no_code=True),
"""CREATE OR ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)
RETURNS (PROJ_ID CHAR(5))
AS
BEGIN
  SUSPEND;
END""")
        self.assertEqual(c.get_sql_for('drop'),"DROP PROCEDURE GET_EMP_PROJ")
        self.assertEqual(c.get_sql_for('alter',code="  /* PASS */"),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  /* PASS */
END""")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('alter',declare="DECLARE VARIABLE i integer;")
            self.assertTupleEqual(cm.exception.args,
                ("Missing required parameter: 'code'.",))
        self.assertEqual(c.get_sql_for('alter',code=''),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',input="IN1 integer",code=''),
"""ALTER PROCEDURE GET_EMP_PROJ (IN1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',output="OUT1 integer",code=''),
"""ALTER PROCEDURE GET_EMP_PROJ
RETURNS (OUT1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',input="IN1 integer",
                                       output="OUT1 integer",code=''),
"""ALTER PROCEDURE GET_EMP_PROJ (IN1 integer)
RETURNS (OUT1 integer)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       input=["IN1 integer","IN2 VARCHAR(10)"],
                                       code=''),
"""ALTER PROCEDURE GET_EMP_PROJ (
  IN1 integer,
  IN2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       output=["OUT1 integer","OUT2 VARCHAR(10)"],
                                       code=''),
"""ALTER PROCEDURE GET_EMP_PROJ
RETURNS (
  OUT1 integer,
  OUT2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',
                                       input=["IN1 integer","IN2 VARCHAR(10)"],
                                       output=["OUT1 integer","OUT2 VARCHAR(10)"],
                                       code=''),
"""ALTER PROCEDURE GET_EMP_PROJ (
  IN1 integer,
  IN2 VARCHAR(10)
)
RETURNS (
  OUT1 integer,
  OUT2 VARCHAR(10)
)
AS
BEGIN
END""")
        self.assertEqual(c.get_sql_for('alter',code="  -- line 1;\n  -- line 2;"),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  -- line 1;
  -- line 2;
END""")
        self.assertEqual(c.get_sql_for('alter',code=["-- line 1;","-- line 2;"]),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
BEGIN
  -- line 1;
  -- line 2;
END""")
        self.assertEqual(c.get_sql_for('alter',code="  /* PASS */",
                                       declare="  -- line 1;\n  -- line 2;"),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
  -- line 1;
  -- line 2;
BEGIN
  /* PASS */
END""")
        self.assertEqual(c.get_sql_for('alter',code="  /* PASS */",
                                       declare=["-- line 1;","-- line 2;"]),
"""ALTER PROCEDURE GET_EMP_PROJ
AS
  -- line 1;
  -- line 2;
BEGIN
  /* PASS */
END""")
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON PROCEDURE GET_EMP_PROJ IS NULL')
    def testRole(self):
        c = self.con.schema.get_role('TEST_ROLE')
        # common properties
        self.assertEqual(c.name,'TEST_ROLE')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['comment','create', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'TEST_ROLE')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.owner_name,'SYSDBA')
        #
        self.assertEqual(c.get_sql_for('create'),"CREATE ROLE TEST_ROLE")
        self.assertEqual(c.get_sql_for('drop'),"DROP ROLE TEST_ROLE")
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON ROLE TEST_ROLE IS NULL')
    def _mockFunction(self,name):
        f = None
        if name == 'STRLEN':
            f = sm.Function(self.con.schema,
                {'RDB$ENTRYPOINT': 'IB_UDF_strlen                  ',
                 'RDB$SYSTEM_FLAG': 0, 'RDB$RETURN_ARGUMENT': 0,
                 'RDB$MODULE_NAME': 'ib_udf', 'RDB$FUNCTION_TYPE': None,
                 'RDB$DESCRIPTION': None,
                 'RDB$FUNCTION_NAME': 'STRLEN                         '})
            f._load_arguments(
                [{'RDB$FIELD_PRECISION': 0, 'RDB$FIELD_LENGTH': 4,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 8, 'RDB$MECHANISM': 0,
                  'RDB$CHARACTER_SET_ID': None, 'RDB$CHARACTER_LENGTH': None,
                  'RDB$FUNCTION_NAME': 'STRLEN                         ',
                  'RDB$ARGUMENT_POSITION': 0},
                 {'RDB$FIELD_PRECISION': None, 'RDB$FIELD_LENGTH': 32767,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 40, 'RDB$MECHANISM': 1,
                  'RDB$CHARACTER_SET_ID': 0, 'RDB$CHARACTER_LENGTH': 32767,
                  'RDB$FUNCTION_NAME': 'STRLEN                         ',
                  'RDB$ARGUMENT_POSITION': 1}])
        elif name == 'STRING2BLOB':
            f = sm.Function(self.con.schema,
                {'RDB$ENTRYPOINT': 'string2blob                    ',
                 'RDB$SYSTEM_FLAG': 0, 'RDB$RETURN_ARGUMENT': 2,
                 'RDB$MODULE_NAME': 'fbudf', 'RDB$FUNCTION_TYPE': None,
                 'RDB$DESCRIPTION': None,
                 'RDB$FUNCTION_NAME': 'STRING2BLOB                    '})
            f._load_arguments(
                [{'RDB$FIELD_PRECISION': None, 'RDB$FIELD_LENGTH': 300,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 37, 'RDB$MECHANISM': 2,
                  'RDB$CHARACTER_SET_ID': 0, 'RDB$CHARACTER_LENGTH': 300,
                  'RDB$FUNCTION_NAME': 'STRING2BLOB                    ',
                  'RDB$ARGUMENT_POSITION': 1},
                 {'RDB$FIELD_PRECISION': None, 'RDB$FIELD_LENGTH': 8,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 261, 'RDB$MECHANISM': 3,
                  'RDB$CHARACTER_SET_ID': None, 'RDB$CHARACTER_LENGTH': None,
                  'RDB$FUNCTION_NAME': 'STRING2BLOB                    ',
                  'RDB$ARGUMENT_POSITION': 2}])
        elif name == 'LTRIM':
            f = sm.Function(self.con.schema,
                {'RDB$ENTRYPOINT': 'IB_UDF_ltrim                   ',
                 'RDB$SYSTEM_FLAG': 0, 'RDB$RETURN_ARGUMENT': 0,
                 'RDB$MODULE_NAME': 'ib_udf', 'RDB$FUNCTION_TYPE': None,
                 'RDB$DESCRIPTION': None,
                 'RDB$FUNCTION_NAME': 'LTRIM                          '})
            f._load_arguments(
                [{'RDB$FIELD_PRECISION': None, 'RDB$FIELD_LENGTH': 255,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 40, 'RDB$MECHANISM': -1,
                  'RDB$CHARACTER_SET_ID': 0, 'RDB$CHARACTER_LENGTH': 255,
                  'RDB$FUNCTION_NAME': 'LTRIM                          ',
                  'RDB$ARGUMENT_POSITION': 0},
                 {'RDB$FIELD_PRECISION': None, 'RDB$FIELD_LENGTH': 255,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 0,
                  'RDB$FIELD_TYPE': 40, 'RDB$MECHANISM': 1,
                  'RDB$CHARACTER_SET_ID': 0, 'RDB$CHARACTER_LENGTH': 255,
                  'RDB$FUNCTION_NAME': 'LTRIM                          ',
                  'RDB$ARGUMENT_POSITION': 1}])
        elif name == 'I64NVL':
            f = sm.Function(self.con.schema,
                {'RDB$ENTRYPOINT': 'idNvl                          ',
                 'RDB$SYSTEM_FLAG': 0, 'RDB$RETURN_ARGUMENT': 0,
                 'RDB$MODULE_NAME': 'fbudf', 'RDB$FUNCTION_TYPE': None,
                 'RDB$DESCRIPTION': None,
                 'RDB$FUNCTION_NAME': 'I64NVL                         '})
            f._load_arguments(
                [{'RDB$FIELD_PRECISION': 18, 'RDB$FIELD_LENGTH': 8,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 1,
                  'RDB$FIELD_TYPE': 16, 'RDB$MECHANISM': 2,
                  'RDB$CHARACTER_SET_ID': None, 'RDB$CHARACTER_LENGTH': None,
                  'RDB$FUNCTION_NAME': 'I64NVL                         ',
                  'RDB$ARGUMENT_POSITION': 0},
                 {'RDB$FIELD_PRECISION': 18, 'RDB$FIELD_LENGTH': 8,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 1,
                  'RDB$FIELD_TYPE': 16, 'RDB$MECHANISM': 2,
                  'RDB$CHARACTER_SET_ID': None, 'RDB$CHARACTER_LENGTH': None,
                  'RDB$FUNCTION_NAME': 'I64NVL                         ',
                  'RDB$ARGUMENT_POSITION': 1},
                 {'RDB$FIELD_PRECISION': 18, 'RDB$FIELD_LENGTH': 8,
                  'RDB$FIELD_SCALE': 0, 'RDB$FIELD_SUB_TYPE': 1,
                  'RDB$FIELD_TYPE': 16, 'RDB$MECHANISM': 2,
                  'RDB$CHARACTER_SET_ID': None, 'RDB$CHARACTER_LENGTH': None,
                  'RDB$FUNCTION_NAME': 'I64NVL                         ',
                  'RDB$ARGUMENT_POSITION': 2}])
        if f:
            return f
        else:
            raise Exception("Udefined function for mock.")
    def testFunctionArgument(self):
        f = self._mockFunction('STRLEN')
        c = f.arguments[0]
        self.assertEqual(len(f.arguments),1)
        # common properties
        self.assertEqual(c.name,'STRLEN_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,[])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'STRLEN_1')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.function.name,'STRLEN')
        self.assertEqual(c.position,1)
        self.assertEqual(c.mechanism,1)
        self.assertEqual(c.field_type,40)
        self.assertEqual(c.length,32767)
        self.assertEqual(c.scale,0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type,0)
        self.assertEqual(c.character_length,32767)
        self.assertEqual(c.character_set.name,'NONE')
        self.assertEqual(c.datatype,'CSTRING(32767)')
        #
        self.assertFalse(c.isbyvalue())
        self.assertTrue(c.isbyreference())
        self.assertFalse(c.isbydescriptor())
        self.assertFalse(c.iswithnull())
        self.assertFalse(c.isfreeit())
        self.assertFalse(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'CSTRING(32767)')
        #
        c = f.returns
        #
        self.assertEqual(c.position,0)
        self.assertEqual(c.mechanism,0)
        self.assertEqual(c.field_type,8)
        self.assertEqual(c.length,4)
        self.assertEqual(c.scale,0)
        self.assertEqual(c.precision,0)
        self.assertEqual(c.sub_type,0)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype,'INTEGER')
        #
        self.assertTrue(c.isbyvalue())
        self.assertFalse(c.isbyreference())
        self.assertFalse(c.isbydescriptor())
        self.assertFalse(c.iswithnull())
        self.assertFalse(c.isfreeit())
        self.assertTrue(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'INTEGER BY VALUE')
        #
        f = self._mockFunction('STRING2BLOB')
        self.assertEqual(len(f.arguments),2)
        c = f.arguments[0]
        self.assertEqual(c.function.name,'STRING2BLOB')
        self.assertEqual(c.position,1)
        self.assertEqual(c.mechanism,2)
        self.assertEqual(c.field_type,37)
        self.assertEqual(c.length,300)
        self.assertEqual(c.scale,0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type,0)
        self.assertEqual(c.character_length,300)
        self.assertEqual(c.character_set.name,'NONE')
        self.assertEqual(c.datatype,'VARCHAR(300)')
        #
        self.assertFalse(c.isbyvalue())
        self.assertFalse(c.isbyreference())
        self.assertTrue(c.isbydescriptor())
        self.assertFalse(c.iswithnull())
        self.assertFalse(c.isfreeit())
        self.assertFalse(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'VARCHAR(300) BY DESCRIPTOR')
        #
        c = f.arguments[1]
        self.assertIs(f.arguments[1],f.returns)
        self.assertEqual(c.function.name,'STRING2BLOB')
        self.assertEqual(c.position,2)
        self.assertEqual(c.mechanism,3)
        self.assertEqual(c.field_type,261)
        self.assertEqual(c.length,8)
        self.assertEqual(c.scale,0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type,0)
        self.assertIsNone(c.character_length)
        self.assertIsNone(c.character_set)
        self.assertEqual(c.datatype,'BLOB')
        #
        self.assertFalse(c.isbyvalue())
        self.assertFalse(c.isbyreference())
        self.assertFalse(c.isbydescriptor())
        self.assertTrue(c.isbydescriptor(any=True))
        self.assertFalse(c.iswithnull())
        self.assertFalse(c.isfreeit())
        self.assertTrue(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'BLOB')
        #
        f = self._mockFunction('LTRIM')
        self.assertEqual(len(f.arguments),1)
        c = f.arguments[0]
        self.assertEqual(c.function.name,'LTRIM')
        self.assertEqual(c.position,1)
        self.assertEqual(c.mechanism,1)
        self.assertEqual(c.field_type,40)
        self.assertEqual(c.length,255)
        self.assertEqual(c.scale,0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type,0)
        self.assertEqual(c.character_length,255)
        self.assertEqual(c.character_set.name,'NONE')
        self.assertEqual(c.datatype,'CSTRING(255)')
        #
        self.assertFalse(c.isbyvalue())
        self.assertTrue(c.isbyreference())
        self.assertFalse(c.isbydescriptor())
        self.assertFalse(c.iswithnull())
        self.assertFalse(c.isfreeit())
        self.assertFalse(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'CSTRING(255)')
        #
        c = f.returns
        self.assertEqual(c.function.name,'LTRIM')
        self.assertEqual(c.position,0)
        self.assertEqual(c.mechanism,1)
        self.assertEqual(c.field_type,40)
        self.assertEqual(c.length,255)
        self.assertEqual(c.scale,0)
        self.assertIsNone(c.precision)
        self.assertEqual(c.sub_type,0)
        self.assertEqual(c.character_length,255)
        self.assertEqual(c.character_set.name,'NONE')
        self.assertEqual(c.datatype,'CSTRING(255)')
        #
        self.assertFalse(c.isbyvalue())
        self.assertTrue(c.isbyreference())
        self.assertFalse(c.isbydescriptor())
        self.assertFalse(c.isbydescriptor(any=True))
        self.assertFalse(c.iswithnull())
        self.assertTrue(c.isfreeit())
        self.assertTrue(c.isreturning())
        self.assertEqual(c.get_sql_definition(),'CSTRING(255)')
        #
        f = self._mockFunction('I64NVL')
        self.assertEqual(len(f.arguments),2)
        for a in f.arguments:
            self.assertEqual(a.datatype,'NUMERIC(18, 0)')
            self.assertTrue(a.isbydescriptor())
            self.assertEqual(a.get_sql_definition(),
                             'NUMERIC(18, 0) BY DESCRIPTOR')
        self.assertEqual(f.returns.datatype,'NUMERIC(18, 0)')
        self.assertTrue(f.returns.isbydescriptor())
        self.assertEqual(f.returns.get_sql_definition(),
                         'NUMERIC(18, 0) BY DESCRIPTOR')
    def testFunction(self):
        c = self._mockFunction('STRLEN')
        self.assertEqual(len(c.arguments),1)
        # common properties
        self.assertEqual(c.name,'STRLEN')
        self.assertIsNone(c.description)
        self.assertIsNone(c.package)
        self.assertIsNone(c.engine_mame)
        self.assertIsNone(c.private_flag)
        self.assertIsNone(c.source)
        self.assertIsNone(c.id)
        self.assertIsNone(c.valid_blr)
        self.assertIsNone(c.security_class)
        self.assertIsNone(c.owner_name)
        self.assertIsNone(c.legacy_flag)
        self.assertIsNone(c.deterministic_flag)
        self.assertListEqual(c.actions,['comment','declare', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'STRLEN')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        self.assertFalse(c.ispackaged())
        #
        self.assertEqual(c.module_name,'ib_udf')
        self.assertEqual(c.entrypoint,'IB_UDF_strlen')
        self.assertEqual(c.returns.name,'STRLEN_0')
        self.assertListEqual([a.name for a in c.arguments],['STRLEN_1'])
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertFalse(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('drop'),"DROP EXTERNAL FUNCTION STRLEN")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('drop',badparam='')
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('declare'),
"""DECLARE EXTERNAL FUNCTION STRLEN
  CSTRING(32767)
RETURNS INTEGER BY VALUE
ENTRY_POINT 'IB_UDF_strlen'
MODULE_NAME 'ib_udf'""")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('declare',badparam='')
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('comment'),
            'COMMENT ON EXTERNAL FUNCTION STRLEN IS NULL')
        #
        c = self._mockFunction('STRING2BLOB')
        self.assertEqual(len(c.arguments),2)
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertTrue(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('declare'),
"""DECLARE EXTERNAL FUNCTION STRING2BLOB
  VARCHAR(300) BY DESCRIPTOR,
  BLOB
RETURNS PARAMETER 2
ENTRY_POINT 'string2blob'
MODULE_NAME 'fbudf'""")
        #
        c = self._mockFunction('LTRIM')
        self.assertEqual(len(c.arguments),1)
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertFalse(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('declare'),
"""DECLARE EXTERNAL FUNCTION LTRIM
  CSTRING(255)
RETURNS CSTRING(255) FREE_IT
ENTRY_POINT 'IB_UDF_ltrim'
MODULE_NAME 'ib_udf'""")
        #
        c = self._mockFunction('I64NVL')
        self.assertEqual(len(c.arguments),2)
        #
        self.assertTrue(c.has_arguments())
        self.assertTrue(c.has_return())
        self.assertFalse(c.has_return_argument())
        #
        self.assertEqual(c.get_sql_for('declare'),
"""DECLARE EXTERNAL FUNCTION I64NVL
  NUMERIC(18, 0) BY DESCRIPTOR,
  NUMERIC(18, 0) BY DESCRIPTOR
RETURNS NUMERIC(18, 0) BY DESCRIPTOR
ENTRY_POINT 'idNvl'
MODULE_NAME 'fbudf'""")
        #
        # Internal PSQL functions (Firebird 3.0)
        if self.con.ods >= fdb.ODS_FB_30:
            c = self.con.schema.get_function('F2')
            # common properties
            self.assertEqual(c.name,'F2')
            self.assertIsNone(c.description)
            self.assertIsNone(c.package)
            self.assertIsNone(c.engine_mame)
            self.assertIsNone(c.private_flag)
            self.assertEqual(c.source,'BEGIN\n  RETURN X+1;\nEND')
            self.assertEqual(c.id,3)
            self.assertTrue(c.valid_blr)
            self.assertEqual(c.security_class,'SQL$588')
            self.assertEqual(c.owner_name,'SYSDBA')
            self.assertEqual(c.legacy_flag,0)
            self.assertEqual(c.deterministic_flag,0)
            #
            self.assertListEqual(c.actions,['create','recreate','alter','create_or_alter','drop'])
            self.assertFalse(c.issystemobject())
            self.assertEqual(c.get_quoted_name(),'F2')
            self.assertListEqual(c.get_dependents(),[])
            self.assertListEqual(c.get_dependencies(),[])
            #
            self.assertIsNone(c.module_name)
            self.assertIsNone(c.entrypoint)
            self.assertEqual(c.returns.name,'F2_0')
            self.assertListEqual([a.name for a in c.arguments],['X'])
            #
            self.assertTrue(c.has_arguments())
            self.assertTrue(c.has_return())
            self.assertFalse(c.has_return_argument())
            self.assertFalse(c.ispackaged())
            #
            self.assertEqual(c.get_sql_for('drop'),"DROP FUNCTION F2")
            self.assertEqual(c.get_sql_for('create'),
    """CREATE FUNCTION F2 (X INTEGER)
    RETURNS INTEGER
    AS
    BEGIN
      RETURN X+1;
    END""")
            self.assertEqual(c.get_sql_for('create',no_code=True),
    """CREATE FUNCTION F2 (X INTEGER)
    RETURNS INTEGER
    AS
    BEGIN
    END""")
            self.assertEqual(c.get_sql_for('recreate'),
    """RECREATE FUNCTION F2 (X INTEGER)
    RETURNS INTEGER
    AS
    BEGIN
      RETURN X+1;
    END""")

            self.assertEqual(c.get_sql_for('create_or_alter'),
    """CREATE OR ALTER FUNCTION F2 (X INTEGER)
    RETURNS INTEGER
    AS
    BEGIN
      RETURN X+1;
    END""")
            with self.assertRaises(fdb.ProgrammingError) as cm:
                c.get_sql_for('alter',declare="DECLARE VARIABLE i integer;",code='')
            self.assertTupleEqual(cm.exception.args,
                ("Missing required parameter: 'returns'.",))
            with self.assertRaises(fdb.ProgrammingError) as cm:
                c.get_sql_for('alter',declare="DECLARE VARIABLE i integer;",returns='INTEGER')
            self.assertTupleEqual(cm.exception.args,
                ("Missing required parameter: 'code'.",))
            self.assertEqual(c.get_sql_for('alter',returns='INTEGER',code=''),
    """ALTER FUNCTION F2
    RETURNS INTEGER
    AS
    BEGIN
    END""")
            self.assertEqual(c.get_sql_for('alter',arguments="IN1 integer",returns='INTEGER',code=''),
    """ALTER FUNCTION F2 (IN1 integer)
    RETURNS INTEGER
    AS
    BEGIN
    END""")
            self.assertEqual(c.get_sql_for('alter',returns='INTEGER',
                                           arguments=["IN1 integer","IN2 VARCHAR(10)"],
                                           code=''),
    """ALTER FUNCTION F2 (
      IN1 integer,
      IN2 VARCHAR(10)
    )
    RETURNS INTEGER
    AS
    BEGIN
    END""")
            #
            c = self.con.schema.get_function('FX')
            self.assertEqual(c.get_sql_for('create'),
    """CREATE FUNCTION FX (
      F TYPE OF "FIRSTNAME",
      L TYPE OF COLUMN CUSTOMER.CONTACT_LAST
    )
    RETURNS VARCHAR(35)
    AS
    BEGIN
      RETURN L || ', ' || F;
    END""")
            #
            c = self.con.schema.get_function('F1')
            self.assertEqual(c.name,'F1')
            self.assertIsNotNone(c.package)
            self.assertIsInstance(c.package,sm.Package)
            self.assertListEqual(c.actions,[])
            self.assertTrue(c.private_flag)
            self.assertTrue(c.ispackaged())

    def testDatabaseFile(self):
        # We have to use mock
        c = sm.DatabaseFile(self.con.schema,{'RDB$FILE_LENGTH': 1000,
                            'RDB$FILE_NAME': '/path/dbfile.f02',
                            'RDB$FILE_START': 500,
                            'RDB$FILE_SEQUENCE': 1})
        # common properties
        self.assertEqual(c.name,'FILE_1')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,[])
        self.assertTrue(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'FILE_1')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.filename,'/path/dbfile.f02')
        self.assertEqual(c.sequence,1)
        self.assertEqual(c.start,500)
        self.assertEqual(c.length,1000)
        #
    def testShadow(self):
        # We have to use mocks
        c = sm.Shadow(self.con.schema,{'RDB$FILE_FLAGS': 1,
                                       'RDB$SHADOW_NUMBER': 3})
        files = []
        files.append(sm.DatabaseFile(self.con.schema,{'RDB$FILE_LENGTH': 500,
                                    'RDB$FILE_NAME': '/path/shadow.sf1',
                                    'RDB$FILE_START': 0,
                                    'RDB$FILE_SEQUENCE': 0}))
        files.append(sm.DatabaseFile(self.con.schema,{'RDB$FILE_LENGTH': 500,
                                    'RDB$FILE_NAME': '/path/shadow.sf2',
                                    'RDB$FILE_START': 1000,
                                    'RDB$FILE_SEQUENCE': 1}))
        files.append(sm.DatabaseFile(self.con.schema,{'RDB$FILE_LENGTH': 0,
                                    'RDB$FILE_NAME': '/path/shadow.sf3',
                                    'RDB$FILE_START': 1500,
                                    'RDB$FILE_SEQUENCE': 2}))
        c.__dict__['_Shadow__files'] = files
        # common properties
        self.assertEqual(c.name,'SHADOW_3')
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['create', 'drop'])
        self.assertFalse(c.issystemobject())
        self.assertEqual(c.get_quoted_name(),'SHADOW_3')
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertEqual(c.id,3)
        self.assertEqual(c.flags,1)
        self.assertListEqual([(f.name,f.filename,f.start,f.length) for f in c.files],
               [('FILE_0', '/path/shadow.sf1', 0, 500),
                ('FILE_1', '/path/shadow.sf2', 1000, 500),
                ('FILE_2', '/path/shadow.sf3', 1500, 0)])
        #
        self.assertFalse(c.isconditional())
        self.assertFalse(c.isinactive())
        self.assertFalse(c.ismanual())
        #
        self.assertEqual(c.get_sql_for('create'),
"""CREATE SHADOW 3 AUTO '/path/shadow.sf1' LENGTH 500
  FILE '/path/shadow.sf2' STARTING AT 1000 LENGTH 500
  FILE '/path/shadow.sf3' STARTING AT 1500""")
        self.assertEqual(c.get_sql_for('drop'),"DROP SHADOW 3")
        self.assertEqual(c.get_sql_for('drop',preserve=True),"DROP SHADOW 3 PRESERVE FILE")
    def testPrivilegeBasic(self):
        p = self.con.schema.get_procedure('ALL_LANGS')
        #
        self.assertIsInstance(p.privileges,list)
        self.assertEqual(len(p.privileges),2)
        c = p.privileges[0]
        # common properties
        self.assertIsNone(c.name)
        self.assertIsNone(c.description)
        self.assertListEqual(c.actions,['grant', 'revoke'])
        self.assertTrue(c.issystemobject())
        self.assertIsNone(c.get_quoted_name())
        self.assertListEqual(c.get_dependents(),[])
        self.assertListEqual(c.get_dependencies(),[])
        #
        self.assertIsInstance(c.user,fdb.services.User)
        self.assertIn(c.user.name,['SYSDBA','PUBLIC'])
        self.assertIsInstance(c.grantor,fdb.services.User)
        self.assertEqual(c.grantor.name,'SYSDBA')
        self.assertEqual(c.privilege,'X')
        self.assertIsInstance(c.subject,sm.Procedure)
        self.assertEqual(c.subject.name,'ALL_LANGS')
        self.assertIn(c.user_name,['SYSDBA','PUBLIC'])
        self.assertEqual(c.user_type,self.con.schema.enum_object_type_codes['USER'])
        self.assertEqual(c.grantor_name,'SYSDBA')
        self.assertEqual(c.subject_name,'ALL_LANGS')
        self.assertEqual(c.subject_type,self.con.schema.enum_object_type_codes['PROCEDURE'])
        self.assertIsNone(c.field_name)
        #
        self.assertFalse(c.has_grant())
        self.assertFalse(c.isselect())
        self.assertFalse(c.isinsert())
        self.assertFalse(c.isupdate())
        self.assertFalse(c.isdelete())
        self.assertTrue(c.isexecute())
        self.assertFalse(c.isreference())
        self.assertFalse(c.ismembership())
        #
        self.assertEqual(c.get_sql_for('grant'),
            "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA")
        self.assertEqual(c.get_sql_for('grant',grantors=[]),
            "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA GRANTED BY SYSDBA")
        self.assertEqual(c.get_sql_for('grant',grantors=['SYSDBA','TEST_USER']),
            "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO SYSDBA")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('grant',badparam=True)
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        self.assertEqual(c.get_sql_for('revoke'),
            "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA")
        self.assertEqual(c.get_sql_for('revoke',grantors=[]),
            "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA GRANTED BY SYSDBA")
        self.assertEqual(c.get_sql_for('revoke',grantors=['SYSDBA','TEST_USER']),
            "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM SYSDBA")
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('revoke',grant_option=True)
        self.assertTupleEqual(cm.exception.args,
            ("Can't revoke grant option that wasn't granted.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            c.get_sql_for('revoke',badparam=True)
        self.assertTupleEqual(cm.exception.args,
            ("Unsupported parameter(s) 'badparam'",))
        c = p.privileges[1]
        self.assertEqual(c.get_sql_for('grant'),
            "GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION")
        self.assertEqual(c.get_sql_for('revoke'),
            "REVOKE EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC")
        self.assertEqual(c.get_sql_for('revoke',grant_option=True),
            "REVOKE GRANT OPTION FOR EXECUTE ON PROCEDURE ALL_LANGS FROM PUBLIC")
        # get_privileges_of()
        u = fdb.services.User('PUBLIC')
        p = self.con.schema.get_privileges_of(u)
        if self.con.ods <= fdb.ODS_FB_20:
            self.assertEqual(len(p),66)
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(len(p),115)
        else:
            self.assertEqual(len(p),68)
        with self.assertRaises(fdb.ProgrammingError) as cm:
            p = self.con.schema.get_privileges_of('PUBLIC')
        self.assertTupleEqual(cm.exception.args,
            ("Unknown user_type code.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            p = self.con.schema.get_privileges_of('PUBLIC',50)
        self.assertTupleEqual(cm.exception.args,
            ("Unknown user_type code.",))
        #
    def testPrivilegeExtended(self):
        def get_privilege(obj,privilege):
            x = [x for x in obj.privileges if x.privilege == privilege]
            return x[0]
        p = []
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ALL_LANGS',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': None}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ALL_LANGS',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ALL_LANGS',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'TEST_ROLE',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ALL_LANGS',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 13,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ALL_LANGS',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'T_USER',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': 'CURRENCY',
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': 'COUNTRY',
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': 'COUNTRY',
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': 'CURRENCY',
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'COUNTRY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'ORG_CHART',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 5,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'ORG_CHART',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 5,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ORG_CHART',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': None}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'ORG_CHART',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'PHONE_LIST',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': 'EMP_NO',
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'U',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'D',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'R',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'RDB$PAGES',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SYSDBA',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'SHIP_ORDER',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': None}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PUBLIC',
                                               'RDB$PRIVILEGE': 'X',
                                               'RDB$RELATION_NAME': 'SHIP_ORDER',
                                               'RDB$OBJECT_TYPE': 5,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 1}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'T_USER',
                                               'RDB$PRIVILEGE': 'M',
                                               'RDB$RELATION_NAME': 'TEST_ROLE',
                                               'RDB$OBJECT_TYPE': 13,
                                               'RDB$USER_TYPE': 8,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'SAVE_SALARY_CHANGE',
                                               'RDB$PRIVILEGE': 'I',
                                               'RDB$RELATION_NAME': 'SALARY_HISTORY',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 2,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PHONE_LIST',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'DEPARTMENT',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 1,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        p.append(sm.Privilege(self.con.schema,{'RDB$USER': 'PHONE_LIST',
                                               'RDB$PRIVILEGE': 'S',
                                               'RDB$RELATION_NAME': 'EMPLOYEE',
                                               'RDB$OBJECT_TYPE': 0,
                                               'RDB$USER_TYPE': 1,
                                               'RDB$FIELD_NAME': None,
                                               'RDB$GRANTOR': 'SYSDBA',
                                               'RDB$GRANT_OPTION': 0}))
        #
        self.con.schema.__dict__['_Schema__privileges'] = p
        # Table
        p = self.con.schema.get_table('COUNTRY')
        self.assertEqual(len(p.privileges),19)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']),5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']),5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'T_USER']),9)
        #
        self.assertTrue(get_privilege(p,'S').isselect())
        self.assertTrue(get_privilege(p,'I').isinsert())
        self.assertTrue(get_privilege(p,'U').isupdate())
        self.assertTrue(get_privilege(p,'D').isdelete())
        self.assertTrue(get_privilege(p,'R').isreference())
        #
        x = p.privileges[0]
        self.assertIsInstance(x.subject,sm.Table)
        self.assertEqual(x.subject.name,p.name)
        # TableColumn
        p = p.get_column('CURRENCY')
        self.assertEqual(len(p.privileges),2)
        x = p.privileges[0]
        self.assertIsInstance(x.subject,sm.Table)
        self.assertEqual(x.field_name,p.name)
        # View
        p = self.con.schema.get_view('PHONE_LIST')
        self.assertEqual(len(p.privileges),11)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']),5)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']),6)
        #
        x = p.privileges[0]
        self.assertIsInstance(x.subject,sm.View)
        self.assertEqual(x.subject.name,p.name)
        # ViewColumn
        p = p.get_column('EMP_NO')
        self.assertEqual(len(p.privileges),1)
        x = p.privileges[0]
        self.assertIsInstance(x.subject,sm.View)
        self.assertEqual(x.field_name,p.name)
        # Procedure
        p = self.con.schema.get_procedure('ORG_CHART')
        self.assertEqual(len(p.privileges),2)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'SYSDBA']),1)
        self.assertEqual(len([x for x in p.privileges if x.user_name == 'PUBLIC']),1)
        #
        x = p.privileges[0]
        self.assertFalse(x.has_grant())
        self.assertIsInstance(x.subject,sm.Procedure)
        self.assertEqual(x.subject.name,p.name)
        #
        x = p.privileges[1]
        self.assertTrue(x.has_grant())
        # Role
        p = self.con.schema.get_role('TEST_ROLE')
        self.assertEqual(len(p.privileges),1)
        x = p.privileges[0]
        self.assertIsInstance(x.user,sm.Role)
        self.assertEqual(x.user.name,p.name)
        self.assertTrue(x.isexecute())
        # Trigger as grantee
        p = self.con.schema.get_table('SALARY_HISTORY')
        x = p.privileges[0]
        self.assertIsInstance(x.user,sm.Trigger)
        self.assertEqual(x.user.name,'SAVE_SALARY_CHANGE')
        # View as grantee
        p = self.con.schema.get_view('PHONE_LIST')
        x = self.con.schema.get_privileges_of(p)
        self.assertEqual(len(x),2)
        x = x[0]
        self.assertIsInstance(x.user,sm.View)
        self.assertEqual(x.user.name,'PHONE_LIST')
        # get_grants()
        self.assertListEqual(sm.get_grants(p.privileges),
            ['GRANT REFERENCES(EMP_NO) ON PHONE_LIST TO PUBLIC',
             'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
             'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON PHONE_LIST TO SYSDBA WITH GRANT OPTION'])
        p = self.con.schema.get_table('COUNTRY')
        self.assertListEqual(sm.get_grants(p.privileges),
            ['GRANT DELETE, INSERT, UPDATE ON COUNTRY TO PUBLIC',
             'GRANT REFERENCES, SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
             'GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE ON COUNTRY TO SYSDBA WITH GRANT OPTION',
             'GRANT DELETE, INSERT, REFERENCES(COUNTRY,CURRENCY), SELECT, UPDATE(COUNTRY,CURRENCY) ON COUNTRY TO T_USER'])
        p = self.con.schema.get_role('TEST_ROLE')
        self.assertListEqual(sm.get_grants(p.privileges),['GRANT EXECUTE ON PROCEDURE ALL_LANGS TO TEST_ROLE WITH GRANT OPTION'])
        p = self.con.schema.get_table('SALARY_HISTORY')
        self.assertListEqual(sm.get_grants(p.privileges),
            ['GRANT INSERT ON SALARY_HISTORY TO TRIGGER SAVE_SALARY_CHANGE'])
        p = self.con.schema.get_procedure('ORG_CHART')
        self.assertListEqual(sm.get_grants(p.privileges),
            ['GRANT EXECUTE ON PROCEDURE ORG_CHART TO PUBLIC WITH GRANT OPTION',
             'GRANT EXECUTE ON PROCEDURE ORG_CHART TO SYSDBA'])
        #
    def testPackage(self):
        if self.con.ods < fdb.ODS_FB_30:
            return
        c = self.con.schema.get_package('TEST')
        # common properties
        self.assertEqual(c.name,'TEST')
        self.assertIsNone(c.description)
        self.assertFalse(c.issystemobject())
        self.assertListEqual(c.actions,
            ['create', 'recreate', 'create_or_alter', 'alter', 'drop'])
        self.assertEqual(c.get_quoted_name(),'TEST')
        self.assertEqual(c.owner_name,'SYSDBA')
        self.assertEqual(c.security_class,'SQL$575')
        self.assertEqual(c.header,"""BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        self.assertEqual(c.body,"""BEGIN
  FUNCTION F1(I INT) RETURNS INT; -- private function

  PROCEDURE P1(I INT) RETURNS (O INT)
  AS
  BEGIN
  END

  FUNCTION F1(I INT) RETURNS INT
  AS
  BEGIN
    RETURN F(I)+10;
  END

  FUNCTION F(X INT) RETURNS INT
  AS
  BEGIN
    RETURN X+1;
  END
END""")
        self.assertListEqual(c.get_dependents(),[])
        self.assertEqual(len(c.get_dependencies()),1)
        self.assertEqual(len(c.functions), 2)
        self.assertEqual(len(c.procedures), 1)
        #
        self.assertEqual(c.get_sql_for('create'),"""CREATE PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        self.assertEqual(c.get_sql_for('create',body=True),"""CREATE PACKAGE BODY TEST
AS
BEGIN
  FUNCTION F1(I INT) RETURNS INT; -- private function

  PROCEDURE P1(I INT) RETURNS (O INT)
  AS
  BEGIN
  END

  FUNCTION F1(I INT) RETURNS INT
  AS
  BEGIN
    RETURN F(I)+10;
  END

  FUNCTION F(X INT) RETURNS INT
  AS
  BEGIN
    RETURN X+1;
  END
END""")
        self.assertEqual(c.get_sql_for('alter',header="FUNCTION F2(I INT) RETURNS INT;"),
"""ALTER PACKAGE TEST
AS
BEGIN
FUNCTION F2(I INT) RETURNS INT;
END""")
        self.assertEqual(c.get_sql_for('drop'),"""DROP PACKAGE TEST""")
        self.assertEqual(c.get_sql_for('drop',body=True),"""DROP PACKAGE BODY TEST""")
        self.assertEqual( c.get_sql_for('create_or_alter'),"""CREATE OR ALTER PACKAGE TEST
AS
BEGIN
  PROCEDURE P1(I INT) RETURNS (O INT); -- public procedure
  FUNCTION F(X INT) RETURNS INT;
END""")
        #
    def testVisitor(self):
        v = SchemaVisitor(self,'create',follow='dependencies')
        c = self.con.schema.get_procedure('ALL_LANGS')
        c.accept_visitor(v)
        self.maxDiff = None
        output = "CREATE TABLE JOB (\n  JOB_CODE JOBCODE NOT NULL,\n" \
            "  JOB_GRADE JOBGRADE NOT NULL,\n" \
            "  JOB_COUNTRY COUNTRYNAME NOT NULL,\n" \
            "  JOB_TITLE VARCHAR(25) NOT NULL,\n" \
            "  MIN_SALARY SALARY NOT NULL,\n" \
            "  MAX_SALARY SALARY NOT NULL,\n" \
            "  JOB_REQUIREMENT BLOB SUB_TYPE TEXT SEGMENT SIZE 400,\n" \
            "  LANGUAGE_REQ VARCHAR(15)[5],\n" \
            "  PRIMARY KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)\n" \
            ")\n" \
            "CREATE PROCEDURE SHOW_LANGS (\n" \
            "  CODE VARCHAR(5),\n" \
            "  GRADE SMALLINT,\n" \
            "  CTY VARCHAR(15)\n" \
            ")\n" \
            "RETURNS (LANGUAGES VARCHAR(15))\n" \
            "AS\n" \
            "DECLARE VARIABLE i INTEGER;\n" \
            "BEGIN\n" \
            "  i = 1;\n" \
            "  WHILE (i <= 5) DO\n" \
            "  BEGIN\n" \
            "    SELECT language_req[:i] FROM joB\n" \
            "    WHERE ((job_code = :code) AND (job_grade = :grade) AND (job_country = :cty)\n" \
            "           AND (language_req IS NOT NULL))\n" \
            "    INTO :languages;\n" \
            "    IF (languages = ' ') THEN  /* Prints 'NULL' instead of blanks */\n" \
            "       languages = 'NULL';         \n" \
            "    i = i +1;\n" \
            "    SUSPEND;\n" \
            "  END\nEND\nCREATE PROCEDURE ALL_LANGS\n" \
            "RETURNS (\n" \
            "  CODE VARCHAR(5),\n" \
            "  GRADE VARCHAR(5),\n" \
            "  COUNTRY VARCHAR(15),\n" \
            "  LANG VARCHAR(15)\n" \
            ")\n" \
            "AS\n" \
            "BEGIN\n" \
            "\tFOR SELECT job_code, job_grade, job_country FROM job \n" \
            "\t\tINTO :code, :grade, :country\n" \
            "\n" \
            "\tDO\n" \
            "\tBEGIN\n" \
            "\t    FOR SELECT languages FROM show_langs \n" \
            " \t\t    (:code, :grade, :country) INTO :lang DO\n" \
            "\t        SUSPEND;\n" \
            "\t    /* Put nice separators between rows */\n" \
            "\t    code = '=====';\n" \
            "\t    grade = '=====';\n" \
            "\t    country = '===============';\n" \
            "\t    lang = '==============';\n" \
            "\t    SUSPEND;\n" \
            "\tEND\n" \
            "    END\n"
        self.assertMultiLineEqual(self.output.getvalue(),output)

        v = SchemaVisitor(self,'drop',follow='dependents')
        c = self.con.schema.get_table('JOB')
        self.clear_output()
        c.accept_visitor(v)
        self.assertEqual(self.output.getvalue(),"""DROP PROCEDURE ALL_LANGS
DROP PROCEDURE SHOW_LANGS
DROP TABLE JOB
""")

    def testScript(self):
        self.maxDiff = None
        self.assertEqual(25,len(sm.SCRIPT_DEFAULT_ORDER))
        s = self.con.schema
        script = s.get_metadata_ddl([sm.SCRIPT_COLLATIONS])
        self.assertListEqual(script,["""CREATE COLLATION TEST_COLLATE
   FOR WIN1250
   FROM WIN_CZ
   NO PAD
   CASE INSENSITIVE
   ACCENT INSENSITIVE
   'DISABLE-COMPRESSIONS=0;DISABLE-EXPANSIONS=0'"""])
        script = s.get_metadata_ddl([sm.SCRIPT_CHARACTER_SETS])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_UDFS])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_GENERATORS])
        self.assertListEqual(script,['CREATE SEQUENCE EMP_NO_GEN',
                                     'CREATE SEQUENCE CUST_NO_GEN'])
        script = s.get_metadata_ddl([sm.SCRIPT_EXCEPTIONS])
        self.assertListEqual(script,["CREATE EXCEPTION UNKNOWN_EMP_ID 'Invalid employee number or project id.'",
                                     "CREATE EXCEPTION REASSIGN_SALES 'Reassign the sales records before deleting this employee.'",
                                     'CREATE EXCEPTION ORDER_ALREADY_SHIPPED \'Order status is "shipped."\'',
                                     "CREATE EXCEPTION CUSTOMER_ON_HOLD 'This customer is on hold.'",
                                     "CREATE EXCEPTION CUSTOMER_CHECK 'Overdue balance -- can not ship.'"])
        script = s.get_metadata_ddl([sm.SCRIPT_DOMAINS])
        self.assertListEqual(script,['CREATE DOMAIN "FIRSTNAME" AS VARCHAR(15)',
                                     'CREATE DOMAIN "LASTNAME" AS VARCHAR(20)',
                                     'CREATE DOMAIN PHONENUMBER AS VARCHAR(20)',
                                     'CREATE DOMAIN COUNTRYNAME AS VARCHAR(15)',
                                     'CREATE DOMAIN ADDRESSLINE AS VARCHAR(30)',
                                     'CREATE DOMAIN EMPNO AS SMALLINT',
                                     "CREATE DOMAIN DEPTNO AS CHAR(3) CHECK (VALUE = '000' OR (VALUE > '0' AND VALUE <= '999') OR VALUE IS NULL)",
                                     'CREATE DOMAIN PROJNO AS CHAR(5) CHECK (VALUE = UPPER (VALUE))',
                                     'CREATE DOMAIN CUSTNO AS INTEGER CHECK (VALUE > 1000)',
                                     "CREATE DOMAIN JOBCODE AS VARCHAR(5) CHECK (VALUE > '99999')",
                                     'CREATE DOMAIN JOBGRADE AS SMALLINT CHECK (VALUE BETWEEN 0 AND 6)',
                                     'CREATE DOMAIN SALARY AS NUMERIC(10, 2) DEFAULT 0 CHECK (VALUE > 0)',
                                     'CREATE DOMAIN BUDGET AS DECIMAL(12, 2) DEFAULT 50000 CHECK (VALUE > 10000 AND VALUE <= 2000000)',
                                     "CREATE DOMAIN PRODTYPE AS VARCHAR(12) DEFAULT 'software' NOT NULL CHECK (VALUE IN ('software', 'hardware', 'other', 'N/A'))",
                                     "CREATE DOMAIN PONUMBER AS CHAR(8) CHECK (VALUE STARTING WITH 'V')"])
        script = s.get_metadata_ddl([sm.SCRIPT_PACKAGE_DEFS])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_FUNCTION_DEFS])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_PROCEDURE_DEFS])
        self.assertListEqual(script,['CREATE PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n  SUSPEND;\nEND', 'CREATE PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nBEGIN\n  SUSPEND;\nEND',
                                     'CREATE PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n  SUSPEND;\nEND'])
        script = s.get_metadata_ddl([sm.SCRIPT_TABLES])
        self.assertListEqual(script,['CREATE TABLE COUNTRY (\n  COUNTRY COUNTRYNAME NOT NULL,\n  CURRENCY VARCHAR(10) NOT NULL\n)',
                                     'CREATE TABLE JOB (\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  JOB_TITLE VARCHAR(25) NOT NULL,\n  MIN_SALARY SALARY NOT NULL,\n  MAX_SALARY SALARY NOT NULL,\n  JOB_REQUIREMENT BLOB SUB_TYPE TEXT SEGMENT SIZE 400,\n  LANGUAGE_REQ VARCHAR(15)[5]\n)',
                                     "CREATE TABLE DEPARTMENT (\n  DEPT_NO DEPTNO NOT NULL,\n  DEPARTMENT VARCHAR(25) NOT NULL,\n  HEAD_DEPT DEPTNO,\n  MNGR_NO EMPNO,\n  BUDGET BUDGET,\n  LOCATION VARCHAR(15),\n  PHONE_NO PHONENUMBER DEFAULT '555-1234'\n)",
                                     'CREATE TABLE EMPLOYEE (\n  EMP_NO EMPNO NOT NULL,\n  FIRST_NAME "FIRSTNAME" NOT NULL,\n  LAST_NAME "LASTNAME" NOT NULL,\n  PHONE_EXT VARCHAR(4),\n  HIRE_DATE TIMESTAMP DEFAULT \'NOW\' NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  JOB_CODE JOBCODE NOT NULL,\n  JOB_GRADE JOBGRADE NOT NULL,\n  JOB_COUNTRY COUNTRYNAME NOT NULL,\n  SALARY SALARY NOT NULL,\n  FULL_NAME COMPUTED BY (last_name || \', \' || first_name)\n)',
                                     'CREATE TABLE CUSTOMER (\n  CUST_NO CUSTNO NOT NULL,\n  CUSTOMER VARCHAR(25) NOT NULL,\n  CONTACT_FIRST "FIRSTNAME",\n  CONTACT_LAST "LASTNAME",\n  PHONE_NO PHONENUMBER,\n  ADDRESS_LINE1 ADDRESSLINE,\n  ADDRESS_LINE2 ADDRESSLINE,\n  CITY VARCHAR(25),\n  STATE_PROVINCE VARCHAR(15),\n  COUNTRY COUNTRYNAME,\n  POSTAL_CODE VARCHAR(12),\n  ON_HOLD CHAR(1) DEFAULT NULL\n)',
                                     'CREATE TABLE T4 (\n  C1 INTEGER,\n  C_OCTETS CHAR(5) CHARACTER SET OCTETS,\n  V_OCTETS VARCHAR(30) CHARACTER SET OCTETS,\n  C_NONE CHAR(5),\n  V_NONE VARCHAR(30),\n  C_WIN1250 CHAR(5) CHARACTER SET WIN1250,\n  V_WIN1250 VARCHAR(30) CHARACTER SET WIN1250,\n  C_UTF8 CHAR(5) CHARACTER SET UTF8,\n  V_UTF8 VARCHAR(30) CHARACTER SET UTF8\n)',
                                     'CREATE TABLE PROJECT (\n  PROJ_ID PROJNO NOT NULL,\n  PROJ_NAME VARCHAR(20) NOT NULL,\n  PROJ_DESC BLOB SUB_TYPE TEXT SEGMENT SIZE 800,\n  TEAM_LEADER EMPNO,\n  PRODUCT PRODTYPE\n)',
                                     'CREATE TABLE EMPLOYEE_PROJECT (\n  EMP_NO EMPNO NOT NULL,\n  PROJ_ID PROJNO NOT NULL\n)',
                                     'CREATE TABLE PROJ_DEPT_BUDGET (\n  FISCAL_YEAR INTEGER NOT NULL,\n  PROJ_ID PROJNO NOT NULL,\n  DEPT_NO DEPTNO NOT NULL,\n  QUART_HEAD_CNT INTEGER[4],\n  PROJECTED_BUDGET BUDGET\n)',
                                     "CREATE TABLE SALARY_HISTORY (\n  EMP_NO EMPNO NOT NULL,\n  CHANGE_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,\n  UPDATER_ID VARCHAR(20) NOT NULL,\n  OLD_SALARY SALARY NOT NULL,\n  PERCENT_CHANGE DOUBLE PRECISION DEFAULT 0 NOT NULL,\n  NEW_SALARY COMPUTED BY (old_salary + old_salary * percent_change / 100)\n)",
                                     "CREATE TABLE SALES (\n  PO_NUMBER PONUMBER NOT NULL,\n  CUST_NO CUSTNO NOT NULL,\n  SALES_REP EMPNO,\n  ORDER_STATUS VARCHAR(7) DEFAULT 'new' NOT NULL,\n  ORDER_DATE TIMESTAMP DEFAULT 'NOW' NOT NULL,\n  SHIP_DATE TIMESTAMP,\n  DATE_NEEDED TIMESTAMP,\n  PAID CHAR(1) DEFAULT 'n',\n  QTY_ORDERED INTEGER DEFAULT 1 NOT NULL,\n  TOTAL_VALUE DECIMAL(9, 2) NOT NULL,\n  DISCOUNT FLOAT DEFAULT 0 NOT NULL,\n  ITEM_TYPE PRODTYPE,\n  AGED COMPUTED BY (ship_date - order_date)\n)",
                                     'CREATE TABLE T3 (\n  C1 INTEGER,\n  C2 CHAR(10) CHARACTER SET UTF8,\n  C3 VARCHAR(10) CHARACTER SET UTF8,\n  C4 BLOB SUB_TYPE TEXT SEGMENT SIZE 80 CHARACTER SET UTF8,\n  C5 BLOB SUB_TYPE BINARY SEGMENT SIZE 80\n)',
                                     'CREATE TABLE T2 (\n  C1 SMALLINT,\n  C2 INTEGER,\n  C3 BIGINT,\n  C4 CHAR(5),\n  C5 VARCHAR(10),\n  C6 DATE,\n  C7 TIME,\n  C8 TIMESTAMP,\n  C9 BLOB SUB_TYPE TEXT SEGMENT SIZE 80,\n  C10 NUMERIC(18, 2),\n  C11 DECIMAL(18, 2),\n  C12 FLOAT,\n  C13 DOUBLE PRECISION,\n  C14 NUMERIC(8, 4),\n  C15 DECIMAL(8, 4),\n  C16 BLOB SUB_TYPE BINARY SEGMENT SIZE 80\n)',
                                     'CREATE TABLE AR (\n  C1 INTEGER,\n  C2 INTEGER[4, 0:3, 2],\n  C3 VARCHAR(15)[0:5, 2],\n  C4 CHAR(5)[5],\n  C5 TIMESTAMP[2],\n  C6 TIME[2],\n  C7 DECIMAL(10, 2)[2],\n  C8 NUMERIC(10, 2)[2],\n  C9 SMALLINT[2],\n  C10 BIGINT[2],\n  C11 FLOAT[2],\n  C12 DOUBLE PRECISION[2],\n  C13 DECIMAL(10, 1)[2],\n  C14 DECIMAL(10, 5)[2],\n  C15 DECIMAL(18, 5)[2]\n)',
                                     'CREATE TABLE T (\n  C1 INTEGER NOT NULL\n)'])
        script = s.get_metadata_ddl([sm.SCRIPT_PRIMARY_KEYS])
        self.assertListEqual(script,['ALTER TABLE COUNTRY ADD PRIMARY KEY (COUNTRY)',
                                     'ALTER TABLE JOB ADD PRIMARY KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)',
                                     'ALTER TABLE DEPARTMENT ADD PRIMARY KEY (DEPT_NO)',
                                     'ALTER TABLE EMPLOYEE ADD PRIMARY KEY (EMP_NO)',
                                     'ALTER TABLE PROJECT ADD PRIMARY KEY (PROJ_ID)',
                                     'ALTER TABLE EMPLOYEE_PROJECT ADD PRIMARY KEY (EMP_NO,PROJ_ID)',
                                     'ALTER TABLE PROJ_DEPT_BUDGET ADD PRIMARY KEY (FISCAL_YEAR,PROJ_ID,DEPT_NO)',
                                     'ALTER TABLE SALARY_HISTORY ADD PRIMARY KEY (EMP_NO,CHANGE_DATE,UPDATER_ID)',
                                     'ALTER TABLE CUSTOMER ADD PRIMARY KEY (CUST_NO)',
                                     'ALTER TABLE SALES ADD PRIMARY KEY (PO_NUMBER)',
                                     'ALTER TABLE T ADD PRIMARY KEY (C1)'],)
        script = s.get_metadata_ddl([sm.SCRIPT_UNIQUE_CONSTRAINTS])
        self.assertListEqual(script,['ALTER TABLE DEPARTMENT ADD UNIQUE (DEPARTMENT)',
                                     'ALTER TABLE PROJECT ADD UNIQUE (PROJ_NAME)'])
        script = s.get_metadata_ddl([sm.SCRIPT_CHECK_CONSTRAINTS])
        self.assertListEqual(script,['ALTER TABLE JOB ADD CHECK (min_salary < max_salary)',
                                     'ALTER TABLE EMPLOYEE ADD CHECK ( salary >= (SELECT min_salary FROM job WHERE\n                        job.job_code = employee.job_code AND\n                        job.job_grade = employee.job_grade AND\n                        job.job_country = employee.job_country) AND\n            salary <= (SELECT max_salary FROM job WHERE\n                        job.job_code = employee.job_code AND\n                        job.job_grade = employee.job_grade AND\n                        job.job_country = employee.job_country))',
                                     "ALTER TABLE CUSTOMER ADD CHECK (on_hold IS NULL OR on_hold = '*')",
                                     'ALTER TABLE PROJ_DEPT_BUDGET ADD CHECK (FISCAL_YEAR >= 1993)',
                                     'ALTER TABLE SALARY_HISTORY ADD CHECK (percent_change between -50 and 50)',
                                     'ALTER TABLE SALES ADD CHECK (total_value >= 0)',
                                     'ALTER TABLE SALES ADD CHECK (ship_date >= order_date OR ship_date IS NULL)',
                                     "ALTER TABLE SALES ADD CHECK (NOT (order_status = 'shipped' AND\n            EXISTS (SELECT on_hold FROM customer\n                    WHERE customer.cust_no = sales.cust_no\n                    AND customer.on_hold = '*')))",
                                     'ALTER TABLE SALES ADD CHECK (date_needed > order_date OR date_needed IS NULL)',
                                     "ALTER TABLE SALES ADD CHECK (paid in ('y', 'n'))",
                                     "ALTER TABLE SALES ADD CHECK (NOT (order_status = 'shipped' AND ship_date IS NULL))",
                                     "ALTER TABLE SALES ADD CHECK (order_status in\n                            ('new', 'open', 'shipped', 'waiting'))",
                                     'ALTER TABLE SALES ADD CHECK (discount >= 0 AND discount <= 1)',
                                     'ALTER TABLE SALES ADD CHECK (qty_ordered >= 1)'])
        script = s.get_metadata_ddl([sm.SCRIPT_FOREIGN_CONSTRAINTS])
        self.assertListEqual(script,['ALTER TABLE JOB ADD FOREIGN KEY (JOB_COUNTRY)\n  REFERENCES COUNTRY (COUNTRY)',
                                     'ALTER TABLE DEPARTMENT ADD FOREIGN KEY (HEAD_DEPT)\n  REFERENCES DEPARTMENT (DEPT_NO)',
                                     'ALTER TABLE DEPARTMENT ADD FOREIGN KEY (MNGR_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
                                     'ALTER TABLE EMPLOYEE ADD FOREIGN KEY (DEPT_NO)\n  REFERENCES DEPARTMENT (DEPT_NO)',
                                     'ALTER TABLE EMPLOYEE ADD FOREIGN KEY (JOB_CODE,JOB_GRADE,JOB_COUNTRY)\n  REFERENCES JOB (JOB_CODE,JOB_GRADE,JOB_COUNTRY)',
                                     'ALTER TABLE CUSTOMER ADD FOREIGN KEY (COUNTRY)\n  REFERENCES COUNTRY (COUNTRY)',
                                     'ALTER TABLE PROJECT ADD FOREIGN KEY (TEAM_LEADER)\n  REFERENCES EMPLOYEE (EMP_NO)',
                                     'ALTER TABLE EMPLOYEE_PROJECT ADD FOREIGN KEY (EMP_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
                                     'ALTER TABLE EMPLOYEE_PROJECT ADD FOREIGN KEY (PROJ_ID)\n  REFERENCES PROJECT (PROJ_ID)',
                                     'ALTER TABLE PROJ_DEPT_BUDGET ADD FOREIGN KEY (DEPT_NO)\n  REFERENCES DEPARTMENT (DEPT_NO)',
                                     'ALTER TABLE PROJ_DEPT_BUDGET ADD FOREIGN KEY (PROJ_ID)\n  REFERENCES PROJECT (PROJ_ID)',
                                     'ALTER TABLE SALARY_HISTORY ADD FOREIGN KEY (EMP_NO)\n  REFERENCES EMPLOYEE (EMP_NO)',
                                     'ALTER TABLE SALES ADD FOREIGN KEY (CUST_NO)\n  REFERENCES CUSTOMER (CUST_NO)',
                                     'ALTER TABLE SALES ADD FOREIGN KEY (SALES_REP)\n  REFERENCES EMPLOYEE (EMP_NO)'])
        script = s.get_metadata_ddl([sm.SCRIPT_INDICES])
        self.assertListEqual(script,['CREATE ASCENDING INDEX MINSALX ON JOB (JOB_COUNTRY,MIN_SALARY)',
                                     'CREATE DESCENDING INDEX MAXSALX ON JOB (JOB_COUNTRY,MAX_SALARY)',
                                     'CREATE DESCENDING INDEX BUDGETX ON DEPARTMENT (BUDGET)',
                                     'CREATE ASCENDING INDEX NAMEX ON EMPLOYEE (LAST_NAME,FIRST_NAME)',
                                     'CREATE ASCENDING INDEX CUSTNAMEX ON CUSTOMER (CUSTOMER)',
                                     'CREATE ASCENDING INDEX CUSTREGION ON CUSTOMER (COUNTRY,CITY)',
                                     'CREATE UNIQUE ASCENDING INDEX PRODTYPEX ON PROJECT (PRODUCT,PROJ_NAME)',
                                     'CREATE ASCENDING INDEX UPDATERX ON SALARY_HISTORY (UPDATER_ID)',
                                     'CREATE DESCENDING INDEX CHANGEX ON SALARY_HISTORY (CHANGE_DATE)',
                                     'CREATE ASCENDING INDEX NEEDX ON SALES (DATE_NEEDED)',
                                     'CREATE ASCENDING INDEX SALESTATX ON SALES (ORDER_STATUS,PAID)',
                                     'CREATE DESCENDING INDEX QTYX ON SALES (ITEM_TYPE,QTY_ORDERED)'])
        script = s.get_metadata_ddl([sm.SCRIPT_VIEWS])
        self.assertListEqual(script,['CREATE VIEW PHONE_LIST (EMP_NO,FIRST_NAME,LAST_NAME,PHONE_EXT,LOCATION,PHONE_NO)\n   AS\n     SELECT\n    emp_no, first_name, last_name, phone_ext, location, phone_no\n    FROM employee, department\n    WHERE employee.dept_no = department.dept_no'])
        script = s.get_metadata_ddl([sm.SCRIPT_PACKAGE_BODIES])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_FUNCTION_BODIES])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_PROCEDURE_BODIES])
        self.assertListEqual(script,['ALTER PROCEDURE GET_EMP_PROJ (EMP_NO SMALLINT)\nRETURNS (PROJ_ID CHAR(5))\nAS\nBEGIN\n\tFOR SELECT proj_id\n\t\tFROM employee_project\n\t\tWHERE emp_no = :emp_no\n\t\tINTO :proj_id\n\tDO\n\t\tSUSPEND;\nEND', 'ALTER PROCEDURE ADD_EMP_PROJ (\n  EMP_NO SMALLINT,\n  PROJ_ID CHAR(5)\n)\nAS\nBEGIN\n\tBEGIN\n\tINSERT INTO employee_project (emp_no, proj_id) VALUES (:emp_no, :proj_id);\n\tWHEN SQLCODE -530 DO\n\t\tEXCEPTION unknown_emp_id;\n\tEND\n\tSUSPEND;\nEND',
                                     'ALTER PROCEDURE SUB_TOT_BUDGET (HEAD_DEPT CHAR(3))\nRETURNS (\n  TOT_BUDGET DECIMAL(12, 2),\n  AVG_BUDGET DECIMAL(12, 2),\n  MIN_BUDGET DECIMAL(12, 2),\n  MAX_BUDGET DECIMAL(12, 2)\n)\nAS\nBEGIN\n\tSELECT SUM(budget), AVG(budget), MIN(budget), MAX(budget)\n\t\tFROM department\n\t\tWHERE head_dept = :head_dept\n\t\tINTO :tot_budget, :avg_budget, :min_budget, :max_budget;\n\tSUSPEND;\nEND',
                                     "ALTER PROCEDURE DELETE_EMPLOYEE (EMP_NUM INTEGER)\nAS\nDECLARE VARIABLE any_sales INTEGER;\nBEGIN\n\tany_sales = 0;\n\n\t/*\n\t *\tIf there are any sales records referencing this employee,\n\t *\tcan't delete the employee until the sales are re-assigned\n\t *\tto another employee or changed to NULL.\n\t */\n\tSELECT count(po_number)\n\tFROM sales\n\tWHERE sales_rep = :emp_num\n\tINTO :any_sales;\n\n\tIF (any_sales > 0) THEN\n\tBEGIN\n\t\tEXCEPTION reassign_sales;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf the employee is a manager, update the department.\n\t */\n\tUPDATE department\n\tSET mngr_no = NULL\n\tWHERE mngr_no = :emp_num;\n\n\t/*\n\t *\tIf the employee is a project leader, update project.\n\t */\n\tUPDATE project\n\tSET team_leader = NULL\n\tWHERE team_leader = :emp_num;\n\n\t/*\n\t *\tDelete the employee from any projects.\n\t */\n\tDELETE FROM employee_project\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete old salary records.\n\t */\n\tDELETE FROM salary_history\n\tWHERE emp_no = :emp_num;\n\n\t/*\n\t *\tDelete the employee.\n\t */\n\tDELETE FROM employee\n\tWHERE emp_no = :emp_num;\n\n\tSUSPEND;\nEND",
                                     'ALTER PROCEDURE DEPT_BUDGET (DNO CHAR(3))\nRETURNS (TOT DECIMAL(12, 2))\nAS\nDECLARE VARIABLE sumb DECIMAL(12, 2);\n\tDECLARE VARIABLE rdno CHAR(3);\n\tDECLARE VARIABLE cnt INTEGER;\nBEGIN\n\ttot = 0;\n\n\tSELECT budget FROM department WHERE dept_no = :dno INTO :tot;\n\n\tSELECT count(budget) FROM department WHERE head_dept = :dno INTO :cnt;\n\n\tIF (cnt = 0) THEN\n\t\tSUSPEND;\n\n\tFOR SELECT dept_no\n\t\tFROM department\n\t\tWHERE head_dept = :dno\n\t\tINTO :rdno\n\tDO\n\t\tBEGIN\n\t\t\tEXECUTE PROCEDURE dept_budget :rdno RETURNING_VALUES :sumb;\n\t\t\ttot = tot + sumb;\n\t\tEND\n\n\tSUSPEND;\nEND',
                                     "ALTER PROCEDURE ORG_CHART\nRETURNS (\n  HEAD_DEPT CHAR(25),\n  DEPARTMENT CHAR(25),\n  MNGR_NAME CHAR(20),\n  TITLE CHAR(5),\n  EMP_CNT INTEGER\n)\nAS\nDECLARE VARIABLE mngr_no INTEGER;\n\tDECLARE VARIABLE dno CHAR(3);\nBEGIN\n\tFOR SELECT h.department, d.department, d.mngr_no, d.dept_no\n\t\tFROM department d\n\t\tLEFT OUTER JOIN department h ON d.head_dept = h.dept_no\n\t\tORDER BY d.dept_no\n\t\tINTO :head_dept, :department, :mngr_no, :dno\n\tDO\n\tBEGIN\n\t\tIF (:mngr_no IS NULL) THEN\n\t\tBEGIN\n\t\t\tmngr_name = '--TBH--';\n\t\t\ttitle = '';\n\t\tEND\n\n\t\tELSE\n\t\t\tSELECT full_name, job_code\n\t\t\tFROM employee\n\t\t\tWHERE emp_no = :mngr_no\n\t\t\tINTO :mngr_name, :title;\n\n\t\tSELECT COUNT(emp_no)\n\t\tFROM employee\n\t\tWHERE dept_no = :dno\n\t\tINTO :emp_cnt;\n\n\t\tSUSPEND;\n\tEND\nEND",
                                     "ALTER PROCEDURE MAIL_LABEL (CUST_NO INTEGER)\nRETURNS (\n  LINE1 CHAR(40),\n  LINE2 CHAR(40),\n  LINE3 CHAR(40),\n  LINE4 CHAR(40),\n  LINE5 CHAR(40),\n  LINE6 CHAR(40)\n)\nAS\nDECLARE VARIABLE customer\tVARCHAR(25);\n\tDECLARE VARIABLE first_name\t\tVARCHAR(15);\n\tDECLARE VARIABLE last_name\t\tVARCHAR(20);\n\tDECLARE VARIABLE addr1\t\tVARCHAR(30);\n\tDECLARE VARIABLE addr2\t\tVARCHAR(30);\n\tDECLARE VARIABLE city\t\tVARCHAR(25);\n\tDECLARE VARIABLE state\t\tVARCHAR(15);\n\tDECLARE VARIABLE country\tVARCHAR(15);\n\tDECLARE VARIABLE postcode\tVARCHAR(12);\n\tDECLARE VARIABLE cnt\t\tINTEGER;\nBEGIN\n\tline1 = '';\n\tline2 = '';\n\tline3 = '';\n\tline4 = '';\n\tline5 = '';\n\tline6 = '';\n\n\tSELECT customer, contact_first, contact_last, address_line1,\n\t\taddress_line2, city, state_province, country, postal_code\n\tFROM CUSTOMER\n\tWHERE cust_no = :cust_no\n\tINTO :customer, :first_name, :last_name, :addr1, :addr2,\n\t\t:city, :state, :country, :postcode;\n\n\tIF (customer IS NOT NULL) THEN\n\t\tline1 = customer;\n\tIF (first_name IS NOT NULL) THEN\n\t\tline2 = first_name || ' ' || last_name;\n\tELSE\n\t\tline2 = last_name;\n\tIF (addr1 IS NOT NULL) THEN\n\t\tline3 = addr1;\n\tIF (addr2 IS NOT NULL) THEN\n\t\tline4 = addr2;\n\n\tIF (country = 'USA') THEN\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state || '  ' || postcode;\n\t\tELSE\n\t\t\tline5 = state || '  ' || postcode;\n\tEND\n\tELSE\n\tBEGIN\n\t\tIF (city IS NOT NULL) THEN\n\t\t\tline5 = city || ', ' || state;\n\t\tELSE\n\t\t\tline5 = state;\n\t\tline6 = country || '    ' || postcode;\n\tEND\n\n\tSUSPEND;\nEND",
                                     "ALTER PROCEDURE SHIP_ORDER (PO_NUM CHAR(8))\nAS\nDECLARE VARIABLE ord_stat CHAR(7);\n\tDECLARE VARIABLE hold_stat CHAR(1);\n\tDECLARE VARIABLE cust_no INTEGER;\n\tDECLARE VARIABLE any_po CHAR(8);\nBEGIN\n\tSELECT s.order_status, c.on_hold, c.cust_no\n\tFROM sales s, customer c\n\tWHERE po_number = :po_num\n\tAND s.cust_no = c.cust_no\n\tINTO :ord_stat, :hold_stat, :cust_no;\n\n\t/* This purchase order has been already shipped. */\n\tIF (ord_stat = 'shipped') THEN\n\tBEGIN\n\t\tEXCEPTION order_already_shipped;\n\t\tSUSPEND;\n\tEND\n\n\t/*\tCustomer is on hold. */\n\tELSE IF (hold_stat = '*') THEN\n\tBEGIN\n\t\tEXCEPTION customer_on_hold;\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tIf there is an unpaid balance on orders shipped over 2 months ago,\n\t *\tput the customer on hold.\n\t */\n\tFOR SELECT po_number\n\t\tFROM sales\n\t\tWHERE cust_no = :cust_no\n\t\tAND order_status = 'shipped'\n\t\tAND paid = 'n'\n\t\tAND ship_date < CAST('NOW' AS TIMESTAMP) - 60\n\t\tINTO :any_po\n\tDO\n\tBEGIN\n\t\tEXCEPTION customer_check;\n\n\t\tUPDATE customer\n\t\tSET on_hold = '*'\n\t\tWHERE cust_no = :cust_no;\n\n\t\tSUSPEND;\n\tEND\n\n\t/*\n\t *\tShip the order.\n\t */\n\tUPDATE sales\n\tSET order_status = 'shipped', ship_date = 'NOW'\n\tWHERE po_number = :po_num;\n\n\tSUSPEND;\nEND",
                                     "ALTER PROCEDURE SHOW_LANGS (\n  CODE VARCHAR(5),\n  GRADE SMALLINT,\n  CTY VARCHAR(15)\n)\nRETURNS (LANGUAGES VARCHAR(15))\nAS\nDECLARE VARIABLE i INTEGER;\nBEGIN\n  i = 1;\n  WHILE (i <= 5) DO\n  BEGIN\n    SELECT language_req[:i] FROM joB\n    WHERE ((job_code = :code) AND (job_grade = :grade) AND (job_country = :cty)\n           AND (language_req IS NOT NULL))\n    INTO :languages;\n    IF (languages = ' ') THEN  /* Prints 'NULL' instead of blanks */\n       languages = 'NULL';         \n    i = i +1;\n    SUSPEND;\n  END\nEND",
                                     "ALTER PROCEDURE ALL_LANGS\nRETURNS (\n  CODE VARCHAR(5),\n  GRADE VARCHAR(5),\n  COUNTRY VARCHAR(15),\n  LANG VARCHAR(15)\n)\nAS\nBEGIN\n\tFOR SELECT job_code, job_grade, job_country FROM job \n\t\tINTO :code, :grade, :country\n\n\tDO\n\tBEGIN\n\t    FOR SELECT languages FROM show_langs \n \t\t    (:code, :grade, :country) INTO :lang DO\n\t        SUSPEND;\n\t    /* Put nice separators between rows */\n\t    code = '=====';\n\t    grade = '=====';\n\t    country = '===============';\n\t    lang = '==============';\n\t    SUSPEND;\n\tEND\n    END"])
        script = s.get_metadata_ddl([sm.SCRIPT_TRIGGERS])
        self.assertListEqual(script,['CREATE TRIGGER SET_EMP_NO FOR EMPLOYEE ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.emp_no is null) then\n    new.emp_no = gen_id(emp_no_gen, 1);\nEND',
                                     "CREATE TRIGGER SAVE_SALARY_CHANGE FOR EMPLOYEE ACTIVE\nAFTER UPDATE POSITION 0\nAS\nBEGIN\n    IF (old.salary <> new.salary) THEN\n        INSERT INTO salary_history\n            (emp_no, change_date, updater_id, old_salary, percent_change)\n        VALUES (\n            old.emp_no,\n            'NOW',\n            user,\n            old.salary,\n            (new.salary - old.salary) * 100 / old.salary);\nEND",
                                     'CREATE TRIGGER SET_CUST_NO FOR CUSTOMER ACTIVE\nBEFORE INSERT POSITION 0\nAS\nBEGIN\n    if (new.cust_no is null) then\n    new.cust_no = gen_id(cust_no_gen, 1);\nEND',
                                     "CREATE TRIGGER POST_NEW_ORDER FOR SALES ACTIVE\nAFTER INSERT POSITION 0\nAS\nBEGIN\n    POST_EVENT 'new_order';\nEND",
                                     'CREATE TRIGGER TR_MULTI FOR COUNTRY ACTIVE\nAFTER INSERT OR UPDATE OR DELETE POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND',
                                     'CREATE TRIGGER TR_CONNECT ACTIVE\nON CONNECT POSITION 0\nAS \nBEGIN \n    /* enter trigger code here */ \nEND'])
        script = s.get_metadata_ddl([sm.SCRIPT_ROLES])
        self.assertListEqual(script,['CREATE ROLE TEST_ROLE'])
        script = s.get_metadata_ddl([sm.SCRIPT_GRANTS])
        self.assertListEqual(script,['GRANT SELECT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON COUNTRY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON JOB TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON JOB TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON JOB TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON JOB TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON JOB TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON DEPARTMENT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON PHONE_LIST TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON EMPLOYEE_PROJECT TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON PROJ_DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON SALARY_HISTORY TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON CUSTOMER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT SELECT ON SALES TO PUBLIC WITH GRANT OPTION',
                                     'GRANT INSERT ON SALES TO PUBLIC WITH GRANT OPTION',
                                     'GRANT UPDATE ON SALES TO PUBLIC WITH GRANT OPTION',
                                     'GRANT DELETE ON SALES TO PUBLIC WITH GRANT OPTION',
                                     'GRANT REFERENCES ON SALES TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE GET_EMP_PROJ TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE ADD_EMP_PROJ TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE SUB_TOT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE DELETE_EMPLOYEE TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE DEPT_BUDGET TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE ORG_CHART TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE MAIL_LABEL TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE SHIP_ORDER TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE SHOW_LANGS TO PUBLIC WITH GRANT OPTION',
                                     'GRANT EXECUTE ON PROCEDURE ALL_LANGS TO PUBLIC WITH GRANT OPTION'])
        script = s.get_metadata_ddl([sm.SCRIPT_COMMENTS])
        self.assertListEqual(script,["COMMENT ON CHARACTER SET NONE IS 'Comment on NONE character set'"])
        script = s.get_metadata_ddl([sm.SCRIPT_SHADOWS])
        self.assertListEqual(script,[])
        script = s.get_metadata_ddl([sm.SCRIPT_INDEX_DEACTIVATIONS])
        self.assertListEqual(script,['ALTER INDEX NEEDX INACTIVE',
                                     'ALTER INDEX SALESTATX INACTIVE',
                                     'ALTER INDEX QTYX INACTIVE',
                                     'ALTER INDEX UPDATERX INACTIVE',
                                     'ALTER INDEX CHANGEX INACTIVE',
                                     'ALTER INDEX PRODTYPEX INACTIVE',
                                     'ALTER INDEX CUSTNAMEX INACTIVE',
                                     'ALTER INDEX CUSTREGION INACTIVE',
                                     'ALTER INDEX NAMEX INACTIVE',
                                     'ALTER INDEX BUDGETX INACTIVE',
                                     'ALTER INDEX MINSALX INACTIVE',
                                     'ALTER INDEX MAXSALX INACTIVE'])
        script = s.get_metadata_ddl([sm.SCRIPT_INDEX_ACTIVATIONS])
        self.assertListEqual(script,['ALTER INDEX NEEDX ACTIVE',
                                     'ALTER INDEX SALESTATX ACTIVE',
                                     'ALTER INDEX QTYX ACTIVE',
                                     'ALTER INDEX UPDATERX ACTIVE',
                                     'ALTER INDEX CHANGEX ACTIVE',
                                     'ALTER INDEX PRODTYPEX ACTIVE',
                                     'ALTER INDEX CUSTNAMEX ACTIVE',
                                     'ALTER INDEX CUSTREGION ACTIVE',
                                     'ALTER INDEX NAMEX ACTIVE',
                                     'ALTER INDEX BUDGETX ACTIVE',
                                     'ALTER INDEX MINSALX ACTIVE',
                                     'ALTER INDEX MAXSALX ACTIVE'])
        script = s.get_metadata_ddl([sm.SCRIPT_SET_GENERATORS])
        self.assertListEqual(script,['ALTER SEQUENCE EMP_NO_GEN RESTART WITH 145',
                                     'ALTER SEQUENCE CUST_NO_GEN RESTART WITH 1015'])
        script = s.get_metadata_ddl([sm.SCRIPT_TRIGGER_DEACTIVATIONS])
        self.assertListEqual(script,['ALTER TRIGGER SET_EMP_NO INACTIVE',
                                     'ALTER TRIGGER SAVE_SALARY_CHANGE INACTIVE',
                                     'ALTER TRIGGER SET_CUST_NO INACTIVE',
                                     'ALTER TRIGGER POST_NEW_ORDER INACTIVE',
                                     'ALTER TRIGGER TR_MULTI INACTIVE',
                                     'ALTER TRIGGER TR_CONNECT INACTIVE'])
        script = s.get_metadata_ddl([sm.SCRIPT_TRIGGER_ACTIVATIONS])
        self.assertListEqual(script,['ALTER TRIGGER SET_EMP_NO ACTIVE',
                                     'ALTER TRIGGER SAVE_SALARY_CHANGE ACTIVE',
                                     'ALTER TRIGGER SET_CUST_NO ACTIVE',
                                     'ALTER TRIGGER POST_NEW_ORDER ACTIVE',
                                     'ALTER TRIGGER TR_MULTI ACTIVE',
                                     'ALTER TRIGGER TR_CONNECT ACTIVE'])

class TestMonitor(FDBTestBase):
    def setUp(self):
        super(TestMonitor,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        self.con = fdb.connect(host=FBTEST_HOST,database=self.dbfile,
                               user=FBTEST_USER,password=FBTEST_PASSWORD)
    def tearDown(self):
        self.con.close()
    def testMonitorBindClose(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        s = fdb.monitor.Monitor()
        self.assertTrue(s.closed)
        s.bind(self.con)
        # properties
        self.assertEqual(s.db.name.upper(),self.dbfile.upper())
        self.assertFalse(s.db.read_only)
        self.assertFalse(s.closed)
        #
        s.close()
        self.assertTrue(s.closed)
        s.bind(self.con)
        self.assertFalse(s.closed)
        #
        s.bind(self.con)
        self.assertFalse(s.closed)
        #
        del s
    def testMonitor(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        m = self.con.monitor
        m.refresh()
        self.assertIsNotNone(m.db)
        self.assertIsInstance(m.db,fdb.monitor.DatabaseInfo)
        self.assertGreater(len(m.attachments),0)
        self.assertIsInstance(m.attachments[0],fdb.monitor.AttachmentInfo)
        self.assertGreater(len(m.transactions),0)
        self.assertIsInstance(m.transactions[0],fdb.monitor.TransactionInfo)
        self.assertGreater(len(m.statements),0)
        self.assertIsInstance(m.statements[0],fdb.monitor.StatementInfo)
        self.assertEqual(len(m.callstack),0)
        self.assertGreater(len(m.iostats),0)
        self.assertIsInstance(m.iostats[0],fdb.monitor.IOStatsInfo)
        if self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(len(m.variables),0)
        elif self.con.ods >= fdb.ODS_FB_25:
            self.assertGreater(len(m.variables),0)
            self.assertIsInstance(m.variables[0],fdb.monitor.ContextVariableInfo)
        #
        att_id = m._con.db_info(fdb.isc_info_attachment_id)
        self.assertEqual(m.get_attachment(att_id).id,att_id)
        tra_id = m._con.trans_info(fdb.isc_info_tra_id)
        self.assertEqual(m.get_transaction(tra_id).id,tra_id)
        stmt_id = None
        for stmt in m.statements:
            if stmt.sql_text == sql:
                stmt_id = stmt.id
        self.assertEqual(m.get_statement(stmt_id).id,stmt_id)
        # m.get_call()
        self.assertIsInstance(m.this_attachment,fdb.monitor.AttachmentInfo)
        self.assertEqual(m.this_attachment.id,
                         self.con.db_info(fdb.isc_info_attachment_id))
        self.assertFalse(m.closed)
        #
        with self.assertRaises(fdb.ProgrammingError) as cm:
            m.close()
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'close' not allowed for embedded Monitor.",))
        with self.assertRaises(fdb.ProgrammingError) as cm:
            m.bind(self.con)
        self.assertTupleEqual(cm.exception.args,
                              ("Call to 'bind' not allowed for embedded Monitor.",))
    def testDatabaseInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        m = self.con.monitor
        m.refresh()
        self.assertEqual(m.db.name.upper(),self.dbfile.upper())
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(m.db.page_size,4096)
        else:
            self.assertEqual(m.db.page_size,8192)
        if self.con.ods == fdb.ODS_FB_20:
            self.assertEqual(m.db.ods,11.0)
        elif self.con.ods == fdb.ODS_FB_21:
            self.assertEqual(m.db.ods,11.1)
        elif self.con.ods == fdb.ODS_FB_25:
            self.assertEqual(m.db.ods,11.2)
        elif self.con.ods >= fdb.ODS_FB_30:
            self.assertEqual(m.db.ods,12.0)
        self.assertIsInstance(m.db.oit,int)
        self.assertIsInstance(m.db.oat,int)
        self.assertIsInstance(m.db.ost,int)
        self.assertIsInstance(m.db.next_transaction,int)
        self.assertIsInstance(m.db.cache_size,int)
        self.assertEqual(m.db.sql_dialect,3)
        self.assertEqual(m.db.shutdown_mode,fdb.monitor.SHUTDOWN_MODE_ONLINE)
        self.assertEqual(m.db.sweep_interval,20000)
        self.assertFalse(m.db.read_only)
        self.assertTrue(m.db.forced_writes)
        self.assertTrue(m.db.reserve_space)
        self.assertIsInstance(m.db.created,datetime.datetime)
        self.assertIsInstance(m.db.pages,int)
        self.assertEqual(m.db.backup_state,fdb.monitor.BACKUP_STATE_NORMAL)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(m.db.crypt_page)
            self.assertIsNone(m.db.owner)
            self.assertIsNone(m.db.security_database)
        else:
            self.assertEqual(m.db.crypt_page,0)
            self.assertEqual(m.db.owner,'SYSDBA')
            self.assertEqual(m.db.security_database,'Default')
        self.assertEqual(m.db.iostats.group,fdb.monitor.STAT_DATABASE)
        self.assertEqual(m.db.iostats.stat_id,m.db.stat_id)
        self.assertIsInstance(m.db.tablestats,types.DictionaryType)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(m.db.tablestats),0)
        else:
            self.assertGreater(len(m.db.tablestats),0)
    def testAttachmentInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_SESSION','TESTVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        m = self.con.monitor
        m.refresh()
        s = m.this_attachment
        #
        self.assertEqual(s.id,self.con.db_info(fdb.isc_info_attachment_id))
        self.assertIsInstance(s.server_pid,int)
        self.assertIn(s.state,[fdb.monitor.STATE_ACTIVE,fdb.monitor.STATE_IDLE])
        self.assertEqual(s.name.upper(),self.dbfile.upper())
        self.assertEqual(s.user,'SYSDBA')
        self.assertEqual(s.role,'NONE')
        if not FBTEST_HOST and self.con.engine_version >= 3.0:
            self.assertIsNone(s.remote_protocol)
            self.assertIsNone(s.remote_address)
            self.assertIsNone(s.remote_pid)
            self.assertIsNone(s.remote_process)
        else:
            self.assertIn(s.remote_protocol,['XNET','TCPv4','TCPv6'])
            self.assertIsInstance(s.remote_address,str)
            self.assertIsInstance(s.remote_pid,int)
            self.assertIsInstance(s.remote_process,str)
        self.assertIsInstance(s.character_set,sm.CharacterSet)
        self.assertIsInstance(s.timestamp,datetime.datetime)
        self.assertIsInstance(s.transactions,list)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(s.auth_method)
            self.assertIsNone(s.client_version)
            self.assertIsNone(s.remote_version)
            self.assertIsNone(s.remote_os_user)
            self.assertIsNone(s.remote_host)
        else:
            self.assertIn(s.auth_method,['Srp','Win_Sspi','Legacy_Auth'])
            self.assertIsInstance(s.client_version,str)
            self.assertEqual(s.remote_version,'P13')
            self.assertIsInstance(s.remote_os_user,str)
            self.assertIsInstance(s.remote_host,str)
        for x in s.transactions:
            self.assertIsInstance(x,fdb.monitor.TransactionInfo)
        self.assertIsInstance(s.statements,list)
        for x in s.statements:
            self.assertIsInstance(x,fdb.monitor.StatementInfo)
        self.assertIsInstance(s.variables,list)
        if self.con.ods >= fdb.ODS_FB_25:
            self.assertGreater(len(s.variables),0)
        else:
            self.assertEqual(len(s.variables),0)
        for x in s.variables:
            self.assertIsInstance(x,fdb.monitor.ContextVariableInfo)
        self.assertEqual(s.iostats.group,fdb.monitor.STAT_ATTACHMENT)
        self.assertEqual(s.iostats.stat_id,s.stat_id)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(m.db.tablestats),0)
        else:
            self.assertGreater(len(m.db.tablestats),0)
        #
        self.assertTrue(s.isactive())
        self.assertFalse(s.isidle())
        self.assertFalse(s.isinternal())
        self.assertTrue(s.isgcallowed())
    def testTransactionInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_TRANSACTION','TESTVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        m = self.con.monitor
        m.refresh()
        s = m.this_attachment.transactions[0]
        #
        self.assertEqual(s.id,m._ic.transaction.trans_info(fdb.isc_info_tra_id))
        self.assertIs(s.attachment,m.this_attachment)
        self.assertIn(s.state,[fdb.monitor.STATE_ACTIVE,fdb.monitor.STATE_IDLE])
        self.assertIsInstance(s.timestamp,datetime.datetime)
        self.assertIsInstance(s.top,int)
        self.assertIsInstance(s.oldest,int)
        self.assertIsInstance(s.oldest_active,int)
        self.assertEqual(s.isolation_mode,fdb.monitor.ISOLATION_READ_COMMITTED_RV)
        self.assertEqual(s.lock_timeout,fdb.monitor.INFINITE_WAIT)
        self.assertIsInstance(s.statements,list)
        for x in s.statements:
            self.assertIsInstance(x,fdb.monitor.StatementInfo)
        self.assertIsInstance(s.variables,list)
        self.assertEqual(s.iostats.group,fdb.monitor.STAT_TRANSACTION)
        self.assertEqual(s.iostats.stat_id,s.stat_id)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(m.db.tablestats),0)
        else:
            self.assertGreater(len(m.db.tablestats),0)
        #
        self.assertTrue(s.isactive())
        self.assertFalse(s.isidle())
        self.assertTrue(s.isreadonly())
        self.assertFalse(s.isautocommit())
        self.assertTrue(s.isautoundo())
        #
        s = m.get_transaction(c.transaction.trans_info(fdb.isc_info_tra_id))
        self.assertIsInstance(s.variables,list)
        if self.con.ods >= fdb.ODS_FB_25:
            self.assertGreater(len(s.variables),0)
        else:
            self.assertEqual(len(s.variables),0)
        for x in s.variables:
            self.assertIsInstance(x,fdb.monitor.ContextVariableInfo)
    def testStatementInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        m = self.con.monitor
        m.refresh()
        s = m.this_attachment.statements[0]
        #
        self.assertIsInstance(s.id,int)
        self.assertIs(s.attachment,m.this_attachment)
        self.assertIs(s.transaction,m.transactions[0])
        self.assertIn(s.state,[fdb.monitor.STATE_ACTIVE,fdb.monitor.STATE_IDLE])
        self.assertIsInstance(s.timestamp,datetime.datetime)
        self.assertEqual(s.sql_text,"select * from mon$database")
        if self.con.ods < fdb.ODS_FB_30:
            self.assertIsNone(s.plan)
        else:
            self.assertEqual(s.plan,'Select Expression\n    -> Table "MON$DATABASE" Full Scan')
        # We have to use mocks for callstack
        stack = []
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':1, 'MON$STATEMENT_ID':s.id-1, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'TRIGGER_1', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+100}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':2, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'TRIGGER_2', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+101}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':3, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':2,
             'MON$OBJECT_NAME':'PROC_1', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':2, 'MON$SOURCE_COLUMN':2, 'MON$STAT_ID':s.stat_id+102}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':4, 'MON$STATEMENT_ID':s.id, 'MON$CALLER_ID':3,
             'MON$OBJECT_NAME':'PROC_2', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':3, 'MON$SOURCE_COLUMN':3, 'MON$STAT_ID':s.stat_id+103}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':5, 'MON$STATEMENT_ID':s.id+1, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'PROC_3', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':s.stat_id+104}))
        m.__dict__['_Monitor__callstack'] = stack
        #
        self.assertListEqual(s.callstack,[stack[1],stack[2],stack[3]])
        self.assertEqual(s.iostats.group,fdb.monitor.STAT_STATEMENT)
        self.assertEqual(s.iostats.stat_id,s.stat_id)
        if self.con.ods < fdb.ODS_FB_30:
            self.assertEqual(len(m.db.tablestats),0)
        else:
            self.assertGreater(len(m.db.tablestats),0)
        #
        self.assertTrue(s.isactive())
        self.assertFalse(s.isidle())
    def testCallStackInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        m = self.con.monitor
        m.refresh()
        stmt = m.this_attachment.statements[0]
        # We have to use mocks for callstack
        stack = []
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':1, 'MON$STATEMENT_ID':stmt.id-1, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'POST_NEW_ORDER', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+100}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':2, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'POST_NEW_ORDER', 'MON$OBJECT_TYPE':2, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+101}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':3, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':2,
             'MON$OBJECT_NAME':'SHIP_ORDER', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':2, 'MON$SOURCE_COLUMN':2, 'MON$STAT_ID':stmt.stat_id+102}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':4, 'MON$STATEMENT_ID':stmt.id, 'MON$CALLER_ID':3,
             'MON$OBJECT_NAME':'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':3, 'MON$SOURCE_COLUMN':3, 'MON$STAT_ID':stmt.stat_id+103}))
        stack.append(fdb.monitor.CallStackInfo(m,
            {'MON$CALL_ID':5, 'MON$STATEMENT_ID':stmt.id+1, 'MON$CALLER_ID':None,
             'MON$OBJECT_NAME':'SUB_TOT_BUDGET', 'MON$OBJECT_TYPE':5, 'MON$TIMESTAMP':datetime.datetime.now(),
             'MON$SOURCE_LINE':1, 'MON$SOURCE_COLUMN':1, 'MON$STAT_ID':stmt.stat_id+104}))
        m.__dict__['_Monitor__callstack'] = stack
        data = m.iostats[0]._attributes
        data['MON$STAT_ID'] = stmt.stat_id+101
        data['MON$STAT_GROUP'] = fdb.monitor.STAT_CALL
        m.__dict__['_Monitor__iostats'].append(fdb.monitor.IOStatsInfo(m,data))
        #
        s = m.get_call(2)
        #
        self.assertEqual(s.id,2)
        self.assertIs(s.statement,m.get_statement(stmt.id))
        self.assertIsNone(s.caller)
        self.assertIsInstance(s.dbobject,sm.Trigger)
        self.assertEqual(s.dbobject.name,'POST_NEW_ORDER')
        self.assertIsInstance(s.timestamp,datetime.datetime)
        self.assertEqual(s.line,1)
        self.assertEqual(s.column,1)
        self.assertEqual(s.iostats.group,fdb.monitor.STAT_CALL)
        self.assertEqual(s.iostats.stat_id,s.stat_id)
        #
        x = m.get_call(3)
        self.assertIs(x.caller,s)
        self.assertIsInstance(x.dbobject,sm.Procedure)
        self.assertEqual(x.dbobject.name,'SHIP_ORDER')
    def testIOStatsInfo(self):
        if self.con.ods < fdb.ODS_FB_21:
            return
        m = self.con.monitor
        m.refresh()
        #
        for io in m.iostats:
            self.assertIs(io,io.owner.iostats)
        #
        s = m.iostats[0]
        self.assertIsInstance(s.owner,fdb.monitor.DatabaseInfo)
        self.assertEqual(s.group,fdb.monitor.STAT_DATABASE)
        self.assertIsInstance(s.reads,int)
        self.assertIsInstance(s.writes,int)
        self.assertIsInstance(s.fetches,int)
        self.assertIsInstance(s.marks,int)
        self.assertIsInstance(s.seq_reads,int)
        self.assertIsInstance(s.idx_reads,int)
        self.assertIsInstance(s.inserts,int)
        self.assertIsInstance(s.updates,int)
        self.assertIsInstance(s.deletes,int)
        self.assertIsInstance(s.backouts,int)
        self.assertIsInstance(s.purges,int)
        self.assertIsInstance(s.expunges,int)
        if self.con.ods >= fdb.ODS_FB_30:
            self.assertIsInstance(s.locks,int)
            self.assertIsInstance(s.waits,int)
            self.assertIsInstance(s.conflits,int)
            self.assertIsInstance(s.backversion_reads,int)
            self.assertIsInstance(s.fragment_reads,int)
            self.assertIsInstance(s.repeated_reads,int)
        else:
            self.assertIsNone(s.locks)
            self.assertIsNone(s.waits)
            self.assertIsNone(s.conflits)
            self.assertIsNone(s.backversion_reads)
            self.assertIsNone(s.fragment_reads)
            self.assertIsNone(s.repeated_reads)
        if self.con.ods >= fdb.ODS_FB_25:
            self.assertIsInstance(s.memory_used,int)
            self.assertIsInstance(s.memory_allocated,int)
            self.assertIsInstance(s.max_memory_used,int)
            self.assertIsInstance(s.max_memory_allocated,int)
        else:
            self.assertIsNone(s.memory_used)
            self.assertIsNone(s.memory_allocated)
            self.assertIsNone(s.max_memory_used)
            self.assertIsNone(s.max_memory_allocated)
    def testContextVariableInfo(self):
        if self.con.ods <= fdb.ODS_FB_21:
            return
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_SESSION','SVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        c = self.con.cursor()
        sql = "select RDB$SET_CONTEXT('USER_TRANSACTION','TVAR','TEST_VALUE') from rdb$database"
        c.execute(sql)
        c.fetchone()
        m = self.con.monitor
        m.refresh()
        #
        self.assertEqual(len(m.variables),2)
        #
        s = m.variables[0]
        self.assertIs(s.attachment,m.this_attachment)
        self.assertIsNone(s.transaction)
        self.assertEqual(s.name,'SVAR')
        self.assertEqual(s.value,'TEST_VALUE')
        self.assertTrue(s.isattachmentvar())
        self.assertFalse(s.istransactionvar())
        #
        s = m.variables[1]
        self.assertIsNone(s.attachment)
        self.assertIs(s.transaction,
            m.get_transaction(c.transaction.trans_info(fdb.isc_info_tra_id)))
        self.assertEqual(s.name,'TVAR')
        self.assertEqual(s.value,'TEST_VALUE')
        self.assertFalse(s.isattachmentvar())
        self.assertTrue(s.istransactionvar())


class TestConnectionWithSchema(FDBTestBase):
    def setUp(self):
        super(TestConnectionWithSchema,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,self.FBTEST_DB)
        #self.con = fdb.connect(dsn=self.dbfile,user=FBTEST_USER,password=FBTEST_PASSWORD)
    def tearDown(self):
        #self.con.close()
        pass
    def testConnectSchema(self):
        s = fdb.connect(host=FBTEST_HOST,database=self.dbfile,user=FBTEST_USER,
                        password=FBTEST_PASSWORD,
                        connection_class=fdb.ConnectionWithSchema)
        if s.ods < fdb.ODS_FB_30:
            self.assertEqual(len(s.tables),15)
        else:
            self.assertEqual(len(s.tables),16)
        self.assertEqual(s.get_table('JOB').name,'JOB')


class TestBugs(FDBTestBase):
    def setUp(self):
        super(TestBugs,self).setUp()
        self.cwd = os.getcwd()
        self.dbpath = os.path.join(self.cwd,'test')
        self.dbfile = os.path.join(self.dbpath,'fbbugs.fdb')
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.con = fdb.create_database(host=FBTEST_HOST,database=self.dbfile,
                                       user=FBTEST_USER,password=FBTEST_PASSWORD)
    def tearDown(self):
        self.con.drop_database()
        self.con.close()
    def test_pyfb_17(self):
        create_table = """
        Create Table table1  (
            ID Integer,
            C1 Integer NOT Null
        );
        """

        create_trigger = """CREATE TRIGGER BIU_Trigger FOR table1
        ACTIVE BEFORE INSERT OR UPDATE POSITION 0
        as
        begin
          if (new.C1 IS NULL) then
          begin
            new.C1 = 1;
          end
        end
        """

        cur = self.con.cursor()
        cur.execute(create_table)
        cur.execute(create_trigger)
        self.con.commit()
        # PYFB-17: fails with fdb, passes with kinterbasdb
        cur.execute('insert into table1 (ID, C1) values(1, ?)', (None, ))
    def test_pyfb_22(self):
        create_table = """
        CREATE TABLE FDBTEST (
            ID INTEGER,
            TEST80 VARCHAR(80),
            TEST128 VARCHAR(128),
            TEST255 VARCHAR(255),
            TEST1024 VARCHAR(1024),
            TESTCLOB BLOB SUB_TYPE 1 SEGMENT SIZE 255
        );
        """
        cur = self.con.cursor()
        cur.execute(create_table)
        self.con.commit()
        # test data
        data = ("1234567890" * 25) + "12345"
        for i in ibase.xrange(255):
            cur.execute("insert into fdbtest (id, test255) values (?, ?)",
                        (i, data[:i]))
        self.con.commit()
        # PYFB-22: fails with fdb, passes with kinterbasdb
        cur.execute("select test255 from fdbtest order by id")
        i = 0
        for row in cur:
            value = row[0]
            self.assertEqual(len(value),i)
            self.assertEqual(value,data[:i])
            i += 1
    def test_pyfb_25(self):
        create_table = """
        CREATE TABLE FDBTEST2 (
            ID INTEGER,
            TEST5000 VARCHAR(5000)
        );
        """
        cur = self.con.cursor()
        cur.execute(create_table)
        self.con.commit()
        # test data
        data = "1234567890" * 500
        cur.execute("insert into fdbtest2 (id, test5000) values (?, ?)",
                    (1, data))
        self.con.commit()
        # PYFB-25: fails with fdb, passes with kinterbasdb
        cur.execute("select test5000 from fdbtest2")
        row = cur.fetchone()
        self.assertEqual(row[0],data)
    def test_pyfb_30(self):
        create_table = """
        CREATE TABLE FDBTEST3 (
            ID INTEGER,
            T_BLOB BLOB sub_type BINARY
        );
        """
        cur = self.con.cursor()
        cur.execute(create_table)
        self.con.commit()
        # test data
        data_bytes = (1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9)
        blob_data = fdb.bs(data_bytes)
        cur.execute("insert into fdbtest3 (id, t_blob) values (?, ?)",
                    (1, blob_data))
        cur.execute("insert into fdbtest3 (id, t_blob) values (?, ?)",
                    (2, BytesIO(blob_data)))
        self.con.commit()
        # PYFB-XX: binary blob trucated at zero-byte
        cur.execute("select t_blob from fdbtest3 where id = 1")
        row = cur.fetchone()
        self.assertEqual(row[0],blob_data)
        cur.execute("select t_blob from fdbtest3 where id = 2")
        row = cur.fetchone()
        self.assertEqual(row[0],blob_data)
        p = cur.prep("select t_blob from fdbtest3 where id = 2")
        p.set_stream_blob('T_BLOB')
        cur.execute(p)
        blob_reader = cur.fetchone()[0]
        value = blob_reader.read()
        self.assertEqual(value,blob_data)
    def test_pyfb_34(self):
        cur = self.con.cursor()
        cur.execute( "select * from RDB$Relations")
        cur.fetchall()
        del cur
    def test_pyfb_35(self):
        create_table = """
        Create Table table1  (
            ID Integer,
            C1 Integer NOT Null
        );
        """

        c = self.con.cursor()
        c.execute(create_table)
        self.con.commit()
        del c

        cur = self.con.cursor()
        with self.assertRaises(fdb.DatabaseError) as cm:
            cur.fetchall()
        self.assertTupleEqual(cm.exception.args,
            ("Cannot fetch from this cursor because it has not executed a statement.",))

        cur.execute("select * from RDB$DATABASE")
        cur.fetchall()
        cur.execute("CREATE SEQUENCE TEST_SEQ_1")
        with self.assertRaises(fdb.DatabaseError) as cm:
            cur.fetchall()
        self.assertTupleEqual(cm.exception.args,
            ("Attempt to fetch row of results after statement that does not produce result set.",))

        cur.execute("insert into table1 (ID,C1) values (1,1) returning ID")
        row = cur.fetchall()
        self.assertListEqual(row,[(1,)])

    def test_pyfb_44(self):
        self.con2 = fdb.connect(host=FBTEST_HOST,database=os.path.join(self.dbpath,self.FBTEST_DB),
                                user=FBTEST_USER,password=FBTEST_PASSWORD)
        try:
            cur = self.con2.cursor()
            now = datetime.datetime(2011,11,13,15,00,1,200)
            cur.execute('insert into T2 (C1,C8) values (?,?)',[3,now.date()])
            self.con2.commit()
            cur.execute('select C1,C8 from T2 where C1 = 3')
            rows = cur.fetchall()
            self.assertListEqual(rows,
                [(3, datetime.datetime(2011, 11, 13, 0, 0, 0, 0))])
        finally:
            self.con2.execute_immediate("delete from t2")
            self.con2.commit()
            self.con2.close()



if __name__ == '__main__':
    unittest.main()

