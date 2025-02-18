#!/usr/bin/env python
# coding: utf-8

import os
import re
import subprocess
import tempfile
import testgres
import time
import six
import unittest
import psutil

import logging.config

from contextlib import contextmanager
from shutil import rmtree

from testgres import \
    InitNodeException, \
    StartNodeException, \
    ExecUtilException, \
    BackupException, \
    QueryException, \
    TimeoutException, \
    TestgresException, \
    InvalidOperationException, \
    NodeApp

from testgres import \
    TestgresConfig, \
    configure_testgres, \
    scoped_config, \
    pop_config

from testgres import \
    NodeStatus, \
    ProcessType, \
    IsolationLevel, \
    get_new_node

from testgres import \
    get_bin_path, \
    get_pg_config, \
    get_pg_version

from testgres import \
    First, \
    Any

# NOTE: those are ugly imports
from testgres import bound_ports
from testgres.utils import PgVer, parse_pg_version
from testgres.node import ProcessProxy


def pg_version_ge(version):
    cur_ver = PgVer(get_pg_version())
    min_ver = PgVer(version)
    return cur_ver >= min_ver


def util_exists(util):
    def good_properties(f):
        return (os.path.exists(f) and  # noqa: W504
                os.path.isfile(f) and  # noqa: W504
                os.access(f, os.X_OK))  # yapf: disable

    # try to resolve it
    if good_properties(get_bin_path(util)):
        return True

    # check if util is in PATH
    for path in os.environ["PATH"].split(os.pathsep):
        if good_properties(os.path.join(path, util)):
            return True


def rm_carriage_returns(out):
    """
    In Windows we have additional '\r' symbols in output.
    Let's get rid of them.
    """
    if os.name == 'nt':
        if isinstance(out, (int, float, complex)):
            return out
        elif isinstance(out, tuple):
            return tuple(rm_carriage_returns(item) for item in out)
        elif isinstance(out, bytes):
            return out.replace(b'\r', b'')
        else:
            return out.replace('\r', '')
    else:
        return out


@contextmanager
def removing(f):
    try:
        yield f
    finally:
        if os.path.isfile(f):
            os.remove(f)
        elif os.path.isdir(f):
            rmtree(f, ignore_errors=True)


class TestgresTests(unittest.TestCase):
    def test_node_repr(self):
        with get_new_node() as node:
            pattern = r"PostgresNode\(name='.+', port=.+, base_dir='.+'\)"
            self.assertIsNotNone(re.match(pattern, str(node)))

    def test_custom_init(self):
        with get_new_node() as node:
            # enable page checksums
            node.init(initdb_params=['-k']).start()

        with get_new_node() as node:
            node.init(
                allow_streaming=True,
                initdb_params=['--auth-local=reject', '--auth-host=reject'])

            hba_file = os.path.join(node.data_dir, 'pg_hba.conf')
            with open(hba_file, 'r') as conf:
                lines = conf.readlines()

                # check number of lines
                self.assertGreaterEqual(len(lines), 6)

                # there should be no trust entries at all
                self.assertFalse(any('trust' in s for s in lines))

    def test_double_init(self):
        with get_new_node().init() as node:
            # can't initialize node more than once
            with self.assertRaises(InitNodeException):
                node.init()

    def test_init_after_cleanup(self):
        with get_new_node() as node:
            node.init().start().execute('select 1')
            node.cleanup()
            node.init().start().execute('select 1')

    @unittest.skipUnless(util_exists('pg_resetwal.exe' if os.name == 'nt' else 'pg_resetwal'), 'pgbench might be missing')
    @unittest.skipUnless(pg_version_ge('9.6'), 'requires 9.6+')
    def test_init_unique_system_id(self):
        # this function exists in PostgreSQL 9.6+
        query = 'select system_identifier from pg_control_system()'

        with scoped_config(cache_initdb=False):
            with get_new_node().init().start() as node0:
                id0 = node0.execute(query)[0]

        with scoped_config(cache_initdb=True,
                           cached_initdb_unique=True) as config:
            self.assertTrue(config.cache_initdb)
            self.assertTrue(config.cached_initdb_unique)

            # spawn two nodes; ids must be different
            with get_new_node().init().start() as node1, \
                    get_new_node().init().start() as node2:

                id1 = node1.execute(query)[0]
                id2 = node2.execute(query)[0]

                # ids must increase
                self.assertGreater(id1, id0)
                self.assertGreater(id2, id1)

    def test_node_exit(self):
        base_dir = None

        with self.assertRaises(QueryException):
            with get_new_node().init() as node:
                base_dir = node.base_dir
                node.safe_psql('select 1')

        # we should save the DB for "debugging"
        self.assertTrue(os.path.exists(base_dir))
        rmtree(base_dir, ignore_errors=True)

        with get_new_node().init() as node:
            base_dir = node.base_dir

        # should have been removed by default
        self.assertFalse(os.path.exists(base_dir))

    def test_double_start(self):
        with get_new_node().init().start() as node:
            # can't start node more than once
            node.start()
            self.assertTrue(node.is_started)

    def test_uninitialized_start(self):
        with get_new_node() as node:
            # node is not initialized yet
            with self.assertRaises(StartNodeException):
                node.start()

    def test_restart(self):
        with get_new_node() as node:
            node.init().start()

            # restart, ok
            res = node.execute('select 1')
            self.assertEqual(res, [(1, )])
            node.restart()
            res = node.execute('select 2')
            self.assertEqual(res, [(2, )])

            # restart, fail
            with self.assertRaises(StartNodeException):
                node.append_conf('pg_hba.conf', 'DUMMY')
                node.restart()

    def test_reload(self):
        with get_new_node() as node:
            node.init().start()

            # change client_min_messages and save old value
            cmm_old = node.execute('show client_min_messages')
            node.append_conf(client_min_messages='DEBUG1')

            # reload config
            node.reload()

            # check new value
            cmm_new = node.execute('show client_min_messages')
            self.assertEqual('debug1', cmm_new[0][0].lower())
            self.assertNotEqual(cmm_old, cmm_new)

    def test_pg_ctl(self):
        with get_new_node() as node:
            node.init().start()

            status = node.pg_ctl(['status'])
            self.assertTrue('PID' in status)

    def test_status(self):
        self.assertTrue(NodeStatus.Running)
        self.assertFalse(NodeStatus.Stopped)
        self.assertFalse(NodeStatus.Uninitialized)

        # check statuses after each operation
        with get_new_node() as node:
            self.assertEqual(node.pid, 0)
            self.assertEqual(node.status(), NodeStatus.Uninitialized)

            node.init()

            self.assertEqual(node.pid, 0)
            self.assertEqual(node.status(), NodeStatus.Stopped)

            node.start()

            self.assertNotEqual(node.pid, 0)
            self.assertEqual(node.status(), NodeStatus.Running)

            node.stop()

            self.assertEqual(node.pid, 0)
            self.assertEqual(node.status(), NodeStatus.Stopped)

            node.cleanup()

            self.assertEqual(node.pid, 0)
            self.assertEqual(node.status(), NodeStatus.Uninitialized)

    def test_psql(self):
        with get_new_node().init().start() as node:

            # check returned values (1 arg)
            res = node.psql('select 1')
            self.assertEqual(rm_carriage_returns(res), (0, b'1\n', b''))

            # check returned values (2 args)
            res = node.psql('postgres', 'select 2')
            self.assertEqual(rm_carriage_returns(res), (0, b'2\n', b''))

            # check returned values (named)
            res = node.psql(query='select 3', dbname='postgres')
            self.assertEqual(rm_carriage_returns(res), (0, b'3\n', b''))

            # check returned values (1 arg)
            res = node.safe_psql('select 4')
            self.assertEqual(rm_carriage_returns(res), b'4\n')

            # check returned values (2 args)
            res = node.safe_psql('postgres', 'select 5')
            self.assertEqual(rm_carriage_returns(res), b'5\n')

            # check returned values (named)
            res = node.safe_psql(query='select 6', dbname='postgres')
            self.assertEqual(rm_carriage_returns(res), b'6\n')

            # check feeding input
            node.safe_psql('create table horns (w int)')
            node.safe_psql('copy horns from stdin (format csv)',
                           input=b"1\n2\n3\n\\.\n")
            _sum = node.safe_psql('select sum(w) from horns')
            self.assertEqual(rm_carriage_returns(_sum), b'6\n')

            # check psql's default args, fails
            with self.assertRaises(QueryException):
                node.psql()

            node.stop()

            # check psql on stopped node, fails
            with self.assertRaises(QueryException):
                node.safe_psql('select 1')

    def test_safe_psql__expect_error(self):
        with get_new_node().init().start() as node:
            err = node.safe_psql('select_or_not_select 1', expect_error=True)
            self.assertTrue(type(err) == str)  # noqa: E721
            self.assertIn('select_or_not_select', err)
            self.assertIn('ERROR:  syntax error at or near "select_or_not_select"', err)

            # ---------
            with self.assertRaises(InvalidOperationException) as ctx:
                node.safe_psql("select 1;", expect_error=True)

            self.assertEqual(str(ctx.exception), "Exception was expected, but query finished successfully: `select 1;`.")

            # ---------
            res = node.safe_psql("select 1;", expect_error=False)
            self.assertEqual(rm_carriage_returns(res), b'1\n')

    def test_transactions(self):
        with get_new_node().init().start() as node:

            with node.connect() as con:
                con.begin()
                con.execute('create table test(val int)')
                con.execute('insert into test values (1)')
                con.commit()

                con.begin()
                con.execute('insert into test values (2)')
                res = con.execute('select * from test order by val asc')
                self.assertListEqual(res, [(1, ), (2, )])
                con.rollback()

                con.begin()
                res = con.execute('select * from test')
                self.assertListEqual(res, [(1, )])
                con.rollback()

                con.begin()
                con.execute('drop table test')
                con.commit()

    def test_control_data(self):
        with get_new_node() as node:

            # node is not initialized yet
            with self.assertRaises(ExecUtilException):
                node.get_control_data()

            node.init()
            data = node.get_control_data()

            # check returned dict
            self.assertIsNotNone(data)
            self.assertTrue(any('pg_control' in s for s in data.keys()))

    def test_backup_simple(self):
        with get_new_node() as master:

            # enable streaming for backups
            master.init(allow_streaming=True)

            # node must be running
            with self.assertRaises(BackupException):
                master.backup()

            # it's time to start node
            master.start()

            # fill node with some data
            master.psql('create table test as select generate_series(1, 4) i')

            with master.backup(xlog_method='stream') as backup:
                with backup.spawn_primary().start() as slave:
                    res = slave.execute('select * from test order by i asc')
                    self.assertListEqual(res, [(1, ), (2, ), (3, ), (4, )])

    def test_backup_multiple(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup1, \
                    node.backup(xlog_method='fetch') as backup2:
                self.assertNotEqual(backup1.base_dir, backup2.base_dir)

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1', destroy=False) as node1, \
                        backup.spawn_primary('node2', destroy=False) as node2:
                    self.assertNotEqual(node1.base_dir, node2.base_dir)

    def test_backup_exhaust(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup:
                # exhaust backup by creating new node
                with backup.spawn_primary():
                    pass

                # now let's try to create one more node
                with self.assertRaises(BackupException):
                    backup.spawn_primary()

    def test_backup_wrong_xlog_method(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            with self.assertRaises(BackupException,
                                   msg='Invalid xlog_method "wrong"'):
                node.backup(xlog_method='wrong')

    def test_pg_ctl_wait_option(self):
        with get_new_node() as node:
            node.init().start(wait=False)
            while True:
                try:
                    node.stop(wait=False)
                    break
                except ExecUtilException:
                    # it's ok to get this exception here since node
                    # could be not started yet
                    pass

    def test_replicate(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            with node.replicate().start() as replica:
                res = replica.execute('select 1')
                self.assertListEqual(res, [(1, )])

                node.execute('create table test (val int)', commit=True)

                replica.catchup()

                res = node.execute('select * from test')
                self.assertListEqual(res, [])

    @unittest.skipUnless(pg_version_ge('9.6'), 'requires 9.6+')
    def test_synchronous_replication(self):
        with get_new_node() as master:
            old_version = not pg_version_ge('9.6')

            master.init(allow_streaming=True).start()

            if not old_version:
                master.append_conf('synchronous_commit = remote_apply')

            # create standby
            with master.replicate() as standby1, master.replicate() as standby2:
                standby1.start()
                standby2.start()

                # check formatting
                self.assertEqual(
                    '1 ("{}", "{}")'.format(standby1.name, standby2.name),
                    str(First(1, (standby1, standby2))))  # yapf: disable
                self.assertEqual(
                    'ANY 1 ("{}", "{}")'.format(standby1.name, standby2.name),
                    str(Any(1, (standby1, standby2))))  # yapf: disable

                # set synchronous_standby_names
                master.set_synchronous_standbys(First(2, [standby1, standby2]))
                master.restart()

                # the following part of the test is only applicable to newer
                # versions of PostgresQL
                if not old_version:
                    master.safe_psql('create table abc(a int)')

                    # Create a large transaction that will take some time to apply
                    # on standby to check that it applies synchronously
                    # (If set synchronous_commit to 'on' or other lower level then
                    # standby most likely won't catchup so fast and test will fail)
                    master.safe_psql(
                        'insert into abc select generate_series(1, 1000000)')
                    res = standby1.safe_psql('select count(*) from abc')
                    self.assertEqual(rm_carriage_returns(res), b'1000000\n')

    @unittest.skipUnless(pg_version_ge('10'), 'requires 10+')
    def test_logical_replication(self):
        with get_new_node() as node1, get_new_node() as node2:
            node1.init(allow_logical=True)
            node1.start()
            node2.init().start()

            create_table = 'create table test (a int, b int)'
            node1.safe_psql(create_table)
            node2.safe_psql(create_table)

            # create publication / create subscription
            pub = node1.publish('mypub')
            sub = node2.subscribe(pub, 'mysub')

            node1.safe_psql('insert into test values (1, 1), (2, 2)')

            # wait until changes apply on subscriber and check them
            sub.catchup()
            res = node2.execute('select * from test')
            self.assertListEqual(res, [(1, 1), (2, 2)])

            # disable and put some new data
            sub.disable()
            node1.safe_psql('insert into test values (3, 3)')

            # enable and ensure that data successfully transferred
            sub.enable()
            sub.catchup()
            res = node2.execute('select * from test')
            self.assertListEqual(res, [(1, 1), (2, 2), (3, 3)])

            # Add new tables. Since we added "all tables" to publication
            # (default behaviour of publish() method) we don't need
            # to explicitly perform pub.add_tables()
            create_table = 'create table test2 (c char)'
            node1.safe_psql(create_table)
            node2.safe_psql(create_table)
            sub.refresh()

            # put new data
            node1.safe_psql('insert into test2 values (\'a\'), (\'b\')')
            sub.catchup()
            res = node2.execute('select * from test2')
            self.assertListEqual(res, [('a', ), ('b', )])

            # drop subscription
            sub.drop()
            pub.drop()

            # create new publication and subscription for specific table
            # (omitting copying data as it's already done)
            pub = node1.publish('newpub', tables=['test'])
            sub = node2.subscribe(pub, 'newsub', copy_data=False)

            node1.safe_psql('insert into test values (4, 4)')
            sub.catchup()
            res = node2.execute('select * from test')
            self.assertListEqual(res, [(1, 1), (2, 2), (3, 3), (4, 4)])

            # explicitly add table
            with self.assertRaises(ValueError):
                pub.add_tables([])    # fail
            pub.add_tables(['test2'])
            node1.safe_psql('insert into test2 values (\'c\')')
            sub.catchup()
            res = node2.execute('select * from test2')
            self.assertListEqual(res, [('a', ), ('b', )])

    @unittest.skipUnless(pg_version_ge('10'), 'requires 10+')
    def test_logical_catchup(self):
        """ Runs catchup for 100 times to be sure that it is consistent """
        with get_new_node() as node1, get_new_node() as node2:
            node1.init(allow_logical=True)
            node1.start()
            node2.init().start()

            create_table = 'create table test (key int primary key, val int); '
            node1.safe_psql(create_table)
            node1.safe_psql('alter table test replica identity default')
            node2.safe_psql(create_table)

            # create publication / create subscription
            sub = node2.subscribe(node1.publish('mypub'), 'mysub')

            for i in range(0, 100):
                node1.execute('insert into test values ({0}, {0})'.format(i))
                sub.catchup()
                res = node2.execute('select * from test')
                self.assertListEqual(res, [(
                    i,
                    i,
                )])
                node1.execute('delete from test')

    @unittest.skipIf(pg_version_ge('10'), 'requires <10')
    def test_logical_replication_fail(self):
        with get_new_node() as node:
            with self.assertRaises(InitNodeException):
                node.init(allow_logical=True)

    def test_replication_slots(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            with node.replicate(slot='slot1').start() as replica:
                replica.execute('select 1')

                # cannot create new slot with the same name
                with self.assertRaises(TestgresException):
                    node.replicate(slot='slot1')

    def test_incorrect_catchup(self):
        with get_new_node() as node:
            node.init(allow_streaming=True).start()

            # node has no master, can't catch up
            with self.assertRaises(TestgresException):
                node.catchup()

    def test_promotion(self):
        with get_new_node() as master:
            master.init().start()
            master.safe_psql('create table abc(id serial)')

            with master.replicate().start() as replica:
                master.stop()
                replica.promote()

                # make standby becomes writable master
                replica.safe_psql('insert into abc values (1)')
                res = replica.safe_psql('select * from abc')
                self.assertEqual(rm_carriage_returns(res), b'1\n')

    def test_dump(self):
        query_create = 'create table test as select generate_series(1, 2) as val'
        query_select = 'select * from test order by val asc'

        with get_new_node().init().start() as node1:

            node1.execute(query_create)
            for format in ['plain', 'custom', 'directory', 'tar']:
                with removing(node1.dump(format=format)) as dump:
                    with get_new_node().init().start() as node3:
                        if format == 'directory':
                            self.assertTrue(os.path.isdir(dump))
                        else:
                            self.assertTrue(os.path.isfile(dump))
                        # restore dump
                        node3.restore(filename=dump)
                        res = node3.execute(query_select)
                        self.assertListEqual(res, [(1, ), (2, )])

    def test_users(self):
        with get_new_node().init().start() as node:
            node.psql('create role test_user login')
            value = node.safe_psql('select 1', username='test_user')
            value = rm_carriage_returns(value)
            self.assertEqual(value, b'1\n')

    def test_poll_query_until(self):
        with get_new_node() as node:
            node.init().start()

            get_time = 'select extract(epoch from now())'
            check_time = 'select extract(epoch from now()) - {} >= 5'

            start_time = node.execute(get_time)[0][0]
            node.poll_query_until(query=check_time.format(start_time))
            end_time = node.execute(get_time)[0][0]

            self.assertTrue(end_time - start_time >= 5)

            # check 0 columns
            with self.assertRaises(QueryException):
                node.poll_query_until(
                    query='select from pg_catalog.pg_class limit 1')

            # check None, fail
            with self.assertRaises(QueryException):
                node.poll_query_until(query='create table abc (val int)')

            # check None, ok
            node.poll_query_until(query='create table def()',
                                  expected=None)    # returns nothing

            # check 0 rows equivalent to expected=None
            node.poll_query_until(
                query='select * from pg_catalog.pg_class where true = false',
                expected=None)

            # check arbitrary expected value, fail
            with self.assertRaises(TimeoutException):
                node.poll_query_until(query='select 3',
                                      expected=1,
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check arbitrary expected value, ok
            node.poll_query_until(query='select 2', expected=2)

            # check timeout
            with self.assertRaises(TimeoutException):
                node.poll_query_until(query='select 1 > 2',
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check ProgrammingError, fail
            with self.assertRaises(testgres.ProgrammingError):
                node.poll_query_until(query='dummy1')

            # check ProgrammingError, ok
            with self.assertRaises(TimeoutException):
                node.poll_query_until(query='dummy2',
                                      max_attempts=3,
                                      sleep_time=0.01,
                                      suppress={testgres.ProgrammingError})

            # check 1 arg, ok
            node.poll_query_until('select true')

    def test_logging(self):
        logfile = tempfile.NamedTemporaryFile('w', delete=True)

        log_conf = {
            'version': 1,
            'handlers': {
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': logfile.name,
                    'formatter': 'base_format',
                    'level': logging.DEBUG,
                },
            },
            'formatters': {
                'base_format': {
                    'format': '%(node)-5s: %(message)s',
                },
            },
            'root': {
                'handlers': ('file', ),
                'level': 'DEBUG',
            },
        }

        logging.config.dictConfig(log_conf)

        with scoped_config(use_python_logging=True):
            node_name = 'master'

            with get_new_node(name=node_name) as master:
                master.init().start()

                # execute a dummy query a few times
                for i in range(20):
                    master.execute('select 1')
                    time.sleep(0.01)

                # let logging worker do the job
                time.sleep(0.1)

                # check that master's port is found
                with open(logfile.name, 'r') as log:
                    lines = log.readlines()
                    self.assertTrue(any(node_name in s for s in lines))

                # test logger after stop/start/restart
                master.stop()
                master.start()
                master.restart()
                self.assertTrue(master._logger.is_alive())

    @unittest.skipUnless(util_exists('pgbench.exe' if os.name == 'nt' else 'pgbench'), 'pgbench might be missing')
    def test_pgbench(self):
        with get_new_node().init().start() as node:

            # initialize pgbench DB and run benchmarks
            node.pgbench_init(scale=2, foreign_keys=True,
                              options=['-q']).pgbench_run(time=2)

            # run TPC-B benchmark
            proc = node.pgbench(stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                options=['-T3'])

            out, _ = proc.communicate()
            out = out.decode('utf-8')

            proc.stdout.close()

            self.assertTrue('tps' in out)

    def test_pg_config(self):
        # check same instances
        a = get_pg_config()
        b = get_pg_config()
        self.assertEqual(id(a), id(b))

        # save right before config change
        c1 = get_pg_config()

        # modify setting for this scope
        with scoped_config(cache_pg_config=False) as config:
            # sanity check for value
            self.assertFalse(config.cache_pg_config)

            # save right after config change
            c2 = get_pg_config()

            # check different instances after config change
            self.assertNotEqual(id(c1), id(c2))

            # check different instances
            a = get_pg_config()
            b = get_pg_config()
            self.assertNotEqual(id(a), id(b))

    def test_config_stack(self):
        # no such option
        with self.assertRaises(TypeError):
            configure_testgres(dummy=True)

        # we have only 1 config in stack
        with self.assertRaises(IndexError):
            pop_config()

        d0 = TestgresConfig.cached_initdb_dir
        d1 = 'dummy_abc'
        d2 = 'dummy_def'

        with scoped_config(cached_initdb_dir=d1) as c1:
            self.assertEqual(c1.cached_initdb_dir, d1)

            with scoped_config(cached_initdb_dir=d2) as c2:
                stack_size = len(testgres.config.config_stack)

                # try to break a stack
                with self.assertRaises(TypeError):
                    with scoped_config(dummy=True):
                        pass

                self.assertEqual(c2.cached_initdb_dir, d2)
                self.assertEqual(len(testgres.config.config_stack), stack_size)

            self.assertEqual(c1.cached_initdb_dir, d1)

        self.assertEqual(TestgresConfig.cached_initdb_dir, d0)

    def test_unix_sockets(self):
        with get_new_node() as node:
            node.init(unix_sockets=False, allow_streaming=True)
            node.start()

            node.execute('select 1')
            node.safe_psql('select 1')

            with node.replicate().start() as r:
                r.execute('select 1')
                r.safe_psql('select 1')

    def test_auto_name(self):
        with get_new_node().init(allow_streaming=True).start() as m:
            with m.replicate().start() as r:
                # check that nodes are running
                self.assertTrue(m.status())
                self.assertTrue(r.status())

                # check their names
                self.assertNotEqual(m.name, r.name)
                self.assertTrue('testgres' in m.name)
                self.assertTrue('testgres' in r.name)

    def test_file_tail(self):
        from testgres.utils import file_tail

        s1 = "the quick brown fox jumped over that lazy dog\n"
        s2 = "abc\n"
        s3 = "def\n"

        with tempfile.NamedTemporaryFile(mode='r+', delete=True) as f:
            sz = 0
            while sz < 3 * 8192:
                sz += len(s1)
                f.write(s1)
            f.write(s2)
            f.write(s3)

            f.seek(0)
            lines = file_tail(f, 3)
            self.assertEqual(lines[0], s1)
            self.assertEqual(lines[1], s2)
            self.assertEqual(lines[2], s3)

            f.seek(0)
            lines = file_tail(f, 1)
            self.assertEqual(lines[0], s3)

    def test_isolation_levels(self):
        with get_new_node().init().start() as node:
            with node.connect() as con:
                # string levels
                con.begin('Read Uncommitted').commit()
                con.begin('Read Committed').commit()
                con.begin('Repeatable Read').commit()
                con.begin('Serializable').commit()

                # enum levels
                con.begin(IsolationLevel.ReadUncommitted).commit()
                con.begin(IsolationLevel.ReadCommitted).commit()
                con.begin(IsolationLevel.RepeatableRead).commit()
                con.begin(IsolationLevel.Serializable).commit()

                # check wrong level
                with self.assertRaises(QueryException):
                    con.begin('Garbage').commit()

    def test_ports_management(self):
        # check that no ports have been bound yet
        self.assertEqual(len(bound_ports), 0)

        with get_new_node() as node:
            # check that we've just bound a port
            self.assertEqual(len(bound_ports), 1)

            # check that bound_ports contains our port
            port_1 = list(bound_ports)[0]
            port_2 = node.port
            self.assertEqual(port_1, port_2)

        # check that port has been freed successfully
        self.assertEqual(len(bound_ports), 0)

    def test_exceptions(self):
        str(StartNodeException('msg', [('file', 'lines')]))
        str(ExecUtilException('msg', 'cmd', 1, 'out'))
        str(QueryException('msg', 'query'))

    def test_version_management(self):
        a = PgVer('10.0')
        b = PgVer('10')
        c = PgVer('9.6.5')
        d = PgVer('15.0')
        e = PgVer('15rc1')
        f = PgVer('15beta4')
        h = PgVer('15.3biha')
        i = PgVer('15.3')
        g = PgVer('15.3.1bihabeta1')
        k = PgVer('15.3.1')

        self.assertTrue(a == b)
        self.assertTrue(b > c)
        self.assertTrue(a > c)
        self.assertTrue(d > e)
        self.assertTrue(e > f)
        self.assertTrue(d > f)
        self.assertTrue(h > f)
        self.assertTrue(h == i)
        self.assertTrue(g == k)
        self.assertTrue(g > h)

        version = get_pg_version()
        with get_new_node() as node:
            self.assertTrue(isinstance(version, six.string_types))
            self.assertTrue(isinstance(node.version, PgVer))
            self.assertEqual(node.version, PgVer(version))

    def test_child_pids(self):
        master_processes = [
            ProcessType.AutovacuumLauncher,
            ProcessType.BackgroundWriter,
            ProcessType.Checkpointer,
            ProcessType.StatsCollector,
            ProcessType.WalSender,
            ProcessType.WalWriter,
        ]

        if pg_version_ge('10'):
            master_processes.append(ProcessType.LogicalReplicationLauncher)

        if pg_version_ge('14'):
            master_processes.remove(ProcessType.StatsCollector)

        repl_processes = [
            ProcessType.Startup,
            ProcessType.WalReceiver,
        ]

        with get_new_node().init().start() as master:

            # master node doesn't have a source walsender!
            with self.assertRaises(TestgresException):
                master.source_walsender

            with master.connect() as con:
                self.assertGreater(con.pid, 0)

            with master.replicate().start() as replica:

                # test __str__ method
                str(master.child_processes[0])

                master_pids = master.auxiliary_pids
                for ptype in master_processes:
                    self.assertIn(ptype, master_pids)

                replica_pids = replica.auxiliary_pids
                for ptype in repl_processes:
                    self.assertIn(ptype, replica_pids)

                # there should be exactly 1 source walsender for replica
                self.assertEqual(len(master_pids[ProcessType.WalSender]), 1)
                pid1 = master_pids[ProcessType.WalSender][0]
                pid2 = replica.source_walsender.pid
                self.assertEqual(pid1, pid2)

                replica.stop()

                # there should be no walsender after we've stopped replica
                with self.assertRaises(TestgresException):
                    replica.source_walsender

    def test_child_process_dies(self):
        # test for FileNotFound exception during child_processes() function
        cmd = ["timeout", "60"] if os.name == 'nt' else ["sleep", "60"]

        with subprocess.Popen(cmd, shell=True) as process:  # shell=True might be needed on Windows
            self.assertEqual(process.poll(), None)
            # collect list of processes currently running
            children = psutil.Process(os.getpid()).children()
            # kill a process, so received children dictionary becomes invalid
            process.kill()
            process.wait()
            # try to handle children list -- missing processes will have ptype "ProcessType.Unknown"
            [ProcessProxy(p) for p in children]

    def test_upgrade_node(self):
        old_bin_dir = os.path.dirname(get_bin_path("pg_config"))
        new_bin_dir = os.path.dirname(get_bin_path("pg_config"))
        node_old = get_new_node(prefix='node_old', bin_dir=old_bin_dir)
        node_old.init()
        node_old.start()
        node_old.stop()
        node_new = get_new_node(prefix='node_new', bin_dir=new_bin_dir)
        node_new.init(cached=False)
        res = node_new.upgrade_from(old_node=node_old)
        node_new.start()
        self.assertTrue(b'Upgrade Complete' in res)

    def test_parse_pg_version(self):
        # Linux Mint
        assert parse_pg_version("postgres (PostgreSQL) 15.5 (Ubuntu 15.5-1.pgdg22.04+1)") == "15.5"
        # Linux Ubuntu
        assert parse_pg_version("postgres (PostgreSQL) 12.17") == "12.17"
        # Windows
        assert parse_pg_version("postgres (PostgreSQL) 11.4") == "11.4"
        # Macos
        assert parse_pg_version("postgres (PostgreSQL) 14.9 (Homebrew)") == "14.9"

    def test_the_same_port(self):
        with get_new_node() as node:
            node.init().start()
            self.assertTrue(node._should_free_port)
            self.assertEqual(type(node.port), int)
            node_port_copy = node.port
            self.assertEqual(rm_carriage_returns(node.safe_psql("SELECT 1;")), b'1\n')

            with get_new_node(port=node.port) as node2:
                self.assertEqual(type(node2.port), int)
                self.assertEqual(node2.port, node.port)
                self.assertFalse(node2._should_free_port)

                with self.assertRaises(StartNodeException) as ctx:
                    node2.init().start()

                self.assertIn("Cannot start node", str(ctx.exception))

            # node is still working
            self.assertEqual(node.port, node_port_copy)
            self.assertTrue(node._should_free_port)
            self.assertEqual(rm_carriage_returns(node.safe_psql("SELECT 3;")), b'3\n')

    class tagPortManagerProxy:
        sm_prev_testgres_reserve_port = None
        sm_prev_testgres_release_port = None

        sm_DummyPortNumber = None
        sm_DummyPortMaxUsage = None

        sm_DummyPortCurrentUsage = None
        sm_DummyPortTotalUsage = None

        def __init__(self, dummyPortNumber, dummyPortMaxUsage):
            assert type(dummyPortNumber) == int  # noqa: E721
            assert type(dummyPortMaxUsage) == int  # noqa: E721
            assert dummyPortNumber >= 0
            assert dummyPortMaxUsage >= 0

            assert __class__.sm_prev_testgres_reserve_port is None
            assert __class__.sm_prev_testgres_release_port is None
            assert testgres.utils.reserve_port == testgres.utils.internal__reserve_port
            assert testgres.utils.release_port == testgres.utils.internal__release_port

            __class__.sm_prev_testgres_reserve_port = testgres.utils.reserve_port
            __class__.sm_prev_testgres_release_port = testgres.utils.release_port

            testgres.utils.reserve_port = __class__._proxy__reserve_port
            testgres.utils.release_port = __class__._proxy__release_port

            assert testgres.utils.reserve_port == __class__._proxy__reserve_port
            assert testgres.utils.release_port == __class__._proxy__release_port

            __class__.sm_DummyPortNumber = dummyPortNumber
            __class__.sm_DummyPortMaxUsage = dummyPortMaxUsage

            __class__.sm_DummyPortCurrentUsage = 0
            __class__.sm_DummyPortTotalUsage = 0

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            assert __class__.sm_DummyPortCurrentUsage == 0

            assert __class__.sm_prev_testgres_reserve_port is not None
            assert __class__.sm_prev_testgres_release_port is not None

            assert testgres.utils.reserve_port == __class__._proxy__reserve_port
            assert testgres.utils.release_port == __class__._proxy__release_port

            testgres.utils.reserve_port = __class__.sm_prev_testgres_reserve_port
            testgres.utils.release_port = __class__.sm_prev_testgres_release_port

            __class__.sm_prev_testgres_reserve_port = None
            __class__.sm_prev_testgres_release_port = None

        def _proxy__reserve_port():
            assert type(__class__.sm_DummyPortMaxUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortTotalUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortCurrentUsage) == int  # noqa: E721
            assert __class__.sm_DummyPortTotalUsage >= 0
            assert __class__.sm_DummyPortCurrentUsage >= 0

            assert __class__.sm_DummyPortTotalUsage <= __class__.sm_DummyPortMaxUsage
            assert __class__.sm_DummyPortCurrentUsage <= __class__.sm_DummyPortTotalUsage

            assert __class__.sm_prev_testgres_reserve_port is not None

            if __class__.sm_DummyPortTotalUsage == __class__.sm_DummyPortMaxUsage:
                return __class__.sm_prev_testgres_reserve_port()

            __class__.sm_DummyPortTotalUsage += 1
            __class__.sm_DummyPortCurrentUsage += 1
            return __class__.sm_DummyPortNumber

        def _proxy__release_port(dummyPortNumber):
            assert type(dummyPortNumber) == int  # noqa: E721

            assert type(__class__.sm_DummyPortMaxUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortTotalUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortCurrentUsage) == int  # noqa: E721
            assert __class__.sm_DummyPortTotalUsage >= 0
            assert __class__.sm_DummyPortCurrentUsage >= 0

            assert __class__.sm_DummyPortTotalUsage <= __class__.sm_DummyPortMaxUsage
            assert __class__.sm_DummyPortCurrentUsage <= __class__.sm_DummyPortTotalUsage

            assert __class__.sm_prev_testgres_release_port is not None

            if __class__.sm_DummyPortCurrentUsage > 0 and dummyPortNumber == __class__.sm_DummyPortNumber:
                assert __class__.sm_DummyPortTotalUsage > 0
                __class__.sm_DummyPortCurrentUsage -= 1
                return

            return __class__.sm_prev_testgres_release_port(dummyPortNumber)

    def test_port_rereserve_during_node_start(self):
        assert testgres.PostgresNode._C_MAX_START_ATEMPTS == 5

        C_COUNT_OF_BAD_PORT_USAGE = 3

        with get_new_node() as node1:
            node1.init().start()
            self.assertTrue(node1._should_free_port)
            self.assertEqual(type(node1.port), int)  # noqa: E721
            node1_port_copy = node1.port
            self.assertEqual(rm_carriage_returns(node1.safe_psql("SELECT 1;")), b'1\n')

            with __class__.tagPortManagerProxy(node1.port, C_COUNT_OF_BAD_PORT_USAGE):
                assert __class__.tagPortManagerProxy.sm_DummyPortNumber == node1.port
                with get_new_node() as node2:
                    self.assertTrue(node2._should_free_port)
                    self.assertEqual(node2.port, node1.port)

                    node2.init().start()

                    self.assertNotEqual(node2.port, node1.port)
                    self.assertTrue(node2._should_free_port)
                    self.assertEqual(__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage, 0)
                    self.assertEqual(__class__.tagPortManagerProxy.sm_DummyPortTotalUsage, C_COUNT_OF_BAD_PORT_USAGE)
                    self.assertTrue(node2.is_started)

                    self.assertEqual(rm_carriage_returns(node2.safe_psql("SELECT 2;")), b'2\n')

            # node1 is still working
            self.assertEqual(node1.port, node1_port_copy)
            self.assertTrue(node1._should_free_port)
            self.assertEqual(rm_carriage_returns(node1.safe_psql("SELECT 3;")), b'3\n')

    def test_port_conflict(self):
        assert testgres.PostgresNode._C_MAX_START_ATEMPTS > 1

        C_COUNT_OF_BAD_PORT_USAGE = testgres.PostgresNode._C_MAX_START_ATEMPTS

        with get_new_node() as node1:
            node1.init().start()
            self.assertTrue(node1._should_free_port)
            self.assertEqual(type(node1.port), int)  # noqa: E721
            node1_port_copy = node1.port
            self.assertEqual(rm_carriage_returns(node1.safe_psql("SELECT 1;")), b'1\n')

            with __class__.tagPortManagerProxy(node1.port, C_COUNT_OF_BAD_PORT_USAGE):
                assert __class__.tagPortManagerProxy.sm_DummyPortNumber == node1.port
                with get_new_node() as node2:
                    self.assertTrue(node2._should_free_port)
                    self.assertEqual(node2.port, node1.port)

                    with self.assertRaises(StartNodeException) as ctx:
                        node2.init().start()

                    self.assertIn("Cannot start node after multiple attempts", str(ctx.exception))

                    self.assertEqual(node2.port, node1.port)
                    self.assertTrue(node2._should_free_port)
                    self.assertEqual(__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage, 1)
                    self.assertEqual(__class__.tagPortManagerProxy.sm_DummyPortTotalUsage, C_COUNT_OF_BAD_PORT_USAGE)
                    self.assertFalse(node2.is_started)

                # node2 must release our dummyPort (node1.port)
                self.assertEqual(__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage, 0)

            # node1 is still working
            self.assertEqual(node1.port, node1_port_copy)
            self.assertTrue(node1._should_free_port)
            self.assertEqual(rm_carriage_returns(node1.safe_psql("SELECT 3;")), b'3\n')

    def test_simple_with_bin_dir(self):
        with get_new_node() as node:
            node.init().start()
            bin_dir = node.bin_dir

        app = NodeApp()
        correct_bin_dir = app.make_simple(base_dir=node.base_dir, bin_dir=bin_dir)
        correct_bin_dir.slow_start()
        correct_bin_dir.safe_psql("SELECT 1;")
        correct_bin_dir.stop()

        while True:
            try:
                app.make_simple(base_dir=node.base_dir, bin_dir="wrong/path")
            except FileNotFoundError:
                break  # Expected error
            except ExecUtilException:
                break  # Expected error

            raise RuntimeError("Error was expected.")  # We should not reach this

        return

    def test_set_auto_conf(self):
        # elements contain [property id, value, storage value]
        testData = [
            ["archive_command",
             "cp '%p' \"/mnt/server/archivedir/%f\"",
             "'cp \\'%p\\' \"/mnt/server/archivedir/%f\""],
            ["restore_command",
             'cp "/mnt/server/archivedir/%f" \'%p\'',
             "'cp \"/mnt/server/archivedir/%f\" \\'%p\\''"],
            ["log_line_prefix",
             "'\n\r\t\b\\\"",
             "'\\\'\\n\\r\\t\\b\\\\\""],
            ["log_connections",
             True,
             "on"],
            ["log_disconnections",
             False,
             "off"],
            ["autovacuum_max_workers",
             3,
             "3"]
        ]

        with get_new_node() as node:
            node.init().start()

            options = {}

            for x in testData:
                options[x[0]] = x[1]

            node.set_auto_conf(options)
            node.stop()
            node.slow_start()

            auto_conf_path = f"{node.data_dir}/postgresql.auto.conf"
            with open(auto_conf_path, "r") as f:
                content = f.read()

                for x in testData:
                    self.assertIn(
                        x[0] + " = " + x[2],
                        content,
                        x[0] + " stored wrong"
                    )


if __name__ == '__main__':
    if os.environ.get('ALT_CONFIG'):
        suite = unittest.TestSuite()

        # Small subset of tests for alternative configs (PG_BIN or PG_CONFIG)
        suite.addTest(TestgresTests('test_pg_config'))
        suite.addTest(TestgresTests('test_pg_ctl'))
        suite.addTest(TestgresTests('test_psql'))
        suite.addTest(TestgresTests('test_replicate'))

        print('Running tests for alternative config:')
        for t in suite:
            print(t)
        print()

        runner = unittest.TextTestRunner()
        runner.run(suite)
    else:
        unittest.main()
