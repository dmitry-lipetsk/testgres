# coding: utf-8
import os
import re
import subprocess
import tempfile

import time
import six
import pytest
import psutil
import logging
import uuid

from contextlib import contextmanager

from .. import testgres

from ..testgres.exceptions import \
    InitNodeException, \
    StartNodeException, \
    ExecUtilException, \
    BackupException, \
    QueryException, \
    TimeoutException, \
    TestgresException, \
    InvalidOperationException

from ..testgres.config import \
    TestgresConfig, \
    configure_testgres, \
    scoped_config, \
    pop_config, testgres_config

from ..testgres import \
    NodeStatus, \
    ProcessType, \
    IsolationLevel, \
    get_remote_node, \
    RemoteOperations

from ..testgres import \
    get_bin_path, \
    get_pg_config, \
    get_pg_version

from ..testgres import \
    First, \
    Any

# NOTE: those are ugly imports
from ..testgres import bound_ports
from ..testgres.utils import PgVer
from ..testgres.utils import file_tail
from ..testgres.node import ProcessProxy, ConnectionParams


def pg_version_ge(version):
    cur_ver = PgVer(get_pg_version())
    min_ver = PgVer(version)
    return cur_ver >= min_ver


def util_exists(util):
    def good_properties(f):
        return (testgres_config.os_ops.path_exists(f) and  # noqa: W504
                testgres_config.os_ops.isfile(f) and  # noqa: W504
                testgres_config.os_ops.is_executable(f))  # yapf: disable

    # try to resolve it
    if good_properties(get_bin_path(util)):
        return True

    # check if util is in PATH
    for path in testgres_config.os_ops.environ("PATH").split(testgres_config.os_ops.pathsep):
        if good_properties(os.path.join(path, util)):
            return True


@contextmanager
def removing(f):
    try:
        yield f
    finally:
        if testgres_config.os_ops.isfile(f):
            testgres_config.os_ops.remove_file(f)

        elif testgres_config.os_ops.isdir(f):
            testgres_config.os_ops.rmdirs(f, ignore_errors=True)


class TestgresRemoteTests:
    sm_conn_params = ConnectionParams(
        host=os.getenv('RDBMS_TESTPOOL1_HOST') or '127.0.0.1',
        username=os.getenv('USER'),
        ssh_key=os.getenv('RDBMS_TESTPOOL_SSHKEY'))

    sm_os_ops = RemoteOperations(sm_conn_params)

    @pytest.fixture(autouse=True, scope="class")
    def implicit_fixture(self):
        prev_ops = testgres_config.os_ops
        assert prev_ops is not None
        assert __class__.sm_os_ops is not None
        testgres_config.set_os_ops(os_ops=__class__.sm_os_ops)
        assert testgres_config.os_ops is __class__.sm_os_ops
        yield
        assert testgres_config.os_ops is __class__.sm_os_ops
        testgres_config.set_os_ops(os_ops=prev_ops)
        assert testgres_config.os_ops is prev_ops

    def test_node_repr(self):
        with __class__.helper__get_node() as node:
            pattern = r"PostgresNode\(name='.+', port=.+, base_dir='.+'\)"
            assert re.match(pattern, str(node)) is not None

    def test_custom_init(self):
        with __class__.helper__get_node() as node:
            # enable page checksums
            node.init(initdb_params=['-k']).start()

        with __class__.helper__get_node() as node:
            node.init(
                allow_streaming=True,
                initdb_params=['--auth-local=reject', '--auth-host=reject'])

            hba_file = os.path.join(node.data_dir, 'pg_hba.conf')
            lines = node.os_ops.readlines(hba_file)

            # check number of lines
            assert (len(lines) >= 6)

            # there should be no trust entries at all
            assert not (any('trust' in s for s in lines))

    def test_init__LANG_С(self):
        # PBCKP-1744
        prev_LANG = os.environ.get("LANG")

        try:
            os.environ["LANG"] = "C"

            with __class__.helper__get_node() as node:
                node.init().start()
        finally:
            __class__.helper__restore_envvar("LANG", prev_LANG)

    def test_init__unk_LANG_and_LC_CTYPE(self):
        # PBCKP-1744
        prev_LANG = os.environ.get("LANG")
        prev_LANGUAGE = os.environ.get("LANGUAGE")
        prev_LC_CTYPE = os.environ.get("LC_CTYPE")
        prev_LC_COLLATE = os.environ.get("LC_COLLATE")

        try:
            # TODO: Pass unkData through test parameter.
            unkDatas = [
                ("UNKNOWN_LANG", "UNKNOWN_CTYPE"),
                ("\"UNKNOWN_LANG\"", "\"UNKNOWN_CTYPE\""),
                ("\\UNKNOWN_LANG\\", "\\UNKNOWN_CTYPE\\"),
                ("\"UNKNOWN_LANG", "UNKNOWN_CTYPE\""),
                ("\\UNKNOWN_LANG", "UNKNOWN_CTYPE\\"),
                ("\\", "\\"),
                ("\"", "\""),
            ]

            errorIsDetected = False

            for unkData in unkDatas:
                logging.info("----------------------")
                logging.info("Unk LANG is [{0}]".format(unkData[0]))
                logging.info("Unk LC_CTYPE is [{0}]".format(unkData[1]))

                os.environ["LANG"] = unkData[0]
                os.environ.pop("LANGUAGE", None)
                os.environ["LC_CTYPE"] = unkData[1]
                os.environ.pop("LC_COLLATE", None)

                assert os.environ.get("LANG") == unkData[0]
                assert not ("LANGUAGE" in os.environ.keys())
                assert os.environ.get("LC_CTYPE") == unkData[1]
                assert not ("LC_COLLATE" in os.environ.keys())

                assert os.getenv('LANG') == unkData[0]
                assert os.getenv('LANGUAGE') is None
                assert os.getenv('LC_CTYPE') == unkData[1]
                assert os.getenv('LC_COLLATE') is None

                exc: ExecUtilException = None
                with __class__.helper__get_node() as node:
                    try:
                        node.init()  # IT RAISES!
                    except InitNodeException as e:
                        exc = e.__cause__
                        assert exc is not None
                        assert isinstance(exc, ExecUtilException)

                if exc is None:
                    logging.warning("We expected an error!")
                    continue

                errorIsDetected = True

                assert isinstance(exc, ExecUtilException)

                errMsg = str(exc)
                logging.info("Error message is {0}: {1}".format(type(exc).__name__, errMsg))

                assert "warning: setlocale: LC_CTYPE: cannot change locale (" + unkData[1] + ")" in errMsg
                assert "initdb: error: invalid locale settings; check LANG and LC_* environment variables" in errMsg
                continue

            if not errorIsDetected:
                pytest.xfail("All the bad data are processed without errors!")

        finally:
            __class__.helper__restore_envvar("LANG", prev_LANG)
            __class__.helper__restore_envvar("LANGUAGE", prev_LANGUAGE)
            __class__.helper__restore_envvar("LC_CTYPE", prev_LC_CTYPE)
            __class__.helper__restore_envvar("LC_COLLATE", prev_LC_COLLATE)

    def test_double_init(self):
        with __class__.helper__get_node().init() as node:
            # can't initialize node more than once
            with pytest.raises(expected_exception=InitNodeException):
                node.init()

    def test_init_after_cleanup(self):
        with __class__.helper__get_node() as node:
            node.init().start().execute('select 1')
            node.cleanup()
            node.init().start().execute('select 1')

    def test_init_unique_system_id(self):
        # this function exists in PostgreSQL 9.6+
        __class__.helper__skip_test_if_util_not_exist("pg_resetwal")
        __class__.helper__skip_test_if_pg_version_is_not_ge('9.6')

        query = 'select system_identifier from pg_control_system()'

        with scoped_config(cache_initdb=False):
            with __class__.helper__get_node().init().start() as node0:
                id0 = node0.execute(query)[0]

        with scoped_config(cache_initdb=True,
                           cached_initdb_unique=True) as config:
            assert (config.cache_initdb)
            assert (config.cached_initdb_unique)

            # spawn two nodes; ids must be different
            with __class__.helper__get_node().init().start() as node1, \
                    __class__.helper__get_node().init().start() as node2:
                id1 = node1.execute(query)[0]
                id2 = node2.execute(query)[0]

                # ids must increase
                assert (id1 > id0)
                assert (id2 > id1)

    def test_node_exit(self):
        with pytest.raises(expected_exception=QueryException):
            with __class__.helper__get_node().init() as node:
                base_dir = node.base_dir
                node.safe_psql('select 1')

        # we should save the DB for "debugging"
        assert (__class__.sm_os_ops.path_exists(base_dir))
        __class__.sm_os_ops.rmdirs(base_dir, ignore_errors=True)

        with __class__.helper__get_node().init() as node:
            base_dir = node.base_dir

        # should have been removed by default
        assert not (__class__.sm_os_ops.path_exists(base_dir))

    def test_double_start(self):
        with __class__.helper__get_node().init().start() as node:
            # can't start node more than once
            node.start()
            assert (node.is_started)

    def test_uninitialized_start(self):
        with __class__.helper__get_node() as node:
            # node is not initialized yet
            with pytest.raises(expected_exception=StartNodeException):
                node.start()

    def test_restart(self):
        with __class__.helper__get_node() as node:
            node.init().start()

            # restart, ok
            res = node.execute('select 1')
            assert (res == [(1,)])
            node.restart()
            res = node.execute('select 2')
            assert (res == [(2,)])

            # restart, fail
            with pytest.raises(expected_exception=StartNodeException):
                node.append_conf('pg_hba.conf', 'DUMMY')
                node.restart()

    def test_reload(self):
        with __class__.helper__get_node() as node:
            node.init().start()

            # change client_min_messages and save old value
            cmm_old = node.execute('show client_min_messages')
            node.append_conf(client_min_messages='DEBUG1')

            # reload config
            node.reload()

            # check new value
            cmm_new = node.execute('show client_min_messages')
            assert ('debug1' == cmm_new[0][0].lower())
            assert (cmm_old != cmm_new)

    def test_pg_ctl(self):
        with __class__.helper__get_node() as node:
            node.init().start()

            status = node.pg_ctl(['status'])
            assert ('PID' in status)

    def test_status(self):
        assert (NodeStatus.Running)
        assert not (NodeStatus.Stopped)
        assert not (NodeStatus.Uninitialized)

        # check statuses after each operation
        with __class__.helper__get_node() as node:
            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Uninitialized)

            node.init()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Stopped)

            node.start()

            assert (node.pid != 0)
            assert (node.status() == NodeStatus.Running)

            node.stop()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Stopped)

            node.cleanup()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Uninitialized)

    def test_psql(self):
        with __class__.helper__get_node().init().start() as node:
            # check returned values (1 arg)
            res = node.psql('select 1')
            assert (res == (0, b'1\n', b''))

            # check returned values (2 args)
            res = node.psql('postgres', 'select 2')
            assert (res == (0, b'2\n', b''))

            # check returned values (named)
            res = node.psql(query='select 3', dbname='postgres')
            assert (res == (0, b'3\n', b''))

            # check returned values (1 arg)
            res = node.safe_psql('select 4')
            assert (res == b'4\n')

            # check returned values (2 args)
            res = node.safe_psql('postgres', 'select 5')
            assert (res == b'5\n')

            # check returned values (named)
            res = node.safe_psql(query='select 6', dbname='postgres')
            assert (res == b'6\n')

            # check feeding input
            node.safe_psql('create table horns (w int)')
            node.safe_psql('copy horns from stdin (format csv)',
                           input=b"1\n2\n3\n\\.\n")
            _sum = node.safe_psql('select sum(w) from horns')
            assert (_sum == b'6\n')

            # check psql's default args, fails
            with pytest.raises(expected_exception=QueryException):
                node.psql()

            node.stop()

            # check psql on stopped node, fails
            with pytest.raises(expected_exception=QueryException):
                node.safe_psql('select 1')

    def test_safe_psql__expect_error(self):
        with __class__.helper__get_node().init().start() as node:
            err = node.safe_psql('select_or_not_select 1', expect_error=True)
            assert (type(err) == str)  # noqa: E721
            assert ('select_or_not_select' in err)
            assert ('ERROR:  syntax error at or near "select_or_not_select"' in err)

            # ---------
            with pytest.raises(
                expected_exception=InvalidOperationException,
                match="^" + re.escape("Exception was expected, but query finished successfully: `select 1;`.") + "$"
            ):
                node.safe_psql("select 1;", expect_error=True)

            # ---------
            res = node.safe_psql("select 1;", expect_error=False)
            assert (res == b'1\n')

    def test_transactions(self):
        with __class__.helper__get_node().init().start() as node:
            with node.connect() as con:
                con.begin()
                con.execute('create table test(val int)')
                con.execute('insert into test values (1)')
                con.commit()

                con.begin()
                con.execute('insert into test values (2)')
                res = con.execute('select * from test order by val asc')
                assert (res == [(1,), (2,)])
                con.rollback()

                con.begin()
                res = con.execute('select * from test')
                assert (res == [(1,)])
                con.rollback()

                con.begin()
                con.execute('drop table test')
                con.commit()

    def test_control_data(self):
        with __class__.helper__get_node() as node:
            # node is not initialized yet
            with pytest.raises(expected_exception=ExecUtilException):
                node.get_control_data()

            node.init()
            data = node.get_control_data()

            # check returned dict
            assert data is not None
            assert (any('pg_control' in s for s in data.keys()))

    def test_backup_simple(self):
        with __class__.helper__get_node() as master:
            # enable streaming for backups
            master.init(allow_streaming=True)

            # node must be running
            with pytest.raises(expected_exception=BackupException):
                master.backup()

            # it's time to start node
            master.start()

            # fill node with some data
            master.psql('create table test as select generate_series(1, 4) i')

            with master.backup(xlog_method='stream') as backup:
                with backup.spawn_primary().start() as slave:
                    res = slave.execute('select * from test order by i asc')
                    assert (res == [(1,), (2,), (3,), (4,)])

    def test_backup_multiple(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup1, \
                    node.backup(xlog_method='fetch') as backup2:
                assert (backup1.base_dir != backup2.base_dir)

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1', destroy=False) as node1, \
                        backup.spawn_primary('node2', destroy=False) as node2:
                    assert (node1.base_dir != node2.base_dir)

    def test_backup_exhaust(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup:
                # exhaust backup by creating new node
                with backup.spawn_primary():
                    pass

                # now let's try to create one more node
                with pytest.raises(expected_exception=BackupException):
                    backup.spawn_primary()

    def test_backup_wrong_xlog_method(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            with pytest.raises(
                expected_exception=BackupException,
                match="^" + re.escape('Invalid xlog_method "wrong"') + "$"
            ):
                node.backup(xlog_method='wrong')

    def test_pg_ctl_wait_option(self):
        C_MAX_ATTEMPTS = 50

        node = __class__.helper__get_node()
        assert node.status() == testgres.NodeStatus.Uninitialized
        node.init()
        assert node.status() == testgres.NodeStatus.Stopped
        node.start(wait=False)
        nAttempt = 0
        while True:
            if nAttempt == C_MAX_ATTEMPTS:
                raise Exception("Could not stop node.")

            nAttempt += 1

            if nAttempt > 1:
                logging.info("Wait 1 second.")
                time.sleep(1)
                logging.info("")

            logging.info("Try to stop node. Attempt #{0}.".format(nAttempt))

            try:
                node.stop(wait=False)
                break
            except ExecUtilException as e:
                # it's ok to get this exception here since node
                # could be not started yet
                logging.info("Node is not stopped. Exception ({0}): {1}".format(type(e).__name__, e))
            continue

        logging.info("OK. Stop command was executed. Let's wait while our node will stop really.")
        nAttempt = 0
        while True:
            if nAttempt == C_MAX_ATTEMPTS:
                raise Exception("Could not stop node.")

            nAttempt += 1
            if nAttempt > 1:
                logging.info("Wait 1 second.")
                time.sleep(1)
                logging.info("")

            logging.info("Attempt #{0}.".format(nAttempt))
            s1 = node.status()

            if s1 == testgres.NodeStatus.Running:
                continue

            if s1 == testgres.NodeStatus.Stopped:
                break

            raise Exception("Unexpected node status: {0}.".format(s1))

        logging.info("OK. Node is stopped.")
        node.cleanup()

    def test_replicate(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            with node.replicate().start() as replica:
                res = replica.execute('select 1')
                assert (res == [(1,)])

                node.execute('create table test (val int)', commit=True)

                replica.catchup()

                res = node.execute('select * from test')
                assert (res == [])

    def test_synchronous_replication(self):
        __class__.helper__skip_test_if_pg_version_is_not_ge("9.6")

        with __class__.helper__get_node() as master:
            old_version = not pg_version_ge('9.6')

            master.init(allow_streaming=True).start()

            if not old_version:
                master.append_conf('synchronous_commit = remote_apply')

            # create standby
            with master.replicate() as standby1, master.replicate() as standby2:
                standby1.start()
                standby2.start()

                # check formatting
                assert (
                    '1 ("{}", "{}")'.format(standby1.name, standby2.name) == str(First(1, (standby1, standby2)))
                )  # yapf: disable
                assert (
                    'ANY 1 ("{}", "{}")'.format(standby1.name, standby2.name) == str(Any(1, (standby1, standby2)))
                )  # yapf: disable

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
                    assert (res == b'1000000\n')

    def test_logical_replication(self):
        __class__.helper__skip_test_if_pg_version_is_not_ge("10")

        with __class__.helper__get_node() as node1, __class__.helper__get_node() as node2:
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
            assert (res == [(1, 1), (2, 2)])

            # disable and put some new data
            sub.disable()
            node1.safe_psql('insert into test values (3, 3)')

            # enable and ensure that data successfully transferred
            sub.enable()
            sub.catchup()
            res = node2.execute('select * from test')
            assert (res == [(1, 1), (2, 2), (3, 3)])

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
            assert (res == [('a',), ('b',)])

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
            assert (res == [(1, 1), (2, 2), (3, 3), (4, 4)])

            # explicitly add table
            with pytest.raises(expected_exception=ValueError):
                pub.add_tables([])  # fail
            pub.add_tables(['test2'])
            node1.safe_psql('insert into test2 values (\'c\')')
            sub.catchup()
            res = node2.execute('select * from test2')
            assert (res == [('a',), ('b',)])

    def test_logical_catchup(self):
        """ Runs catchup for 100 times to be sure that it is consistent """
        __class__.helper__skip_test_if_pg_version_is_not_ge("10")

        with __class__.helper__get_node() as node1, __class__.helper__get_node() as node2:
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
                assert (res == [(i, i, )])
                node1.execute('delete from test')

    def test_logical_replication_fail(self):
        __class__.helper__skip_test_if_pg_version_is_ge("10")

        with __class__.helper__get_node() as node:
            with pytest.raises(expected_exception=InitNodeException):
                node.init(allow_logical=True)

    def test_replication_slots(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            with node.replicate(slot='slot1').start() as replica:
                replica.execute('select 1')

                # cannot create new slot with the same name
                with pytest.raises(expected_exception=TestgresException):
                    node.replicate(slot='slot1')

    def test_incorrect_catchup(self):
        with __class__.helper__get_node() as node:
            node.init(allow_streaming=True).start()

            # node has no master, can't catch up
            with pytest.raises(expected_exception=TestgresException):
                node.catchup()

    def test_promotion(self):
        with __class__.helper__get_node() as master:
            master.init().start()
            master.safe_psql('create table abc(id serial)')

            with master.replicate().start() as replica:
                master.stop()
                replica.promote()

                # make standby becomes writable master
                replica.safe_psql('insert into abc values (1)')
                res = replica.safe_psql('select * from abc')
                assert (res == b'1\n')

    def test_dump(self):
        query_create = 'create table test as select generate_series(1, 2) as val'
        query_select = 'select * from test order by val asc'

        with __class__.helper__get_node().init().start() as node1:

            node1.execute(query_create)
            for format in ['plain', 'custom', 'directory', 'tar']:
                with removing(node1.dump(format=format)) as dump:
                    with __class__.helper__get_node().init().start() as node3:
                        if format == 'directory':
                            assert (node1.os_ops.isdir(dump))
                        else:
                            assert (node1.os_ops.isfile(dump))
                        # restore dump
                        node3.restore(filename=dump)
                        res = node3.execute(query_select)
                        assert (res == [(1,), (2,)])

    def test_users(self):
        with __class__.helper__get_node().init().start() as node:
            node.psql('create role test_user login')
            value = node.safe_psql('select 1', username='test_user')
            assert (b'1\n' == value)

    def test_poll_query_until(self):
        with __class__.helper__get_node() as node:
            node.init().start()

            get_time = 'select extract(epoch from now())'
            check_time = 'select extract(epoch from now()) - {} >= 5'

            start_time = node.execute(get_time)[0][0]
            node.poll_query_until(query=check_time.format(start_time))
            end_time = node.execute(get_time)[0][0]

            assert (end_time - start_time >= 5)

            # check 0 columns
            with pytest.raises(expected_exception=QueryException):
                node.poll_query_until(
                    query='select from pg_catalog.pg_class limit 1')

            # check None, fail
            with pytest.raises(expected_exception=QueryException):
                node.poll_query_until(query='create table abc (val int)')

            # check None, ok
            node.poll_query_until(query='create table def()',
                                  expected=None)  # returns nothing

            # check 0 rows equivalent to expected=None
            node.poll_query_until(
                query='select * from pg_catalog.pg_class where true = false',
                expected=None)

            # check arbitrary expected value, fail
            with pytest.raises(expected_exception=TimeoutException):
                node.poll_query_until(query='select 3',
                                      expected=1,
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check arbitrary expected value, ok
            node.poll_query_until(query='select 2', expected=2)

            # check timeout
            with pytest.raises(expected_exception=TimeoutException):
                node.poll_query_until(query='select 1 > 2',
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check ProgrammingError, fail
            with pytest.raises(expected_exception=testgres.ProgrammingError):
                node.poll_query_until(query='dummy1')

            # check ProgrammingError, ok
            with pytest.raises(expected_exception=TimeoutException):
                node.poll_query_until(query='dummy2',
                                      max_attempts=3,
                                      sleep_time=0.01,
                                      suppress={testgres.ProgrammingError})

            # check 1 arg, ok
            node.poll_query_until('select true')

    def test_logging(self):
        C_MAX_ATTEMPTS = 50
        # This name is used for testgres logging, too.
        C_NODE_NAME = "testgres_tests." + __class__.__name__ + "test_logging-master-" + uuid.uuid4().hex

        logging.info("Node name is [{0}]".format(C_NODE_NAME))

        with tempfile.NamedTemporaryFile('w', delete=True) as logfile:
            formatter = logging.Formatter(fmt="%(node)-5s: %(message)s")
            handler = logging.FileHandler(filename=logfile.name)
            handler.formatter = formatter
            logger = logging.getLogger(C_NODE_NAME)
            assert logger is not None
            assert len(logger.handlers) == 0

            try:
                # It disables to log on the root level
                logger.propagate = False
                logger.addHandler(handler)

                with scoped_config(use_python_logging=True):
                    with __class__.helper__get_node(name=C_NODE_NAME) as master:
                        logging.info("Master node is initilizing")
                        master.init()

                        logging.info("Master node is starting")
                        master.start()

                        logging.info("Dummy query is executed a few times")
                        for _ in range(20):
                            master.execute('select 1')
                            time.sleep(0.01)

                        # let logging worker do the job
                        time.sleep(0.1)

                        logging.info("Master node log file is checking")
                        nAttempt = 0

                        while True:
                            assert nAttempt <= C_MAX_ATTEMPTS
                            if nAttempt == C_MAX_ATTEMPTS:
                                raise Exception("Test failed!")

                            # let logging worker do the job
                            time.sleep(0.1)

                            nAttempt += 1

                            logging.info("Attempt {0}".format(nAttempt))

                            # check that master's port is found
                            with open(logfile.name, 'r') as log:
                                lines = log.readlines()

                            assert lines is not None
                            assert type(lines) == list  # noqa: E721

                            def LOCAL__test_lines():
                                for s in lines:
                                    if any(C_NODE_NAME in s for s in lines):
                                        logging.info("OK. We found the node_name in a line \"{0}\"".format(s))
                                        return True
                                    return False

                            if LOCAL__test_lines():
                                break

                            logging.info("Master node log file does not have an expected information.")
                            continue

                        # test logger after stop/start/restart
                        logging.info("Master node is stopping...")
                        master.stop()
                        logging.info("Master node is staring again...")
                        master.start()
                        logging.info("Master node is restaring...")
                        master.restart()
                        assert (master._logger.is_alive())
            finally:
                # It is a hack code to logging cleanup
                logging._acquireLock()
                assert logging.Logger.manager is not None
                assert C_NODE_NAME in logging.Logger.manager.loggerDict.keys()
                logging.Logger.manager.loggerDict.pop(C_NODE_NAME, None)
                assert not (C_NODE_NAME in logging.Logger.manager.loggerDict.keys())
                assert not (handler in logging._handlers.values())
                logging._releaseLock()
        # GO HOME!
        return

    def test_pgbench(self):
        __class__.helper__skip_test_if_util_not_exist("pgbench")

        with __class__.helper__get_node().init().start() as node:
            # initialize pgbench DB and run benchmarks
            node.pgbench_init(scale=2, foreign_keys=True,
                              options=['-q']).pgbench_run(time=2)

            # run TPC-B benchmark
            proc = node.pgbench(stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                options=['-T3'])
            out = proc.communicate()[0]
            assert (b'tps = ' in out)

    def test_pg_config(self):
        # check same instances
        a = get_pg_config()
        b = get_pg_config()
        assert (id(a) == id(b))

        # save right before config change
        c1 = get_pg_config()

        # modify setting for this scope
        with scoped_config(cache_pg_config=False) as config:
            # sanity check for value
            assert not (config.cache_pg_config)

            # save right after config change
            c2 = get_pg_config()

            # check different instances after config change
            assert (id(c1) != id(c2))

            # check different instances
            a = get_pg_config()
            b = get_pg_config()
            assert (id(a) != id(b))

    def test_config_stack(self):
        # no such option
        with pytest.raises(expected_exception=TypeError):
            configure_testgres(dummy=True)

        # we have only 1 config in stack
        with pytest.raises(expected_exception=IndexError):
            pop_config()

        d0 = TestgresConfig.cached_initdb_dir
        d1 = 'dummy_abc'
        d2 = 'dummy_def'

        with scoped_config(cached_initdb_dir=d1) as c1:
            assert (c1.cached_initdb_dir == d1)

            with scoped_config(cached_initdb_dir=d2) as c2:
                stack_size = len(testgres.config.config_stack)

                # try to break a stack
                with pytest.raises(expected_exception=TypeError):
                    with scoped_config(dummy=True):
                        pass

                assert (c2.cached_initdb_dir == d2)
                assert (len(testgres.config.config_stack) == stack_size)

            assert (c1.cached_initdb_dir == d1)

        assert (TestgresConfig.cached_initdb_dir == d0)

    def test_unix_sockets(self):
        with __class__.helper__get_node() as node:
            node.init(unix_sockets=False, allow_streaming=True)
            node.start()

            res_exec = node.execute('select 1')
            res_psql = node.safe_psql('select 1')
            assert (res_exec == [(1,)])
            assert (res_psql == b'1\n')

            with node.replicate().start() as r:
                res_exec = r.execute('select 1')
                res_psql = r.safe_psql('select 1')
                assert (res_exec == [(1,)])
                assert (res_psql == b'1\n')

    def test_auto_name(self):
        with __class__.helper__get_node().init(allow_streaming=True).start() as m:
            with m.replicate().start() as r:
                # check that nodes are running
                assert (m.status())
                assert (r.status())

                # check their names
                assert (m.name != r.name)
                assert ('testgres' in m.name)
                assert ('testgres' in r.name)

    def test_file_tail(self):
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
            assert (lines[0] == s1)
            assert (lines[1] == s2)
            assert (lines[2] == s3)

            f.seek(0)
            lines = file_tail(f, 1)
            assert (lines[0] == s3)

    def test_isolation_levels(self):
        with __class__.helper__get_node().init().start() as node:
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
                with pytest.raises(expected_exception=QueryException):
                    con.begin('Garbage').commit()

    def test_ports_management(self):
        assert bound_ports is not None
        assert type(bound_ports) == set  # noqa: E721

        if len(bound_ports) != 0:
            logging.warning("bound_ports is not empty: {0}".format(bound_ports))

        stage0__bound_ports = bound_ports.copy()

        with __class__.helper__get_node() as node:
            assert bound_ports is not None
            assert type(bound_ports) == set  # noqa: E721

            assert node.port is not None
            assert type(node.port) == int  # noqa: E721

            logging.info("node port is {0}".format(node.port))

            assert node.port in bound_ports
            assert node.port not in stage0__bound_ports

            assert stage0__bound_ports <= bound_ports
            assert len(stage0__bound_ports) + 1 == len(bound_ports)

            stage1__bound_ports = stage0__bound_ports.copy()
            stage1__bound_ports.add(node.port)

            assert stage1__bound_ports == bound_ports

        # check that port has been freed successfully
        assert bound_ports is not None
        assert type(bound_ports) == set  # noqa: E721
        assert bound_ports == stage0__bound_ports

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

        assert (a == b)
        assert (b > c)
        assert (a > c)
        assert (d > e)
        assert (e > f)
        assert (d > f)

        version = get_pg_version()
        with __class__.helper__get_node() as node:
            assert (isinstance(version, six.string_types))
            assert (isinstance(node.version, PgVer))
            assert (node.version == PgVer(version))

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

        def LOCAL__test_auxiliary_pids(
            node: testgres.PostgresNode,
            expectedTypes: list[ProcessType]
        ) -> list[ProcessType]:
            # returns list of the absence processes
            assert node is not None
            assert type(node) == testgres.PostgresNode  # noqa: E721
            assert expectedTypes is not None
            assert type(expectedTypes) == list  # noqa: E721

            pids = node.auxiliary_pids
            assert pids is not None  # noqa: E721
            assert type(pids) == dict  # noqa: E721

            result = list[ProcessType]()
            for ptype in expectedTypes:
                if not (ptype in pids):
                    result.append(ptype)
            return result

        def LOCAL__check_auxiliary_pids__multiple_attempts(
                node: testgres.PostgresNode,
                expectedTypes: list[ProcessType]):
            assert node is not None
            assert type(node) == testgres.PostgresNode  # noqa: E721
            assert expectedTypes is not None
            assert type(expectedTypes) == list  # noqa: E721

            nAttempt = 0

            while nAttempt < 5:
                nAttempt += 1

                logging.info("Test pids of [{0}] node. Attempt #{1}.".format(
                    node.name,
                    nAttempt
                ))

                if nAttempt > 1:
                    time.sleep(1)

                absenceList = LOCAL__test_auxiliary_pids(node, expectedTypes)
                assert absenceList is not None
                assert type(absenceList) == list  # noqa: E721
                if len(absenceList) == 0:
                    logging.info("Bingo!")
                    return

                logging.info("These processes are not found: {0}.".format(absenceList))
                continue

            raise Exception("Node {0} does not have the following processes: {1}.".format(
                node.name,
                absenceList
            ))

        with __class__.helper__get_node().init().start() as master:

            # master node doesn't have a source walsender!
            with pytest.raises(expected_exception=TestgresException):
                master.source_walsender

            with master.connect() as con:
                assert (con.pid > 0)

            with master.replicate().start() as replica:

                # test __str__ method
                str(master.child_processes[0])

                LOCAL__check_auxiliary_pids__multiple_attempts(
                    master,
                    master_processes)

                LOCAL__check_auxiliary_pids__multiple_attempts(
                    replica,
                    repl_processes)

                master_pids = master.auxiliary_pids

                # there should be exactly 1 source walsender for replica
                assert (len(master_pids[ProcessType.WalSender]) == 1)
                pid1 = master_pids[ProcessType.WalSender][0]
                pid2 = replica.source_walsender.pid
                assert (pid1 == pid2)

                replica.stop()

                # there should be no walsender after we've stopped replica
                with pytest.raises(expected_exception=TestgresException):
                    replica.source_walsender

    # TODO: Why does not this test work with remote host?
    def test_child_process_dies(self):
        nAttempt = 0

        while True:
            if nAttempt == 5:
                raise Exception("Max attempt number is exceed.")

            nAttempt += 1

            logging.info("Attempt #{0}".format(nAttempt))

            # test for FileNotFound exception during child_processes() function
            with subprocess.Popen(["sleep", "60"]) as process:
                r = process.poll()

                if r is not None:
                    logging.warning("process.pool() returns an unexpected result: {0}.".format(r))
                    continue

                assert r is None
                # collect list of processes currently running
                children = psutil.Process(os.getpid()).children()
                # kill a process, so received children dictionary becomes invalid
                process.kill()
                process.wait()
                # try to handle children list -- missing processes will have ptype "ProcessType.Unknown"
                [ProcessProxy(p) for p in children]
                break

    @staticmethod
    def helper__get_node(name=None):
        assert __class__.sm_conn_params is not None
        return get_remote_node(name=name, conn_params=__class__.sm_conn_params)

    @staticmethod
    def helper__restore_envvar(name, prev_value):
        if prev_value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = prev_value

    @staticmethod
    def helper__skip_test_if_util_not_exist(name: str):
        assert type(name) == str  # noqa: E721
        if not util_exists(name):
            pytest.skip('might be missing')

    @staticmethod
    def helper__skip_test_if_pg_version_is_not_ge(version: str):
        assert type(version) == str  # noqa: E721
        if not pg_version_ge(version):
            pytest.skip('requires {0}+'.format(version))

    @staticmethod
    def helper__skip_test_if_pg_version_is_ge(version: str):
        assert type(version) == str  # noqa: E721
        if pg_version_ge(version):
            pytest.skip('requires <{0}'.format(version))
