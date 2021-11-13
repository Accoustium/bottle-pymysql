import unittest

import bottle
import pymysql
from pymysql import OperationalError

import bottle_pymysql


class BottlePyMySQLTest(unittest.TestCase):
    USER = "root"
    PASS = ""
    DBNAME = "bottle_pymysql_test"
    SOCKET = "/var/run/mysqld/mysqld.sock"
    DBHOST = "127.0.0.1"

    # copy from https://github.com/bottlepy/bottle-sqlite
    def test_with_keyword(self):
        def test(pymydb):
            self.assertTrue(isinstance(pymydb, pymysql.cursors.Cursor))

        self._run(test)

    def test_without_keyword(self):
        def test_1():
            pass

        self._run(test_1)

        def test_2(**kw):
            self.assertFalse("pymydb" in kw)

        self._run(test_2)

    def test_install_conflicts(self):

        app = self._app()  # install default
        app = self._app(app=app, keyword="pymydb2")  # install another

        def test(pymydb, pymydb2):
            self.assertEqual(type(pymydb), type(pymydb2))

        self._run(test, app)

    def test_echo(self):
        def test(pymydb):
            pymydb.execute(""" SELECT 1 AS TEST """)

            self.assertEqual({"TEST": 1}, pymydb.fetchone())

        # test normal
        self._run(test, self._app(dbhost=self.DBHOST))

        # test socket
        self._run(test, self._app(dbunixsocket=self.SOCKET))

    def _bad_connection_raise(self, app, word, test):
        try:

            def empty(pymydb):
                pass

            self._run(empty, app)

        except OperationalError as e:
            # self.assertTrue(word in e.args[1])
            # assert word in e.args[1]
            test(word, e)
            return

        self.fail("should not success")

    def test_bad_connection_host(self):
        def test(word, e):
            assert word in e.args[1]

        # bad host
        host = "255.255.255.255"
        self._bad_connection_raise(self._app(dbhost=host), host, test)

    def test_bad_connection_sock(self):
        def test(word, e):
            assert hasattr(e, "original_exception")
            oex = e.original_exception
            assert oex.errno == 2

        # bad sock
        sock = "/not_exits.sock"
        self._bad_connection_raise(self._app(dbunixsocket=sock), sock, test)

    def test_dictrow(self):
        def equal_type(t):
            def test_type(pymydb):
                pymydb.execute(""" SELECT 1 AS TEST """)
                self.assertEqual(type(pymydb.fetchone()), t)

            return test_type

        # default
        self._run(equal_type(dict), app=self._app())

        # disable dictrows
        self._run(equal_type(tuple), app=self._app(dictrows=False))

    def test_timezone(self):
        def equal_tz(tz):
            def query_timezone(pymydb):
                pymydb.execute(""" SELECT @@session.time_zone as TZ;""")
                self.assertEqual({"TZ": tz}, pymydb.fetchone())

            return query_timezone

        tz = "-08:00"
        self._run(equal_tz(tz), app=self._app(timezone=tz))

        # default tz
        self._run(equal_tz("SYSTEM"))

    def test_crud(self):

        self._create_test_table()

        def crud(pymydb):
            data = "test"

            # insert
            rows = pymydb.execute(
                """ INSERT INTO `bottle_mysql_test` VALUE (NULL, %s) """,
                (data,),
            )
            self.assertEqual(rows, 1)

            pymydb.execute(""" SELECT last_insert_id() as ID """)

            data_id = pymydb.fetchone()["ID"]
            self.assertTrue(data_id > 0)

            # select
            pymydb.execute(
                """ SELECT * FROM `bottle_mysql_test` WHERE `id` = %s""",
                (data_id,),
            )
            self.assertEqual({"id": data_id, "text": data}, pymydb.fetchone())

            # update
            data = "new"
            rows = pymydb.execute(
                """ UPDATE `bottle_mysql_test` SET `text` = %s WHERE `id` = %s""",
                (data, data_id),
            )
            self.assertEqual(rows, 1)

            pymydb.execute(
                """ SELECT * FROM `bottle_mysql_test` WHERE `id` = %s""",
                (data_id,),
            )
            self.assertEqual({"id": data_id, "text": data}, pymydb.fetchone())

            # delete
            rows = pymydb.execute(
                """ DELETE FROM `bottle_mysql_test` WHERE `id` = %s""",
                (data_id,),
            )
            self.assertEqual(rows, 1)

        self._run(crud)

    def test_autocommit(self):
        self._create_test_table()

        self._run(self._insert_one)

        self.assert_records(1)

    def test_not_autocommit(self):
        self._create_test_table()

        app = self._app()

        # config with override
        @app.get("/", pymysql={"autocommit": False})
        def insert(pymydb):
            self._insert_one(pymydb)

        self._request(app, "/")
        self.assert_records(0)

        # config with construct
        self._run(self._insert_one, self._app(autocommit=False))
        self.assert_records(0)

    def test_commit_on_redirect(self):
        self._create_test_table()

        def test(pymydb):
            self._insert_one(pymydb)
            bottle.redirect("/")

        self._run(test)
        self.assert_records(1)

    def test_commit_on_abort(self):
        self._create_test_table()

        def test(pymydb):
            self._insert_one(pymydb)
            bottle.abort()

        self._run(test)
        self.assert_records(0)

    def test_escape_string(self):
        self._create_test_table()

        def test(pymydb):
            self._insert_one(pymydb, "test")
            pymydb.execute(
                "SELECT COUNT(*) AS c FROM `bottle_mysql_test` WHERE `text` LIKE '%%%s%%'"
                % pymydb.escape_string("te")
            )
            self.assertEqual({"c": 1}, pymydb.fetchone())

        self._run(test)
        self.assert_records(1)

    def assert_records(self, count):
        def query_count(pymydb):
            pymydb.execute("""SELECT COUNT(1) AS c FROM `bottle_mysql_test`""")
            self.assertEqual({"c": count}, pymydb.fetchone())

        self._run(query_count)

    def _insert_one(self, pymydb, data="test"):
        rows = pymydb.execute(
            """ INSERT INTO `bottle_mysql_test` VALUE (NULL, %s) """, (data,)
        )
        self.assertEqual(rows, 1)

    def _create_test_table(self):
        def init(pymydb):
            pymydb.execute("""DROP TABLE IF EXISTS `bottle_mysql_test`; """)

            pymydb.execute(
                """
            CREATE TABLE `bottle_mysql_test` (
              `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
              `text` varchar(11) DEFAULT NULL,
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """
            )

        self._run(init)

    def _run(self, f, app=None):

        if not app:
            app = self._app()

        app.get("/")(f)
        self._request(app, "/")

    def _app(self, **kwargs):
        app = kwargs.pop("app", bottle.Bottle(catchall=False))

        kwargs.setdefault("dbuser", self.USER)
        kwargs.setdefault("dbpass", self.PASS)
        kwargs.setdefault("dbname", self.DBNAME)
        kwargs.setdefault("dbhost", self.DBHOST)
        plugin = bottle_pymysql.Plugin(**kwargs)

        app.install(plugin)

        return app

    def _request(self, app, path, method="GET"):
        return app(
            {"PATH_INFO": path, "REQUEST_METHOD": method}, lambda x, y: None
        )


if __name__ == "__main__":
    unittest.main()
