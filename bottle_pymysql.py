'''
Bottle-MySQL is a plugin that integrates MySQL with your Bottle
application. It automatically connects to a database at the beginning of a
request, passes the database handle to the route callback and closes the
connection afterwards.

To automatically detect routes that need a database connection, the plugin
searches for route callbacks that require a `pymydb` keyword argument
(configurable) and skips routes that do not. This removes any overhead for
routes that don't need a database connection.

Results are returned as dictionaries.

Usage Example::

    import bottle
    import bottle_pymysql

    app = bottle.Bottle()
    # dbhost is optional, default is localhost
    plugin = bottle_pymysql.Plugin(dbuser='user', dbpass='pass', dbname='db')
    app.install(plugin)

    @app.route('/show/:<tem>')
    def show(item, pymydb):
        pymydb.execute('SELECT * from items where name="%s"', (item,))
        row = pymydb.fetchone()
        if row:
            return template('showitem', page=row)
        return HTTPError(404, "Page not found")
'''

__author__ = "Alexandr N. Zamaraev"
__version__ = '0.1.3'
__license__ = 'MIT'

### CUT HERE (see setup.py)

import inspect
import pymysql
import pymysql.cursors as cursors
import bottle


# PluginError is defined to bottle >= 0.10
if not hasattr(bottle, 'PluginError'):
    class PluginError(bottle.BottleException):
        pass

    bottle.PluginError = PluginError


class PyMySQLPlugin(object):
    '''
    This plugin passes a mysql database handle to route callbacks
    that accept a `pymydb` keyword argument. If a callback does not expect
    such a parameter, no connection is made. You can override the database
    settings on a per-route basis.
    '''

    name = 'pymysql'
    api = 2

    def __init__(
        self, dbuser=None, dbpass=None, dbname=None, dbhost='localhost',
        dbport=3306, dbunixsocket=None, autocommit=True, dictrows=True,
        dbread_timeout=None, dbwrite_timeout=None,
        keyword='pymydb', charset='utf8', timezone=None
    ):
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbunixsocket = dbunixsocket
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.dbname = dbname
        self.dbread_timeout = dbread_timeout
        self.dbwrite_timeout = dbwrite_timeout
        self.autocommit = autocommit
        self.dictrows = dictrows
        self.keyword = keyword
        self.charset = charset
        self.timezone = timezone

    def setup(self, app):
        '''
        Make sure that other installed plugins don't affect the same keyword argument.
        '''
        for other in app.plugins:
            if not isinstance(other, PyMySQLPlugin):
                continue
            if other.keyword == self.keyword:
                raise PluginError(
                    "Found another pymysql plugin with conflicting settings (non-unique keyword).")
            elif other.name == self.name:
                self.name += '_%s' % self.keyword

    def apply(self, callback, route):
        # hack to support bottle v0.9.x
        if bottle.__version__.startswith('0.9'):
            config = route['config']
            _callback = route['callback']
        else:
            config = route.config
            _callback = route.callback

        # Override global configuration with route-specific values.
        if "pymysql" in config:
            # support for configuration before `ConfigDict` namespaces
            g = lambda key, default: config.get('pymysql', {}).get(key, default)
        else:
            g = lambda key, default: config.get('pymysql.' + key, default)

        keyword = g('keyword', self.keyword)
        # Test if the original callback accepts a 'pymydb' keyword.
        # Ignore it if it does not need a database handle.
        _args = inspect.getargspec(_callback)
        if keyword not in _args.args:
            return callback

        dbhost = g('dbhost', self.dbhost)
        dbport = g('dbport', self.dbport)
        dbunixsocket = g('dbunixsocket', self.dbunixsocket)
        dbuser = g('dbuser', self.dbuser)
        dbpass = g('dbpass', self.dbpass)
        dbname = g('dbname', self.dbname)
        dbread_timeout = g('dbread_timeout', self.dbread_timeout)
        dbwrite_timeout = g('dbwrite_timeout', self.dbwrite_timeout)
        autocommit = g('autocommit', self.autocommit)
        dictrows = g('dictrows', self.dictrows)
        charset = g('charset', self.charset)
        timezone = g('timezone', self.timezone)

        def wrapper(*args, **kwargs):
            # Connect to the database
            con = None
            try:

                kw = {
                    'user': dbuser,
                    'passwd': dbpass,
                    'db': dbname,
                    'charset': charset,
                }

                if dictrows:
                    kw['cursorclass'] = cursors.DictCursor

                if dbunixsocket:
                    kw['unix_socket'] = dbunixsocket
                else:
                    kw['host'] = dbhost
                    kw['port'] = dbport

                if dbread_timeout is not None:
                    kw['read_timeout'] = int(dbread_timeout)

                if dbwrite_timeout is not None:
                    kw['write_timeout'] = int(dbwrite_timeout)

                con = pymysql.connect(**kw)

                cur = con.cursor()
                cur.escape_string = con.escape_string
                if timezone:
                    cur.execute("set time_zone=%s", (timezone, ))

            except bottle.HTTPResponse as e:
                raise bottle.HTTPError(500, "Database Error", e)

            # Add the connection handle as a keyword argument.
            kwargs[keyword] = cur

            try:
                rv = callback(*args, **kwargs)
                if autocommit:
                    con.commit()
            except pymysql.IntegrityError as e:
                con.rollback()
                raise bottle.HTTPError(500, "Database Error", e)
            except bottle.HTTPError:
                raise
            except bottle.HTTPResponse:
                if autocommit:
                    con.commit()
                raise
            finally:
                if con:
                    con.close()
            return rv

        # Replace the route callback with the wrapped one.
        return wrapper


Plugin = PyMySQLPlugin
