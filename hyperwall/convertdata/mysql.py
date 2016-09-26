import os
import os.path

class DummyDBCursor(object):
    def __init__(self):
        pass
    def execute(self, cmd, args):
        print('MySQL query:', cmd, 'with args:', args)
        return 0 # no hits

class DummyDBConnection(object):
    def __init__(self):
        pass
    def cursor(self) :
        return DummyDBCursor()
    def close(self) :
        return

def dbconnect(db_host="", verbose=False):
    # if explicitly asked for dummy, use it
    if db_host == "dummy":
        if verbose:
            print('using dummy database connection')
        return DummyDBConnection()

    # get MySQL env variables
    try:
        pscp_mysql_host = os.environ['PSCP_MYSQL_HOST']
        pscp_mysql_user = os.environ['PSCP_MYSQL_USER']
        pscp_mysql_pwd = os.environ['PSCP_MYSQL_PWD']
        pscp_mysql_db = os.environ['PSCP_MYSQL_DB']
        pscp_mysql_socket = os.environ['PSCP_MYSQL_SOCKET']
    except KeyError:
        print('PSCP_MYSQL environment variables not set')
        print('resorting to local testing mode')
        return DummyDBConnection()

    # if no db_host specified
    if not db_host or db_host == "":
        if os.path.exists(pscp_mysql_socket):
            db_host = pscp_mysql_socket
        else:
            #this_host = socket.gethostname()
            db_host = pscp_mysql_host

    # connect to our meta-data database
    error = None
    try:
        from . import pymysql
        if db_host.endswith(".sock"):
            db_connection = pymysql.connect(unix_socket=db_host, user=pscp_mysql_user, passwd=pscp_mysql_pwd, db=pscp_mysql_db, use_unicode=True, charset='utf8')
        else:
            db_connection = pymysql.connect(host=db_host, user=pscp_mysql_user, passwd=pscp_mysql_pwd, db=pscp_mysql_db, use_unicode=True, charset='utf8')
        if verbose:
            print('opened connection to MySQL database', db_host)
        return db_connection

    except pymysql.err.OperationalError as e:
        error = e
        if db_host == 'localhost':
            db_host='127.0.0.1'
            try:
                db_connection = pymysql.connect(host=db_host, user=pscp_mysql_user, passwd=pscp_mysql_pwd, db=pscp_mysql_db, use_unicode=True, charset='utf8')
                return db_connection
            except Exception as e:
                error = e

    except Exception as e:
        error = e

    print('error opening MySQL database', db_host, ';', str(error))
    print('resorting to local testing mode')
    return DummyDBConnection()

def execute_mutating_query(db_cursor, dry_run, cmd, arg):
    if dry_run:
        print('a dry-run, so not executing the following MySQL command:')
        print(cmd, arg)
    else:
        try:
            db_cursor.execute(cmd, arg)
        except Exception as e:
            print("ERROR MySQL: %s" % str(e))
            return False
    return True
