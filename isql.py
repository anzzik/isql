import MySQLdb
import MySQLdb.cursors
import pymssql 
import time
import dbconf

def db_open(sql_conf_name):
    if sql_conf_name not in dbconf.sql_configurations:
        raise Exception(sql_conf_name + ' not found in configuration table')

    cfg = dbconf.sql_configurations[sql_conf_name]
    conn = sql_libs[cfg["sql_type"]]["open"](sql_conf_name)
    ctx = {
            "conn": conn,
            "result_max_size": 0,
            "sql_type": cfg["sql_type"]
            }

    return ctx

def q(db_ctx, q, args):
    fn = _get_lib_fn(db_ctx, "q")
    res = fn(db_ctx, q, args)
    return res

def q_many(db_ctx, q, args):
    fn = _get_lib_fn(db_ctx, "q_many")
    res = fn(db_ctx, q, args)
    return res

def db_close(db_ctx):
    fn = _get_lib_fn(db_ctx, "close")
    res = fn(db_ctx)

def commit(db_ctx):
    fn = _get_lib_fn(db_ctx, "commit")
    res = fn(db_ctx)

def rollback(db_ctx):
    fn = _get_lib_fn(db_ctx, "rollback")
    res = fn(db_ctx)

def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def set_result_maxsize(db_ctx, size):
    db_ctx['result_max_size'] = size;

def get_result_maxsize(db_ctx):
    return db_ctx["result_max_size"]


def _get_lib_fn(db_ctx, fn_name):
    sql_type = db_ctx["sql_type"]
    fn = sql_libs[db_ctx["sql_type"]][fn_name]
    return fn

def _get_conn(db_ctx):
    return db_ctx["conn"]

def _mssql_open(sql_conf):
    conf = dbconf.sql_configurations[sql_conf]
    db = pymssql.connect(
            server = conf["host"],
            user = conf["user"],
            password = conf["passwd"],
            database = conf["db"],
            charset = conf["charset"],
            as_dict = True,
            autocommit = conf["autocommit"]
            )

    return db

def _mssql_row_gen(db_ctx, cursor):
    r_max_size = get_result_maxsize(db_ctx)
    while True:
        rows = cursor.fetchmany(r_max_size)
        if len(rows) == 0:
            break;
        for r in rows:
            yield r


def _mssql_q(db_ctx, query, args):
    conn = _get_conn(db_ctx)
    c = conn.cursor()
    r_max_size = db_ctx['result_max_size']

    res = None;
    c.execute(query, args)

    if r_max_size > 0:
        res = _mssql_row_gen(db_ctx, c)
    else:
        res = c.fetchall()

    return res

def _mssql_q_many(db_ctx, query, args):
    conn = _get_conn(db_ctx)
    c = conn.cursor()

    res = None;
    c.executemany(query, args)
    res = c.fetchall();

    return res

def _mssql_close(db_ctx):
    conn = _get_conn(db_ctx)
    conn.close()

def _mssql_commit(db_ctx):
    conn = _get_conn(db_ctx)
    conn.commit()

def _mssql_rollback(db_ctx):
    conn = _get_conn(db_ctx)
    conn.rollback()

def _mysql_open(sql_conf):
    c_class = MySQLdb.cursors.DictCursor
    conf = dbconf.sql_configurations[sql_conf]

    db = MySQLdb.connect(
            host = conf["host"],
            user = conf["user"],
            passwd = conf["passwd"],
            db = conf["db"],
            charset = conf["charset"],
            cursorclass = c_class
            )

    db.autocommit(conf["autocommit"])

    return db

def _mysql_close(db_ctx):
    conn = _get_conn(db_ctx)
    conn.close()

def _mysql_row_gen(db_ctx, cursor):
    r_max_size = get_result_maxsize(db_ctx)
    while True:
        rows = cursor.fetchmany(r_max_size)
        if len(rows) == 0:
            break;
        for r in rows:
            yield r

def _mysql_q(db_ctx, query, args):
    conn = _get_conn(db_ctx)
    c = conn.cursor()
    r_max_size = db_ctx['result_max_size'];
    res = None
    try:
        c.execute(query, args)
        if r_max_size > 0:
            res = _mysql_row_gen(db_ctx, c)
        else:
            res = c.fetchall()

    except MySQLdb.Error, e:
        try:
            print(e)
            print "MySQL Error [%d]: %s (%s)" % (e.args[0], e.args[1], query)
            quit()
        except IndexError:
            print "MySQL Error: %s" % str(e)
            quit()

    return res

def _mysql_q_many(db_ctx, query, args):
    conn = _get_conn(db_ctx)
    c = conn.cursor()
    c.executemany(query, args)

def _mysql_commit(db_ctx):
    conn = _get_conn(db_ctx)
    conn.commit()

def _mysql_rollback(db_ctx):
    conn = _get_conn(db_ctx)
    conn.rollback()

sql_libs = {
        "mysql": {
            "open": _mysql_open,
            "close": _mysql_close,
            "q": _mysql_q,
            "q_many": _mysql_q_many,
            "commit": _mysql_commit,
            "rollback": _mysql_rollback
            },
        "mssql": {
            "open": _mssql_open,
            "close": _mssql_close,
            "q": _mssql_q,
            "q_many": _mssql_q_many,
            "commit": _mssql_commit,
            "rollback": _mssql_rollback
            }
        }

