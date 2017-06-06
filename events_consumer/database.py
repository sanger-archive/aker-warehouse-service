from ConfigParser import ConfigParser

def setup_database(env='development'):
    if env in ('development', 'test'):
        import sqlite3
        conn = sqlite3.connect(':memory:')
        create_schema(conn)
    elif env in ('staging', 'production'):
        import psycopg2
        details = connection_details(env)
        conn = psycopg2.connect(**details)
    else:
        raise ValueError("Unrecognised environment: %r"%env)
    return conn


def create_schema(conn):
    with open('schema.sql', 'r') as fin:
        conn.executescript(fin.read())

def connection_details(env):
    filename = '%s_db.txt'%env
    config = ConfigParser()
    config.read(filename)
    values = config.defaults()
    return {
        'host': values['host'],
        'port': int(values['port']),
        'database': values['database'],
        'user': values['user'],
        'password': values['password'],
    }
