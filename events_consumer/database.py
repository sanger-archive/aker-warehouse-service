import psycopg2
from ConfigParser import ConfigParser

def db_connect(env='development'):
    if env not in ('development', 'test', 'staging', 'production'):
        raise ValueError("Unrecognised environment: %r"%env)
    details = connection_details(env)
    return psycopg2.connect(**details)

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
