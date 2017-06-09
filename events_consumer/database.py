import psycopg2
from ConfigParser import ConfigParser

def db_connect(config):
    details = connection_details(config)
    return psycopg2.connect(**details)

def connection_details(config):
    return {
        'host': config.database.host,
        'port': config.database.port,
        'database': config.database.database,
        'user': config.database.user,
        'password': config.database.password,
    }
