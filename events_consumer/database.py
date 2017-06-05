def setup_database(env='development'):
    if env in ('development', 'test'):
        import sqlite3
        conn = sqlite3.connect(':memory:')
        create_schema(conn)
    elif env=='staging':
        import psycopg2
        conn = psycopg2.connect(TODO('connection details'))
    elif env=='production':
        TODO # prod database
    else:
        raise ValueError("Unrecognised environment: %r"%env)
    return conn


def create_schema(conn):
    with open('schema.sql', 'r') as fin:
        conn.executescript(fin.read())
