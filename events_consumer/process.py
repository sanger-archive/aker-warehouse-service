def process_message(conn, message):
    conn.isolation_level = None
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    finished = False
    try:
        save_message(cursor, message)
        cursor.execute('COMMIT')
        finished = True
    finally:
        if not finished:
            cursor.execute('ROLLBACK')


def save_message(cursor, message):
    event_type_id = find_or_create_type(cursor, message.event_type, 'event_types')
    role_type_set = { role.role_type for role in message.roles }
    role_type_map = { role_type: find_or_create_type(cursor, role_type, 'role_types') for role_type in role_type_set }
    subject_type_set = { role.subject_type for role in message.roles }
    subject_type_map = { subject_type: find_or_create_type(cursor, subject_type, 'subject_types') for subject_type in subject_type_set }

    event_id = create_event(cursor, message.lims_id, message.uuid, event_type_id, message.timestamp, message.user_identifier)

    for role in message.roles:
        subject_type_id = subject_type_map[role.subject_type]
        subject_id = find_or_create_subject(cursor, role.subject_uuid, role.subject_friendly_name, subject_type_id)
        role_type_id = role_type_map[role.role_type]
        create_role(cursor, event_id, subject_id, role_type_id)

    create_metadata(cursor, event_id, message.metadata)

def create_event(cursor, lims_id, uuid, event_type_id, timestamp, user_identifier):
    cursor.execute(
        '''INSERT INTO events
           (lims_id, uuid, event_type_id, occurred_at, user_identifier, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''',
        (lims_id, uuid, event_type_id, timestamp, user_identifier)
    )
    return cursor.lastrowid

def create_role(cursor, event_id, subject_id, role_type_id):
    cursor.execute(
        '''INSERT INTO roles (event_id, subject_id, role_type_id, created_at, updated_at)
           VALUES (?,?,?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''',
        (event_id, subject_id, role_type_id)
    )
    return cursor.lastrowid

def create_metadata(cursor, event_id, metadata):
    for key,values in metadata.iteritems():
        if not isinstance(values, (list, tuple)):
            values = [values]
        for v in values:
            cursor.execute(
                '''INSERT INTO metadata
                   (event_id, data_key, data_value, created_at, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''',
                (event_id, key, v)
            )

def find_or_create_subject(cursor, uuid, friendly_name, subject_type_id):
    cursor.execute('SELECT id FROM subjects WHERE uuid=?', (uuid,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        '''INSERT INTO subjects
            (uuid, friendly_name, subject_type_id, created_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''',
        (uuid, friendly_name, subject_type_id)
    )
    return cursor.lastrowid

def find_or_create_type(cursor, name, table):
    cursor.execute('SELECT id FROM %s WHERE name=?'%table, (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        '''INSERT INTO %s (name, created_at, updated_at) VALUES
           (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)'''%table,
        (name,)
    )
    return cursor.lastrowid

