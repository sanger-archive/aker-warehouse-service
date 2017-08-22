class Trunc(object):
    """Utility class for truncating strings (and printing a message about it)."""
    print_truncation = True
    def __init__(self, length, desc):
        self._length = length
        self._desc = desc
    def __len__(self):
        return self._length
    def __call__(self, string):
        if len(string) > len(self):
            if self.print_truncation:
                print "Truncating %s to %d characters: %r"%(self._desc, len(self), string)
            string = string[:len(self)]
        return string

# These numbers must match the columns lengths in the schema
Trunc.type_name = Trunc(64, 'type name')
Trunc.friendly_name = Trunc(128, 'friendly name')
Trunc.lims_id = Trunc(8, 'lims id')
Trunc.user = Trunc(255, 'user identifier')
Trunc.metadata_key = Trunc(64, 'metadata key')
Trunc.metadata_value = Trunc(255, 'metadata value')

def process_message(db, message):
    # transaction:
    with db:
        with db.cursor() as cursor:
            save_message(cursor, message)

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
    return event_id

def create_event(cursor, lims_id, uuid, event_type_id, timestamp, user_identifier):
    lims_id = Trunc.lims_id(lims_id)
    user_identifier = Trunc.user(user_identifier)
    cursor.execute(
        '''INSERT INTO events
           (lims_id, uuid, event_type_id, occurred_at, user_identifier)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING id''',
        (lims_id, uuid, event_type_id, timestamp, user_identifier)
    )
    return cursor.fetchone()[0]

def create_role(cursor, event_id, subject_id, role_type_id):
    cursor.execute(
        '''INSERT INTO roles (event_id, subject_id, role_type_id)
           VALUES (%s, %s, %s)
           RETURNING id''',
        (event_id, subject_id, role_type_id)
    )
    return cursor.fetchone()[0]

def create_metadata(cursor, event_id, metadata):
    for key,values in metadata.iteritems():
        key = Trunc.metadata_key(key)
        if not isinstance(values, (list, tuple)):
            values = [values]
        for v in values:
            v = Trunc.metadata_value(v)
            cursor.execute(
                '''INSERT INTO metadata
                   (event_id, data_key, data_value)
                   VALUES (%s, %s, %s)''',
                (event_id, key, v)
            )

def find_or_create_subject(cursor, uuid, friendly_name, subject_type_id):
    friendly_name = Trunc.friendly_name(friendly_name)
    cursor.execute('SELECT id FROM subjects WHERE uuid=%s', (uuid,))
    result = cursor.fetchone()
    if not result:
        cursor.execute(
            '''INSERT INTO subjects
                (uuid, friendly_name, subject_type_id)
                VALUES (%s, %s, %s)
                RETURNING id''',
            (uuid, friendly_name, subject_type_id)
        )
        result = cursor.fetchone()
    return result[0]

def find_or_create_type(cursor, name, table):
    name = Trunc.type_name(name)
    cursor.execute('SELECT id FROM {} WHERE name=%s'.format(table), (name,))
    result = cursor.fetchone()
    if not result:
        cursor.execute(
            'INSERT INTO {} (name) VALUES (%s) RETURNING id'.format(table),
            (name,)
        )
        result = cursor.fetchone()
    return result[0]

