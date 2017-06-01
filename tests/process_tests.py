from nose.tools import *
from parameterized import parameterized
import unittest
import json
from uuid import uuid4

from events_consumer import Message
from events_consumer import setup_database
from events_consumer.process import *

def new_uuid():
    return str(uuid4())

class ProcessTests(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.conn = setup_database('test')
        cursor = cls.conn.cursor()
        cls.event_type_id = find_or_create_type(cursor, 'apocalypse', 'event_types')
        cls.subject_type_id = find_or_create_type(cursor, 'monkey', 'subject_types')
        cls.role_type_id = find_or_create_type(cursor, 'instigator', 'role_types')
        cls.conn.isolation_level = None
        cls.cursor = cls.conn.cursor()
        cls.some_timestamp = 1496159601659

    @classmethod
    def teardown_class(cls):
        cls.conn.close()

    def setup(self):
        self.cursor = ProcessTests.cursor
        self.cursor.execute('BEGIN')

    def teardown(self):
        self.cursor.execute('ROLLBACK')

    def test_create_event(self):
        uuid = new_uuid()
        user = 'dr6@sanger.ac.uk'
        event_id = create_event(self.cursor, uuid, ProcessTests.event_type_id, ProcessTests.some_timestamp, user)
        self.cursor.execute('''SELECT lims_id, uuid, event_type_id, occurred_at, user_identifier
              FROM events WHERE id=?''', (event_id,))
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'aker')
        self.assertEqual(result[1], uuid)
        self.assertEqual(result[2], ProcessTests.event_type_id)
        self.assertEqual(result[3], ProcessTests.some_timestamp)
        self.assertEqual(result[4], user)

    def test_create_role(self):
        event_id = create_event(self.cursor, new_uuid(), ProcessTests.event_type_id, ProcessTests.some_timestamp, 'dr6@sanger.ac.uk')
        subject_id = find_or_create_subject(self.cursor, str(uuid4()), 'Timmy', ProcessTests.subject_type_id)
        role_id = create_role(self.cursor, event_id, subject_id, ProcessTests.role_type_id)
        self.cursor.execute('SELECT event_id, subject_id, role_type_id FROM roles WHERE id=?', (role_id,))
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], event_id)
        self.assertEqual(result[1], subject_id)
        self.assertEqual(result[2], ProcessTests.role_type_id)

    def test_create_metadata(self):
        event_id = create_event(self.cursor, new_uuid(), ProcessTests.event_type_id, ProcessTests.some_timestamp, 'dr6@sanger.ac.uk')
        metadata = { "weapon": "banana gun", "mood": "angry" }
        create_metadata(self.cursor, event_id, metadata)
        self.cursor.execute('SELECT data_key, data_value FROM metadata WHERE event_id=?', (event_id,))
        results = self.cursor.fetchall()
        self.assertEqual(len(results), 2)

        if results[0][0]=='mood':
            results[0], results[1] = results[1],results[0]

        self.assertEqual(results[0][0], 'weapon')
        self.assertEqual(results[0][1], 'banana gun')
        self.assertEqual(results[1][0], 'mood')
        self.assertEqual(results[1][1], 'angry')

    def test_find_or_create_subject(self):
        uuid = new_uuid()
        name = 'Timmy'
        subject_id = find_or_create_subject(self.cursor, uuid, name, ProcessTests.subject_type_id)
        self.cursor.execute('SELECT uuid, friendly_name, subject_type_id FROM subjects WHERE id=?', (subject_id,))
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], uuid)
        self.assertEqual(result[1], name)
        self.assertEqual(result[2], ProcessTests.subject_type_id)

        # Testing when subject already exists, get that subject_id back
        same_subject_id = find_or_create_subject(self.cursor, uuid, name, ProcessTests.subject_type_id)
        self.assertEqual(subject_id, same_subject_id)

    @parameterized.expand(['event_types', 'role_types', 'subject_types'])
    def test_find_or_create_type(self, table_name):
        type_id = find_or_create_type(self.cursor, 'bubbles', table_name)
        self.cursor.execute('SELECT name FROM '+table_name+' WHERE id=?', (type_id, ))
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'bubbles')

        same_type_id = find_or_create_type(self.cursor, 'bubbles', table_name)
        self.assertEqual(type_id, same_type_id)



