from parameterized import parameterized
import unittest
import mock
import json
from itertools import izip
from uuid import uuid4
from datetime import datetime
import dateutil.parser

from events_consumer import Message
from events_consumer import Config
from events_consumer import db_connect
from events_consumer.process import *

def new_uuid():
    return str(uuid4())

SOME_TIMESTAMP = dateutil.parser.parse('2017-06-06T12:13:58.509575')

class ProcessTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = Config('test.txt')
        db = db_connect(config)
        with db:
            with db.cursor() as cursor:
                cls.event_type_id = find_or_create_type(cursor, 'apocalypse', 'event_types')
                cls.subject_type_id = find_or_create_type(cursor, 'monkey', 'subject_types')
                cls.role_type_id = find_or_create_type(cursor, 'instigator', 'role_types')
        cls.db = db

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def setUp(self):
        self.cursor = self.db.cursor()

    def tearDown(self):
        self.db.rollback()
        self.cursor.close()

    def query(self, sql, parameters, multiple=False):
        self.cursor.execute(sql, parameters)
        if multiple:
            return self.cursor.fetchall()
        return self.cursor.fetchone()

    def test_create_event(self):
        uuid = new_uuid()
        user = 'dr6@sanger.ac.uk'
        event_id = create_event(self.cursor, 'aker', uuid, self.event_type_id,
                                SOME_TIMESTAMP, user)
        result = self.query(
            '''SELECT lims_id, uuid, event_type_id, occurred_at, user_identifier
              FROM events WHERE id=%s''',
            (event_id,)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'aker')
        self.assertEqual(result[1], uuid)
        self.assertEqual(result[2], self.event_type_id)
        self.assertEqual(result[3], SOME_TIMESTAMP)
        self.assertEqual(result[4], user)

    def test_create_role(self):
        event_id = create_event(self.cursor, 'aker', new_uuid(), self.event_type_id,
                                SOME_TIMESTAMP, 'dr6@sanger.ac.uk')
        subject_id = find_or_create_subject(self.cursor, new_uuid(), 'Timmy', self.subject_type_id)
        role_id = create_role(self.cursor, event_id, subject_id, self.role_type_id)
        result = self.query(
            'SELECT event_id, subject_id, role_type_id FROM roles WHERE id=%s',
            (role_id,)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result[0], event_id)
        self.assertEqual(result[1], subject_id)
        self.assertEqual(result[2], self.role_type_id)

    def test_create_metadata(self):
        event_id = create_event(self.cursor, 'aker', new_uuid(), self.event_type_id,
                                SOME_TIMESTAMP, 'dr6@sanger.ac.uk')
        metadata = { "weapon": "banana gun", "mood": ["confused", "angry"] }

        create_metadata(self.cursor, event_id, metadata)

        results = self.query(
            'SELECT data_key, data_value FROM metadata WHERE event_id=%s',
            (event_id,),
            multiple=True
        )
        self.assertEqual(len(results), 3)
        results.sort()

        self.assertEqual(results[0], ('mood', 'angry'))
        self.assertEqual(results[1], ('mood', 'confused'))
        self.assertEqual(results[2], ('weapon', 'banana gun'))

    def test_find_or_create_subject(self):
        uuid = new_uuid()
        name = 'Timmy'
        subject_id = find_or_create_subject(self.cursor, uuid, name, self.subject_type_id)
        result = self.query(
            'SELECT uuid, friendly_name, subject_type_id FROM subjects WHERE id=%s',
            (subject_id,)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result[0], uuid)
        self.assertEqual(result[1], name)
        self.assertEqual(result[2], self.subject_type_id)

        # Testing when subject already exists, get that subject_id back
        same_subject_id = find_or_create_subject(self.cursor, uuid, name, self.subject_type_id)
        self.assertEqual(subject_id, same_subject_id)

    @parameterized.expand(['event_types', 'role_types', 'subject_types'])
    def test_find_or_create_type(self, table_name):
        type_id = find_or_create_type(self.cursor, 'bubbles', table_name)
        result = self.query('SELECT name FROM {} WHERE id=%s'.format(table_name), (type_id,))
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'bubbles')

        # Should get the same id back the second time
        same_type_id = find_or_create_type(self.cursor, 'bubbles', table_name)
        self.assertEqual(type_id, same_type_id)

    def test_save_message(self):
        message = Message(
            event_type="invasion",
            lims_id="banana",
            uuid=new_uuid(),
            timestamp=SOME_TIMESTAMP,
            user_identifier='dr6@sanger.ac.uk',
            roles = [
                    Message.Role('work_order', 'work_order', 'Work Order 11', new_uuid()),
                    Message.Role('project', 'project', 'Viruses', new_uuid()),
                    Message.Role('product', 'product', 'Ham sandwich', new_uuid()),
            ],
            metadata = {
                'comment': 'Do my work',
                'colour': ['red', 'green', 'blue'],
            },
        )
        event_id = save_message(self.cursor, message)

        # Check the event
        result = self.query(
            '''SELECT e.lims_id, e.uuid, et.name, e.occurred_at, e.user_identifier
              FROM events e JOIN event_types et ON (e.event_type_id=et.id)
              WHERE e.id=%s''',
            (event_id,)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result[0], message.lims_id)
        self.assertEqual(result[1], message.uuid)
        self.assertEqual(result[2], message.event_type)
        self.assertEqual(result[3], message.timestamp)
        self.assertEqual(result[4], message.user_identifier)

        # Check the roles/subjects
        results = self.query(
            '''SELECT rt.name, st.name, s.friendly_name, s.uuid
                FROM roles r JOIN role_types rt ON (r.role_type_id=rt.id)
                  JOIN subjects s ON (r.subject_id=s.id)
                  JOIN subject_types st ON (s.subject_type_id=st.id)
                WHERE r.event_id=%s''',
            (event_id,),
            multiple=True
        )
        self.assertEqual(len(results), len(message.roles))
        results.sort()
        for actual, expected in izip(results, sorted(message.roles)):
            self.assertEqual(actual, tuple(expected))

        # Check the metadata
        results = self.query(
            'SELECT data_key, data_value FROM metadata WHERE event_id=%s',
            (event_id,),
            multiple=True
        )
        self.assertEqual(len(results), 4)
        results.sort()
        self.assertEqual(results[0], ('colour', 'blue'))
        self.assertEqual(results[1], ('colour', 'green'))
        self.assertEqual(results[2], ('colour', 'red'))
        self.assertEqual(results[3], ('comment', 'Do my work'))

    @parameterized.expand([[True], [False]])
    def test_process_message(self, success):
        data = 'TESTDATA_%s'%datetime.now().isoformat()
        replacement = mock_save_message_success if success else mock_save_message_failure
        with mock.patch('events_consumer.process.save_message', new=replacement):
            if success:
                process_message(self.db, data)
            else:
                with self.assertRaises(ValueError):
                    process_message(self.db, data)
        with self.db.cursor() as cursor:
            cursor.execute('SELECT * FROM subject_types WHERE name=%s', (data,))
            results = cursor.fetchone()
            if success:
                # The row data should have been added
                self.assertTrue(results)
            else:
                # The row data should have been rolled back
                self.assertFalse(results)

def mock_save_message_success(cursor, data):
    cursor.execute('INSERT INTO subject_types (name) VALUES (%s)', (data,))

def mock_save_message_failure(cursor, data):
    cursor.execute('INSERT INTO subject_types (name) VALUES (%s)', (data,))
    raise ValueError('Something went wrong')

