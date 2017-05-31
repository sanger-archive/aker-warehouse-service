from nose.tools import *
import unittest
import json

from events_consumer.message import Message


class MessageTests(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.message = Message(
            event_type = "aker.events.work_order.submitted",
            timestamp = 1496159601659,
            user_identifier = 'dr6@sanger.ac.uk',
            roles = [
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            ],
            metadata = {
                'comment': 'Do my work',
                'desired_completion_date': 1496159601659,
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
            },
        )

    @classmethod
    def teardown_class(cls):
        cls.message = None

    def test_init(self):
        self.assertEqual(MessageTests.message.event_type, "aker.events.work_order.submitted")
        self.assertEqual(MessageTests.message.timestamp, 1496159601659)
        self.assertEqual(MessageTests.message.user_identifier, 'dr6@sanger.ac.uk')
        self.assertEqual(MessageTests.message.roles, [
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            ])
        self.assertEqual(MessageTests.message.metadata, {
                'comment': 'Do my work',
                'desired_completion_date': 1496159601659,
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
            })

    def test_from_json(self):
        message_as_json = '''
            {
               "timestamp":1496159601659,
               "event_type":"aker.events.work_order.submitted",
               "user_identifier":"dr6@sanger.ac.uk",
               "roles":[
                  {
                     "subject_type":"work_order",
                     "subject_friendly_name":"Work Order 11",
                     "role_type":"work_order",
                     "subject_uuid":"work_order_uuid_..."
                  },
                  {
                     "subject_type":"project",
                     "subject_friendly_name":"Viruses",
                     "role_type":"project",
                     "subject_uuid":"project_uuid_..."
                  },
                  {
                     "subject_type":"product",
                     "subject_friendly_name":"Ham sandwich",
                     "role_type":"product",
                     "subject_uuid":"product_uuid_..."
                  }
               ],
               "metadata":{
                  "comment":"Do my work",
                  "quoted_price":"5102.52",
                  "desired_completion_date":1496159601659,
                  "zipkin_trace_id":"a_uuid_...",
                  "num_materials":52
               }
            }'''

        message = Message.from_json(message_as_json)
        self.assertEqual(message.event_type, "aker.events.work_order.submitted")
        self.assertEqual(message.timestamp, 1496159601659)
        self.assertEqual(message.user_identifier, 'dr6@sanger.ac.uk')

        expected_roles = (
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            )

        self.assertEqual(message.roles, expected_roles)
        self.assertEqual(message.metadata, {
                'comment': 'Do my work',
                'desired_completion_date': 1496159601659,
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
            })
