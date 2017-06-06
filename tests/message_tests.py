import unittest
import dateutil.parser

from events_consumer import Message

TIMESTAMP_STRING = '2017-06-06T12:13:58.509575'

class MessageTests(unittest.TestCase):
    def test_init(self):
        message = Message(
            event_type = "aker.events.work_order.submitted",
            lims_id = "aker",
            uuid = "690bf1dd-98bb-451c-8277-88a78d53beea",
            timestamp = dateutil.parser.parse(TIMESTAMP_STRING),
            user_identifier = 'dr6@sanger.ac.uk',
            roles = [
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            ],
            metadata = {
                'comment': 'Do my work',
                'desired_completion_date': '2017-12-05',
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
                'primary_colours': ['red', 'green', 'blue'],
            },
        )
        self.assertEqual(message.event_type, "aker.events.work_order.submitted")
        self.assertEqual(message.lims_id, "aker")
        self.assertEqual(message.uuid, "690bf1dd-98bb-451c-8277-88a78d53beea")
        self.assertEqual(message.timestamp.isoformat(), TIMESTAMP_STRING)
        self.assertEqual(message.user_identifier, 'dr6@sanger.ac.uk')
        self.assertEqual(message.roles, [
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            ])
        self.assertEqual(message.metadata, {
                'comment': 'Do my work',
                'desired_completion_date': '2017-12-05',
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
                'primary_colours': ['red', 'green', 'blue'],
            })

    def test_from_json(self):
        message_as_json = '''
            {
               "event_type":"aker.events.work_order.submitted",
               "lims_id":"aker",
               "uuid":"690bf1dd-98bb-451c-8277-88a78d53beea",
               "timestamp":"2017-06-06T12:13:58.509575",
               "user_identifier":"dr6@sanger.ac.uk",
               "roles":[
                  {
                     "role_type":"work_order",
                     "subject_type":"work_order",
                     "subject_friendly_name":"Work Order 11",
                     "subject_uuid":"work_order_uuid_..."
                  },
                  {
                     "role_type":"project",
                     "subject_type":"project",
                     "subject_friendly_name":"Viruses",
                     "subject_uuid":"project_uuid_..."
                  },
                  {
                     "role_type":"product",
                     "subject_type":"product",
                     "subject_friendly_name":"Ham sandwich",
                     "subject_uuid":"product_uuid_..."
                  }
               ],
               "metadata":{
                  "comment":"Do my work",
                  "quoted_price":"5102.52",
                  "desired_completion_date":"2017-12-05",
                  "zipkin_trace_id":"a_uuid_...",
                  "num_materials":52,
                  "primary_colours":["red", "green", "blue"]
               }
            }'''

        message = Message.from_json(message_as_json)
        self.assertEqual(message.event_type, "aker.events.work_order.submitted")
        self.assertEqual(message.lims_id, "aker")
        self.assertEqual(message.uuid, "690bf1dd-98bb-451c-8277-88a78d53beea")
        self.assertEqual(message.timestamp.isoformat(), TIMESTAMP_STRING)
        self.assertEqual(message.user_identifier, 'dr6@sanger.ac.uk')

        expected_roles = (
                    Message.Role('work_order', 'work_order', 'Work Order 11', 'work_order_uuid_...'),
                    Message.Role('project', 'project', 'Viruses', 'project_uuid_...'),
                    Message.Role('product', 'product', 'Ham sandwich', 'product_uuid_...'),
            )

        self.assertEqual(message.roles, expected_roles)
        self.assertEqual(message.metadata, {
                'comment': 'Do my work',
                'desired_completion_date': '2017-12-05',
                'zipkin_trace_id': 'a_uuid_...',
                'quoted_price': '5102.52',
                'num_materials': 52,
                'primary_colours': ['red', 'green', 'blue'],
            })
