from nose.tools import *
import unittest
import json

from events_consumer.message import Message

class MessageTests(unittest.TestCase):

  @classmethod
  def setup_class(cls):
    cls.message = Message(
      originator = "Work Order",
      domain_object = "work_order",
      zipkin_trace_id = "abcd1234",
      id_user_in_GUI = 123,
      state = "submitted",
      timestamp = 1496159601659,
      product = '30x Human WGS',
      urgency = 'Level X'
    )

  @classmethod
  def teardown_class(cls):
    cls.message = None

  def test_init(self):
    self.assertEqual(self.message.originator, "Work Order")
    self.assertEqual(self.message.domain_object, "work_order")
    self.assertEqual(self.message.zipkin_trace_id, "abcd1234")
    self.assertEqual(self.message.id_user_in_GUI, 123)
    self.assertEqual(self.message.state, "submitted")
    self.assertEqual(self.message.timestamp, 1496159601659)
    self.assertEqual(self.message.metadata, {"product": "30x Human WGS", "urgency": "Level X"})

  def test_from_json(self):
    message_as_json = json.dumps({
      "originator": "Work Order",
      "domain_object": "work_order",
      "zipkin_trace_id": "abcd1234",
      "id_user_in_GUI": 123,
      "state": "submitted",
      "timestamp": 1496159601659,
      "product": "30x Human WGS",
      "urgency": "Level X"
    })
    message = Message.from_json(message_as_json)
    self.assertTrue(isinstance(message, Message))

    self.assertEqual(message.originator, "Work Order")
    self.assertEqual(message.domain_object, "work_order")
    self.assertEqual(message.zipkin_trace_id, "abcd1234")
    self.assertEqual(message.id_user_in_GUI, 123)
    self.assertEqual(message.state, "submitted")
    self.assertEqual(message.timestamp, 1496159601659)
    self.assertEqual(message.metadata, {"product": "30x Human WGS", "urgency": "Level X"})