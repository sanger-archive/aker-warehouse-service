"""
This class represents a message sent from an Aker application or service
"""
import json

class Message(object):
  REQUIRED_ARGS = ['originator', 'domain_object', 'zipkin_trace_id',
                   'id_user_in_GUI', 'state', 'timestamp']

  """Class representing a message sent from an Aker application or service"""
  def __init__(self, **kwargs):
    """
      Initializer for the Message class.

      :param **kwargs

      Any key-pair values that aren't in REQUIRED_ARGS will be assigned to the metadata property
    """
    for arg in self.REQUIRED_ARGS:
      if not kwargs.get(arg):
        raise ValueError, "Message must receive kwargs: " + ",".join(self.REQUIRED_ARGS)

      setattr(self, "_"+arg, kwargs[arg])
      del kwargs[arg]

    if len(kwargs) > 0:
      self._metadata = kwargs

  @property
  def originator(self):
    """Getter for _originator property"""
    return self._originator

  @property
  def domain_object(self):
    """Getter for _domain_object property"""
    return self._domain_object

  @property
  def zipkin_trace_id(self):
    """Getter for _zipkin_trace_id property"""
    return self._zipkin_trace_id

  @property
  def id_user_in_GUI(self):
    """Getter for _id_user_in_GUI property"""
    return self._id_user_in_GUI

  @property
  def state(self):
    """Getter for _state property"""
    return self._state

  @property
  def timestamp(self):
    """Getter for _timestamp property"""
    return self._timestamp

  @property
  def metadata(self):
    """Getter for _metadata property"""
    return self._metadata

  @classmethod
  def from_json(cls, message_as_json):
    """
    Parses the json given and uses the result to create a new Message object.

    Raises a ValueError if the json can not be parsed
    :param message_as_json JSON representation of the message
    :type message_as_json string
    :return a new Message built from the provided JSON
    :rtype Message
    """
    return Message(**json.loads(message_as_json))