"""
This class represents a message sent from an Aker application or service
"""
import json

from collections import namedtuple

class Message(object):

    Role = namedtuple('Role', 'role_type subject_type subject_friendly_name uuid')
    Role.__new__.__defaults__ = (None,)

    """Class representing a message sent from an Aker application or service"""
    def __init__(self, event_type, timestamp, roles, metadata):
        """
            Initializer for the Message class.
            :param event_type name of the type of the event
            :param timestamp time of event
            :param roles links to entities involved in the event
            :param metadata metadata of event
        """
        self._event_type = event_type
        self._timestamp = timestamp
        self._roles = roles
        self._metadata = metadata

    @property
    def event_type(self):
        """Getter for _event_type property"""
        return self._event_type

    @property
    def timestamp(self):
        """Getter for _timestamp property"""
        return self._timestamp

    @property
    def roles(self):
        """Getter for _roles property"""
        return self._roles

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
        data = json.loads(message_as_json)
        data['roles'] = tuple(Message.Role(**role_data) for role_data in data['roles'])
        return cls(**data)

    def __repr__(self):
        return 'Message(event_type={!r}, timestamp={!r}, roles={!r}, metadata={!r})'.format(
            self.event_type, self.timestamp, self.roles, self.metadata)

