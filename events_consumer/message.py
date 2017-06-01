"""
This class represents a message sent from an Aker application or service
"""
import json

from collections import namedtuple

class Message(object):

    Role = namedtuple('Role', 'role_type subject_type subject_friendly_name subject_uuid')

    """Class representing a message sent from an Aker application or service"""
    def __init__(self, event_type, uuid, timestamp, user_identifier, roles, metadata):
        """
            Initializer for the Message class.
            :param event_type name of the type of the event
            :param uuid the uuid for the event
            :param timestamp time of event
            :param user_identifier the user performing this event
            :param roles links to entities involved in the event
            :param metadata metadata of event
        """
        self._event_type = event_type
        self._uuid = uuid
        self._timestamp = timestamp
        self._user_identifier = user_identifier
        self._roles = roles
        self._metadata = metadata

    @property
    def event_type(self):
        """The name of the event type of the event"""
        return self._event_type

    @property
    def uuid(self):
        """The uuid for the event"""
        return self._uuid

    @property
    def timestamp(self):
        """The time the event happened"""
        return self._timestamp

    @property
    def roles(self):
        """The roles linking this event to subjects"""
        return self._roles

    @property
    def metadata(self):
        """The metadata linked to this event"""
        return self._metadata

    @property
    def user_identifier(self):
        """The user (email address) responsible for this event"""
        return self._user_identifier

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
        return 'Message(event_type={!r}, uuid={!r}, timestamp={}, user_identifier={!r}, roles={}, metadata={})'.format(
            self.event_type, self.uuid, self.timestamp, self.user_identifier, self.roles, self.metadata)

