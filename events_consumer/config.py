"""
Class to represent all config for the events_consumer
"""

from collections import namedtuple
from ConfigParser import ConfigParser

class Config(object):

    QueueConfig = namedtuple('QueueConfig', 'user password host port virtual_host queue')
    EmailConfig = namedtuple('EmailConfig', 'from_address to smtp_address')
    ProcessConfig = namedtuple('ProcessConfig', 'logfile errorlog pidfile')
    DatabaseConfig = namedtuple('DatabaseConfig', 'host port database user password')

    def __init__(self, config_file_path):
        config = ConfigParser()
        config.read(config_file_path)

        self._queue = self._queue_config(config, "MessageQueue")
        self._process = self._process_config(config, "Process")
        self._email = self._email_config(config, "Email")
        self._database = self._database_config(config, "Database")

    @property
    def message_queue(self):
        return self._queue

    @property
    def process(self):
        return self._process

    @property
    def email(self):
        return self._email

    @property
    def database(self):
        return self._database

    def _queue_config(self, config, section):
        return self.QueueConfig(
            config.get(section, 'user'),
            config.get(section, 'password'),
            config.get(section, 'host'),
            config.getint(section, 'port'),
            config.get(section, 'virtual_host'),
            config.get(section, 'queue'),
        )

    def _email_config(self, config, section):
        return self.EmailConfig(
            config.get(section, 'from_address'),
            config.get(section, 'to'),
            config.get(section, 'smtp_address'),
        )

    def _process_config(self, config, section):
        return self.ProcessConfig(
            config.get(section, 'logfile'),
            config.get(section, 'errorlog'),
            config.get(section, 'pidfile'),
        )

    def _database_config(self, config, section):
        return self.DatabaseConfig(
            config.get(section, 'host'),
            config.getint(section, 'port'),
            config.get(section, 'database'),
            config.get(section, 'user'),
            config.get(section, 'password'),
        )