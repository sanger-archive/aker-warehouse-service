-- CGAP event-driven reporting tables

DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS subjects;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS event_types;
DROP TABLE IF EXISTS role_types;
DROP TABLE IF EXISTS subject_types;

CREATE TABLE subject_types (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
)
;

CREATE TABLE subjects (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, uuid VARCHAR (36) NOT NULL UNIQUE
, friendly_name VARCHAR(64) NOT NULL
, subject_type_id INT NOT NULL
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
, CONSTRAINT fk_subject_to_type FOREIGN KEY (subject_type_id) REFERENCES subject_types (id)
)
;

CREATE INDEX subject_friendly_name ON subjects (friendly_name);

CREATE TABLE event_types (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
)
;

CREATE TABLE events (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, lims_id VARCHAR(8) NOT NULL
, uuid VARCHAR(36) NOT NULL UNIQUE
, event_type_id INT NOT NULL
, occurred_at DATETIME NOT NULL
, user_identifier VARCHAR(16) NOT NULL
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
, CONSTRAINT fk_event_to_type FOREIGN KEY (event_type_id) REFERENCES event_types (id)
)
;

CREATE TABLE role_types (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
)
;

CREATE TABLE roles (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, event_id INT NOT NULL
, subject_id INT NOT NULL
, role_type_id INT NOT NULL
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
, CONSTRAINT fk_role_to_event FOREIGN KEY (event_id) REFERENCES events (id)
, CONSTRAINT fk_role_to_subject FOREIGN KEY (subject_id) REFERENCES subjects (id)
, CONSTRAINT fk_role_to_type FOREIGN KEY (role_type_id) REFERENCES role_types (id)
)
;

CREATE TABLE metadata (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
, event_id INT NOT NULL
, data_key VARCHAR(64) NOT NULL
, data_value VARCHAR(64) NULL
, created_at DATETIME NOT NULL
, updated_at DATETIME NOT NULL
, CONSTRAINT fk_metadata_to_event FOREIGN KEY (event_id) REFERENCES events (id)
)
;

CREATE INDEX metadata_key ON metadata (data_key);

