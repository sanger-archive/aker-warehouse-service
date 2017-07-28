DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS subjects;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS event_types;
DROP TABLE IF EXISTS role_types;
DROP TABLE IF EXISTS subject_types;

CREATE TABLE subject_types (
  id SERIAL NOT NULL PRIMARY KEY
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

CREATE TABLE subjects (
  id SERIAL NOT NULL PRIMARY KEY
, uuid VARCHAR(36) NOT NULL UNIQUE
, friendly_name VARCHAR(128) NOT NULL
, subject_type_id INT NOT NULL
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT fk_subject_to_type FOREIGN KEY (subject_type_id) REFERENCES subject_types (id)
)
;

CREATE INDEX subjects_friendly_name ON subjects (friendly_name);

CREATE TABLE event_types (
  id SERIAL NOT NULL PRIMARY KEY
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

CREATE TABLE events (
  id SERIAL NOT NULL PRIMARY KEY
, lims_id VARCHAR(8) NOT NULL
, uuid VARCHAR(36) NOT NULL
, event_type_id INT NOT NULL
, occurred_at TIMESTAMP NOT NULL
, user_identifier VARCHAR(255) NOT NULL
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT fk_event_to_type FOREIGN KEY (event_type_id) REFERENCES event_types (id)
)
;

CREATE INDEX events_uuid ON events (uuid);

CREATE TABLE role_types (
  id SERIAL NOT NULL PRIMARY KEY
, name VARCHAR(64) NOT NULL UNIQUE
, description VARCHAR(64) NOT NULL DEFAULT ''
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

CREATE TABLE roles (
  id SERIAL NOT NULL PRIMARY KEY
, event_id INT NOT NULL
, subject_id INT NOT NULL
, role_type_id INT NOT NULL
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT fk_role_to_event FOREIGN KEY (event_id) REFERENCES events (id)
, CONSTRAINT fk_role_to_subject FOREIGN KEY (subject_id) REFERENCES subjects (id)
, CONSTRAINT fk_role_to_type FOREIGN KEY (role_type_id) REFERENCES role_types (id)
)
;

CREATE TABLE metadata (
  id SERIAL NOT NULL PRIMARY KEY
, event_id INT NOT NULL
, data_key VARCHAR(64) NOT NULL
, data_value VARCHAR(64) NULL
, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT fk_metadata_to_event FOREIGN KEY (event_id) REFERENCES events (id)
)
;

CREATE INDEX metadata_data_key ON metadata (data_key);

--/
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- If you want to run this command in DBViz, add a line here containing a single slash /

CREATE TRIGGER update_subject_types_updated_at BEFORE UPDATE ON subject_types FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_role_types_updated_at BEFORE UPDATE ON role_types FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_event_types_updated_at BEFORE UPDATE ON event_types FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_events_updated_at BEFORE UPDATE ON events FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_subjects_updated_at BEFORE UPDATE ON subjects FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_roles_updated_at BEFORE UPDATE ON roles FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_metadata_updated_at BEFORE UPDATE ON metadata FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
