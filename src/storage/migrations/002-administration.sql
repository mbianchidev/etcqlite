CREATE TABLE configuration_history (
    configuration_id INTEGER PRIMARY KEY CHECK(configuration_id > 0),
    base INTEGER NOT NULL CHECK(base >= 0),
    activation_request BLOB CHECK(activation_request IS NULL OR length(activation_request) = 16),
    configuration BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE configuration_transition (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    request_id BLOB NOT NULL CHECK(length(request_id) = 16),
    configuration BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE node_incarnation (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    incarnation BLOB NOT NULL CHECK(length(incarnation) = 16),
    checksum BLOB NOT NULL
) STRICT;
