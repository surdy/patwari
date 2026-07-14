CREATE TABLE archive_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner_namespace TEXT NOT NULL UNIQUE,
    archive_instance_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
