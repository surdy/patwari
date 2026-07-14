-- A short-lived cross-process maintenance lease prevents new archive
-- mutations while a backup waits for and holds the filesystem lock. The
-- filesystem lock is the final ownership mechanism (and is automatically
-- released if a process dies); this row makes the pending maintenance window
-- visible to application requests so they fail fast instead of starving it.
CREATE TABLE maintenance_gate (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    exclusive_token TEXT,
    exclusive_until_unix INTEGER,
    CHECK (
        (exclusive_token IS NULL AND exclusive_until_unix IS NULL)
        OR
        (exclusive_token IS NOT NULL AND exclusive_until_unix IS NOT NULL)
    )
);

INSERT INTO maintenance_gate (singleton, exclusive_token, exclusive_until_unix)
VALUES (1, NULL, NULL);
