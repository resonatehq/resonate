-- The single-table MySQL backend needs its own database.
--
-- MySQL's schema *is* its database, so `persistence_mysql_single.rs` cannot
-- share one with `persistence_mysql.rs` the way the two Postgres backends
-- share theirs across the `resonate` and `public` schemas: they would share
-- the `promises` table, and the differential would be comparing a backend
-- against itself.
--
-- The official mysql image runs everything in /docker-entrypoint-initdb.d once,
-- on a fresh data directory. An existing container will not pick this up —
-- recreate it:
--
--   docker compose -f diff/docker-compose-diff.yml up -d --force-recreate mysql
CREATE DATABASE IF NOT EXISTS resonate_single;
GRANT ALL ON `resonate_single`.* TO 'resonate'@'%';
