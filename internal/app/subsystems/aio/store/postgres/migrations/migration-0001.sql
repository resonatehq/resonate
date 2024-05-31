-- CreateTable
CREATE TABLE IF NOT EXISTS migrations (
    id    INTEGER,
    PRIMARY KEY(id) 
);

--- CreateSchemaVersion 
INSERT INTO migrations (id) VALUES (1) ON CONFLICT(id) DO NOTHING;