--- CreateSchemaVersion 
INSERT INTO migrations (id) VALUES (1) ON CONFLICT(id) DO NOTHING;