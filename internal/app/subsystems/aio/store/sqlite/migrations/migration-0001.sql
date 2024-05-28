-- Insert initial migration record
INSERT INTO migrations (id) VALUES (1) ON CONFLICT(id) DO NOTHING;

-- -- DropInvocationColumn 
-- ALTER TABLE promises DROP COLUMN invocation; 

-- -- DropOldInvocationIndex 
-- DROP INDEX x;
-- DROP INDEX x;

-- -- AddNewInvocationIndex
-- CREATE INDEX 
-- CREATE INDEX 