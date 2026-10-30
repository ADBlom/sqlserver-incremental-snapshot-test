-- One-time DBA setup that lets a NON-sysadmin, NON-db_owner, read-only login compute the
-- "tracked commit" watermark on the primary and on a readable AG secondary.
-- fn_dblog itself is hard-gated to sysadmin / db_owner (error 9010), so it is wrapped in a procedure
-- that runs as the database owner. No TRUSTWORTHY and no certificate signing was needed in the test.
-- Test environment: SQL Server 2022 CU27, database owner = sa, tracked table dbo.wm.

-- 1. On EVERY AG replica (server-level objects are not replicated). Use the same SID on all replicas.
--    CREATE LOGIN dbz WITH PASSWORD = '...', SID = 0x...;

-- 2. On the PRIMARY (user, role membership and the procedure replicate to the secondary):
--    USE testdb;
--    CREATE USER dbz FOR LOGIN dbz;
--    ALTER ROLE db_datareader ADD MEMBER dbz;      -- plus the CDC gating role, if any

CREATE OR ALTER PROCEDURE dbo.dbz_tracked_commits @s varchar(40)
WITH EXECUTE AS OWNER
AS
SET NOCOUNT ON;
-- One row per transaction in the log from @s ('0x00000097:00013e38:0003' form) onwards:
-- (txn id, begin name, commit LSN, touches dbo.wm, min previous LSN). Filtering lives in the client.
SELECT [Transaction ID],
  MAX(CASE WHEN Operation = 'LOP_BEGIN_XACT'  THEN [Transaction Name] END),
  MAX(CASE WHEN Operation = 'LOP_COMMIT_XACT' THEN [Current LSN] END),
  MAX(CASE WHEN Operation IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS','LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS')
            AND Context IN ('LCX_CLUSTERED','LCX_HEAP') AND AllocUnitName LIKE 'dbo.wm.%' THEN 1 ELSE 0 END),
  MIN(CASE WHEN Operation <> 'LOP_BEGIN_XACT' THEN [Previous LSN] END)
FROM fn_dblog(@s, NULL)
WHERE [Transaction ID] <> '0000:00000000'
GROUP BY [Transaction ID];
GO
-- GRANT EXECUTE ON dbo.dbz_tracked_commits TO dbz;
