-- Run on either node. Shows per-database replica sync state, redo queue size,
-- and log send queue size -- watch redo_queue_size to see whether the secondary
-- is keeping pace with the primary during your test run.
SELECT
    ars.replica_server_name,
    dbcs.database_name,
    drs.synchronization_state_desc,
    drs.redo_queue_size,
    drs.redo_rate,
    drs.log_send_queue_size,
    drs.log_send_rate,
    drs.last_redone_time,
    drs.last_received_time,
    drs.last_hardened_time
FROM sys.dm_hadr_database_replica_states AS drs
JOIN sys.availability_replicas AS ars
    ON drs.replica_id = ars.replica_id
JOIN sys.dm_hadr_database_replica_cluster_states AS dbcs
    ON drs.replica_id = dbcs.replica_id AND drs.group_database_id = dbcs.group_database_id;

-- Run on the PRIMARY only. Shows how far behind the CDC capture job itself is
-- from the live end of the log -- this is a separate source of lag, additive to
-- AG replication lag, and worth logging alongside your observations.
SELECT * FROM sys.dm_cdc_log_scan_sessions;

SELECT TOP 20 *
FROM cdc.lsn_time_mapping
ORDER BY tran_id DESC;

-- Current max LSN visible in the CDC metadata (run on either node --
-- this is exactly what sys.fn_cdc_get_max_lsn() returns).
SELECT sys.fn_cdc_get_max_lsn();
