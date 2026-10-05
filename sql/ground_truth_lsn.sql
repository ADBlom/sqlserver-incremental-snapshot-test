-- Ground truth: for each row in dbo.orders, what LSN did the CDC capture job
-- actually assign it? Run this against the PRIMARY (node1) for the authoritative
-- values, and again against the SECONDARY (node2) to check whether/when the same
-- rows show up there once redone.
--
-- __$start_lsn is the LSN of the transaction that made the change, exactly the
-- kind of value returned by sys.fn_cdc_get_max_lsn().

SELECT
    ct.id,
    ct.__$start_lsn,
    sys.fn_cdc_map_lsn_to_time(ct.__$start_lsn) AS commit_time,
    ct.__$operation   -- 1=delete, 2=insert, 3=update(before), 4=update(after)
FROM cdc.dbo_orders_CT AS ct
ORDER BY ct.__$start_lsn;

-- Quick lookup for a single row id (parameterize @id from your analysis script):
-- SELECT __$start_lsn FROM cdc.dbo_orders_CT WHERE id = @id AND __$operation = 2;
