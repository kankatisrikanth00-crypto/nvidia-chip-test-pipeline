-- ─── Query 1: Yield by fab — THE KEY INSIGHT ──────────────────────
-- Reveals Samsung_3nm has ~7pp lower yield than TSMC fabs
SELECT 
    fab,
    chip_type,
    COUNT(*) as total_tests,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passes,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failures,
    ROUND(SUM(CASE WHEN passed THEN 1.0 ELSE 0.0 END) * 100 / COUNT(*), 2) as yield_pct
FROM chip_tests
GROUP BY fab, chip_type
ORDER BY yield_pct ASC;


-- ─── Query 2: Daily yield trend ────────────────────────────────────
SELECT 
    event_date,
    fab,
    COUNT(*) as tests,
    ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END) * 100, 2) as yield_pct
FROM chip_tests
GROUP BY event_date, fab
ORDER BY event_date, fab;


-- ─── Query 3: Anomaly chip detection ──────────────────────────────
SELECT 
    chip_id,
    chip_type,
    fab,
    COUNT(*) as failure_count
FROM chip_tests
WHERE NOT passed
GROUP BY chip_id, chip_type, fab
HAVING COUNT(*) >= 3
ORDER BY failure_count DESC
LIMIT 20;


-- ─── Query 4: Failure rate by test type and fab ────────────────────
SELECT 
    test_type,
    fab,
    COUNT(*) as total,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failures,
    ROUND(AVG(measurement), 3) as avg_measurement,
    ROUND(STDDEV(measurement), 3) as stddev_measurement
FROM chip_tests
GROUP BY test_type, fab
ORDER BY failures DESC;


-- ─── Query 5: CTAS — gold layer daily aggregation ─────────────────
-- Builds an optimized Parquet table from the raw chip_tests data
CREATE TABLE daily_yield_summary
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['event_date'],
    external_location = 's3://nvidia-demo-processed-YOURNAME/daily_yield_summary/'
)
AS
SELECT 
    fab,
    chip_type,
    family,
    COUNT(*) as total_tests,
    ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END) * 100, 2) as yield_pct,
    ROUND(AVG(measurement), 3) as avg_measurement,
    event_date
FROM chip_tests
GROUP BY fab, chip_type, family, event_date;
