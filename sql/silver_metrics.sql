-- Silver metrics: raw windowed output from the streaming job.
-- Each row represents a 5-minute window of events.

-- 1. All windows ordered by most recent first
SELECT
    window_start,
    window_end,
    events_count,
    carts_count,
    purchases_count
FROM silver_metrics
ORDER BY window_start DESC
LIMIT 100;

-- 2. Conversion rate per window (purchases / add_to_cart)
-- A spike or drop here can indicate a data quality issue upstream.
SELECT
    window_start,
    window_end,
    carts_count,
    purchases_count,
    CASE
        WHEN carts_count = 0 THEN NULL
        ELSE ROUND(100.0 * purchases_count / carts_count, 1)
    END AS conversion_rate_pct
FROM silver_metrics
ORDER BY window_start DESC;

-- 3. Commerce event share per window (add_to_cart + purchase vs total)
-- Useful for detecting a sudden drop in commerce traffic
-- relative to page_views — may indicate schema drift or producer issue.
SELECT
    window_start,
    events_count,
    carts_count + purchases_count AS commerce_events,
    ROUND(100.0 * (carts_count + purchases_count) / NULLIF(events_count, 0), 1) AS commerce_pct
FROM silver_metrics
ORDER BY window_start DESC;
