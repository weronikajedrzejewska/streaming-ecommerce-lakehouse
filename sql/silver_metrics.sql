-- Example query against Silver metrics output loaded as a table/view.
SELECT
  window_start,
  window_end,
  events_count,
  carts_count,
  purchases_count
FROM silver_metrics
ORDER BY window_start DESC
LIMIT 100;
