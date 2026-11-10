WITH snapshots(t0) AS (
  VALUES (TIMESTAMPTZ '2025-02-01 00:00:00+00'),
         (TIMESTAMPTZ '2025-03-01 00:00:00+00')
), totals AS (
  SELECT c.customer_id AS unit, s.t0,
    count(o.ordered_at) FILTER (WHERE o.ordered_at < s.t0) AS past_n,
    count(o.amount) FILTER (WHERE o.ordered_at < s.t0) AS past_nn,
    sum(o.amount) FILTER (WHERE o.ordered_at < s.t0) AS past_sum,
    count(o.ordered_at) FILTER (WHERE o.ordered_at >= s.t0) AS future_n,
    count(o.amount) FILTER (WHERE o.ordered_at >= s.t0) AS future_nn,
    sum(o.amount) FILTER (WHERE o.ordered_at >= s.t0) AS future_sum
  FROM customers c CROSS JOIN snapshots s
  LEFT JOIN orders o ON o.customer_id = c.customer_id
    AND o.ordered_at >= s.t0 - INTERVAL '30 days'
    AND o.ordered_at < s.t0 + INTERVAL '30 days'
  GROUP BY c.customer_id, s.t0
)
SELECT unit, t0,
  past_n::BIGINT AS orders_30d,
  TRUE AS orders_30d__complete, NULL::VARCHAR AS orders_30d__reason,
  CASE WHEN past_n = 0 THEN 0.0 ELSE past_sum END AS spend_30d,
  TRUE AS spend_30d__complete,
  CASE WHEN past_n > 0 AND past_nn = 0 THEN 'all_values_null' END AS spend_30d__reason,
  CASE WHEN t0 + INTERVAL '30 days' <= TIMESTAMPTZ '2025-03-21 00:00:00+00'
       THEN CASE WHEN future_n = 0 THEN 0.0 ELSE future_sum END END AS spend_next_30d,
  t0 + INTERVAL '30 days' <= TIMESTAMPTZ '2025-03-21 00:00:00+00'
    AS spend_next_30d__complete,
  CASE WHEN t0 + INTERVAL '30 days' > TIMESTAMPTZ '2025-03-21 00:00:00+00'
       THEN 'incomplete_coverage'
       WHEN future_n > 0 AND future_nn = 0 THEN 'all_values_null'
       END AS spend_next_30d__reason
FROM totals ORDER BY unit, t0
