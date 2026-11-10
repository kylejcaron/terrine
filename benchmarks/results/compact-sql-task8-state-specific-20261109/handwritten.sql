SELECT unit, t0, CASE WHEN before_total <= 0 THEN 'low' WHEN before_total > 0 THEN 'high' END AS from_state,
  CASE WHEN after_total <= 0 THEN 'low' WHEN after_total > 0 THEN 'high' END AS to_state
FROM (
WITH anchors AS (
  SELECT u.unit_id AS unit, s.t0 FROM users AS u
  CROSS JOIN (VALUES (TIMESTAMPTZ '2025-01-08 00:00:00+00:00'), (TIMESTAMPTZ '2025-01-15 00:00:00+00:00')) AS s(t0)
)
SELECT a.unit, a.t0,
  CASE WHEN COUNT(e.value) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '-604800 seconds' AND e.event_time < a.t0 + INTERVAL '0 seconds') = 0 AND COUNT(e.event_id) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '-604800 seconds' AND e.event_time < a.t0 + INTERVAL '0 seconds') > 0 THEN NULL ELSE COALESCE(SUM(e.value) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '-604800 seconds' AND e.event_time < a.t0 + INTERVAL '0 seconds'), 0) END AS before_total,
  CASE WHEN COUNT(e.value) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '0 seconds' AND e.event_time < a.t0 + INTERVAL '604800 seconds') = 0 AND COUNT(e.event_id) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '0 seconds' AND e.event_time < a.t0 + INTERVAL '604800 seconds') > 0 THEN NULL ELSE COALESCE(SUM(e.value) FILTER (WHERE e.event_time >= a.t0 + INTERVAL '0 seconds' AND e.event_time < a.t0 + INTERVAL '604800 seconds'), 0) END AS after_total
FROM anchors AS a
LEFT JOIN events AS e ON e.unit_id = a.unit AND e.event_time >= a.t0 + INTERVAL '-604800 seconds' AND e.event_time < a.t0 + INTERVAL '604800 seconds'
GROUP BY a.unit, a.t0
) AS totals
