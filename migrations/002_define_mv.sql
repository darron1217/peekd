-- Create the table for materialized view
CREATE TABLE slot_message_hourly_rollup (
    hour DateTime,
    node_region String,
    node_alias String,
    topic_group String,
    topic String,
    sum_seen_count Float64,
    sum_total_bytes Float64,
    avg_latency Float64,
    p50_latency Float64,
    p90_latency Float64,
    p95_latency Float64,
    avg_duplication Float64,
    p50_duplication Float64,
    p90_duplication Float64,
    p95_duplication Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, node_region, node_alias, topic_group, topic)
TTL hour + INTERVAL 30 DAY;

-- Create the materialized view
CREATE MATERIALIZED VIEW slot_message_hourly_mv
REFRESH EVERY 1 HOUR APPEND
TO slot_message_hourly_rollup 
AS
WITH toStartOfHour(now()) AS target_hour
SELECT
    toStartOfHour(slot_start_time) AS hour,
    node_region,
    node_alias,
    topic_group,
    topic,
    sum(seen_count) AS sum_seen_count,
    sum(seen_count * size_bytes) AS sum_total_bytes,
    avg(latency_ms) AS avg_latency,
    quantile(0.5)(latency_ms) AS p50_latency,
    quantile(0.9)(latency_ms) AS p90_latency,
    quantile(0.95)(latency_ms) AS p95_latency,
    avg(seen_count) - 1 AS avg_duplication,
    quantile(0.5)(seen_count) - 1 AS p50_duplication,
    quantile(0.9)(seen_count) - 1 AS p90_duplication,
    quantile(0.95)(seen_count) - 1 AS p95_duplication
FROM slot_message_stats
WHERE slot_start_time >= target_hour - INTERVAL 1 HOUR
  AND slot_start_time < target_hour
  AND (node_alias, toStartOfHour(slot_start_time)) IN (
        SELECT node_alias, hour
        FROM (
            SELECT
                toStartOfHour(slot_start_time) AS hour,
                node_alias,
                count(DISTINCT slot_start_time) AS slot_count
            FROM slot_message_stats
            WHERE slot_start_time >= target_hour - INTERVAL 1 HOUR
              AND slot_start_time < target_hour
            GROUP BY hour, node_alias
            HAVING slot_count = 300
        )
    )
  AND latency_ms < 768000
GROUP BY hour, node_region, node_alias, topic_group, topic;