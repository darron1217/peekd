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
    avg(seen_count) AS avg_duplication,
    quantile(0.5)(seen_count) AS p50_duplication,
    quantile(0.9)(seen_count) AS p90_duplication,
    quantile(0.95)(seen_count) AS p95_duplication
FROM slot_message_stats
WHERE slot_start_time >= toStartOfHour(now()) - INTERVAL 1 HOUR
  AND slot_start_time < toStartOfHour(now())
  AND node_alias IN (
        SELECT node_alias
        FROM (
            SELECT
                toStartOfHour(slot_start_time) AS hour,
                node_alias,
                min(first_arrival_time) AS first_seen,
                max(first_arrival_time) AS last_seen
            FROM slot_message_stats
            WHERE slot_start_time >= toStartOfHour(now()) - INTERVAL 1 HOUR
              AND slot_start_time < toStartOfHour(now())
            GROUP BY hour, node_alias
        )
        WHERE first_seen <= hour + INTERVAL 30 SECOND
          AND last_seen >= hour + INTERVAL 1 HOUR - INTERVAL 30 SECOND
    )
GROUP BY hour, node_region, node_alias, topic_group, topic;