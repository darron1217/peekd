-- Create the table for materialized view
CREATE TABLE slot_message_hourly_rollup (
    hour DateTime,
    topic_group String,
    topic String,
    node_region String,
    node_alias String,
    sum_seen_count Float64,
    sum_total_bytes Float64,
    avg_latency Float64,
    avg_duplication Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, topic_group, topic)
TTL hour + INTERVAL 30 DAY;

-- Create the materialized view
CREATE MATERIALIZED VIEW slot_message_hourly_mv
REFRESH EVERY 1 HOUR APPEND
TO slot_message_hourly_rollup 
AS
SELECT
    toStartOfHour(slot_start_time) AS hour,
    topic_group,
    topic,
    node_region,
    node_alias,
    sum(seen_count) AS sum_seen_count,
    sum(seen_count * size_bytes) AS sum_total_bytes,
    avg(latency_ms) AS avg_latency,
    avg(seen_count) AS avg_duplication
FROM slot_message_stats
WHERE slot_start_time >= now() - INTERVAL 2 HOUR -- safety margin
GROUP BY hour, topic_group, topic, node_region, node_alias;