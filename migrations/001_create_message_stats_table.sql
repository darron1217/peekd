CREATE TABLE message_stats (
    slot                UInt64,         -- Beacon slot number (6-second interval)
    topic               String,         -- e.g., block, attestation, blob
    node_region         String,         -- e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
    message_id          String,         -- Unique message identifier
    slot_start_time     DateTime,       -- Corresponding timestamp for slot
    first_arrival_time  DateTime,       -- When message was first seen
    latency_ms          UInt32,         -- Arrival delay from slot start
    size_bytes          UInt32,         -- Message size in bytes
    seen_count          UInt32,         -- How many times the message was received in this slot
    node_id             String,         -- ID of the watcher node
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(slot_start_time)
ORDER BY (slot, topic, node_region, message_id);