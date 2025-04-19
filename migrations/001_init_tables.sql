CREATE TABLE general_message_histories (
	arrival_time        DateTime,       -- When message was first seen
    topic               String,         -- e.g., voluntary_exit, proposer_slashing, attester_slashing
    node_region         String,         -- e.g., "EAST_ASIA", "CENTRAL_EUROPE", "NORTH_AMERICA" etc.
    node_id             String,         -- ID of the watcher node
    message_id          String,         -- Unique message identifier
    size_bytes          UInt32,         -- Message size in bytes
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(arrival_time)
ORDER BY (arrival_time, topic, node_region, node_id);

CREATE TABLE slot_message_stats (
    slot                UInt64,         -- Beacon slot number (6-second interval)
    topic               String,         -- e.g., beacon_block, beacon_attestation, blob_sidecar
    node_region         String,         -- e.g., "EAST_ASIA", "CENTRAL_EUROPE", "NORTH_AMERICA" etc.
    node_id             String,         -- ID of the watcher node
    message_id          String,         -- Unique message identifier
    slot_start_time     DateTime,       -- Corresponding timestamp for slot
    first_arrival_time  DateTime,       -- When message was first seen
    latency_ms          UInt32,         -- Arrival delay from slot start
    size_bytes          UInt32,         -- Message size in bytes
    seen_count          UInt32,         -- How many times the message was received in this slot
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(slot_start_time)
ORDER BY (slot, topic, node_region, node_id);