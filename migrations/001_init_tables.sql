-- general_message_histories
CREATE TABLE general_message_histories (
    arrival_time        DateTime,       -- When message was first seen
    fork_version        String,         -- e.g., 6a95a1a9
    topic               String,         -- e.g., voluntary_exit
    node_region         String,         -- e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
    node_alias          String,         -- Alias of the watcher node
    node_peer_count     UInt32,         -- Peer Count of the watcher node
    message_id          String,         -- Unique message identifier
    size_bytes          UInt32,         -- Message size in bytes
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(arrival_time)
ORDER BY (arrival_time, node_region, node_alias, topic);

-- slot_message_stats
CREATE TABLE slot_message_stats (
    slot                UInt64,         -- Beacon slot number (6-second interval)
    topic_group         String,         -- e.g., block, attestation, blob
    fork_version        String,         -- e.g., 6a95a1a9
    topic               String,         -- e.g., beacon_attestation_10
    node_region         String,         -- e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
    node_alias          String,         -- Alias of the watcher node
    node_peer_count     UInt32,         -- Peer Count of the watcher node
    message_id          String,         -- Unique message identifier
    slot_start_time     DateTime,       -- Corresponding timestamp for slot
    first_arrival_time  DateTime,       -- When message was first seen
    latency_ms          UInt32,         -- Arrival delay from slot start
    size_bytes          UInt32,         -- Message size in bytes
    seen_count          UInt32,         -- How many times the message was received in this slot
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(slot_start_time)
ORDER BY (toStartOfHour(slot_start_time), node_region, node_alias, topic_group, topic);