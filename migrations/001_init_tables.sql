-- general_message_histories
CREATE TABLE general_message_histories (
    arrival_time        DateTime,       -- When message was first seen
    topic_group         String,         -- e.g., voluntary_exit, proposer_slashing, attester_slashing
    topic               String,         -- e.g., /eth2/6a95a1a9/voluntary_exit/ssz_snappy
    node_region         String,         -- e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
    node_alias          String,         -- Alias of the watcher node
    node_peer_count     UInt32,         -- Peercount of the watcher node
    message_id          String,         -- Unique message identifier
    size_bytes          UInt32,         -- Message size in bytes
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(arrival_time)
ORDER BY (arrival_time, topic_group, topic, node_region, node_alias);

-- slot_message_stats
CREATE TABLE slot_message_stats (
    slot                UInt64,         -- Beacon slot number (6-second interval)
    topic_group         String,         -- e.g., block, attestation, blob
    topic               String,         -- e.g., /eth2/6a95a1a9/beacon_attestation_10/ssz_snappy
    node_region         String,         -- e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
    node_alias          String,         -- Alias of the watcher node
    node_peer_count     UInt32,         -- Peercount of the watcher node
    message_id          String,         -- Unique message identifier
    slot_start_time     DateTime,       -- Corresponding timestamp for slot
    first_arrival_time  DateTime,       -- When message was first seen
    latency_ms          UInt32,         -- Arrival delay from slot start
    size_bytes          UInt32,         -- Message size in bytes
    seen_count          UInt32,         -- How many times the message was received in this slot
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(slot_start_time)
ORDER BY (slot, topic_group, topic, node_region, node_alias);