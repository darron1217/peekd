package repository

import (
	"time"
)

// GeneralMessageHistory represents message history as stored in the database
type GeneralMessageHistory struct {
	ArrivalTime   time.Time // When message was first seen
	TopicGroup    string    // e.g., voluntary_exit, proposer_slashing, attester_slashing
	Topic         string    // e.g., /eth2/6a95a1a9/voluntary_exit/ssz_snappy
	NodeRegion    string    // e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
	NodeAlias     string    // Alias of the watcher node
	NodePeerCount uint32    // Peer Count of the watcher node
	MessageID     string    // Unique message identifier
	SizeBytes     uint32    // Message size in bytes
}

// SlotMessageStats represents message statistics as stored in the database
type SlotMessageStats struct {
	Slot             uint64    // Beacon slot number (6-second interval)
	TopicGroup       string    // e.g., block, attestation, blob
	Topic            string    // e.g.,
	NodeRegion       string    // e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
	NodeAlias        string    // Alias of the watcher node
	NodePeerCount    uint32    // Peer Count of the watcher node
	MessageID        string    // Unique message identifier
	SlotStartTime    time.Time // Corresponding timestamp for slot
	FirstArrivalTime time.Time // When message was first seen
	LatencyMS        uint32    // Arrival delay from slot start
	SizeBytes        uint32    // Message size in bytes
	SeenCount        uint32    // How many times the message was received in this slot
}
