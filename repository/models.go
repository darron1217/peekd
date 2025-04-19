package repository

import (
	"time"
)

// MessageStats represents message statistics as stored in the database
type MessageStats struct {
	Slot             uint64    // Beacon slot number (6-second interval)
	Topic            string    // e.g., block, attestation, blob
	NodeRegion       string    // e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
	MessageID        string    // Unique message identifier
	SlotStartTime    time.Time // Corresponding timestamp for slot
	FirstArrivalTime time.Time // When message was first seen
	LatencyMS        uint32    // Arrival delay from slot start
	SizeBytes        uint32    // Message size in bytes
	SeenCount        uint32    // How many times the message was received in this slot
	NodeID           string    // ID of the watcher node
}
