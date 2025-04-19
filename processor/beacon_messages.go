package processor

import (
	"time"
)

type GeneralMessageMetadata struct {
	PeerID     string    `json:"PeerID"`
	Topic      string    `json:"Topic"`
	MsgID      string    `json:"MsgID"`
	MsgSize    int       `json:"MsgSize"`
	MsgArrival time.Time `json:"MsgArrival"`
}

type SlotMessageMetadata struct {
	PeerID         string        `json:"PeerID"`
	Topic          string        `json:"Topic"`
	MsgID          string        `json:"MsgID"`
	MsgSize        int           `json:"MsgSize"`
	MsgArrival     time.Time     `json:"MsgArrival"`
	MsgDelayInSlot time.Duration `json:"MsgDelayInSlot"`
	Slot           uint64        `json:"Slot"`
}
