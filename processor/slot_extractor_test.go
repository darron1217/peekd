package processor

import (
	"testing"

	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	ethtypes "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
)

func TestSlotExtractorRegistry_BeaconBlocks(t *testing.T) {
	registry := newSlotExtractorRegistry()

	tests := []struct {
		name string
		msg  interface{}
		slot primitives.Slot
	}{
		{"SignedBeaconBlock", &ethtypes.SignedBeaconBlock{Block: &ethtypes.BeaconBlock{Slot: 100}}, 100},
		{"SignedBeaconBlockAltair", &ethtypes.SignedBeaconBlockAltair{Block: &ethtypes.BeaconBlockAltair{Slot: 200}}, 200},
		{"SignedBeaconBlockBellatrix", &ethtypes.SignedBeaconBlockBellatrix{Block: &ethtypes.BeaconBlockBellatrix{Slot: 300}}, 300},
		{"SignedBeaconBlockCapella", &ethtypes.SignedBeaconBlockCapella{Block: &ethtypes.BeaconBlockCapella{Slot: 400}}, 400},
		{"SignedBeaconBlockDeneb", &ethtypes.SignedBeaconBlockDeneb{Block: &ethtypes.BeaconBlockDeneb{Slot: 500}}, 500},
		{"SignedBeaconBlockElectra", &ethtypes.SignedBeaconBlockElectra{Block: &ethtypes.BeaconBlockElectra{Slot: 600}}, 600},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, slot, err := registry.extract(tt.msg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kind != slotMessage {
				t.Fatalf("expected slotMessage, got %v", kind)
			}
			if slot != tt.slot {
				t.Errorf("expected slot=%d, got %d", tt.slot, slot)
			}
		})
	}
}

func TestSlotExtractorRegistry_Attestations(t *testing.T) {
	registry := newSlotExtractorRegistry()

	tests := []struct {
		name string
		msg  interface{}
		slot primitives.Slot
	}{
		{"Attestation", &ethtypes.Attestation{Data: &ethtypes.AttestationData{Slot: 100}}, 100},
		{"SingleAttestation", &ethtypes.SingleAttestation{Data: &ethtypes.AttestationData{Slot: 200}}, 200},
		{"AggregateAndProof", &ethtypes.SignedAggregateAttestationAndProof{
			Message: &ethtypes.AggregateAttestationAndProof{
				Aggregate: &ethtypes.Attestation{Data: &ethtypes.AttestationData{Slot: 300}},
			},
		}, 300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, slot, err := registry.extract(tt.msg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kind != slotMessage {
				t.Fatalf("expected slotMessage, got %v", kind)
			}
			if slot != tt.slot {
				t.Errorf("expected slot=%d, got %d", tt.slot, slot)
			}
		})
	}
}

func TestSlotExtractorRegistry_GeneralMessages(t *testing.T) {
	registry := newSlotExtractorRegistry()

	tests := []struct {
		name string
		msg  interface{}
	}{
		{"ProposerSlashing", &ethtypes.ProposerSlashing{}},
		{"AttesterSlashing", &ethtypes.AttesterSlashing{}},
		{"AttesterSlashingElectra", &ethtypes.AttesterSlashingElectra{}},
		{"VoluntaryExit", &ethtypes.VoluntaryExit{}},
		{"BLSToExecutionChange", &ethtypes.BLSToExecutionChange{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, _, err := registry.extract(tt.msg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kind != generalMessage {
				t.Fatalf("expected generalMessage, got %v", kind)
			}
		})
	}
}

func TestSlotExtractorRegistry_SyncCommittee(t *testing.T) {
	registry := newSlotExtractorRegistry()

	tests := []struct {
		name string
		msg  interface{}
		slot primitives.Slot
	}{
		{"SyncCommitteeMessage", &ethtypes.SyncCommitteeMessage{Slot: 100}, 100},
		{"SyncCommitteeContribution", &ethtypes.SyncCommitteeContribution{Slot: 200}, 200},
		{"ContributionAndProof", &ethtypes.SignedContributionAndProof{
			Message: &ethtypes.ContributionAndProof{
				Contribution: &ethtypes.SyncCommitteeContribution{Slot: 300},
			},
		}, 300},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, slot, err := registry.extract(tt.msg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kind != slotMessage {
				t.Fatalf("expected slotMessage, got %v", kind)
			}
			if slot != tt.slot {
				t.Errorf("expected slot=%d, got %d", tt.slot, slot)
			}
		})
	}
}

func TestSlotExtractorRegistry_BlobSidecar(t *testing.T) {
	registry := newSlotExtractorRegistry()

	msg := &ethtypes.BlobSidecar{
		SignedBlockHeader: &ethtypes.SignedBeaconBlockHeader{
			Header: &ethtypes.BeaconBlockHeader{Slot: 777},
		},
	}

	kind, slot, err := registry.extract(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != slotMessage {
		t.Fatalf("expected slotMessage, got %v", kind)
	}
	if slot != 777 {
		t.Errorf("expected slot=777, got %d", slot)
	}
}

func TestSlotExtractorRegistry_UnknownType(t *testing.T) {
	registry := newSlotExtractorRegistry()

	_, _, err := registry.extract("unknown-type")
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}
