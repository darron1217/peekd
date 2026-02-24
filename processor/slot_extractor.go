package processor

import (
	"fmt"
	"reflect"

	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	ethtypes "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
)

type messageKind int

const (
	slotMessage messageKind = iota
	generalMessage
)

type slotExtractorFunc func(msg interface{}) primitives.Slot

type slotExtractorRegistry struct {
	slotExtractors    map[reflect.Type]slotExtractorFunc
	generalExtractors map[reflect.Type]struct{}
}

func newSlotExtractorRegistry() *slotExtractorRegistry {
	r := &slotExtractorRegistry{
		slotExtractors:    make(map[reflect.Type]slotExtractorFunc),
		generalExtractors: make(map[reflect.Type]struct{}),
	}
	r.registerDefaults()
	return r
}

func (r *slotExtractorRegistry) registerSlotExtractor(msg interface{}, fn slotExtractorFunc) {
	r.slotExtractors[reflect.TypeOf(msg)] = fn
}

func (r *slotExtractorRegistry) registerGeneralMessage(msg interface{}) {
	r.generalExtractors[reflect.TypeOf(msg)] = struct{}{}
}

func (r *slotExtractorRegistry) extract(msg interface{}) (messageKind, primitives.Slot, error) {
	t := reflect.TypeOf(msg)

	if fn, ok := r.slotExtractors[t]; ok {
		return slotMessage, fn(msg), nil
	}
	if _, ok := r.generalExtractors[t]; ok {
		return generalMessage, 0, nil
	}
	return 0, 0, fmt.Errorf("unsupported message type: %T", msg)
}

func (r *slotExtractorRegistry) registerDefaults() {
	// beacon_block
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlock)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlock).Block.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlockAltair)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlockAltair).Block.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlockBellatrix)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlockBellatrix).Block.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlockCapella)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlockCapella).Block.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlockDeneb)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlockDeneb).Block.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedBeaconBlockElectra)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedBeaconBlockElectra).Block.Slot
	})

	// beacon_aggregate_and_proof
	r.registerSlotExtractor((*ethtypes.SignedAggregateAttestationAndProof)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedAggregateAttestationAndProof).Message.Aggregate.Data.Slot
	})
	r.registerSlotExtractor((*ethtypes.SignedAggregateAttestationAndProofElectra)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedAggregateAttestationAndProofElectra).Message.Aggregate.Data.Slot
	})

	// beacon_sync_committee_contribution_and_proof
	r.registerSlotExtractor((*ethtypes.SignedContributionAndProof)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SignedContributionAndProof).Message.Contribution.Slot
	})

	// beacon_attestation
	r.registerSlotExtractor((*ethtypes.Attestation)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.Attestation).Data.Slot
	})
	r.registerSlotExtractor((*ethtypes.AttestationElectra)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.AttestationElectra).Data.Slot
	})
	r.registerSlotExtractor((*ethtypes.SingleAttestation)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SingleAttestation).Data.Slot
	})

	// sync_committee
	r.registerSlotExtractor((*ethtypes.SyncCommitteeMessage)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SyncCommitteeMessage).Slot
	})
	r.registerSlotExtractor((*ethtypes.SyncCommitteeContribution)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.SyncCommitteeContribution).Slot
	})

	// blob_sidecar
	r.registerSlotExtractor((*ethtypes.BlobSidecar)(nil), func(msg interface{}) primitives.Slot {
		return msg.(*ethtypes.BlobSidecar).SignedBlockHeader.Header.Slot
	})

	// general messages (no slot)
	r.registerGeneralMessage((*ethtypes.ProposerSlashing)(nil))
	r.registerGeneralMessage((*ethtypes.AttesterSlashing)(nil))
	r.registerGeneralMessage((*ethtypes.AttesterSlashingElectra)(nil))
	r.registerGeneralMessage((*ethtypes.VoluntaryExit)(nil))
	r.registerGeneralMessage((*ethtypes.BLSToExecutionChange)(nil))
}
