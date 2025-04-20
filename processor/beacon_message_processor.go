package processor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/a41-official/peekd/eth"
	"github.com/a41-official/peekd/repository"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type BeaconMessageProcessor struct {
	enc          encoder.NetworkEncoding
	repo         repository.Repository
	beaconConfig *params.BeaconChainConfig
	genesisTime  time.Time
}

func NewBeaconMessageProcessor(repo repository.Repository) *BeaconMessageProcessor {
	slog.Info("successfully created beacon message processor")
	
	return &BeaconMessageProcessor{
		repo:         repo,
		enc:          encoder.SszNetworkEncoder{},
		beaconConfig: eth.GetBeaconChainConfig(),
		genesisTime:  eth.GetGenesisConfig().GenesisTime,
	}
}

func (p *BeaconMessageProcessor) Process(ctx context.Context, msg *pubsub.Message, dst ssz.Unmarshaler) error {
	if err := p.enc.DecodeGossip(msg.Data, dst); err != nil {
		return errors.Wrap(err, "failed to decode gossip message")
	}

	switch d := dst.(type) {
	// --- global topics ---

	// beacon_block
	case *ethtypes.SignedBeaconBlock:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block phase0 received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockAltair:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block altair received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockBellatrix:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block bellatrix received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockCapella:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block capella received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockDeneb:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block deneb received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockElectra:
		metadata := p.newSlotMetadata(msg, d.Block.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("block electra received", "topic", msg.Topic, "data", msg.Data)

	// beacon_aggregate_and_proof
	case *ethtypes.SignedAggregateAttestationAndProof:
		metadata := p.newSlotMetadata(msg, d.Message.Aggregate.Data.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("aggregate attestation and proof received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedAggregateAttestationAndProofElectra:
		metadata := p.newSlotMetadata(msg, d.Message.Aggregate.Data.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("aggregate attestation and proof electra received", "topic", msg.Topic, "data", msg.Data)

	// beacon_sync_committee_contribution_and_proof
	case *ethtypes.SignedContributionAndProof:
		metadata := p.newSlotMetadata(msg, d.Message.Contribution.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("sync committee contribution and proof received", "topic", msg.Topic, "data", msg.Data)

	// proposer_slashing
	case *ethtypes.ProposerSlashing:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
		slog.Debug("proposer slashing received", "topic", msg.Topic, "data", msg.Data)

	// attester_slashing
	case *ethtypes.AttesterSlashing:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
		slog.Debug("attester slashing received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.AttesterSlashingElectra:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
		slog.Debug("attester slashing electra received", "topic", msg.Topic, "data", msg.Data)

	// voluntary_exit
	case *ethtypes.VoluntaryExit:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
		slog.Debug("voluntary exit received", "topic", msg.Topic, "data", msg.Data)

	// bls to execution change
	case *ethtypes.BLSToExecutionChange:
		metadata := p.newGeneralMetadata(msg)
		p.processGeneralMessageMetadata(metadata)
		slog.Debug("bls to execution change received", "topic", msg.Topic, "data", msg.Data)

	// --- subnet topics ---

	// beacon_attestation
	case *ethtypes.Attestation:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("attestation received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.AttestationElectra:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("attestation electra received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SingleAttestation:
		metadata := p.newSlotMetadata(msg, d.Data.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("single attestation received", "topic", msg.Topic, "data", msg.Data)

	// sync committee message
	case *ethtypes.SyncCommitteeMessage:
		metadata := p.newSlotMetadata(msg, d.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("sync committee message received", "topic", msg.Topic, "data", msg.Data)

	// sync committee contribution
	case *ethtypes.SyncCommitteeContribution:
		metadata := p.newSlotMetadata(msg, d.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("sync committee contribution received", "topic", msg.Topic, "data", msg.Data)

	// blob sidecar
	case *ethtypes.BlobSidecar:
		metadata := p.newSlotMetadata(msg, d.SignedBlockHeader.Header.Slot)
		p.processSlotMessageMetadata(metadata)
		slog.Debug("blob sidecar received", "topic", msg.Topic, "data", msg.Data)

	default:
		return fmt.Errorf("unsupported message type: %T", dst)
	}

	return nil
}

func (p *BeaconMessageProcessor) processGeneralMessageMetadata(
	metadata *GeneralMessageMetadata,
) error {

	slog.Debug("processing general message metadata", "topic", metadata.Topic, "msg_id", metadata.MsgID, "msg_size", metadata.MsgSize)

	// TODO: process metadata

	return nil
}

func (p *BeaconMessageProcessor) processSlotMessageMetadata(
	metadata *SlotMessageMetadata,
) error {

	slog.Debug("processing slot message metadata", "topic", metadata.Topic, "msg_id", metadata.MsgID, "msg_size", metadata.MsgSize, "msg_delay_in_slot", metadata.MsgDelayInSlot, "slot", metadata.Slot)

	// TODO: process metadata

	return nil
}

func (p *BeaconMessageProcessor) newGeneralMetadata(msg *pubsub.Message) *GeneralMessageMetadata {
	msgArrival := time.Now()
	return &GeneralMessageMetadata{
		PeerID:     msg.ReceivedFrom.String(),
		Topic:      msg.GetTopic(),
		MsgID:      hex.EncodeToString([]byte(msg.ID)),
		MsgSize:    len(msg.Data),
		MsgArrival: msgArrival,
	}
}

func (p *BeaconMessageProcessor) newSlotMetadata(msg *pubsub.Message, slot primitives.Slot) *SlotMessageMetadata {
	msgArrival := time.Now()
	return &SlotMessageMetadata{
		PeerID:         msg.ReceivedFrom.String(),
		Topic:          msg.GetTopic(),
		MsgID:          hex.EncodeToString([]byte(msg.ID)),
		MsgSize:        len(msg.Data),
		MsgArrival:     msgArrival,
		MsgDelayInSlot: p.getDelayInSlot(msgArrival, slot),
		Slot:           uint64(slot),
	}
}

func (p *BeaconMessageProcessor) getDelayInSlot(arrivalTime time.Time, slot primitives.Slot) time.Duration {
	// get slot time since genesis
	slotTime := p.genesisTime.Add((time.Duration(slot) * time.Second * time.Duration(p.beaconConfig.SecondsPerSlot)))

	// compare the arrival time to the base-slot time
	inSlotTime := arrivalTime.Sub(slotTime)
	return inSlotTime
}
