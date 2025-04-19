package processor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/a41-official/peekd/repository"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type BeaconMessageProcessor struct {
	enc  encoder.NetworkEncoding
	repo repository.Repository
}

func NewBeaconMessageProcessor(repo repository.Repository) *BeaconMessageProcessor {
	return &BeaconMessageProcessor{
		repo: repo,
		enc:  encoder.SszNetworkEncoder{},
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
		blockMessage, err := p.renderBlockMessage(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render phase0 block message")
		}
		slog.Debug("block phase0 received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockAltair:
		blockMessage, err := p.renderBlockMessageAltair(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render altair block message")
		}
		slog.Debug("block altair received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockBellatrix:
		blockMessage, err := p.renderBlockMessageBellatrix(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render bellatrix block message")
		}
		slog.Debug("block bellatrix received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockCapella:
		blockMessage, err := p.renderBlockMessageCapella(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render capella block message")
		}
		slog.Debug("block capella received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockDeneb:
		blockMessage, err := p.renderBlockMessageDeneb(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render deneb block message")
		}
		slog.Debug("block deneb received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockElectra:
		blockMessage, err := p.renderBlockMessageElectra(msg, d)
		p.processBeaconMessageMetadata(&blockMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render electra block message")
		}
		slog.Debug("block electra received", "topic", msg.Topic, "data", msg.Data)

	// beacon_aggregate_and_proof
	case *ethtypes.SignedAggregateAttestationAndProof:
		aggregateAttestationAndProofMessage, err := p.renderAggregateAttestationAndProofMessage(msg, d)
		p.processBeaconMessageMetadata(&aggregateAttestationAndProofMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render aggregate attestation and proof message")
		}
		slog.Debug("aggregate attestation and proof received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedAggregateAttestationAndProofElectra:
		aggregateAttestationAndProofElectraMessage, err := p.renderAggregateAttestationAndProofElectraMessage(msg, d)
		p.processBeaconMessageMetadata(&aggregateAttestationAndProofElectraMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render aggregate attestation and proof electra message")
		}
		slog.Debug("aggregate attestation and proof electra received", "topic", msg.Topic, "data", msg.Data)

	// beacon_sync_committee_contribution_and_proof
	case *ethtypes.SignedContributionAndProof:
		syncCommitteeContributionAndProofMessage, err := p.renderSyncCommitteeContributionAndProofMessage(msg, d)
		p.processBeaconMessageMetadata(&syncCommitteeContributionAndProofMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render sync committee contribution and proof message")
		}
		slog.Debug("sync committee contribution and proof received", "topic", msg.Topic, "data", msg.Data)

	// proposer_slashing
	case *ethtypes.ProposerSlashing:
		proposerSlashingMessage, err := p.renderProposerSlashingMessage(msg, d)
		p.processBeaconMessageMetadata(&proposerSlashingMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render proposer slashing message")
		}
		slog.Debug("proposer slashing received", "topic", msg.Topic, "data", msg.Data)

	// attester_slashing
	case *ethtypes.AttesterSlashing:
		attesterSlashingMessage, err := p.renderAttesterSlashingMessage(msg, d)
		p.processBeaconMessageMetadata(&attesterSlashingMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render attester slashing message")
		}
		slog.Debug("attester slashing received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.AttesterSlashingElectra:
		attesterSlashingElectraMessage, err := p.renderAttesterSlashingElectraMessage(msg, d)
		p.processBeaconMessageMetadata(&attesterSlashingElectraMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render attester slashing electra message")
		}
		slog.Debug("attester slashing electra received", "topic", msg.Topic, "data", msg.Data)

	// voluntary_exit
	case *ethtypes.VoluntaryExit:
		voluntaryExitMessage, err := p.renderVoluntaryExitMessage(msg, d)
		p.processBeaconMessageMetadata(&voluntaryExitMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render voluntary exit message")
		}
		slog.Debug("voluntary exit received", "topic", msg.Topic, "data", msg.Data)

	// bls to execution change
	case *ethtypes.BLSToExecutionChange:
		blsToExecutionChangeMessage, err := p.renderBLSToExecutionChangeMessage(msg, d)
		p.processBeaconMessageMetadata(&blsToExecutionChangeMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render bls to execution change message")
		}
		slog.Debug("bls to execution change received", "topic", msg.Topic, "data", msg.Data)

	// --- subnet topics ---

	// beacon_attestation
	case *ethtypes.Attestation:
		attestationMessage, err := p.renderAttestationMessage(msg, d)
		p.processBeaconMessageMetadata(&attestationMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render attestation message")
		}
		slog.Debug("attestation message", "data", attestationMessage.Metadata)
	case *ethtypes.AttestationElectra:
		attestationElectraMessage, err := p.renderAttestationElectraMessage(msg, d)
		p.processBeaconMessageMetadata(&attestationElectraMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render attestation electra message")
		}
		slog.Debug("attestation electra received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SingleAttestation:
		singleAttestationMessage, err := p.renderSingleAttestationMessage(msg, d)
		p.processBeaconMessageMetadata(&singleAttestationMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render single attestation message")
		}
		slog.Debug("single attestation received", "topic", msg.Topic, "data", msg.Data)

	// sync committee message
	case *ethtypes.SyncCommitteeMessage:
		syncCommitteeMessage, err := p.renderSyncCommitteeMessage(msg, d)
		p.processBeaconMessageMetadata(&syncCommitteeMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render sync committee message")
		}
		slog.Debug("sync committee message received", "topic", msg.Topic, "data", msg.Data)

	// blob sidecar
	case *ethtypes.BlobSidecar:
		blobSidecarMessage, err := p.renderBlobSidecarMessage(msg, d)
		p.processBeaconMessageMetadata(&blobSidecarMessage.Metadata)
		if err != nil {
			return errors.Wrap(err, "failed to render blob sidecar message")
		}
		slog.Debug("blob sidecar received", "topic", msg.Topic, "data", msg.Data)

	default:
		return fmt.Errorf("unsupported message type: %T", dst)
	}

	return nil
}

func (p *BeaconMessageProcessor) processBeaconMessageMetadata(
	metadata *BeaconMessageMetadata,
) error {

	slog.Info("processing beacon message metadata", "metadata", metadata)

	// TODO: process metadata

	return nil
}

func (p *BeaconMessageProcessor) renderBlockMessage(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlock,
) (*Phase0BlockMessage, error) {
	return &Phase0BlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlockMessageAltair(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockAltair,
) (*AltairBlockMessage, error) {
	return &AltairBlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlockMessageBellatrix(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockBellatrix,
) (*BellatrixBlockMessage, error) {
	return &BellatrixBlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlockMessageCapella(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockCapella,
) (*CapellaBlockMessage, error) {
	return &CapellaBlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlockMessageDeneb(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockDeneb,
) (*DenebBlockMessage, error) {
	return &DenebBlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlockMessageElectra(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockElectra,
) (*ElectraBlockMessage, error) {
	return &ElectraBlockMessage{
		Metadata: newMetadata(msg),
		Block:    block,
	}, nil
}

func (p *BeaconMessageProcessor) renderAggregateAttestationAndProofMessage(
	msg *pubsub.Message,
	agg *ethtypes.SignedAggregateAttestationAndProof,
) (*AggregateAttestationAndProofMessage, error) {
	return &AggregateAttestationAndProofMessage{
		Metadata:                     newMetadata(msg),
		AggregateAttestationAndProof: agg,
	}, nil
}

func (p *BeaconMessageProcessor) renderAggregateAttestationAndProofElectraMessage(
	msg *pubsub.Message,
	agg *ethtypes.SignedAggregateAttestationAndProofElectra,
) (*AggregateAttestationAndProofElectraMessage, error) {
	return &AggregateAttestationAndProofElectraMessage{
		Metadata:                     newMetadata(msg),
		AggregateAttestationAndProof: agg,
	}, nil
}

func (p *BeaconMessageProcessor) renderSyncCommitteeContributionAndProofMessage(
	msg *pubsub.Message,
	proof *ethtypes.SignedContributionAndProof,
) (*SyncCommitteeContributionAndProofMessage, error) {
	return &SyncCommitteeContributionAndProofMessage{
		Metadata:                          newMetadata(msg),
		SyncCommitteeContributionAndProof: proof,
	}, nil
}

func (p *BeaconMessageProcessor) renderProposerSlashingMessage(
	msg *pubsub.Message,
	slashing *ethtypes.ProposerSlashing,
) (*ProposerSlashingMessage, error) {
	return &ProposerSlashingMessage{
		Metadata:         newMetadata(msg),
		ProposerSlashing: slashing,
	}, nil
}

func (p *BeaconMessageProcessor) renderAttesterSlashingMessage(
	msg *pubsub.Message,
	slashing *ethtypes.AttesterSlashing,
) (*AttesterSlashingMessage, error) {
	return &AttesterSlashingMessage{
		Metadata:         newMetadata(msg),
		AttesterSlashing: slashing,
	}, nil
}

func (p *BeaconMessageProcessor) renderAttesterSlashingElectraMessage(
	msg *pubsub.Message,
	slashing *ethtypes.AttesterSlashingElectra,
) (*AttesterSlashingElectraMessage, error) {
	return &AttesterSlashingElectraMessage{
		Metadata:         newMetadata(msg),
		AttesterSlashing: slashing,
	}, nil
}

func (p *BeaconMessageProcessor) renderVoluntaryExitMessage(
	msg *pubsub.Message,
	exit *ethtypes.VoluntaryExit,
) (*VoluntaryExitMessage, error) {
	return &VoluntaryExitMessage{
		Metadata:      newMetadata(msg),
		VoluntaryExit: exit,
	}, nil
}

func (p *BeaconMessageProcessor) renderBLSToExecutionChangeMessage(
	msg *pubsub.Message,
	change *ethtypes.BLSToExecutionChange,
) (*BLSToExecutionChangeMessage, error) {
	return &BLSToExecutionChangeMessage{
		Metadata:             newMetadata(msg),
		BLSToExecutionChange: change,
	}, nil
}

func (p *BeaconMessageProcessor) renderAttestationMessage(
	msg *pubsub.Message,
	att *ethtypes.Attestation,
) (*AttestationMessage, error) {
	return &AttestationMessage{
		Metadata:    newMetadata(msg),
		Attestation: att,
	}, nil
}

func (p *BeaconMessageProcessor) renderAttestationElectraMessage(
	msg *pubsub.Message,
	att *ethtypes.AttestationElectra,
) (*AttestationElectraMessage, error) {
	return &AttestationElectraMessage{
		Metadata:    newMetadata(msg),
		Attestation: att,
	}, nil
}

func (p *BeaconMessageProcessor) renderSingleAttestationMessage(
	msg *pubsub.Message,
	att *ethtypes.SingleAttestation,
) (*SingleAttestationMessage, error) {
	return &SingleAttestationMessage{
		Metadata:          newMetadata(msg),
		SingleAttestation: att,
	}, nil
}

func (p *BeaconMessageProcessor) renderSyncCommitteeMessage(
	msg *pubsub.Message,
	sync *ethtypes.SyncCommitteeMessage,
) (*SyncCommitteeMessage, error) {
	return &SyncCommitteeMessage{
		Metadata:      newMetadata(msg),
		SyncCommittee: sync,
	}, nil
}

func (p *BeaconMessageProcessor) renderBlobSidecarMessage(
	msg *pubsub.Message,
	blob *ethtypes.BlobSidecar,
) (*BlobSidecarMessage, error) {
	return &BlobSidecarMessage{
		Metadata: newMetadata(msg),
		Blob:     blob,
	}, nil
}

func newMetadata(msg *pubsub.Message) BeaconMessageMetadata {
	return BeaconMessageMetadata{
		PeerID:  msg.ReceivedFrom.String(),
		Topic:   msg.GetTopic(),
		Seq:     msg.GetSeqno(),
		MsgID:   hex.EncodeToString([]byte(msg.ID)),
		MsgSize: len(msg.Data),
	}
}
