package processor

import (
	"context"
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

	switch dst.(type) {
	// block
	case *ethtypes.SignedBeaconBlock:
		slog.Info("block phase0 received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockAltair:
		slog.Info("block altair received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockBellatrix:
		slog.Info("block bellatrix received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockCapella:
		slog.Info("block capella received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockDeneb:
		slog.Info("block deneb received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedBeaconBlockElectra:
		slog.Info("block electra received", "topic", msg.Topic, "data", msg.Data)

	// blob sidecar
	case *ethtypes.BlobSidecar:
		slog.Info("blob sidecar received", "topic", msg.Topic, "data", msg.Data)

	// attestation
	case *ethtypes.Attestation:
		slog.Info("attestation received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.AttestationElectra:
		slog.Info("attestation electra received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SingleAttestation:
		slog.Info("single attestation received", "topic", msg.Topic, "data", msg.Data)

	// aggregate attestation and proof
	case *ethtypes.SignedAggregateAttestationAndProof:
		slog.Info("aggregate attestation and proof received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedAggregateAttestationAndProofElectra:
		slog.Info("aggregate attestation and proof electra received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.SignedContributionAndProof:
		slog.Info("contribution and proof received", "topic", msg.Topic, "data", msg.Data)

	// sync committee message
	case *ethtypes.SyncCommitteeMessage:
		slog.Info("sync committee message received", "topic", msg.Topic, "data", msg.Data)

	// proposer slashing
	case *ethtypes.ProposerSlashing:
		slog.Info("proposer slashing received", "topic", msg.Topic, "data", msg.Data)

	// attester slashing
	case *ethtypes.AttesterSlashing:
		slog.Info("attester slashing received", "topic", msg.Topic, "data", msg.Data)
	case *ethtypes.AttesterSlashingElectra:
		slog.Info("attester slashing electra received", "topic", msg.Topic, "data", msg.Data)

	// voluntary exit
	case *ethtypes.VoluntaryExit:
		slog.Info("voluntary exit received", "topic", msg.Topic, "data", msg.Data)

	// bls to execution change
	case *ethtypes.BLSToExecutionChange:
		slog.Info("bls to execution change received", "topic", msg.Topic, "data", msg.Data)

	default:
		return fmt.Errorf("unsupported message type: %T", dst)
	}

	return nil
}
