package gossip

import (
	"bytes"
	"context"
	"log/slog"
	"strings"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type TopicHandler = func(context.Context, *pubsub.Message) error

func (gs *GossipSub) mappingTopicToHandler(topic string) TopicHandler {

	switch {
	case strings.Contains(topic, p2p.GossipBlockMessage):
		return gs.beaconBlockHandler
	case strings.Contains(topic, p2p.GossipBlobSidecarMessage):
		return gs.blobSidecarHandler
	case strings.Contains(topic, p2p.GossipAggregateAndProofMessage):
		return gs.beaconAggregateAndProofHandler
	case strings.Contains(topic, p2p.GossipAttestationMessage):
		return gs.beaconAttestationHandler
	case strings.Contains(topic, p2p.GossipContributionAndProofMessage):
		return gs.syncCommitteeContributionAndProofHandler
	case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
		return gs.syncCommitteeHandler
	case strings.Contains(topic, p2p.GossipProposerSlashingMessage):
		return gs.proposerSlashingHandler
	case strings.Contains(topic, p2p.GossipAttesterSlashingMessage):
		return gs.attesterSlashingHandler
	case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
		return gs.blsToExecutionChangeHandler
	case strings.Contains(topic, p2p.GossipExitMessage):
		return gs.voluntaryExitHandler
	default:
		slog.With("topic", topic).
			Warn("Noop gossip handler is set to unknown topic")
		return gs.noopHandler
	}
}

func (gs *GossipSub) noopHandler(ctx context.Context, msg *pubsub.Message) error {
	return nil
}

func (gs *GossipSub) beaconBlockHandler(ctx context.Context, msg *pubsub.Message) error {

	var block ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion):
		block = &ethtypes.SignedBeaconBlock{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion):
		block = &ethtypes.SignedBeaconBlockAltair{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion):
		block = &ethtypes.SignedBeaconBlockBellatrix{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion):
		block = &ethtypes.SignedBeaconBlockCapella{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion):
		block = &ethtypes.SignedBeaconBlockDeneb{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		block = &ethtypes.SignedBeaconBlockElectra{}
	default:
		return errors.New("unknown fork version for handling block")
	}

	gs.messageProcessor.Process(ctx, msg, block)

	slog.With("block", block).
		Debug("handled block from gossip")
	return nil
}

func (gs *GossipSub) blobSidecarHandler(ctx context.Context, msg *pubsub.Message) error {

	var blob ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		blob = &ethtypes.BlobSidecar{}
	default:
		return errors.New("unknown fork version for handling blob")
	}

	gs.messageProcessor.Process(ctx, msg, blob)

	slog.With("blob", blob).
		Debug("handled blob from gossip")
	return nil
}

func (gs *GossipSub) beaconAggregateAndProofHandler(ctx context.Context, msg *pubsub.Message) error {

	var aggProof ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion):
		aggProof = &ethtypes.SignedAggregateAttestationAndProof{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		aggProof = &ethtypes.SignedAggregateAttestationAndProofElectra{}
	default:
		return errors.New("unknown fork version for handling aggregate proof")
	}

	gs.messageProcessor.Process(ctx, msg, aggProof)

	slog.With("aggregate_proof", aggProof).
		Debug("handled aggregate proof from gossip")
	return nil
}

func (gs *GossipSub) beaconAttestationHandler(ctx context.Context, msg *pubsub.Message) error {

	var att ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion):
		att = &ethtypes.Attestation{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		att = &ethtypes.SingleAttestation{}
	default:
		return errors.New("unknown fork version for handling attestation")
	}

	gs.messageProcessor.Process(ctx, msg, att)

	slog.With("attestation", att).
		Debug("handled aggregate proof from gossip")
	return nil
}

func (gs *GossipSub) syncCommitteeContributionAndProofHandler(ctx context.Context, msg *pubsub.Message) error {

	var ctrProof ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		ctrProof = &ethtypes.SignedContributionAndProof{}
	default:
		return errors.New("unknown fork version for handling contribution proof")
	}

	gs.messageProcessor.Process(ctx, msg, ctrProof)

	slog.With("contribution_proof", ctrProof).
		Debug("handled contribution proof from gossip")
	return nil
}

func (gs *GossipSub) syncCommitteeHandler(ctx context.Context, msg *pubsub.Message) error {

	var sync ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		sync = &ethtypes.SyncCommitteeMessage{}
	default:
		return errors.New("unknown fork version for handling sync committee")
	}

	gs.messageProcessor.Process(ctx, msg, sync)

	slog.With("sync_committee", sync).
		Debug("handled sync committee from gossip")
	return nil
}

func (gs *GossipSub) proposerSlashingHandler(ctx context.Context, msg *pubsub.Message) error {

	var pSlash ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		pSlash = &ethtypes.ProposerSlashing{}
	default:
		return errors.New("unknown fork version for handling proposer slashing")
	}

	gs.messageProcessor.Process(ctx, msg, pSlash)

	slog.With("proposer_slashing", pSlash).
		Debug("handled proposer slashing from gossip")
	return nil
}

func (gs *GossipSub) attesterSlashingHandler(ctx context.Context, msg *pubsub.Message) error {

	var aSlash ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		aSlash = &ethtypes.AttesterSlashing{}
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		aSlash = &ethtypes.AttesterSlashingElectra{}
	default:
		return errors.New("unknown fork version for handling attester slashing")
	}

	gs.messageProcessor.Process(ctx, msg, aSlash)

	slog.With("attester_slashing", aSlash).
		Debug("handled attester slashing from gossip")
	return nil
}

func (gs *GossipSub) blsToExecutionChangeHandler(ctx context.Context, msg *pubsub.Message) error {

	var blsExec ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		blsExec = &ethtypes.BLSToExecutionChange{}
	default:
		return errors.New("unknown fork version for handling bls execution change")
	}

	gs.messageProcessor.Process(ctx, msg, blsExec)

	slog.With("bls_execution_change", blsExec).
		Debug("handled bls execution change from gossip")
	return nil
}

func (gs *GossipSub) voluntaryExitHandler(ctx context.Context, msg *pubsub.Message) error {

	var exit ssz.Unmarshaler
	switch {
	case bytes.Equal(gs.forkVersion[:], gs.beaconConfig.GenesisForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.AltairForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.BellatrixForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.CapellaForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.DenebForkVersion),
		bytes.Equal(gs.forkVersion[:], gs.beaconConfig.ElectraForkVersion):
		exit = &ethtypes.VoluntaryExit{}
	default:
		return errors.New("unknown fork version for handling voluntary exit")
	}

	gs.messageProcessor.Process(ctx, msg, exit)

	slog.With("voluntary_exit", exit).
		Debug("handled voluntary exit from gossip")
	return nil
}
