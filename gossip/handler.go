package gossip

import (
	"bytes"
	"context"
	"log/slog"
	"strings"

	"github.com/a41-official/peekd/eth"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type TopicHandler = func(context.Context, *pubsub.Message) error

func (gs *GossipSub) mappingTopicToHandler(network, topic string) TopicHandler {

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
		return gs.noopHandler
	}
}

func (gs *GossipSub) noopHandler(ctx context.Context, msg *pubsub.Message) error {
	return nil
}

func (gs *GossipSub) beaconBlockHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var block ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
		block = &ethtypes.SignedBeaconBlock{}
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
		block = &ethtypes.SignedBeaconBlockAltair{}
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
		block = &ethtypes.SignedBeaconBlockBellatrix{}
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
		block = &ethtypes.SignedBeaconBlockCapella{}
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
		block = &ethtypes.SignedBeaconBlockDeneb{}
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		block = &ethtypes.SignedBeaconBlockElectra{}
	default:
		return errors.New("unknown fork version for handling block")
	}

	if err := gs.enc.DecodeGossip(msg.Data, block); err != nil {
		return errors.Wrap(err, "failed to decode block from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("block", block).
		Debug("handled block from gossip")
	return nil
}

func (gs *GossipSub) blobSidecarHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var blob ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		blob = &ethtypes.BlobSidecar{}
	default:
		return errors.New("unknown fork version for handling blob")
	}

	if err := gs.enc.DecodeGossip(msg.Data, blob); err != nil {
		return errors.Wrap(err, "failed to decode blob from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("blob", blob).
		Debug("handled blob from gossip")
	return nil
}

func (gs *GossipSub) beaconAggregateAndProofHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var aggProof ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
		aggProof = &ethtypes.SignedAggregateAttestationAndProof{}
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		aggProof = &ethtypes.SignedAggregateAttestationAndProofElectra{}
	default:
		return errors.New("unknown fork version for handling aggregate proof")
	}

	if err := gs.enc.DecodeGossip(msg.Data, aggProof); err != nil {
		return errors.Wrap(err, "failed to decode aggregate proof from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("aggregate_proof", aggProof).
		Debug("handled aggregate proof from gossip")
	return nil
}

func (gs *GossipSub) beaconAttestationHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var att ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
		att = &ethtypes.Attestation{}
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		att = &ethtypes.SingleAttestation{}
	default:
		return errors.New("unknown fork version for handling attestation")
	}

	if err := gs.enc.DecodeGossip(msg.Data, att); err != nil {
		return errors.Wrap(err, "failed to decode attestation from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("attestation", att).
		Debug("handled aggregate proof from gossip")
	return nil
}

func (gs *GossipSub) syncCommitteeContributionAndProofHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var ctrProof ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		ctrProof = &ethtypes.SignedContributionAndProof{}
	default:
		return errors.New("unknown fork version for handling contribution proof")
	}

	if err := gs.enc.DecodeGossip(msg.Data, ctrProof); err != nil {
		return errors.Wrap(err, "failed to decode contribution proof from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("contribution_proof", ctrProof).
		Debug("handled contribution proof from gossip")
	return nil
}

func (gs *GossipSub) syncCommitteeHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var sync ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		sync = &ethtypes.SyncCommitteeMessage{}
	default:
		return errors.New("unknown fork version for handling sync committee")
	}

	if err := gs.enc.DecodeGossip(msg.Data, sync); err != nil {
		return errors.Wrap(err, "failed to decode sync committee from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("sync_committee", sync).
		Debug("handled sync committee from gossip")
	return nil
}

func (gs *GossipSub) proposerSlashingHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var pSlash ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		pSlash = &ethtypes.ProposerSlashing{}
	default:
		return errors.New("unknown fork version for handling proposer slashing")
	}

	if err := gs.enc.DecodeGossip(msg.Data, pSlash); err != nil {
		return errors.Wrap(err, "failed to decode proposer slashing from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("proposer_slashing", pSlash).
		Debug("handled proposer slashing from gossip")
	return nil
}

func (gs *GossipSub) attesterSlashingHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var aSlash ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
		aSlash = &ethtypes.AttesterSlashing{}
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		aSlash = &ethtypes.AttesterSlashingElectra{}
	default:
		return errors.New("unknown fork version for handling attester slashing")
	}

	if err := gs.enc.DecodeGossip(msg.Data, aSlash); err != nil {
		return errors.Wrap(err, "failed to decode attester slashing from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("attester_slashing", aSlash).
		Debug("handled attester slashing from gossip")
	return nil
}

func (gs *GossipSub) blsToExecutionChangeHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var blsExec ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		blsExec = &ethtypes.BLSToExecutionChange{}
	default:
		return errors.New("unknown fork version for handling bls execution change")
	}

	if err := gs.enc.DecodeGossip(msg.Data, blsExec); err != nil {
		return errors.Wrap(err, "failed to decode bls execution change from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("bls_execution_change", blsExec).
		Debug("handled bls execution change from gossip")
	return nil
}

func (gs *GossipSub) voluntaryExitHandler(ctx context.Context, msg *pubsub.Message) error {
	forkVersion := eth.GetCurrentForkVersion(gs.ethNetwork)
	beaconConfig := eth.GetBeaconChainConfig(gs.ethNetwork)

	var exit ssz.Unmarshaler
	switch {
	case bytes.Equal(forkVersion[:], beaconConfig.GenesisForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.AltairForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.BellatrixForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.CapellaForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.DenebForkVersion):
	case bytes.Equal(forkVersion[:], beaconConfig.ElectraForkVersion):
		exit = &ethtypes.VoluntaryExit{}
	default:
		return errors.New("unknown fork version for handling voluntary exit")
	}

	if err := gs.enc.DecodeGossip(msg.Data, exit); err != nil {
		return errors.Wrap(err, "failed to decode voluntary exit from gossip data")
	}

	gs.messageProcessor.Process(ctx, msg)

	slog.With("voluntary_exit", exit).
		Debug("handled voluntary exit from gossip")
	return nil
}
