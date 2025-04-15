package gossip

import (
	"context"
	"github.com/a41-official/peekd/eth"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"log/slog"
	"strings"
)

type TopicHandler = func(context.Context, *pubsub.Message) error

func mappingTopicToHandler(network, topic string) TopicHandler {
	enc := encoder.SszNetworkEncoder{}

	switch {
	case strings.Contains(topic, p2p.GossipBlockMessage):
		return beaconBlockHandler(network, enc)
	case strings.Contains(topic, p2p.GossipBlobSidecarMessage):
		return blobSidecarHandler(network, enc)
	case strings.Contains(topic, p2p.GossipAggregateAndProofMessage):
		return beaconAggregateAndProofHandler(network, enc)
	case strings.Contains(topic, p2p.GossipAttestationMessage):
		return beaconAttestationHandler(network, enc)
	case strings.Contains(topic, p2p.GossipContributionAndProofMessage):
		return syncCommitteeContributionAndProofHandler(network, enc)
	case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
		return syncCommitteeHandler(network, enc)
	case strings.Contains(topic, p2p.GossipProposerSlashingMessage):
		return proposerSlashingHandler(network, enc)
	case strings.Contains(topic, p2p.GossipAttesterSlashingMessage):
		return attesterSlashingHandler(network, enc)
	case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
		return blsToExecutionChangeHandler(network, enc)
	case strings.Contains(topic, p2p.GossipExitMessage):
		return voluntaryExitHandler(network, enc)
	default:
		return noopHandler()
	}
}

func noopHandler() TopicHandler {
	return func(_ context.Context, _ *pubsub.Message) error {
		return nil
	}
}

func beaconBlockHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var block ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
			block = &ethtypes.SignedBeaconBlock{}
		case beaconConfig.AltairForkVersion:
			block = &ethtypes.SignedBeaconBlockAltair{}
		case beaconConfig.BellatrixForkVersion:
			block = &ethtypes.SignedBeaconBlockBellatrix{}
		case beaconConfig.CapellaForkVersion:
			block = &ethtypes.SignedBeaconBlockCapella{}
		case beaconConfig.DenebForkVersion:
			block = &ethtypes.SignedBeaconBlockDeneb{}
		case beaconConfig.ElectraForkVersion:
			block = &ethtypes.SignedBeaconBlockElectra{}
		default:
			return errors.New("unknown fork version for handling block")
		}

		if err := enc.DecodeGossip(msg.Data, block); err != nil {
			return errors.Wrap(err, "failed to decode block from gossip data")
		}

		// TODO: need to custom

		slog.With("block", block).
			Debug("handled block from gossip")
		return nil
	}
}

func blobSidecarHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var blob ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			blob = &ethtypes.BlobSidecar{}
		default:
			return errors.New("unknown fork version for handling blob")
		}

		if err := enc.DecodeGossip(msg.Data, blob); err != nil {
			return errors.Wrap(err, "failed to decode blob from gossip data")
		}

		// TODO: need to custom

		slog.With("blob", blob).
			Debug("handled blob from gossip")
		return nil
	}
}

func beaconAggregateAndProofHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var aggProof ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
			aggProof = &ethtypes.SignedAggregateAttestationAndProof{}
		case beaconConfig.ElectraForkVersion:
			aggProof = &ethtypes.SignedAggregateAttestationAndProofElectra{}
		default:
			return errors.New("unknown fork version for handling aggregate proof")
		}

		if err := enc.DecodeGossip(msg.Data, aggProof); err != nil {
			return errors.Wrap(err, "failed to decode aggregate proof from gossip data")
		}

		// TODO: need to custom

		slog.With("aggregate_proof", aggProof).
			Debug("handled aggregate proof from gossip")
		return nil
	}
}

func beaconAttestationHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var att ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
			att = &ethtypes.Attestation{}
		case beaconConfig.ElectraForkVersion:
			att = &ethtypes.SingleAttestation{}
		default:
			return errors.New("unknown fork version for handling attestation")
		}

		if err := enc.DecodeGossip(msg.Data, att); err != nil {
			return errors.Wrap(err, "failed to decode attestation from gossip data")
		}

		// TODO: need to custom

		slog.With("attestation", att).
			Debug("handled aggregate proof from gossip")
		return nil
	}
}

func syncCommitteeContributionAndProofHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var ctrProof ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			ctrProof = &ethtypes.SignedContributionAndProof{}
		default:
			return errors.New("unknown fork version for handling contribution proof")
		}

		if err := enc.DecodeGossip(msg.Data, ctrProof); err != nil {
			return errors.Wrap(err, "failed to decode contribution proof from gossip data")
		}

		// TODO: need to custom

		slog.With("contribution_proof", ctrProof).
			Debug("handled contribution proof from gossip")
		return nil
	}
}

func syncCommitteeHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var sync ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			sync = &ethtypes.SyncCommitteeMessage{}
		default:
			return errors.New("unknown fork version for handling sync committee")
		}

		if err := enc.DecodeGossip(msg.Data, sync); err != nil {
			return errors.Wrap(err, "failed to decode sync committee from gossip data")
		}

		// TODO: need to custom

		slog.With("sync_committee", sync).
			Debug("handled sync committee from gossip")
		return nil
	}
}

func proposerSlashingHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var pSlash ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			pSlash = &ethtypes.ProposerSlashing{}
		default:
			return errors.New("unknown fork version for handling proposer slashing")
		}

		if err := enc.DecodeGossip(msg.Data, pSlash); err != nil {
			return errors.Wrap(err, "failed to decode proposer slashing from gossip data")
		}

		// TODO: need to custom

		slog.With("proposer_slashing", pSlash).
			Debug("handled proposer slashing from gossip")
		return nil
	}
}

func attesterSlashingHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var aSlash ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
			aSlash = &ethtypes.AttesterSlashing{}
		case beaconConfig.ElectraForkVersion:
			aSlash = &ethtypes.AttesterSlashingElectra{}
		default:
			return errors.New("unknown fork version for handling attester slashing")
		}

		if err := enc.DecodeGossip(msg.Data, aSlash); err != nil {
			return errors.Wrap(err, "failed to decode attester slashing from gossip data")
		}

		// TODO: need to custom

		slog.With("attester_slashing", aSlash).
			Debug("handled attester slashing from gossip")
		return nil
	}
}

func blsToExecutionChangeHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var blsExec ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			blsExec = &ethtypes.BLSToExecutionChange{}
		default:
			return errors.New("unknown fork version for handling bls execution change")
		}

		if err := enc.DecodeGossip(msg.Data, blsExec); err != nil {
			return errors.Wrap(err, "failed to decode bls execution change from gossip data")
		}

		// TODO: need to custom

		slog.With("bls_execution_change", blsExec).
			Debug("handled bls execution change from gossip")
		return nil
	}
}

func voluntaryExitHandler(network string, enc encoder.NetworkEncoding) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		forkVersion := eth.GetCurrentForkVersion(network)
		beaconConfig := eth.GetBeaconChainConfig(network)

		var exit ssz.Unmarshaler
		switch forkVersion[:] {
		case beaconConfig.GenesisForkVersion:
		case beaconConfig.AltairForkVersion:
		case beaconConfig.BellatrixForkVersion:
		case beaconConfig.CapellaForkVersion:
		case beaconConfig.DenebForkVersion:
		case beaconConfig.ElectraForkVersion:
			exit = &ethtypes.VoluntaryExit{}
		default:
			return errors.New("unknown fork version for handling voluntary exit")
		}

		if err := enc.DecodeGossip(msg.Data, exit); err != nil {
			return errors.Wrap(err, "failed to decode voluntary exit from gossip data")
		}

		// TODO: need to custom

		slog.With("voluntary_exit", exit).
			Debug("handled voluntary exit from gossip")
		return nil
	}
}
