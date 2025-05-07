package reqresp

import (
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/types"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v6/consensus-types/wrapper"
	ethtypes "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/a41-official/peekd/eth"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/pkg/errors"
	ssz "github.com/prysmaticlabs/fastssz"
	"io"
	"log/slog"
	"time"
)

func mappingTopicToHandler(topic string) network.StreamHandler {
	switch topic {
	case p2p.RPCPingTopicV1:
		return InboundPingHandler()
	case p2p.RPCGoodByeTopicV1:
		return InboundGoodbyeHandler()
	case p2p.RPCMetaDataTopicV1, p2p.RPCMetaDataTopicV2:
		return InboundMetadataHandler()
	case p2p.RPCStatusTopicV1:
		return InboundStatusHandler()
	default:
		slog.With("topic", topic).
			Warn("Noop handler is set to unknown reqresp topic")
		return noopHandler()
	}
}

func noopHandler() network.StreamHandler {
	return func(_ network.Stream) {}
}

func InboundPingHandler() network.StreamHandler {
	return func(stream network.Stream) {
		defer stream.Close()

		req := primitives.SSZUint64(0)
		if err := readRequest(stream, &req); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to read ping request on stream")
			return
		}

		seqNum := primitives.SSZUint64(0)
		if err := writeResponse(stream, &seqNum); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to write ping response on stream")
			return
		}
	}
}

func InboundGoodbyeHandler() network.StreamHandler {
	return func(stream network.Stream) {
		defer stream.Close()

		req := primitives.SSZUint64(0)
		if err := readRequest(stream, &req); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to read goodbye request on stream")
			return
		}

		goodbyeCode, found := types.GoodbyeCodeMessages[req]
		if !found {
			slog.With("protocol", stream.Protocol()).
				Debug("failed to read goodbye request code on stream")
			return
		}

		slog.With("protocol", stream.Protocol()).
			With("goodbye_code", goodbyeCode).
			Debug("received goodbye request on stream")
	}
}

func InboundMetadataHandler() network.StreamHandler {
	metadata := wrapper.WrappedMetadataV1(
		&ethtypes.MetaDataV1{
			Attnets:  eth.GetAttestationAllSubnetBitvector(),
			Syncnets: eth.GetSyncCommitteeAllSubnetBitvector(),
		})

	return func(stream network.Stream) {
		defer stream.Close()

		if err := writeResponse(stream, metadata); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to write metadata response")
			return
		}
	}
}

func InboundStatusHandler() network.StreamHandler {
	return func(stream network.Stream) {
		defer stream.Close()

		status := &ethtypes.Status{}
		if err := readRequest(stream, status); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to read status request on stream")
			return
		}

		if err := writeResponse(stream, status); err != nil {
			slog.With("protocol", stream.Protocol()).
				With("error", err).
				Debug("failed to write status response on stream")
			return
		}
	}
}

func OutboundPingHandler(stream network.Stream) error {
	defer stream.Close()

	req := primitives.SSZUint64(0)
	if err := writeRequest(stream, &req); err != nil {
		return errors.Wrap(err, "failed to write ping request on stream")
	}

	resp := primitives.SSZUint64(0)
	if err := readResponse(stream, &resp); err != nil {
		return errors.Wrap(err, "failed to read ping response on stream")
	}

	return nil
}

func OutboundMetadataHandler(stream network.Stream) error {
	defer stream.Close()

	resp := &ethtypes.MetaDataV1{}
	if err := readResponse(stream, resp); err != nil {
		return errors.Wrap(err, "failed to read metadata response")
	}

	return nil
}

func readRequest(stream network.Stream, data ssz.Unmarshaler) error {
	if err := stream.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return errors.New("failed to set read deadline on stream")
	}

	if err := enc.DecodeWithMaxLength(stream, data); err != nil {
		return errors.Wrap(err, "failed to decode data on stream")
	}

	if err := stream.CloseRead(); err != nil {
		return errors.Wrap(err, "failed to close read request stream")
	}

	return nil
}

func readResponse(stream network.Stream, data ssz.Unmarshaler) error {
	if err := stream.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return errors.Wrap(err, "failed to set read deadline on stream")
	}

	respCode := make([]byte, 1)
	if _, err := io.ReadFull(stream, respCode); err != nil {
		return errors.Wrap(err, "failed to read response code on stream")
	}

	if int(respCode[0]) != 0 {
		errData, err := io.ReadAll(stream)
		if err != nil {
			return errors.Wrap(err, "failed to read response error data on stream")
		}

		err = errors.New(string(errData))
		return errors.Wrapf(err, "received error code %d response on stream", int(respCode[0]))
	}

	if err := enc.DecodeWithMaxLength(stream, data); err != nil {
		return errors.Wrap(err, "failed to decode data on stream")
	}

	if err := stream.CloseRead(); err != nil {
		return errors.Wrap(err, "failed to close read response stream")
	}

	return nil
}

func writeRequest(stream network.Stream, data ssz.Marshaler) error {
	if err := stream.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return errors.Wrap(err, "failed to set write deadline on stream")
	}

	if _, err := enc.EncodeWithMaxLength(stream, data); err != nil {
		return errors.Wrap(err, "failed to encode data on stream")
	}

	if err := stream.CloseWrite(); err != nil {
		return errors.Wrap(err, "failed to close write request stream")
	}

	return nil
}

func writeResponse(stream network.Stream, data ssz.Marshaler) error {
	if err := stream.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return errors.Wrap(err, "failed to set write deadline on stream")
	}

	successCode := []byte{0}
	if _, err := stream.Write(successCode); err != nil {
		return errors.Wrap(err, "failed to write success response code on stream")
	}

	if _, err := enc.EncodeWithMaxLength(stream, data); err != nil {
		return errors.Wrap(err, "failed to encode data on stream")
	}

	if err := stream.CloseWrite(); err != nil {
		return errors.Wrap(err, "failed to close write response stream")
	}

	return nil
}
