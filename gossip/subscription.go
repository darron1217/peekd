package gossip

import (
	"context"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"log/slog"
)

type Subscription struct {
	hostID       peer.ID
	subscription *pubsub.Subscription
	handler      TopicHandler
}

func NewSubscription(ethNetwork string, hostID peer.ID, subscription *pubsub.Subscription) *Subscription {
	return &Subscription{
		hostID:       hostID,
		subscription: subscription,
		handler:      mappingTopicToHandler(ethNetwork, subscription.Topic()),
	}
}

func (s *Subscription) Serve(ctx context.Context) error {
	slog.Info("starting gossip subscription")
	defer slog.Info("stopping gossip subscription")

	defer s.subscription.Cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := s.subscription.Next(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get next gossip subscription message")
		}

		if msg.ReceivedFrom == s.hostID {
			continue
		}

		if err = s.handler(ctx, msg); err != nil {
			slog.With("peer_id", msg.ReceivedFrom).
				With("topic", msg.GetTopic()).
				Error("failed to handle gossipSub message")
			continue
		}
	}
}
