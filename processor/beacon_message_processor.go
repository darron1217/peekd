package processor

import (
	"context"
	"log/slog"

	"github.com/a41-official/peekd/repository"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type BeaconMessageProcessor struct {
	repo repository.Repository
}

func NewBeaconMessageProcessor(repo repository.Repository) *BeaconMessageProcessor {
	return &BeaconMessageProcessor{repo: repo}
}

func (p *BeaconMessageProcessor) Process(ctx context.Context, msg *pubsub.Message) error {

	slog.Info("msg received", "topic", msg.Topic, "data", msg.Data)

	return nil
}
