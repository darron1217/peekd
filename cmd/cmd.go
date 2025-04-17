package main

import (
	"context"
	"errors"
	"github.com/urfave/cli/v3"
	"log/slog"
	"net/mail"
	"os"
	"os/signal"
	"syscall"
)

const (
	EnvPrefix = "PEEKD"
)

var cmd = &cli.Command{
	Name:      "peekd",
	Usage:     "a ethereum p2p network data collector and analyzer for network diagnostics and protocol research",
	UsageText: "peekd [commands] [flags...]",
	Version:   "", // TODO: add version
	Authors: []any{
		&mail.Address{
			Name:    "Snow Park",
			Address: "sinabro2dev@gmail.com",
		},
		// TODO: add author our team member
	},
	Copyright:             "", // TODO: add copyright as license
	EnableShellCompletion: true,
	Before:                rootBefore,
	After:                 rootAfter,
	DefaultCommand:        CmdWatcher,
	Commands: []*cli.Command{
		cmdWatcher,
	},
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	go func() {
		defer cancel()
		defer signal.Stop(signals)

		select {
		case <-ctx.Done():
		case sig := <-signals:
			slog.With("signal", sig.String()).
				Info("received termination signal")
		}
	}()

	if err := cmd.Run(ctx, os.Args); err != nil && !errors.Is(err, context.Canceled) {
		slog.With("error", err).
			Error("terminated abnormally")
		os.Exit(1)
	}
}

func rootBefore(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	// TODO: need to custom
	return ctx, nil
}

func rootAfter(ctx context.Context, cmd *cli.Command) error {
	// TODO: need to custom
	return nil
}
