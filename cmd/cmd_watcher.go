package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/a41-official/peekd/watcher"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/urfave/cli/v3"
)

const (
	CmdWatcher = "watcher"

	FlagLogLevel                 = "log-level"
	FlagEcdsaPrivateKeyHex       = "ecdsa-private-key-hex"
	FlagListenIp                 = "listen-ip"
	FlagUDPPort                  = "udp-port"
	FlagTCPPort                  = "tcp-port"
	FlagNetwork                  = "network"
	FlagEstimateActiveValidators = "estimate-active-validators"

	// DB related flags
	FlagDbType     = "db-type"
	FlagDbHost     = "db-host"
	FlagDbPort     = "db-port"
	FlagDbName     = "db-name"
	FlagDbUser     = "db-user"
	FlagDbPassword = "db-password"

	// Node related flags
	FlagNodeAlias  = "node-alias"
	FlagNodeRegion = "node-region"
)

var cmdWatcher = &cli.Command{
	Name:   CmdWatcher,
	Usage:  "watching to ethereum p2p network",
	Action: launchWatcher,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    FlagLogLevel,
			Usage:   "Log level",
			Sources: cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "LOG_LEVEL")),
		},
		&cli.StringFlag{
			Name:    FlagEcdsaPrivateKeyHex,
			Usage:   "ECDSA Private Key Hex",
			Sources: cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "ECDSA_PRIVATE_KEY_HEX")),
		},
		&cli.StringFlag{
			Name:        FlagListenIp,
			Usage:       "Listen IP for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "LISTEN_IP")),
			DefaultText: "127.0.0.1",
		},
		&cli.IntFlag{
			Name:        FlagUDPPort,
			Usage:       "UDP port for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "UDP_PORT")),
			DefaultText: "9090",
		},
		&cli.IntFlag{
			Name:        FlagTCPPort,
			Usage:       "TCP port for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "TCP_PORT")),
			DefaultText: "9090",
		},
		&cli.StringFlag{
			Name:        FlagNetwork,
			Usage:       "ethereum network",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "NETWORK")),
			DefaultText: params.MainnetName,
		},
		&cli.UintFlag{
			Name:        FlagEstimateActiveValidators,
			Usage:       "estimate active validators in ethereum network",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "ESTIMATE_ACTIVE_VALIDATORS")),
			DefaultText: "0",
		},
		// DB related flags
		&cli.StringFlag{
			Name:        FlagDbType,
			Usage:       "Database type (clickhouse)",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_TYPE")),
			DefaultText: "clickhouse",
		},
		&cli.StringFlag{
			Name:        FlagDbHost,
			Usage:       "Database host",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_HOST")),
			DefaultText: "localhost",
		},
		&cli.IntFlag{
			Name:        FlagDbPort,
			Usage:       "Database port",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_PORT")),
			DefaultText: "9000",
		},
		&cli.StringFlag{
			Name:        FlagDbName,
			Usage:       "Database name",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_NAME")),
			DefaultText: "peekd",
		},
		&cli.StringFlag{
			Name:        FlagDbUser,
			Usage:       "Database user",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_USER")),
			DefaultText: "default",
		},
		&cli.StringFlag{
			Name:        FlagDbPassword,
			Usage:       "Database password",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "DB_PASSWORD")),
			DefaultText: "",
		},
		&cli.StringFlag{
			Name:        FlagNodeAlias,
			Usage:       "Node Alias",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "NODE_ALIAS")),
			DefaultText: "",
		},
		&cli.StringFlag{
			Name:        FlagNodeRegion,
			Usage:       "Node region",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "NODE_REGION")),
			DefaultText: "",
		},
	},
}

func launchWatcher(ctx context.Context, cmd *cli.Command) error {
	opts := make([]watcher.WatcherOptionFunc, 0)
	if cmd.IsSet(FlagLogLevel) {
		logLvl := new(slog.LevelVar)
		switch strings.ToLower(cmd.String(FlagLogLevel)) {
		case "debug":
			logLvl.Set(slog.LevelDebug)
		case "info":
			logLvl.Set(slog.LevelInfo)
		case "warn":
			logLvl.Set(slog.LevelWarn)
		case "error":
			logLvl.Set(slog.LevelError)
		default:
			logLvl.Set(slog.LevelInfo)
		}

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLvl,
		}))
		slog.SetDefault(logger)
	}
	if cmd.IsSet(FlagEcdsaPrivateKeyHex) {
		opts = append(opts, watcher.WithECDSAPrivateKeyHex(cmd.String(FlagEcdsaPrivateKeyHex)))
	}
	if cmd.IsSet(FlagListenIp) {
		opts = append(opts, watcher.WithListenIP(cmd.String(FlagListenIp)))
	}
	if cmd.IsSet(FlagUDPPort) {
		opts = append(opts, watcher.WithPortUDP(int(cmd.Int(FlagUDPPort))))
	}
	if cmd.IsSet(FlagTCPPort) {
		opts = append(opts, watcher.WithPortTCP(int(cmd.Int(FlagTCPPort))))
	}
	if cmd.IsSet(FlagNetwork) {
		opts = append(opts, watcher.WithEthNetwork(cmd.String(FlagNetwork)))
	}
	if cmd.IsSet(FlagEstimateActiveValidators) {
		opts = append(opts, watcher.WithEstimateActiveValidators(cmd.Uint(FlagEstimateActiveValidators)))
	}
	if cmd.IsSet(FlagDbType) {
		opts = append(opts, watcher.WithDBType(cmd.String(FlagDbType)))
	}
	if cmd.IsSet(FlagDbHost) {
		opts = append(opts, watcher.WithDBHost(cmd.String(FlagDbHost)))
	}
	if cmd.IsSet(FlagDbPort) {
		opts = append(opts, watcher.WithDBPort(int(cmd.Int(FlagDbPort))))
	}
	if cmd.IsSet(FlagDbName) {
		opts = append(opts, watcher.WithDBName(cmd.String(FlagDbName)))
	}
	if cmd.IsSet(FlagDbUser) {
		opts = append(opts, watcher.WithDBUser(cmd.String(FlagDbUser)))
	}
	if cmd.IsSet(FlagDbPassword) {
		opts = append(opts, watcher.WithDBPassword(cmd.String(FlagDbPassword)))
	}
	if cmd.IsSet(FlagNodeAlias) {
		opts = append(opts, watcher.WithNodeAlias(cmd.String(FlagNodeAlias)))
	}
	if cmd.IsSet(FlagNodeRegion) {
		opts = append(opts, watcher.WithNodeRegion(cmd.String(FlagNodeRegion)))
	}

	slog.Info("starting watcher")
	defer slog.Info("stopping watcher")

	w, err := watcher.NewWatcher(opts...)
	if err != nil {
		return errors.Wrap(err, "failed to create watcher")
	}

	return w.Serve(ctx)
}
