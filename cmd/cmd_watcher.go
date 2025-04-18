package main

import (
	"context"
	"fmt"
	"github.com/a41-official/peekd/repository"
	"github.com/a41-official/peekd/repository/clickhouse"
	"github.com/a41-official/peekd/watcher"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/urfave/cli/v3"
	"log/slog"
)

const (
	CmdWatcher                   = "watcher"
	FlagEcdsaPrivateKeyHex       = "ecdsa-private-key-hex"
	FlagIp                       = "ip"
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
)

var cmdWatcher = &cli.Command{
	Name:   CmdWatcher,
	Usage:  "watching to ethereum p2p network",
	Action: launchWatcher,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    FlagEcdsaPrivateKeyHex,
			Usage:   "ECDSA Private Key Hex",
			Sources: cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "ECDSA_PRIVATE_KEY_HEX")),
		},
		&cli.StringFlag{
			Name:        FlagIp,
			Usage:       "IP for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "IP")),
			DefaultText: "127.0.0.1",
		},
		&cli.IntFlag{
			Name:        FlagUDPPort,
			Usage:       "UDP port for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "UDP_PORT")),
			DefaultText: "8080",
		},
		&cli.IntFlag{
			Name:        FlagTCPPort,
			Usage:       "TCP port for p2p networking",
			Sources:     cli.EnvVars(fmt.Sprintf("%s_%s", EnvPrefix, "TCP_PORT")),
			DefaultText: "8080",
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
	},
}

func launchWatcher(ctx context.Context, cmd *cli.Command) error {
	slog.Info("starting watcher")
	defer slog.Info("stopping watcher")

	opts := make([]watcher.WatcherOptionFunc, 0)
	if cmd.IsSet(FlagEcdsaPrivateKeyHex) {
		opts = append(opts, watcher.WithECDSAPrivateKeyHex(cmd.String(FlagEcdsaPrivateKeyHex)))
	}
	if cmd.IsSet(FlagIp) {
		opts = append(opts, watcher.WithIP(cmd.String(FlagIp)))
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

	// initialize repository
	repo, err := initializeRepository(ctx, cmd)
	if err != nil {
		return errors.Wrap(err, "failed to initialize repository")
	}
	defer repo.Close()

	opts = append(opts, watcher.WithRepository(repo))

	w, err := watcher.NewWatcher(opts...)
	if err != nil {
		return errors.Wrap(err, "failed to create watcher")
	}

	return w.Serve(ctx)
}

func initializeRepository(ctx context.Context, cmd *cli.Command) (repository.Repository, error) {
	dbType := cmd.String(FlagDbType)

	switch dbType {
	case "clickhouse":
		// Configure ClickHouse
		chConfig := clickhouse.Config{
			Host:     cmd.String(FlagDbHost),
			Port:     uint16(cmd.Int(FlagDbPort)),
			Database: cmd.String(FlagDbName),
			Username: cmd.String(FlagDbUser),
			Password: cmd.String(FlagDbPassword),
		}

		// Configure repository
		repoConfig := repository.Config{
			Type:       repository.RepositoryTypeClickHouse,
			ClickHouse: chConfig,
		}

		// Initialize repository
		repo, err := repository.NewRepository(repoConfig)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create repository")
		}
		slog.Info("repository initialized", "type", dbType)

		return repo, nil
	default:
		return nil, errors.Errorf("unsupported database type: %s", dbType)
	}
}
