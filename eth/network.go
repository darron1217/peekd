package eth

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
	"time"
)

type GenesisConfig struct {
	GenesisTime          time.Time
	GenesisValidatorRoot []byte
}

func GetGenesisConfig(network string) *GenesisConfig {
	switch network {
	case params.MainnetName:
		return &GenesisConfig{
			GenesisTime:          time.Unix(1606824023, 0),
			GenesisValidatorRoot: hexutil.MustDecode("0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"),
		}
	case params.HoodiName:
		return &GenesisConfig{
			GenesisTime:          time.Unix(1742213400, 0),
			GenesisValidatorRoot: hexutil.MustDecode("0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f"),
		}
	default:
		return &GenesisConfig{
			GenesisTime:          time.Unix(1606824023, 0),
			GenesisValidatorRoot: hexutil.MustDecode("0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"),
		}
	}
}

func GetBeaconNetworkConfig(network string) *params.NetworkConfig {
	switch network {
	case params.MainnetName:
		return params.BeaconNetworkConfig()
	case params.HoodiName:
		params.UseHoodiNetworkConfig()
		return params.BeaconNetworkConfig()
	default:
		return params.BeaconNetworkConfig()
	}
}

func GetBeaconChainConfig(network string) *params.BeaconChainConfig {
	switch network {
	case params.MainnetName:
		return params.MainnetConfig()
	case params.HoodiName:
		return params.HoodiConfig()
	default:
		return params.MainnetConfig()
	}
}

func GetForkVersion(network string, epoch primitives.Epoch) [4]byte {
	beaconConfig := GetBeaconChainConfig(network)

	switch {
	case epoch < beaconConfig.AltairForkEpoch:
		return [4]byte(beaconConfig.GenesisForkVersion)
	case epoch < beaconConfig.BellatrixForkEpoch:
		return [4]byte(beaconConfig.AltairForkVersion)
	case epoch < beaconConfig.CapellaForkEpoch:
		return [4]byte(beaconConfig.BellatrixForkVersion)
	case epoch < beaconConfig.DenebForkEpoch:
		return [4]byte(beaconConfig.CapellaForkVersion)
	case epoch < beaconConfig.ElectraForkEpoch:
		return [4]byte(beaconConfig.DenebForkVersion)
	default:
		return [4]byte(beaconConfig.DenebForkVersion)
	}
}

func GetCurrentForkVersion(network string) [4]byte {
	genesisTime := GetGenesisConfig(network).GenesisTime
	curEpoch := slots.ToEpoch(slots.Since(genesisTime))
	return GetForkVersion(network, curEpoch)
}

func HasSubnets(network string, rawTopic string) (uint64, bool) {
	beaconConfig := GetBeaconChainConfig(network)

	switch rawTopic {
	case p2p.BlobSubnetTopicFormat:
		return beaconConfig.BlobsidecarSubnetCount, true
	case p2p.AttestationSubnetTopicFormat:
		return beaconConfig.AttestationSubnetCount, true
	case p2p.SyncCommitteeSubnetTopicFormat:
		return beaconConfig.SyncCommitteeSubnetCount, true
	default:
		return uint64(0), false
	}
}
