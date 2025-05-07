package eth

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
	"log/slog"
	"time"
)

var (
	network string
)

func GetNetwork() string {
	switch network {
	case params.MainnetName, params.HoodiName:
		return network
	default:
		panic(errors.New("failed to get ethereum network"))
	}
}

func SetNetwork(net string) error {
	switch net {
	case params.MainnetName, params.HoodiName:
		network = net
	default:
		return errors.New("network must be set only for supported ethereum")
	}

	slog.With("network", network).
		Info("successfully set ethereum network")

	return nil
}

type GenesisConfig struct {
	GenesisTime          time.Time
	GenesisValidatorRoot []byte
}

func GetGenesisConfig() *GenesisConfig {
	switch GetNetwork() {
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
		panic(errors.New("failed to get ethereum network"))
	}
}

func GetBeaconNetworkConfig() *params.NetworkConfig {
	switch GetNetwork() {
	case params.MainnetName:
		return params.BeaconNetworkConfig()
	case params.HoodiName:
		params.UseHoodiNetworkConfig()
		return params.BeaconNetworkConfig()
	default:
		panic(errors.New("failed to get ethereum network"))
	}
}

func GetBeaconChainConfig() *params.BeaconChainConfig {
	switch GetNetwork() {
	case params.MainnetName:
		return params.MainnetConfig()
	case params.HoodiName:
		return params.HoodiConfig()
	default:
		panic(errors.New("failed to get ethereum network"))
	}

}

func GetForkVersion(epoch primitives.Epoch) [4]byte {
	beaconConfig := GetBeaconChainConfig()

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
	case epoch < beaconConfig.FuluForkEpoch:
		return [4]byte(beaconConfig.ElectraForkVersion)
	default:
		return [4]byte(beaconConfig.ElectraForkVersion)
	}
}

func GetCurrentForkVersion() [4]byte {
	genesisTime := GetGenesisConfig().GenesisTime
	curEpoch := slots.ToEpoch(slots.Since(genesisTime))
	return GetForkVersion(curEpoch)
}

func HasSubnets(rawTopic string) (uint64, bool) {
	beaconConfig := GetBeaconChainConfig()

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

func GetSlotDuration() time.Duration {
	return 1 * time.Second * time.Duration(GetBeaconChainConfig().SecondsPerSlot)
}

func GetEpochDuration() time.Duration {
	return GetSlotDuration() * time.Duration(GetBeaconChainConfig().SlotsPerEpoch)
}

func GetAttestationAllSubnetBitvector() bitfield.Bitvector64 {
	attestBitV := bitfield.NewBitvector64()
	for i := uint64(0); i < GetBeaconChainConfig().AttestationSubnetCount; i++ {
		attestBitV.SetBitAt(i, true)
	}
	return attestBitV
}

func GetSyncCommitteeAllSubnetBitvector() bitfield.Bitvector4 {
	syncBitV := bitfield.Bitvector4{byte(0x00)}
	for i := uint64(0); i < GetBeaconChainConfig().SyncCommitteeSubnetCount; i++ {
		syncBitV.SetBitAt(i, true)
	}
	return syncBitV
}
