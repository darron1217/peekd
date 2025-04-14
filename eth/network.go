package eth

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"time"
)

type GenesisConfig struct {
	GenesisTime          time.Time
	GenesisValidatorRoot []byte
}

var GenesisConfigs = map[string]*GenesisConfig{
	params.MainnetName: {
		GenesisTime:          time.Unix(1606824023, 0),
		GenesisValidatorRoot: hexutil.MustDecode("0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"),
	},
	params.HoodiName: {
		GenesisTime:          time.Unix(1742213400, 0),
		GenesisValidatorRoot: hexutil.MustDecode("0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f"),
	},
}
