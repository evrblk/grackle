package tables

import (
	"testing"

	"github.com/evrblk/yellowstone-common/honey"
	"github.com/stretchr/testify/require"
)

func TestRegisterGracklePrefixes(t *testing.T) {
	registry := honey.NewBaseTableRegistry(1)
	RegisterGracklePrefixes(registry)

	require.EqualValues(t, Grackle["Grackle.LocksCore.Locks.Table"].Bytes(), []byte{0x01})
}
