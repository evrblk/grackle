package tables

import (
	"testing"

	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"
)

func TestRegisterGracklePrefixes(t *testing.T) {
	registry := monsterax.NewBaseTableRegistry(1)
	RegisterGracklePrefixes(registry)

	require.EqualValues(t, Grackle["Grackle.LocksCore.Locks.Table"].Bytes(), []byte{0x01})
}
