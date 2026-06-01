package preview

import (
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	"github.com/evrblk/grackle/pkg/server/preview"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/tables"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func setupGrackleApiServer(t *testing.T) *preview.GrackleApiServer {
	dataStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	coresFactory := &monsteragen.GrackleNonclusteredApplicationCoresFactory{
		GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleWaitGroupsCoreApi {
			return waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleSemaphoresCoreApi {
			return semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleNamespacesCoreApi {
			return namespaces.NewCore(dataStore, lowerBound, upperBound)
		},
		GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleLocksCoreApi {
			return locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleBarriersCoreApi {
			return barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
	}
	grackleCoreApiClient := monsteragen.NewGrackleCoreApiNonclusteredStub(8, coresFactory, &sharding.GrackleShardKeyCalculator{})

	grackleApiGatewayServer := preview.NewGrackleApiServer(grackleCoreApiClient)

	return grackleApiGatewayServer
}
