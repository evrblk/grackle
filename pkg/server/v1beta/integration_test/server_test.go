package integration_test

import (
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	"github.com/evrblk/grackle/pkg/server/v1beta"
	"github.com/evrblk/grackle/pkg/tables"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

func init() {
	registry := monsterax.NewBaseTableRegistry(1)
	tables.RegisterGracklePrefixes(registry)
}

func setupGrackleApiServer(t *testing.T) *v1beta.GrackleApiServer {
	dataStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	coresFactory := &coreapis.GrackleNonclusteredApplicationCoresFactory{
		GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleWaitGroupsCoreApi {
			return waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleSemaphoresCoreApi {
			return semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleNamespacesCoreApi {
			return namespaces.NewCore(dataStore, lowerBound, upperBound)
		},
		GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleLocksCoreApi {
			return locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
		GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleBarriersCoreApi {
			return barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
		},
	}
	grackleCoreApiClient := coreapis.NewGrackleNonclusteredStub(8, coresFactory)

	grackleApiGatewayServer := v1beta.NewGrackleApiServer(grackleCoreApiClient)

	return grackleApiGatewayServer
}
