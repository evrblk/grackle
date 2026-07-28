package sharding

import (
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/utils"
)

func ByAccount(accountId uint64) cluster.ShardKey {
	return utils.GetShardKey(utils.ConcatBytes(accountId))
}

func ByAccountAndNamespace(accountId uint64, namespaceId uint64) cluster.ShardKey {
	return utils.GetShardKey(utils.ConcatBytes(accountId, namespaceId))
}
