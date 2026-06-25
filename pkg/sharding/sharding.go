package sharding

import (
	"github.com/evrblk/monstera/utils"
)

func ByAccount(accountId uint64) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(accountId), 4)
}

func ByAccountAndNamespace(accountId uint64, namespaceId uint64) []byte {
	return utils.GetTruncatedHash(utils.ConcatBytes(accountId, namespaceId), 4)
}
