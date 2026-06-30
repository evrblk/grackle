package pagination

import (
	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/yellowstone-common/honey"
)

const (
	DefaultPaginationLimit = 100
	MaxPaginationLimit     = 250
)

func GetLimitWithDefaults(requestedLimit int) int {
	if requestedLimit > 0 && requestedLimit < MaxPaginationLimit {
		return requestedLimit
	} else if requestedLimit <= 0 {
		return DefaultPaginationLimit
	}

	return MaxPaginationLimit
}

func CoreToMonstera(paginationToken *corepb.PaginationToken) *honey.PaginationToken {
	if paginationToken == nil {
		return nil
	}

	return &honey.PaginationToken{
		Key:     paginationToken.Value,
		Reverse: paginationToken.Type == corepb.PaginationToken_PREVIOUS,
	}
}

func MonsteraToCore(monsteraPaginationToken *honey.PaginationToken) *corepb.PaginationToken {
	if monsteraPaginationToken == nil {
		return nil
	}

	result := &corepb.PaginationToken{
		Value: monsteraPaginationToken.Key,
	}

	if monsteraPaginationToken.Reverse {
		result.Type = corepb.PaginationToken_PREVIOUS
	} else {
		result.Type = corepb.PaginationToken_NEXT
	}

	return result
}
