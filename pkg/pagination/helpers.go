package pagination

import (
	monsterax "github.com/evrblk/monstera/x"

	"github.com/evrblk/grackle/pkg/corepb"
)

func GetLimitWithDefaults(requestedLimit int) int {
	maxLimit := 250
	defaultLimit := 100

	if requestedLimit > 0 && requestedLimit < maxLimit {
		return requestedLimit
	} else if requestedLimit <= 0 {
		return defaultLimit
	}

	return maxLimit
}

func CoreToMonstera(paginationToken *corepb.PaginationToken) *monsterax.PaginationToken {
	if paginationToken == nil {
		return nil
	}

	return &monsterax.PaginationToken{
		Key:     paginationToken.Value,
		Reverse: paginationToken.Type == corepb.PaginationToken_PREVIOUS,
	}
}

func MonsteraToCore(monsteraPaginationToken *monsterax.PaginationToken) *corepb.PaginationToken {
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
