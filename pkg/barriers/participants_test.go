package barriers

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestParticipantsTable_Create(t *testing.T) {
	t.Run("create participant", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()
		participant := &corepb.BarrierParticipant{
			ProcessId:  "process_1",
			Generation: 1,
			ArrivedAt:  rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, accountId, namespaceId, barrierId, participant)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify participant was created
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, accountId, namespaceId, barrierId, participant.Generation, participant.ProcessId)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, participant.ProcessId, actual.ProcessId)
		require.Equal(t, participant.Generation, actual.Generation)
		require.Equal(t, participant.ArrivedAt, actual.ArrivedAt)
	})

	t.Run("create multiple participants for same barrier", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		numParticipants := 5
		for i := range numParticipants {
			participant := &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("process_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId, participant)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// Verify all participants exist
		txn := badgerStore.View()
		defer txn.Discard()

		for i := range numParticipants {
			actual, err := table.Get(txn, accountId, namespaceId, barrierId, 1, fmt.Sprintf("process_%d", i))
			require.NoError(t, err)
			require.NotNil(t, actual)
			require.Equal(t, fmt.Sprintf("process_%d", i), actual.ProcessId)
		}
	})

	t.Run("create participants in different generations", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// Same process_id can participate in multiple generations independently.
		for gen := uint64(1); gen <= 3; gen++ {
			participant := &corepb.BarrierParticipant{
				ProcessId:  "process_1",
				Generation: gen,
				ArrivedAt:  rand.Int64(),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId, participant)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// Each generation row is independently readable.
		txn := badgerStore.View()
		defer txn.Discard()
		for gen := uint64(1); gen <= 3; gen++ {
			actual, err := table.Get(txn, accountId, namespaceId, barrierId, gen, "process_1")
			require.NoError(t, err)
			require.Equal(t, gen, actual.Generation)
		}
	})

	t.Run("create overwrites existing participant with same key", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// First write.
		txn := badgerStore.Update()
		err = table.Create(txn, accountId, namespaceId, barrierId, &corepb.BarrierParticipant{
			ProcessId:  "process_1",
			Generation: 1,
			ArrivedAt:  1000,
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Second write with the same (generation, process_id) overwrites the row.
		txn = badgerStore.Update()
		err = table.Create(txn, accountId, namespaceId, barrierId, &corepb.BarrierParticipant{
			ProcessId:  "process_1",
			Generation: 1,
			ArrivedAt:  2000,
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// The latest write is what we read back.
		txn = badgerStore.View()
		defer txn.Discard()
		actual, err := table.Get(txn, accountId, namespaceId, barrierId, 1, "process_1")
		require.NoError(t, err)
		require.EqualValues(t, 2000, actual.ArrivedAt)
	})
}

func TestParticipantsTable_Get(t *testing.T) {
	t.Run("get existing participant", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()
		participant := &corepb.BarrierParticipant{
			ProcessId:  "process_1",
			Generation: 1,
			ArrivedAt:  rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, accountId, namespaceId, barrierId, participant)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		actual, err := table.Get(txn, accountId, namespaceId, barrierId, participant.Generation, participant.ProcessId)
		txn.Discard()

		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, participant.ProcessId, actual.ProcessId)
		require.Equal(t, participant.Generation, actual.Generation)
		require.Equal(t, participant.ArrivedAt, actual.ArrivedAt)
	})

	t.Run("get non-existent participant", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		_, err = table.Get(txn, rand.Uint64(), rand.Uint32(), rand.Uint64(), 1, "missing")
		txn.Discard()

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestParticipantsTable_Delete(t *testing.T) {
	t.Run("delete existing participant", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()
		participant := &corepb.BarrierParticipant{
			ProcessId:  "process_1",
			Generation: 1,
			ArrivedAt:  rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, accountId, namespaceId, barrierId, participant)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete the participant.
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId, barrierId, participant.Generation, participant.ProcessId)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Subsequent Get returns ErrNotFound.
		txn = badgerStore.View()
		_, err = table.Get(txn, accountId, namespaceId, barrierId, participant.Generation, participant.ProcessId)
		txn.Discard()
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("delete non-existent participant", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		// Delete on a never-created row is idempotent.
		txn := badgerStore.Update()
		err = table.Delete(txn, rand.Uint64(), rand.Uint32(), rand.Uint64(), 1, "missing")
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	})

	t.Run("delete only the targeted row", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// Three participants under the same barrier.
		txn := badgerStore.Update()
		for i := range 3 {
			err = table.Create(txn, accountId, namespaceId, barrierId, &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("process_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			})
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Delete only process_1.
		txn = badgerStore.Update()
		err = table.Delete(txn, accountId, namespaceId, barrierId, 1, "process_1")
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// process_0 and process_2 are still there; process_1 is gone.
		txn = badgerStore.View()
		defer txn.Discard()
		_, err = table.Get(txn, accountId, namespaceId, barrierId, 1, "process_0")
		require.NoError(t, err)
		_, err = table.Get(txn, accountId, namespaceId, barrierId, 1, "process_1")
		require.Error(t, err)
		_, err = table.Get(txn, accountId, namespaceId, barrierId, 1, "process_2")
		require.NoError(t, err)
	})
}

func TestParticipantsTable_List(t *testing.T) {
	t.Run("list participants for barrier", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		numParticipants := 5
		for i := range numParticipants {
			participant := &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("process_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId, participant)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, namespaceId, barrierId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.participants, numParticipants)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list participants with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		numParticipants := 10
		for i := range numParticipants {
			participant := &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("process_%03d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId, participant)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, namespaceId, barrierId, nil, 3)
		require.NoError(t, err)
		require.NotNil(t, page1)
		require.Len(t, page1.participants, 3)
		require.NotNil(t, page1.nextPaginationToken)
		require.Nil(t, page1.previousPaginationToken)

		page2, err := table.List(txn, accountId, namespaceId, barrierId, page1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2)
		require.Len(t, page2.participants, 3)
		require.NotNil(t, page2.nextPaginationToken)
		require.NotNil(t, page2.previousPaginationToken)

		// Pages contain disjoint participants.
		require.NotEqual(t, page1.participants[0].ProcessId, page2.participants[0].ProcessId)
	})

	t.Run("list empty barrier", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, rand.Uint64(), rand.Uint32(), rand.Uint64(), nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result.participants)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list participants from different barriers are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId1 := rand.Uint64()
		barrierId2 := rand.Uint64()

		// 3 participants on barrier 1.
		for i := range 3 {
			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId1, &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("b1_p_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		// 5 participants on barrier 2.
		for i := range 5 {
			txn := badgerStore.Update()
			err := table.Create(txn, accountId, namespaceId, barrierId2, &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("b2_p_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result1, err := table.List(txn, accountId, namespaceId, barrierId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.participants, 3)

		result2, err := table.List(txn, accountId, namespaceId, barrierId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.participants, 5)
	})

	t.Run("list returns participants across generations", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		barrierId := rand.Uint64()

		// 2 participants in gen 1, 3 in gen 2.
		txn := badgerStore.Update()
		for i := range 2 {
			err = table.Create(txn, accountId, namespaceId, barrierId, &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("g1_p_%d", i),
				Generation: 1,
				ArrivedAt:  rand.Int64(),
			})
			require.NoError(t, err)
		}
		for i := range 3 {
			err = table.Create(txn, accountId, namespaceId, barrierId, &corepb.BarrierParticipant{
				ProcessId:  fmt.Sprintf("g2_p_%d", i),
				Generation: 2,
				ArrivedAt:  rand.Int64(),
			})
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// List is scoped by barrier, not by generation — it returns rows for all generations.
		txn = badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, namespaceId, barrierId, nil, 100)
		require.NoError(t, err)
		require.Len(t, result.participants, 5)

		gens := make(map[uint64]int)
		for _, p := range result.participants {
			gens[p.Generation]++
		}
		require.Equal(t, 2, gens[1])
		require.Equal(t, 3, gens[2])
	})
}

func TestParticipantsTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		table := newParticipantsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}
