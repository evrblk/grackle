package waitgroups

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestJobsTable_Create(t *testing.T) {
	t.Run("create wait group job", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroupJob := &corepb.WaitGroupJob{
			Id: &corepb.WaitGroupJobId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
				ProcessId:   "process_123",
			},
			CompletedAt: rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroupJob)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)

		// Verify job was created
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroupJob.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroupJob.Id.ProcessId, actual.Id.ProcessId)
		require.Equal(t, waitGroupJob.CompletedAt, actual.CompletedAt)
	})

	t.Run("create multiple jobs for same wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId := rand.Uint64()

		// Create multiple jobs
		numJobs := 5
		for i := range numJobs {
			waitGroupJob := &corepb.WaitGroupJob{
				Id: &corepb.WaitGroupJobId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId,
					ProcessId:   fmt.Sprintf("process_%d", i),
				},
				CompletedAt: rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroupJob)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Verify all jobs exist
		txn := badgerStore.View()
		defer txn.Discard()

		for i := range numJobs {
			jobId := &corepb.WaitGroupJobId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: waitGroupId,
				ProcessId:   fmt.Sprintf("process_%d", i),
			}

			actual, err := table.Get(txn, jobId)
			require.NoError(t, err)
			require.NotNil(t, actual)
			require.Equal(t, fmt.Sprintf("process_%d", i), actual.Id.ProcessId)
		}
	})
}

func TestJobsTable_Get(t *testing.T) {
	t.Run("get existing wait group job", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroupJob := &corepb.WaitGroupJob{
			Id: &corepb.WaitGroupJobId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
				ProcessId:   "process_123",
			},
			CompletedAt: rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroupJob)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get job
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroupJob.Id)
		require.NoError(t, err)
		require.NotNil(t, actual)
		require.Equal(t, waitGroupJob.Id.AccountId, actual.Id.AccountId)
		require.Equal(t, waitGroupJob.Id.NamespaceId, actual.Id.NamespaceId)
		require.Equal(t, waitGroupJob.Id.WaitGroupId, actual.Id.WaitGroupId)
		require.Equal(t, waitGroupJob.Id.ProcessId, actual.Id.ProcessId)
		require.Equal(t, waitGroupJob.CompletedAt, actual.CompletedAt)
	})

	t.Run("get nonexistent wait group job", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		jobId := &corepb.WaitGroupJobId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
			ProcessId:   "nonexistent_process",
		}

		actual, err := table.Get(txn, jobId)
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestJobsTable_Delete(t *testing.T) {
	t.Run("delete existing wait group job", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		waitGroupJob := &corepb.WaitGroupJob{
			Id: &corepb.WaitGroupJobId{
				AccountId:   rand.Uint64(),
				NamespaceId: rand.Uint32(),
				WaitGroupId: rand.Uint64(),
				ProcessId:   "process_123",
			},
			CompletedAt: rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, waitGroupJob)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete job
		txn = badgerStore.Update()
		err = table.Delete(txn, waitGroupJob.Id)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify deletion
		txn = badgerStore.View()
		defer txn.Discard()

		actual, err := table.Get(txn, waitGroupJob.Id)
		require.Error(t, err)
		require.Nil(t, actual)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("delete nonexistent wait group job does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		jobId := &corepb.WaitGroupJobId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
			ProcessId:   "nonexistent_process",
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, jobId)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)
	})

	t.Run("delete one job does not affect others", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId := rand.Uint64()

		// Create multiple jobs
		job1 := &corepb.WaitGroupJob{
			Id: &corepb.WaitGroupJobId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: waitGroupId,
				ProcessId:   "process_1",
			},
			CompletedAt: rand.Int64(),
		}

		job2 := &corepb.WaitGroupJob{
			Id: &corepb.WaitGroupJobId{
				AccountId:   accountId,
				NamespaceId: namespaceId,
				WaitGroupId: waitGroupId,
				ProcessId:   "process_2",
			},
			CompletedAt: rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, job1)
		require.NoError(t, err)
		err = table.Create(txn, job2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Delete job1
		txn = badgerStore.Update()
		err = table.Delete(txn, job1.Id)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify job1 is deleted
		txn = badgerStore.View()
		defer txn.Discard()

		actual1, err := table.Get(txn, job1.Id)
		require.Error(t, err)
		require.Nil(t, actual1)

		// Verify job2 still exists
		actual2, err := table.Get(txn, job2.Id)
		require.NoError(t, err)
		require.NotNil(t, actual2)
		require.Equal(t, "process_2", actual2.Id.ProcessId)
	})
}

func TestJobsTable_List(t *testing.T) {
	t.Run("list jobs for wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId := rand.Uint64()

		// Create multiple jobs
		numJobs := 5
		for i := range numJobs {
			waitGroupJob := &corepb.WaitGroupJob{
				Id: &corepb.WaitGroupJobId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId,
					ProcessId:   fmt.Sprintf("process_%d", i),
				},
				CompletedAt: rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroupJob)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List all jobs
		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, namespaceId, waitGroupId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.jobs, numJobs)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list jobs with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId := rand.Uint64()

		// Create multiple jobs
		numJobs := 10
		for i := range numJobs {
			waitGroupJob := &corepb.WaitGroupJob{
				Id: &corepb.WaitGroupJobId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId,
					ProcessId:   fmt.Sprintf("process_%03d", i),
				},
				CompletedAt: rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroupJob)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// List first page
		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, namespaceId, waitGroupId, nil, 3)
		require.NoError(t, err)
		require.NotNil(t, page1)
		require.Len(t, page1.jobs, 3)
		require.NotNil(t, page1.nextPaginationToken)
		require.Nil(t, page1.previousPaginationToken)

		// List second page
		page2, err := table.List(txn, accountId, namespaceId, waitGroupId, page1.nextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2)
		require.Len(t, page2.jobs, 3)
		require.NotNil(t, page2.nextPaginationToken)
		require.NotNil(t, page2.previousPaginationToken)

		// Verify different pages have different jobs
		require.NotEqual(t, page1.jobs[0].Id.ProcessId, page2.jobs[0].Id.ProcessId)
	})

	t.Run("list empty wait group", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, rand.Uint64(), rand.Uint32(), rand.Uint64(), nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.jobs, 0)
		require.Nil(t, result.nextPaginationToken)
		require.Nil(t, result.previousPaginationToken)
	})

	t.Run("list jobs from different wait groups are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		accountId := rand.Uint64()
		namespaceId := rand.Uint32()
		waitGroupId1 := rand.Uint64()
		waitGroupId2 := rand.Uint64()

		// Create jobs for wait group 1
		for i := range 3 {
			waitGroupJob := &corepb.WaitGroupJob{
				Id: &corepb.WaitGroupJobId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId1,
					ProcessId:   fmt.Sprintf("wg1_process_%d", i),
				},
				CompletedAt: rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroupJob)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		// Create jobs for wait group 2
		for i := range 5 {
			waitGroupJob := &corepb.WaitGroupJob{
				Id: &corepb.WaitGroupJobId{
					AccountId:   accountId,
					NamespaceId: namespaceId,
					WaitGroupId: waitGroupId2,
					ProcessId:   fmt.Sprintf("wg2_process_%d", i),
				},
				CompletedAt: rand.Int64(),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, waitGroupJob)
			require.NoError(t, err)
			err = txn.Commit()
			require.NoError(t, err)
		}

		txn := badgerStore.View()
		defer txn.Discard()

		// List wait group 1 jobs
		result1, err := table.List(txn, accountId, namespaceId, waitGroupId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.jobs, 3)

		// List wait group 2 jobs
		result2, err := table.List(txn, accountId, namespaceId, waitGroupId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.jobs, 5)
	})
}

func TestJobsTable_GetTableKeyRange(t *testing.T) {
	t.Run("get table key range", func(t *testing.T) {
		table := newJobsTable([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})

		keyRange := table.GetTableKeyRange()
		require.NotNil(t, keyRange)
	})
}
