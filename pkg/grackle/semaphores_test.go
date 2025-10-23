package grackle

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/monstera"
)

func TestAcquireSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore
	createResponse, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})

	require.NoError(t, err)
	require.NotNil(t, createResponse.Semaphore)
	require.Equal(t, "test description", createResponse.Semaphore.Description)
	require.EqualValues(t, 2, createResponse.Semaphore.Permits)

	// T+1m: Acquire semaphore
	response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, response1.Semaphore)
	require.True(t, response1.Success)
	require.Len(t, response1.Semaphore.SemaphoreHolders, 1)
	require.Equal(t, "process_1", response1.Semaphore.SemaphoreHolders[0].ProcessId)

	// T+2m: Get semaphore
	getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Semaphore)
	require.Len(t, getResponse.Semaphore.SemaphoreHolders, 1)

	// T+62m: Get semaphore after expiration
	getResponse2, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(62 * time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse2.Semaphore)
	require.Len(t, getResponse2.Semaphore.SemaphoreHolders, 0)
}

func TestAcquireSemaphoreRepeatedly(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Acquire semaphore
	response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.True(t, response1.Success)

	// T+2m: Acquire same semaphore with same process (should extend expiration)
	response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.True(t, response2.Success)
	require.Len(t, response2.Semaphore.SemaphoreHolders, 1)
	require.EqualValues(t, now.Add(2*time.Minute).Add(time.Hour).UnixNano(), response2.Semaphore.SemaphoreHolders[0].ExpiresAt)
}

func TestAcquireSemaphoreWithMultiplePermits(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore with 2 permits
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: First process acquires semaphore
	response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.True(t, response1.Success)

	// T+2m: Second process acquires semaphore
	response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
		ProcessId:   "process_2",
		ExpiresAt:   now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.True(t, response2.Success)
	require.Len(t, response2.Semaphore.SemaphoreHolders, 2)

	// T+3m: Third process tries to acquire semaphore (should fail)
	response3, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(3 * time.Minute).UnixNano(),
		ProcessId:   "process_3",
		ExpiresAt:   now.Add(3 * time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.False(t, response3.Success)
	require.Len(t, response3.Semaphore.SemaphoreHolders, 2)
}

func TestReleaseSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Acquire semaphore
	response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.True(t, response1.Success)

	// T+2m: Release semaphore
	response2, err := semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
		ProcessId:   "process_1",
	})

	require.NoError(t, err)
	require.NotNil(t, response2.Semaphore)
	require.Len(t, response2.Semaphore.SemaphoreHolders, 0)
}

func TestUpdateSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Update semaphore
	_, err = semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Description: "updated description",
		Permits:     3,
		Now:         now.Add(time.Minute).UnixNano(),
	})

	require.NoError(t, err)

	// T+2m: Get updated semaphore
	response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
	})

	require.NoError(t, err)
	require.Equal(t, "updated description", response.Semaphore.Description)
	require.EqualValues(t, 3, response.Semaphore.Permits)
}

func TestUpdateSemaphoreWithInsufficientPermits(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore with 3 permits
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           3,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: First process acquires semaphore
	response1, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response1.Success)

	// T+2m: Second process acquires semaphore
	response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
		ProcessId:   "process_2",
		ExpiresAt:   now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response2.Success)

	// T+3m: Third process acquires semaphore
	response3, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(3 * time.Minute).UnixNano(),
		ProcessId:   "process_3",
		ExpiresAt:   now.Add(3 * time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response3.Success)

	// Verify we have 3 holders
	getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(4 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Len(t, getResponse.Semaphore.SemaphoreHolders, 3)

	// T+5m: Try to update semaphore to reduce permits to 2 (less than current holders)
	_, err = semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Description: "updated description",
		Permits:     2,
		Now:         now.Add(5 * time.Minute).UnixNano(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "there are currently more holders than the new amount of permits")

	// Verify the semaphore was not updated
	getResponse2, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(6 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Equal(t, "test description", getResponse2.Semaphore.Description) // Description should not be updated
	require.EqualValues(t, 3, getResponse2.Semaphore.Permits)                // Permits should not be updated
	require.Len(t, getResponse2.Semaphore.SemaphoreHolders, 3)               // All holders should still be there
}

func TestUpdateNonExistingSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "non_existing_semaphore",
	}

	// Try to update a non-existing semaphore
	_, err := semaphoresCore.UpdateSemaphore(&corepb.UpdateSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
		Description: "updated description",
		Permits:     3,
		Now:         now.UnixNano(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestGetNonExistingSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "non_existing_semaphore",
	}

	// Try to get a non-existing semaphore
	_, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
		Now:         now.UnixNano(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestAcquireNonExistingSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "non_existing_semaphore",
	}

	// Try to acquire a non-existing semaphore
	_, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
		Now:         now.UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestReleaseNonExistingSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "non_existing_semaphore",
	}

	// Try to release a non-existing semaphore
	_, err := semaphoresCore.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
		Now:         now.UnixNano(),
		ProcessId:   "process_1",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestCreateSemaphoreWithDuplicateName(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create first semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// Try to create a second semaphore with the same name
	_, err = semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "duplicate description",
		Permits:                           3,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestDeleteSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// T+0: Create semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Verify semaphore exists
	getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, getResponse.Semaphore)

	// T+2m: Delete semaphore
	_, err = semaphoresCore.DeleteSemaphore(&corepb.DeleteSemaphoreRequest{
		SemaphoreId: semaphoreId,
	})
	require.NoError(t, err)

	// T+3m: Verify semaphore no longer exists
	_, err = semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(3 * time.Minute).UnixNano(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestDeleteNonExistingSemaphore(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	accountId := rand.Uint64()
	nonExistingSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: time.Now().UnixNano(),
		SemaphoreName:      "non_existing_semaphore",
	}

	// Try to delete a non-existing semaphore (should succeed without error)
	_, err := semaphoresCore.DeleteSemaphore(&corepb.DeleteSemaphoreRequest{
		SemaphoreId: nonExistingSemaphoreId,
	})

	require.NoError(t, err)
}

func TestListSemaphores(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create first semaphore
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore_1",
		Description:                       "test description 1",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// Create second semaphore
	_, err = semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore_2",
		Description:                       "test description 2",
		Permits:                           3,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// List semaphores
	response, err := semaphoresCore.ListSemaphores(&corepb.ListSemaphoresRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
	})

	require.NoError(t, err)
	require.Len(t, response.Semaphores, 2)
}

func TestSnapshotAndRestoreSemaphores(t *testing.T) {
	now := time.Now()

	accountId := rand.Uint64()
	semaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "test_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "test_semaphore",
	}

	// Create two semaphore cores for testing snapshot and restore
	semaphoresCore1 := newSemaphoresCore()
	semaphoresCore2 := newSemaphoresCore()

	// T+0: Create semaphore
	_, err := semaphoresCore1.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "test_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// T+1m: Acquire semaphore
	response1, err := semaphoresCore1.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(time.Minute).UnixNano(),
		ProcessId:   "process_1",
		ExpiresAt:   now.Add(time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response1.Success)

	// Take snapshot at this point
	snapshot := semaphoresCore1.Snapshot()

	// T+2m: Acquire semaphore with second process (after snapshot)
	response2, err := semaphoresCore1.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(2 * time.Minute).UnixNano(),
		ProcessId:   "process_2",
		ExpiresAt:   now.Add(2 * time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response2.Success)

	// T+3m: Release first process (after snapshot)
	_, err = semaphoresCore1.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(3 * time.Minute).UnixNano(),
		ProcessId:   "process_1",
	})
	require.NoError(t, err)

	// Write snapshot to buffer
	buf := bytes.NewBuffer(nil)
	err = snapshot.Write(buf)
	require.NoError(t, err)

	// Restore snapshot to second core
	err = semaphoresCore2.Restore(io.NopCloser(buf))
	require.NoError(t, err)

	// T+4m: Check that the restored state matches the snapshot state
	// The semaphore should exist with one holder (process_1)
	response3, err := semaphoresCore2.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(4 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, response3.Semaphore)
	require.Len(t, response3.Semaphore.SemaphoreHolders, 1)
	require.Equal(t, "process_1", response3.Semaphore.SemaphoreHolders[0].ProcessId)

	// T+5m: Try to acquire with a new process in restored state (should succeed)
	response4, err := semaphoresCore2.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(5 * time.Minute).UnixNano(),
		ProcessId:   "process_3",
		ExpiresAt:   now.Add(5 * time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.True(t, response4.Success)
	require.Len(t, response4.Semaphore.SemaphoreHolders, 2)

	// T+6m: Try to acquire with a third process in restored state (should fail - no more permits)
	response5, err := semaphoresCore2.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(6 * time.Minute).UnixNano(),
		ProcessId:   "process_4",
		ExpiresAt:   now.Add(6 * time.Minute).Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	require.False(t, response5.Success)

	// T+7m: Release process_1 in restored state
	_, err = semaphoresCore2.ReleaseSemaphore(&corepb.ReleaseSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(7 * time.Minute).UnixNano(),
		ProcessId:   "process_1",
	})
	require.NoError(t, err)

	// T+8m: Verify only process_3 remains in restored state
	response6, err := semaphoresCore2.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: semaphoreId,
		Now:         now.Add(8 * time.Minute).UnixNano(),
	})
	require.NoError(t, err)
	require.Len(t, response6.Semaphore.SemaphoreHolders, 1)
	require.Equal(t, "process_3", response6.Semaphore.SemaphoreHolders[0].ProcessId)

	// Verify that process_2 from the original core is not in the restored state
	// (it was acquired after the snapshot)
	for _, holder := range response6.Semaphore.SemaphoreHolders {
		require.NotEqual(t, "process_2", holder.ProcessId)
	}
}

func TestSemaphoresDeleteNamespace(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()
	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	// Create a semaphore in the namespace
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore",
		Description:                       "test description",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 100,
	})
	require.NoError(t, err)

	// Mark the namespace as deleted using SemaphoresDeleteNamespace
	deleteResponse, err := semaphoresCore.SemaphoresDeleteNamespace(&corepb.SemaphoresDeleteNamespaceRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Verify that the namespace is marked as deleted by checking the deleted namespaces list
	txn := semaphoresCore.badgerStore.Update()
	defer txn.Discard()

	deletedNamespaces, err := semaphoresCore.listGCRecords(txn, 100)
	require.NoError(t, err)
	require.Len(t, deletedNamespaces, 1)
	require.Equal(t, namespaceId.AccountId, deletedNamespaces[0].NamespaceTimestampedId.AccountId)
	require.Equal(t, namespaceId.NamespaceName, deletedNamespaces[0].NamespaceTimestampedId.NamespaceName)
}

func TestCreateSemaphoreMaxLimitReached(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()

	accountId := rand.Uint64()
	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: "test_namespace",
	}

	maxSemaphores := int64(3)

	// Create semaphores up to the limit
	for i := 0; i < int(maxSemaphores); i++ {
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
				AccountId:          namespaceId.AccountId,
				NamespaceName:      namespaceId.NamespaceName,
				NamespaceCreatedAt: now.UnixNano(),
			},
			Name:                              fmt.Sprintf("test_semaphore_%d", i),
			Description:                       fmt.Sprintf("test description %d", i),
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: maxSemaphores,
		})
		require.NoError(t, err, "Failed to create semaphore %d", i)
	}

	// Try to create one more semaphore (should fail)
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "test_semaphore_limit_exceeded",
		Description:                       "test description limit exceeded",
		Permits:                           2,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: maxSemaphores,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "max number of semaphores per namespace reached")
}

func TestRunSemaphoresGarbageCollectionWithDeletedNamespace(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()
	accountId := rand.Uint64()
	namespaceName := "test_namespace_for_gc"

	namespaceId := &corepb.NamespaceId{
		AccountId:     accountId,
		NamespaceName: namespaceName,
	}

	// Create some semaphores in the namespace
	semaphoreIds := make([]*corepb.SemaphoreId, 10)
	for i := range len(semaphoreIds) {
		semaphoreIds[i] = &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      namespaceName,
			NamespaceCreatedAt: now.UnixNano(),
			SemaphoreName:      fmt.Sprintf("semaphore_%d", i),
		}
	}

	// Create semaphores in the namespace
	for i, semaphoreId := range semaphoreIds {
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
				AccountId:          accountId,
				NamespaceName:      namespaceName,
				NamespaceCreatedAt: now.UnixNano(),
			},
			Name:                              semaphoreId.SemaphoreName,
			Description:                       fmt.Sprintf("test description %d", i),
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		// Acquire semaphores
		response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
			ProcessId:   fmt.Sprintf("process_%d", i),
			ExpiresAt:   now.Add(time.Hour).UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.True(t, response.Success)
	}

	// Verify that semaphores in a different namespace are accessible after GC
	differentNamespaceSemaphoreId := &corepb.SemaphoreId{
		AccountId:          accountId,
		NamespaceName:      "different_namespace",
		NamespaceCreatedAt: now.UnixNano(),
		SemaphoreName:      "different_semaphore",
	}

	// Create and acquire a semaphore in a different namespace
	_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          accountId,
			NamespaceName:      "different_namespace",
			NamespaceCreatedAt: now.UnixNano(),
		},
		Name:                              "different_semaphore",
		Description:                       "different description",
		Permits:                           1,
		Now:                               now.UnixNano(),
		MaxNumberOfSemaphoresPerNamespace: 10,
	})
	require.NoError(t, err)

	acquireResponse, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
		SemaphoreId: differentNamespaceSemaphoreId,
		Now:         now.UnixNano(),
		ProcessId:   "process_different",
		ExpiresAt:   now.Add(time.Hour).UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, acquireResponse.Semaphore)
	require.True(t, acquireResponse.Success)

	// Verify semaphores exist by getting them
	for _, semaphoreId := range semaphoreIds {
		getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
		})

		require.NoError(t, err)
		require.NotNil(t, getResponse.Semaphore)
		require.Len(t, getResponse.Semaphore.SemaphoreHolders, 1)
	}

	// Mark the namespace as deleted using SemaphoresDeleteNamespace
	deleteResponse, err := semaphoresCore.SemaphoresDeleteNamespace(&corepb.SemaphoresDeleteNamespaceRequest{
		NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
			AccountId:          namespaceId.AccountId,
			NamespaceName:      namespaceId.NamespaceName,
			NamespaceCreatedAt: now.UnixNano(),
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, deleteResponse)

	// Run garbage collection to clean up the deleted namespace
	gcResponse, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
		Now:                        now.UnixNano(),
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 100,
		MaxVisitedSemaphores:       1000,
	})

	require.NoError(t, err)
	require.NotNil(t, gcResponse)

	// Verify that semaphores in the deleted namespace are no longer accessible
	for _, semaphoreId := range semaphoreIds {
		_, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	}

	// Verify the different namespace semaphore still exists after GC
	getResponse, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
		SemaphoreId: differentNamespaceSemaphoreId,
		Now:         now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, getResponse.Semaphore)
	require.Len(t, getResponse.Semaphore.SemaphoreHolders, 1)
}

func TestRunSemaphoresGarbageCollectionWithMultipleExpiringSemaphores(t *testing.T) {
	semaphoresCore := newSemaphoresCore()

	now := time.Now()
	accountId := rand.Uint64()
	namespaceName := "test_namespace_gc"

	// Create more semaphores than MaxVisitedSemaphores to test the limit
	const numSemaphores = 15
	const maxVisitedSemaphores = 10

	// Create semaphores with different scenarios:
	// - Semaphores 0-4: All holders will expire (should be updated to have no holders)
	// - Semaphores 5-9: Some holders will expire, some will remain (should be updated)
	// - Semaphores 10-14: All holders will remain (should be updated but not emptied)

	semaphoreIds := make([]*corepb.SemaphoreId, numSemaphores)
	for i := range numSemaphores {
		semaphoreIds[i] = &corepb.SemaphoreId{
			AccountId:          accountId,
			NamespaceName:      namespaceName,
			NamespaceCreatedAt: now.UnixNano(),
			SemaphoreName:      fmt.Sprintf("semaphore_%d", i),
		}
	}

	// Create and acquire semaphores with different expiration scenarios
	for i, semaphoreId := range semaphoreIds {
		// Create semaphore
		_, err := semaphoresCore.CreateSemaphore(&corepb.CreateSemaphoreRequest{
			NamespaceTimestampedId: &corepb.NamespaceTimestampedId{
				AccountId:          accountId,
				NamespaceName:      namespaceName,
				NamespaceCreatedAt: now.UnixNano(),
			},
			Name:                              semaphoreId.SemaphoreName,
			Description:                       fmt.Sprintf("test description %d", i),
			Permits:                           2,
			Now:                               now.UnixNano(),
			MaxNumberOfSemaphoresPerNamespace: 100,
		})
		require.NoError(t, err)

		if i < 5 {
			// Semaphores 0-4: All holders will expire
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d", i),
				ExpiresAt:   now.Add(30 * time.Minute).UnixNano(), // Will expire
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.True(t, response.Success)

			// Add a second holder that will also expire
			response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d_second", i),
				ExpiresAt:   now.Add(30 * time.Minute).UnixNano(), // Will expire
			})
			require.NoError(t, err)
			require.NotNil(t, response2.Semaphore)
			require.True(t, response2.Success)
		} else if i < 10 {
			// Semaphores 5-9: Some holders will expire, some will remain
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d", i),
				ExpiresAt:   now.Add(30 * time.Minute).UnixNano(), // Will expire
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.True(t, response.Success)

			// Add a second holder that will remain
			response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d_second", i),
				ExpiresAt:   now.Add(2 * time.Hour).UnixNano(), // Will remain
			})
			require.NoError(t, err)
			require.NotNil(t, response2.Semaphore)
			require.True(t, response2.Success)
		} else {
			// Semaphores 10-14: All holders will remain
			response, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d", i),
				ExpiresAt:   now.Add(2 * time.Hour).UnixNano(), // Will remain
			})
			require.NoError(t, err)
			require.NotNil(t, response.Semaphore)
			require.True(t, response.Success)

			// Add a second holder that will also remain
			response2, err := semaphoresCore.AcquireSemaphore(&corepb.AcquireSemaphoreRequest{
				SemaphoreId: semaphoreId,
				Now:         now.UnixNano(),
				ProcessId:   fmt.Sprintf("process_%d_second", i),
				ExpiresAt:   now.Add(3 * time.Hour).UnixNano(), // Will remain
			})
			require.NoError(t, err)
			require.NotNil(t, response2.Semaphore)
			require.True(t, response2.Success)
		}
	}

	// Verify all semaphores exist and have holders before garbage collection
	for _, semaphoreId := range semaphoreIds {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreId,
			Now:         now.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 2)
	}

	// Run garbage collection at the moment when some semaphores expire (T+31 minutes)
	gcTime := now.Add(31 * time.Minute)
	gcResponse, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
		Now:                        gcTime.UnixNano(),
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 100,
		MaxVisitedSemaphores:       maxVisitedSemaphores,
	})

	require.NoError(t, err)
	require.NotNil(t, gcResponse)

	// Verify the state of semaphores after garbage collection
	// Note: We use the public GetSemaphore method which internally calls checkSemaphoreExpiration
	// to verify the true state of the semaphores after garbage collection

	// Semaphores 0-4 should have no holders (all expired)
	for i := 0; i < 5; i++ {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreIds[i],
			Now:         gcTime.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 0, "Semaphore %d should have no holders", i)
	}

	// Semaphores 5-9 should still have one holder remaining
	for i := 5; i < 10; i++ {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreIds[i],
			Now:         gcTime.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 1, "Semaphore %d should have exactly one holder remaining", i)
		require.Equal(t, fmt.Sprintf("process_%d_second", i), response.Semaphore.SemaphoreHolders[0].ProcessId)
	}

	// Semaphores 10-14 should still have both holders
	for i := 10; i < numSemaphores; i++ {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreIds[i],
			Now:         gcTime.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 2, "Semaphore %d should have both holders remaining", i)
		holderProcessIds := make([]string, len(response.Semaphore.SemaphoreHolders))
		for j, holder := range response.Semaphore.SemaphoreHolders {
			holderProcessIds[j] = holder.ProcessId
		}
		require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d", i))
		require.Contains(t, holderProcessIds, fmt.Sprintf("process_%d_second", i))
	}

	// Run garbage collection again to process the remaining semaphores
	// This should process semaphores 5-14 since semaphores 0-4 were already processed
	gcResponse2, err := semaphoresCore.RunSemaphoresGarbageCollection(&corepb.RunSemaphoresGarbageCollectionRequest{
		Now:                        gcTime.UnixNano(),
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 100,
		MaxVisitedSemaphores:       maxVisitedSemaphores,
	})

	require.NoError(t, err)
	require.NotNil(t, gcResponse2)

	// Verify that semaphores 5-9 still have their remaining holders
	for i := 5; i < 10; i++ {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreIds[i],
			Now:         gcTime.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 1, "Semaphore %d should still have one holder after second GC", i)
	}

	// Verify that semaphores 10-14 still have all their holders
	for i := 10; i < numSemaphores; i++ {
		response, err := semaphoresCore.GetSemaphore(&corepb.GetSemaphoreRequest{
			SemaphoreId: semaphoreIds[i],
			Now:         gcTime.UnixNano(),
		})
		require.NoError(t, err)
		require.NotNil(t, response.Semaphore)
		require.Len(t, response.Semaphore.SemaphoreHolders, 2, "Semaphore %d should still have both holders after second GC", i)
	}
}

func newSemaphoresCore() *SemaphoresCore {
	return NewSemaphoresCore(monstera.NewBadgerInMemoryStore(), []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
