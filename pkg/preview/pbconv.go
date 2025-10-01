package preview

import (
	"encoding/base64"

	"google.golang.org/protobuf/proto"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/corepb"
)

func namespaceToFront(namespace *corepb.Namespace) *gracklepb.Namespace {
	if namespace == nil {
		return nil
	}

	return &gracklepb.Namespace{
		Name:        namespace.Id.NamespaceName,
		Description: namespace.Description,
		CreatedAt:   namespace.CreatedAt,
		UpdatedAt:   namespace.UpdatedAt,
	}
}

func namespacesToFront(namespaces []*corepb.Namespace) []*gracklepb.Namespace {
	frontNamespaces := make([]*gracklepb.Namespace, len(namespaces))
	for i, namespace := range namespaces {
		frontNamespaces[i] = namespaceToFront(namespace)
	}
	return frontNamespaces
}

func waitGroupToFront(waitGroup *corepb.WaitGroup) *gracklepb.WaitGroup {
	if waitGroup == nil {
		return nil
	}

	return &gracklepb.WaitGroup{
		Name:        waitGroup.Id.WaitGroupName,
		Description: waitGroup.Description,
		CreatedAt:   waitGroup.CreatedAt,
		UpdatedAt:   waitGroup.UpdatedAt,
		Counter:     waitGroup.Counter,
		ExpiresAt:   waitGroup.ExpiresAt,
	}
}

func waitGroupsToFront(waitGroups []*corepb.WaitGroup) []*gracklepb.WaitGroup {
	frontWaitGroups := make([]*gracklepb.WaitGroup, len(waitGroups))
	for i, waitGroup := range waitGroups {
		frontWaitGroups[i] = waitGroupToFront(waitGroup)
	}
	return frontWaitGroups
}

func locksToFront(locks []*corepb.Lock) []*gracklepb.Lock {
	frontLocks := make([]*gracklepb.Lock, len(locks))
	for i, lock := range locks {
		frontLocks[i] = lockToFront(lock)
	}
	return frontLocks
}

func lockToFront(lock *corepb.Lock) *gracklepb.Lock {
	if lock == nil {
		return nil
	}

	return &gracklepb.Lock{
		Name:            lock.Id.LockName,
		State:           gracklepb.LockState(lock.State),
		LockedAt:        lock.LockedAt,
		WriteLockHolder: lockHolderToFront(lock.WriteLockHolder),
		ReadLockHolders: lockHoldersToFront(lock.ReadLockHolders),
	}
}

func lockHolderToFront(lockHolder *corepb.LockHolder) *gracklepb.LockHolder {
	if lockHolder == nil {
		return nil
	}

	return &gracklepb.LockHolder{
		ProcessId: lockHolder.ProcessId,
		LockedAt:  lockHolder.LockedAt,
		ExpiresAt: lockHolder.ExpiresAt,
	}
}

func lockHoldersToFront(lockHolders []*corepb.LockHolder) []*gracklepb.LockHolder {
	frontLockHolders := make([]*gracklepb.LockHolder, len(lockHolders))
	for i, lockHolder := range lockHolders {
		frontLockHolders[i] = lockHolderToFront(lockHolder)
	}
	return frontLockHolders
}

func semaphoreToFront(semaphore *corepb.Semaphore) *gracklepb.Semaphore {
	if semaphore == nil {
		return nil
	}

	return &gracklepb.Semaphore{
		Name:             semaphore.Id.SemaphoreName,
		Description:      semaphore.Description,
		CreatedAt:        semaphore.CreatedAt,
		UpdatedAt:        semaphore.UpdatedAt,
		Permits:          semaphore.Permits,
		SemaphoreHolders: semaphoreHoldersToFront(semaphore.SemaphoreHolders),
	}
}

func semaphoresToFront(semaphores []*corepb.Semaphore) []*gracklepb.Semaphore {
	frontSemaphores := make([]*gracklepb.Semaphore, len(semaphores))
	for i, semaphore := range semaphores {
		frontSemaphores[i] = semaphoreToFront(semaphore)
	}
	return frontSemaphores
}

func semaphoreHolderToFront(semaphoreHolder *corepb.SemaphoreHolder) *gracklepb.SemaphoreHolder {
	if semaphoreHolder == nil {
		return nil
	}

	return &gracklepb.SemaphoreHolder{
		ProcessId: semaphoreHolder.ProcessId,
		LockedAt:  semaphoreHolder.LockedAt,
		ExpiresAt: semaphoreHolder.ExpiresAt,
	}
}

func semaphoreHoldersToFront(semaphoreHolders []*corepb.SemaphoreHolder) []*gracklepb.SemaphoreHolder {
	frontSemaphoreHolders := make([]*gracklepb.SemaphoreHolder, len(semaphoreHolders))
	for i, semaphoreHolder := range semaphoreHolders {
		frontSemaphoreHolders[i] = semaphoreHolderToFront(semaphoreHolder)
	}
	return frontSemaphoreHolders
}

func paginationTokenToFront(paginationToken *corepb.PaginationToken) (string, error) {
	if paginationToken == nil {
		return "", nil
	}

	data, err := proto.Marshal(paginationToken)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func paginationTokenFromFront(paginationTokenBase64 string) (*corepb.PaginationToken, error) {
	if paginationTokenBase64 == "" {
		return nil, nil
	}

	paginationTokenBytes, err := base64.StdEncoding.DecodeString(paginationTokenBase64)
	if err != nil {
		return nil, err
	}

	paginationToken := &corepb.PaginationToken{}
	err = proto.Unmarshal(paginationTokenBytes, paginationToken)
	if err != nil {
		return nil, err
	}

	return paginationToken, nil
}
