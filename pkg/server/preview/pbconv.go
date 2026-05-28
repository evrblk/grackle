package preview

import (
	"encoding/base64"

	"google.golang.org/protobuf/proto"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/ids"
)

func namespaceToFront(namespace *corepb.Namespace) *gracklepb.Namespace {
	if namespace == nil {
		return nil
	}

	return &gracklepb.Namespace{
		Name:        namespace.Name,
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

func waitGroupJobsToFront(jobs []*corepb.WaitGroupJob) []*gracklepb.WaitGroupJob {
	frontWaitGroupJobs := make([]*gracklepb.WaitGroupJob, len(jobs))
	for i, job := range jobs {
		frontWaitGroupJobs[i] = waitGroupJobToFront(job)
	}
	return frontWaitGroupJobs
}

func waitGroupJobToFront(job *corepb.WaitGroupJob) *gracklepb.WaitGroupJob {
	if job == nil {
		return nil
	}

	return &gracklepb.WaitGroupJob{
		ProcessId:   job.Id.ProcessId,
		CompletedAt: job.CompletedAt,
	}
}

func waitGroupToFront(waitGroup *corepb.WaitGroup) *gracklepb.WaitGroup {
	if waitGroup == nil {
		return nil
	}

	return &gracklepb.WaitGroup{
		Name:        waitGroup.Name,
		Description: waitGroup.Description,
		CreatedAt:   waitGroup.CreatedAt,
		UpdatedAt:   waitGroup.UpdatedAt,
		Counter:     waitGroup.Counter,
		Completed:   waitGroup.Completed,
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
		Name:        lock.Id.LockName,
		State:       gracklepb.LockState(lock.State),
		LockedAt:    lock.LockedAt,
		LockHolders: lockHoldersToFront(lock.LockHolders, lock.Id.AccountId, lock.Id.NamespaceId),
	}
}

func lockHolderToFront(lockHolder *corepb.LockHolder, accountId uint64, namespaceId uint32) *gracklepb.LockHolder {
	if lockHolder == nil {
		return nil
	}

	return &gracklepb.LockHolder{
		LeaseId: ids.EncodeLeaseId(&corepb.LeaseId{
			AccountId:   accountId,
			NamespaceId: namespaceId,
			LeaseId:     lockHolder.LeaseId,
		}),
		LockedAt: lockHolder.LockedAt,
	}
}

func lockHoldersToFront(lockHolders []*corepb.LockHolder, accountId uint64, namespaceId uint32) []*gracklepb.LockHolder {
	frontLockHolders := make([]*gracklepb.LockHolder, len(lockHolders))
	for i, lockHolder := range lockHolders {
		frontLockHolders[i] = lockHolderToFront(lockHolder, accountId, namespaceId)
	}
	return frontLockHolders
}

func semaphoreToFront(semaphore *corepb.Semaphore) *gracklepb.Semaphore {
	if semaphore == nil {
		return nil
	}

	return &gracklepb.Semaphore{
		Name:        semaphore.Name,
		Description: semaphore.Description,
		CreatedAt:   semaphore.CreatedAt,
		UpdatedAt:   semaphore.UpdatedAt,
		Permits:     semaphore.Permits,
		// SemaphoreHolders: semaphoreHoldersToFront(semaphore.SemaphoreHolders),
	}
}

func semaphoresToFront(semaphores []*corepb.Semaphore) []*gracklepb.Semaphore {
	frontSemaphores := make([]*gracklepb.Semaphore, len(semaphores))
	for i, semaphore := range semaphores {
		frontSemaphores[i] = semaphoreToFront(semaphore)
	}
	return frontSemaphores
}

func semaphoreHolderToFront(holder *corepb.SemaphoreHolder) *gracklepb.SemaphoreHolder {
	if holder == nil {
		return nil
	}

	return &gracklepb.SemaphoreHolder{
		LeaseId: ids.EncodeLeaseId(&corepb.LeaseId{
			AccountId:   holder.Id.AccountId,
			NamespaceId: holder.Id.NamespaceId,
			LeaseId:     holder.Id.LeaseId,
		}),
		LockedAt: holder.LockedAt,
		Weight:   holder.Weight,
	}
}

func semaphoreHoldersToFront(semaphoreHolders []*corepb.SemaphoreHolder) []*gracklepb.SemaphoreHolder {
	frontSemaphoreHolders := make([]*gracklepb.SemaphoreHolder, len(semaphoreHolders))
	for i, semaphoreHolder := range semaphoreHolders {
		frontSemaphoreHolders[i] = semaphoreHolderToFront(semaphoreHolder)
	}
	return frontSemaphoreHolders
}

func barrierToFront(barrier *corepb.Barrier) *gracklepb.Barrier {
	if barrier == nil {
		return nil
	}

	return &gracklepb.Barrier{
		Name:              barrier.Name,
		Description:       barrier.Description,
		ExpectedProcesses: barrier.ExpectedProcesses,
		ArrivedProcesses:  barrier.ArrivedProcesses,
		Generation:        barrier.Generation,
		CreatedAt:         barrier.CreatedAt,
		UpdatedAt:         barrier.UpdatedAt,
	}
}

func barriersToFront(barriers []*corepb.Barrier) []*gracklepb.Barrier {
	frontBarriers := make([]*gracklepb.Barrier, len(barriers))
	for i, barrier := range barriers {
		frontBarriers[i] = barrierToFront(barrier)
	}
	return frontBarriers
}

func barrierParticipantToFront(participant *corepb.BarrierParticipant) *gracklepb.BarrierParticipant {
	if participant == nil {
		return nil
	}

	return &gracklepb.BarrierParticipant{}
}

func barrierParticipantsToFront(participants []*corepb.BarrierParticipant) []*gracklepb.BarrierParticipant {
	frontParticipants := make([]*gracklepb.BarrierParticipant, len(participants))
	for i, participant := range participants {
		frontParticipants[i] = barrierParticipantToFront(participant)
	}
	return frontParticipants
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

func leaseToFront(lease *corepb.Lease) *gracklepb.Lease {
	if lease == nil {
		return nil
	}

	return &gracklepb.Lease{
		LeaseId:   ids.EncodeLeaseId(lease.Id),
		ProcessId: lease.ProcessId,
		CreatedAt: lease.CreatedAt,
		ExpiresAt: lease.ExpiresAt,
	}
}

func leasesToFront(leases []*corepb.Lease) []*gracklepb.Lease {
	frontLeases := make([]*gracklepb.Lease, len(leases))
	for i, lease := range leases {
		frontLeases[i] = leaseToFront(lease)
	}
	return frontLeases
}
