package workers

import (
	"context"
	"log"
	"sync"
	"time"

	monsterax "github.com/evrblk/monstera/x"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/grackle/pkg/corepb"
	"github.com/evrblk/grackle/pkg/monsteragen"
)

type GrackleLocksGCWorker struct {
	coreApiClient monsteragen.GrackleCoreApi
	worker        *workers.IntervalWorker
}

func NewGrackleLocksGCWorker(coreApiClient monsteragen.GrackleCoreApi) *GrackleLocksGCWorker {
	return &GrackleLocksGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleLocksGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleLocksGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleLocksGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleLocks")
	if err != nil {
		log.Printf("ListShards(\"GrackleLocks\"): %v", err)
		return // TODO
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			w.runGarbageCollection(shardId, now)
			done.Done()
		}(shard, now, done)
	}

	done.Wait()
}

func (w *GrackleLocksGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer monsterax.MeasureSince(grackleLocksGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunLocksGarbageCollection(context.TODO(), &corepb.RunLocksGarbageCollectionRequest{
		Now:                   now.UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 1000,
		MaxVisitedLocks:       1000,
	}, shardId)
	if err != nil {
		grackleLocksGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunLocksGarbageCollection failed: %v", err)
	}
}
