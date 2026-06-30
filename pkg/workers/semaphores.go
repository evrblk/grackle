package workers

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
)

type GrackleSemaphoresGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	worker        *workers.IntervalWorker
}

func NewGrackleSemaphoresGCWorker(coreApiClient coreapis.GrackleClientApi) *GrackleSemaphoresGCWorker {
	return &GrackleSemaphoresGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleSemaphoresGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleSemaphoresGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleSemaphoresGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleSemaphores")
	if err != nil {
		log.Printf("ListShards(\"GrackleSemaphores\"): %v", err)
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

func (w *GrackleSemaphoresGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer metrics.MeasureSince(grackleSemaphoresGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunSemaphoresGarbageCollection(context.TODO(), &corepb.RunSemaphoresGarbageCollectionRequest{
		Now:                        now.UnixNano(),
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 1000,
		GcRecordHoldersPageSize:    1000,
		MaxVisited:                 1000,
	}, shardId)
	if err != nil {
		grackleSemaphoresGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunSemaphoresGarbageCollection failed: %v", err)
	}
}
