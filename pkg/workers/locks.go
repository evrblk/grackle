package workers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
)

type GrackleLocksGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	logger        *slog.Logger
	worker        *workers.IntervalWorker
}

// NewGrackleLocksGCWorker builds a GrackleLocksGCWorker logging to logger,
// or to slog.Default() if logger is nil.
func NewGrackleLocksGCWorker(coreApiClient coreapis.GrackleClientApi, logger *slog.Logger) *GrackleLocksGCWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &GrackleLocksGCWorker{
		coreApiClient: coreApiClient,
		logger:        logger,
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
		w.logger.Error("ListShards failed", "application", "GrackleLocks", "error", err)
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
	defer metrics.MeasureSince(grackleLocksGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunLocksGarbageCollection(context.TODO(), &corepb.RunLocksGarbageCollectionRequest{
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 1000,
		MaxVisitedLocks:       1000,
	}, shardId)
	if err != nil {
		grackleLocksGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunLocksGarbageCollection failed", "shard_id", shardId, "error", err)
	}
}
