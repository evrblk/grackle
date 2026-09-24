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

type GrackleWaitGroupsGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	logger        *slog.Logger
	worker        *workers.IntervalWorker
}

// NewGrackleWaitGroupsGCWorker builds a GrackleWaitGroupsGCWorker logging to
// logger, or to slog.Default() if logger is nil.
func NewGrackleWaitGroupsGCWorker(coreApiClient coreapis.GrackleClientApi, logger *slog.Logger) *GrackleWaitGroupsGCWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &GrackleWaitGroupsGCWorker{
		coreApiClient: coreApiClient,
		logger:        logger,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleWaitGroupsGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleWaitGroupsGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleWaitGroupsGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleWaitGroups")
	if err != nil {
		w.logger.Error("ListShards failed", "application", "GrackleWaitGroups", "error", err)
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

func (w *GrackleWaitGroupsGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer metrics.MeasureSince(grackleWaitGroupsGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunWaitGroupsGarbageCollection(context.TODO(), &corepb.RunWaitGroupsGarbageCollectionRequest{
		GcRecordsPageSize:          100,
		GcRecordWaitGroupsPageSize: 1000,
		MaxDeletedObjects:          1000,
	}, shardId)
	if err != nil {
		grackleWaitGroupsGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunWaitGroupsGarbageCollection failed", "shard_id", shardId, "error", err)
	}
}
