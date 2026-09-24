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

type GrackleSemaphoresGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	logger        *slog.Logger
	worker        *workers.IntervalWorker
}

// NewGrackleSemaphoresGCWorker builds a GrackleSemaphoresGCWorker logging to
// logger, or to slog.Default() if logger is nil.
func NewGrackleSemaphoresGCWorker(coreApiClient coreapis.GrackleClientApi, logger *slog.Logger) *GrackleSemaphoresGCWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &GrackleSemaphoresGCWorker{
		coreApiClient: coreApiClient,
		logger:        logger,
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
		w.logger.Error("ListShards failed", "application", "GrackleSemaphores", "error", err)
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
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 1000,
		GcRecordHoldersPageSize:    1000,
		MaxVisited:                 1000,
	}, shardId)
	if err != nil {
		grackleSemaphoresGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunSemaphoresGarbageCollection failed", "shard_id", shardId, "error", err)
	}
}
