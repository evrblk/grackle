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

type GrackleBarriersGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	logger        *slog.Logger
	worker        *workers.IntervalWorker
}

// NewGrackleBarriersGCWorker builds a GrackleBarriersGCWorker logging to
// logger, or to slog.Default() if logger is nil.
func NewGrackleBarriersGCWorker(coreApiClient coreapis.GrackleClientApi, logger *slog.Logger) *GrackleBarriersGCWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &GrackleBarriersGCWorker{
		coreApiClient: coreApiClient,
		logger:        logger,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleBarriersGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleBarriersGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleBarriersGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleBarriers")
	if err != nil {
		w.logger.Error("ListShards failed", "application", "GrackleBarriers", "error", err)
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

func (w *GrackleBarriersGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer metrics.MeasureSince(grackleBarriersGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunBarriersGarbageCollection(context.TODO(), &corepb.RunBarriersGarbageCollectionRequest{
		GcRecordsPageSize:            100,
		GcRecordBarriersPageSize:     1000,
		GcRecordParticipantsPageSize: 1000,
		MaxVisited:                   1000,
	}, shardId)
	if err != nil {
		grackleBarriersGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunBarriersGarbageCollection failed", "shard_id", shardId, "error", err)
	}
}
