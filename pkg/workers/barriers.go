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

type GrackleBarriersGCWorker struct {
	coreApiClient monsteragen.GrackleCoreApi
	worker        *workers.IntervalWorker
}

func NewGrackleBarriersGCWorker(coreApiClient monsteragen.GrackleCoreApi) *GrackleBarriersGCWorker {
	return &GrackleBarriersGCWorker{
		coreApiClient: coreApiClient,
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
		log.Printf("ListShards(\"GrackleBarriers\"): %v", err)
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
	defer monsterax.MeasureSince(grackleBarriersGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunBarriersGarbageCollection(context.TODO(), &corepb.RunBarriersGarbageCollectionRequest{
		Now: now.UnixNano(),
	}, shardId)
	if err != nil {
		grackleBarriersGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunBarriersGarbageCollection failed: %v", err)
	}
}
