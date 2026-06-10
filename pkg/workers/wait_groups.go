package workers

import (
	"context"
	"log"
	"sync"
	"time"

	monsterax "github.com/evrblk/monstera/x"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/corepb"
)

type GrackleWaitGroupsGCWorker struct {
	coreApiClient coreapis.GrackleClientApi
	worker        *workers.IntervalWorker
}

func NewGrackleWaitGroupsGCWorker(coreApiClient coreapis.GrackleClientApi) *GrackleWaitGroupsGCWorker {
	return &GrackleWaitGroupsGCWorker{
		coreApiClient: coreApiClient,
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
		log.Printf("ListShards(\"GrackleWaitGroups\"): %v", err)
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
	defer monsterax.MeasureSince(grackleWaitGroupsGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunWaitGroupsGarbageCollection(context.TODO(), &corepb.RunWaitGroupsGarbageCollectionRequest{
		Now: now.UnixNano(),
	}, shardId)
	if err != nil {
		grackleWaitGroupsGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunWaitGroupsGarbageCollection failed: %v", err)
	}
}
