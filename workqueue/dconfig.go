package workqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/watcher"
)

func dConfigDataKey(namespace, fileSystemName, snapshotName string) string {
	return namespace + ":" + fileSystemName + "-" + snapshotName + ":data"
}

func dConfigDataKeyFromW(w *Workqueue) string {
	return dConfigDataKey(w.dconfigNamespace, w.fileSystemName, w.snapshotName)
}

type DConfigData struct {
	Epoch             uint64 `redis:"epoch"`
	MaxNodeThroughput uint64 `redis:"max_node_throughput"`
}

type DConfigClient struct {
	w *Workqueue
}

func (d DConfigClient) GetData() (DConfigData, error) {
	conn := d.w.pool.Get()
	defer conn.Close()

	repl, err := redis.Values(conn.Do("HGETALL", dConfigDataKeyFromW(d.w)))
	if err != nil {
		return DConfigData{}, fmt.Errorf("DConfigClient.GetData: loading from Redis: %w", err)
	}

	var data DConfigData

	err = redis.ScanStruct(repl, &data)
	if err != nil {
		return DConfigData{}, fmt.Errorf("DConfigClient.GetData: parsing Redis reply: %w", err)
	}

	return data, nil
}

func (d DConfigClient) StartPublisher(ctx context.Context) *DConfigPublisher {
	return &DConfigPublisher{
		ctx: ctx,
		w:   d.w,
	}
}

func (d DConfigClient) StartConsumer(ctx context.Context) (*DConfigConsumer, error) {
	w := newDConfigConsumer(ctx, d.w)
	err := w.start()
	if err != nil {
		return nil, fmt.Errorf("DConfigClient.StartConsumer: %w", err)
	}
	return w, nil
}

type DConfigPublisher struct {
	ctx  context.Context
	w    *Workqueue
	data DConfigData
}

func (d *DConfigPublisher) MutatePublishData(f func(*DConfigData)) (bool, error) {
	newData := d.data
	f(&newData)
	if newData == d.data {
		return false, nil
	}

	newData.Epoch = d.data.Epoch + 1

	conn := d.w.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HSET", redis.Args{}.Add(dConfigDataKeyFromW(d.w)).AddFlat(newData)...)
	if err != nil {
		return true, fmt.Errorf("DConfigPublisher.MutatePublishData: %w", err)
	}

	return true, nil
}

func (d *DConfigPublisher) Close() error {
	return nil
}

type DConfigConsumer struct {
	client        DConfigClient
	fieldLogger   log.Interface
	probeInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	publisher watcher.Publisher[DConfigData]
}

func newDConfigConsumer(ctx context.Context, w *Workqueue) *DConfigConsumer {
	ctx, cancel := context.WithCancel(ctx)

	consumer := &DConfigConsumer{
		client: w.DConfig(),
		fieldLogger: w.config.Logger.WithFields(log.Fields{
			"component":  "workqueue.DConfigConsumer",
			"filesystem": w.fileSystemName,
			"snapshot":   w.snapshotName,
		}),
		probeInterval: w.config.DConfigProbeInterval,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}

	return consumer
}

func (d *DConfigConsumer) start() error {
	// FUTR: Could auto-start whenever there is > 0 Watchers

	data, err := d.client.GetData()
	if err != nil {
		return fmt.Errorf("DConfigConsumer.start: get initial data: %w", err)
	}
	d.publisher.Publish(data)

	go d.run()
	return nil
}

func (d *DConfigConsumer) run() {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

L:
	for {
		timer.Reset(d.probeInterval)
		select {
		case <-timer.C:
			data, err := d.client.GetData()
			if err != nil {
				d.fieldLogger.WithError(err).WithFields(log.Fields{
					"action": "skipping",
				}).Warn("Error while getting dconfig data from Redis")
				continue
			}
			if data.Epoch != d.publisher.Get().Epoch {
				d.publisher.Publish(data)
			}
		case <-d.ctx.Done():
			break L
		}
	}
	close(d.done)
}

func (d *DConfigConsumer) Watch() *DConfigWatcher {
	return &DConfigWatcher{*d.publisher.Watch()}
}

func (d *DConfigConsumer) GetData() DConfigData {
	return d.publisher.Get()
}

func (d *DConfigConsumer) Close() error {
	d.cancel()
	<-d.done
	return nil
}

type DConfigWatcher struct {
	watcher.Watcher[DConfigData]
}
