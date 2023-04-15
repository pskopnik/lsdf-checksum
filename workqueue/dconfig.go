package workqueue

import (
	"context"

	"github.com/gomodule/redigo/redis"
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
		return DConfigData{}, err
	}

	var data DConfigData

	err = redis.ScanStruct(repl, &data)
	if err != nil {
		return DConfigData{}, err
	}

	return data, nil
}

func (d DConfigClient) StartPublisher(ctx context.Context) *DConfigPublisher {
	return &DConfigPublisher{
		ctx: ctx,
		w:   d.w,
	}
}

func (d DConfigClient) StartWatcher(ctx context.Context) *DConfigWatcher {
	return &DConfigWatcher{
		ctx: ctx,
		w:   d.w,
	}
}

type DConfigPublisher struct {
	ctx  context.Context
	w    *Workqueue
	data DConfigData
}

func (d *DConfigPublisher) MutatePublishData(f func(*DConfigData)) error {
	newData := d.data
	f(&newData)
	if newData == d.data {
		return nil
	}

	newData.Epoch = d.data.Epoch + 1

	conn := d.w.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HSET", redis.Args{}.Add(dConfigDataKeyFromW(d.w)).AddFlat(newData)...)
	if err != nil {
		return err
	}

	return nil
}

func (d *DConfigPublisher) Close() error {
	return nil
}

type DConfigWatcher struct {
	ctx context.Context
	w   *Workqueue
}

func (d *DConfigWatcher) Close() error {
	return nil
}
