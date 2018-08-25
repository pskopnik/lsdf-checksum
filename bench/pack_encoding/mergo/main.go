package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/imdario/mergo"

	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
)

var (
	errInvalidType error = errors.New("Invalid type.")
)

var (
	sliceOfWorkPackFile reflect.Type = reflect.ValueOf([]workqueue.WorkPackFile{}).Type()
	mapStringInterface  reflect.Type = reflect.ValueOf(map[string]interface{}{}).Type()
)

type sliceTransformer struct{}

func (s sliceTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	fmt.Println("called (timeTransformer).Transformer(…)", typ)
	if typ == sliceOfWorkPackFile {
		return func(dst, src reflect.Value) error {
			dstType := dst.Type()
			if dstType.Kind() != reflect.Slice {
				fmt.Println(dstType)
				return errInvalidType
			}

			srcLen := src.Len()

			dstVal := reflect.MakeSlice(dstType, srcLen, srcLen)

			for i := 0; i < srcLen; i++ {
				err := mergo.Map(
					dstVal.Index(i).Addr().Interface(),
					src.Index(i).Elem().Interface(),
					mergo.WithTransformers(sliceTransformer{}),
				)
				fmt.Printf(
					"%+v %+v\n",
					dstVal.Index(i).Addr().Interface(),
					src.Index(i).Elem().Interface(),
				)
				if err != nil {
					return err
				}
			}

			dst.Set(dstVal)

			return nil
		}
	}
	return nil
}

var sampleWorkPack workqueue.WorkPack = workqueue.WorkPack{
	FileSystemName: "gpfs2",
	SnapshotName:   "lsdf-checksum-run-1",
	Files: []workqueue.WorkPackFile{
		workqueue.WorkPackFile{
			Id:   3,
			Path: "/asdf",
		},
		workqueue.WorkPackFile{
			Id:   5,
			Path: "/ghjj/asdff",
		},
	},
}

func main() {
	jobArgs := make(map[string]interface{})

	err := propagateJob(jobArgs, &sampleWorkPack)
	if err != nil {
		panic(err)
	}

	p, err := json.Marshal(jobArgs)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(p))

	argMap := make(map[string]interface{})

	err = json.Unmarshal(p, &argMap)
	if err != nil {
		panic(err)
	}

	workPack, err := retrievePack(argMap)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", workPack)
}

func propagateJob(jobArgs map[string]interface{}, workPack *workqueue.WorkPack) error {
	err := mergo.Map(&jobArgs, workPack, mergo.WithOverride)
	if err != nil {
		return err
	}

	return nil
}

func retrievePack(jobArgs map[string]interface{}) (*workqueue.WorkPack, error) {
	workPack := &workqueue.WorkPack{Files: []workqueue.WorkPackFile{workqueue.WorkPackFile{}}}

	err := mergo.Map(workPack, jobArgs, mergo.WithTransformers(sliceTransformer{}))
	if err != nil {
		return nil, err
	}

	return workPack, nil
}
