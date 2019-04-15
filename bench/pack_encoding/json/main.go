package main

import (
	"encoding/json"
	"fmt"

	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
)

var sampleWorkPack workqueue.WorkPack = workqueue.WorkPack{
	FileSystemName: "gpfs2",
	SnapshotName:   "lsdf-checksum-run-1",
	Files: []workqueue.WorkPackFile{
		workqueue.WorkPackFile{
			ID:   3,
			Path: "/asdf",
		},
		workqueue.WorkPackFile{
			ID:   5,
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
	jsonBuf, err := json.Marshal(workPack)
	if err != nil {
		return err
	}

	jobArgs["pack"] = string(jsonBuf)

	return nil
}

func retrievePack(jobArgs map[string]interface{}) (*workqueue.WorkPack, error) {
	jsonBuf := []byte(jobArgs["pack"].(string))

	workPack := &workqueue.WorkPack{}

	err := json.Unmarshal(jsonBuf, workPack)
	if err != nil {
		return nil, err
	}

	return workPack, nil
}
