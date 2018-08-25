package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
)

var (
	errUnexpectedTailingBytes error = errors.New("unexpected tailing bytes")
)

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
	msgpBuf, err := workPack.MarshalMsg(nil)
	if err != nil {
		return err
	}

	base64BufLen := base64.RawStdEncoding.EncodedLen(len(msgpBuf))
	base64Buf := make([]byte, base64BufLen)

	base64.RawStdEncoding.Encode(base64Buf, msgpBuf)

	jobArgs["pack"] = string(base64Buf)

	return nil
}

func retrievePack(jobArgs map[string]interface{}) (*workqueue.WorkPack, error) {
	base64Buf := []byte(jobArgs["pack"].(string))

	msgpBufLen := base64.RawStdEncoding.DecodedLen(len(base64Buf))
	msgpBuf := make([]byte, msgpBufLen)

	_, err := base64.RawStdEncoding.Decode(msgpBuf, base64Buf)
	if err != nil {
		return nil, err
	}

	workPack := &workqueue.WorkPack{}

	msgpBuf, err = workPack.UnmarshalMsg(msgpBuf)
	if err != nil {
		return nil, err
	}
	if len(msgpBuf) != 0 {
		return nil, errUnexpectedTailingBytes
	}

	return workPack, nil
}
