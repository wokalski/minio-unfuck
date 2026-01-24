package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/cmd"
)

func mustGetPutObjReader(data io.Reader, size int64, md5hex, sha256hex string) *cmd.PutObjReader {
	hr, err := hash.NewReader(context.Background(), data, size, md5hex, sha256hex, size)
	if err != nil {
		fmt.Println("Error in mustGetPutObjReader: ", err)
	}
	return cmd.NewPutObjReader(hr)
}

var globalTestTmpDir = os.TempDir()

func newTestObjectLayer(ctx context.Context, endpointServerPools cmd.EndpointServerPools) (newObject cmd.ObjectLayer, err error) {
	cmd.InitAllSubsystems(ctx)

	return cmd.NewErasureServerPools(ctx, endpointServerPools)
}

// initObjectLayer - Instantiates object layer and returns it.
func initObjectLayer(ctx context.Context, endpointServerPools cmd.EndpointServerPools) (cmd.ObjectLayer, []cmd.StorageAPI, error) {
	objLayer, err := newTestObjectLayer(ctx, endpointServerPools)
	if err != nil {
		fmt.Println("Failed to create a new obj layer: ", err)
		return nil, nil, err
	}

	var formattedDisks []cmd.StorageAPI
	// Should use the object layer tests for validating cache.
	if z, ok := objLayer.(*cmd.ErasureServerPools); ok {
		formattedDisks = z.ServerPools[0].GetDisks(0)()
	}
	// Success.
	return objLayer, formattedDisks, nil
}

func mustGetNewEndpoints(poolIdx int, drivesPerSet int, args ...string) (endpoints cmd.Endpoints) {
	endpoints, err := cmd.NewEndpoints(args...)
	if err != nil {
		panic(err)
	}
	for i := range endpoints {
		endpoints[i].SetPoolIndex(poolIdx)
		endpoints[i].SetSetIndex(i / drivesPerSet)
		endpoints[i].SetDiskIndex(i % drivesPerSet)
	}
	return endpoints
}

func mustGetPoolEndpoints(poolIdx int, args ...string) cmd.EndpointServerPools {
	drivesPerSet := len(args)
	setCount := 2
	if len(args) >= 16 {
		drivesPerSet = 16
		setCount = len(args) / 16
	}
	endpoints := mustGetNewEndpoints(poolIdx, drivesPerSet, args...)
	return []cmd.PoolEndpoints{{
		SetCount:     setCount,
		DrivesPerSet: drivesPerSet,
		Endpoints:    endpoints,
		CmdLine:      strings.Join(args, " "),
	}}
}

func prepareErasure(ctx context.Context) (cmd.ObjectLayer, []string, error) {
	fsDirs := []string{}

	for i := 0; i <= 0; i++ {
		for j := 1; j <= 4; j++ {
			for k := 0; k < 4; k++ {
			  fsDirs = append(fsDirs, fmt.Sprintf(".disks/storage%d", (k * 4) + (j + (i * 16))))
			}
		}
	}

	fmt.Println("Using dirs: ", fsDirs)

	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, fsDirs...))
	if err != nil {
		return nil, nil, err
	}

	return obj, fsDirs, nil
}

func main() {
	ctx := context.Background()
	obj, _, err := prepareErasure(ctx)
	if err != nil {
		fmt.Println("Error preparing erasure: ", err)
		return
	}
	defer obj.Shutdown(ctx)

	z := obj.(*cmd.ErasureServerPools)
	xl := z.ServerPools[0].Sets[0]
	fmt.Println("Prepared erasure server pools: ", z, xl)

	bucket := "recordings"
	object := "12e6r9jZSmuSQjNj2rUSOx.wav"

	_, err = xl.GetObjectInfo(ctx, bucket, object, cmd.ObjectOptions{})
	if err != nil {
		fmt.Println("First read object failed: ", err)
		return
	}
	reader, err := xl.GetObjectNInfo(ctx, bucket, object, nil, nil, cmd.ObjectOptions{})
	if err != nil {
		fmt.Println("Error reading object data: ", err)
	}
	fmt.Println(reader.ObjInfo.Name)
	data, err := io.ReadAll(reader)
	if err != nil {
		fmt.Println("Error readinga ll fromm from obj")
		return
	}
	err = os.WriteFile(object, data, 0644)
	if err != nil {
		fmt.Println("Error writing file: ", err)
	}
	fmt.Println("Wrote file successfully")
}
