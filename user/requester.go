package main

import (
	"context"
	"log"
	"strconv"

	"google.golang.org/grpc"

	// appcomm "github.com/lei6669/appManager/appcomm"
	appcomm "github.com/armadanet/appManager/user/appcomm"
)

func main() {
	// Connect Application manager
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	// conn, err := grpc.Dial("localhost:8888", opts...)
	conn, err := grpc.Dial("3.91.145.215:8888", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := appcomm.NewApplicationManagerClient(conn)
	ctx := context.Background()

	// Call Query() connection
	list, err := client.QueryTaskList(ctx, &appcomm.Query{
		ClientId: &appcomm.UUID{Value: strconv.Itoa(1)},
		// Rochester
		// GeoLocation: &appcomm.Location{
		// 	Lat: 44.02,
		// 	Lon: -92.47,
		// },
		// Minnespolis
		GeoLocation: &appcomm.Location{
			Lat: 44.98,
			Lon: -93.24,
		},
		// Duluth
		// GeoLocation: &appcomm.Location{
		// 	Lat: 46.79,
		// 	Lon: -92.11,
		// },
		Tag:   []string{"This is a tag"},
		AppId: &appcomm.UUID{Value: strconv.Itoa(1)},
	})
	if err != nil {
		log.Println(err)
		return
	}
	taskList := list.TaskList
	// print out deploy results
	for i := 0; i < len(taskList); i++ {
		log.Println(taskList[i].Ip + ":" + taskList[i].Port)
	}

}
