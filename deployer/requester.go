package main

import (
  "context"
  "log"
  //"os"
  //"os/signal"
  //"syscall"
  "google.golang.org/grpc"
  appcomm "github.com/armadanet/appManager/appcomm"
)

func main() {
  // Connect Application manager
  var opts []grpc.DialOption
  opts = append(opts, grpc.WithInsecure())
  opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial("localhost:8888", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
  client := appcomm.NewApplicationManagerClient(conn)
  // // Set os signal channel
  // signalChan := make(chan os.Signal, 1)
  // signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
  // Get client context
  ctx := context.Background()
  // construct the Original task request
  taskSpec := appcomm.TaskSpec{
		// Filters:     []string{"Resource", "Affinity"},
    Filters:     []string{"Resource", "Affinity"},
    Sort:        "Geolocation",
		ResourceMap: map[string]*appcomm.ResourceRequirement{},
		Ports:       map[string]string{},
		IsPublic:    false,
		NumReplicas: 1,
		// CargoSpec: &appcomm.CargoReq{
		// 	Size:     1,
		// 	NReplica: 3,
		// },
		DataSources: &appcomm.Location{Lat: 44.9583511, Lon: -93.18629},
	}
	taskSpec.ResourceMap["CPU"] = &appcomm.ResourceRequirement{
		Weight:    0.5,
		Requested: 2,
		Required:  true,
	}
	taskSpec.ResourceMap["Memory"] = &appcomm.ResourceRequirement{
		Weight:    0.5,
		Requested: 2000000,  // Bytes
		Required:  true,
	}
	taskSpec.Ports["80"] = ""
	request := &appcomm.TaskRequest{
		AppId:    &appcomm.UUID{Value: "App_1"},
		Image:    "nikhs247/armadataskimage",
		Command:  []string{"0.0.0.0", "8080"},
		Tty:      true,
		Limits:   &appcomm.TaskLimits{CpuShares: 2},
		Taskspec: &taskSpec,
		Port:     8080,
		TaskId:   &appcomm.UUID{Value: "task_0"},
	}

  // Start Request() connection
  appStatus, err := client.SubmitApplication(ctx, &appcomm.Application{
    AppId:            &appcomm.UUID{Value: "App_1"},
    NumOfDuplication: 3,
    TaskRequest:      request,
  })
  if err != nil {
    log.Println(err)
    return
  }

  // print out deploy results
  for {
    deployResult, err := appStatus.Recv()
    if err != nil {
      log.Println(err)
      return
    }
    log.Println(deployResult)
  }

}
