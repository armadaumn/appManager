package appManager

import (
  "context"
  "errors"
  "encoding/json"
  "strconv"
  // "net"
  // "os"
  // "sync"
  //"time"
  // "os/signal"
  // "syscall"
  // "io"
  "math/rand"
  "log"
  // "fmt"
  "google.golang.org/grpc"
  spincomm "github.com/armadanet/appManager/spincomm"
  appcomm "github.com/armadanet/appManager/appcomm"
)

func (s *AppManagerServer) SubmitApplication(application *appcomm.Application, appStatus appcomm.ApplicationManager_SubmitApplicationServer) error {
  // Get server & request context
  reqCtx := appStatus.Context()
  serverCtx, cancel := context.WithCancel(s.ctx)  // serverCtx derived from spinner server
  defer cancel()
  // store the application
  newApplication := &Application{
    appId: application.AppId,
    numOfDuplication: int(application.NumOfDuplication),
    taskRequest: application.TaskRequest,
  }
  if err := s.appTable.AddApplication(newApplication); err != nil {
    log.Println("rpc SubmitApplication(): AppId already exists in the system")
    return errors.New("Duplicate AppId in the system")
  }
  // build connection to Spinner
  dialurl := "ec2-34-238-242-228.compute-1.amazonaws.com:5912"
  var opts []grpc.DialOption
  opts = append(opts, grpc.WithInsecure())
  conn, err := grpc.Dial(dialurl, opts...)
	if err != nil {
    log.Println("rpc SubmitApplication(): Connect to Spinner fails")
		return err
	}
  defer conn.Close()
  client := spincomm.NewSpinnerClient(conn)
  // get the taskRequest content from the initial application request
  originalRequest := CopyFromAppToSpin(newApplication.taskRequest)
  // TODO: split the geoLocation of the initial request

  // For each duplication, send a task request to Spinner
  for i := 0; i < int(application.NumOfDuplication); i++ {
    lat := originalRequest.Taskspec.DataSources.Lat + rand.Float64()*0.2
    lon := originalRequest.Taskspec.DataSources.Lon + rand.Float64()*0.2
    log.Print(lat)
    log.Print(lat)
    log.Println("=========")
    // lat := rand.Float64() * 100
    // lon := rand.Float64() * 100
    tid := i+1
    request := CopyRequest(originalRequest, "t"+strconv.Itoa(tid), lat, lon)
    // use a new routine to send out this request
    go func() {
      log.Println("Submitting task "+strconv.Itoa(tid)+" to Spinner")
      spinnnerReqCtx := context.Background()
      stream, err := client.Request(spinnnerReqCtx, request)
      if err != nil {
    		log.Println("rpc SubmitApplication(): Send task request to Spinner fail")
      }
      // Periodically receive task deployment update from Spinner
      for {
    		taskLog, err := stream.Recv()
    		if err != nil {
          // no resource
          if err.Error() == "no resource" {
            log.Println("No resource ==> task deployment fails")
          // task fail ==> remove it
          } else {
            log.Println("rpc SubmitApplication(): Response from task request fail")
            s.taskTable.RemoveTask(strconv.Itoa(tid))
          }
          break
    		}
        // Based on the taskLog, update the task table in application manager
        newTask := &Task{
          taskId: taskLog.TaskId,
          appId: originalRequest.AppId,
          // Status of this task: created, scheduled, running, interrupt, finish, cancel, noResource
          status: "running",  // assume the task must be running if response sent back
          ip: taskLog.Ip,
          port: taskLog.Port,
          // TODO: add geoLocation from response TaskLog
          geoLocation: &spincomm.Location{
            Lat: 1.1,
            Lon: 1.1,
          },
        }
        log.Println("New task update: "+newTask.taskId.Value)
        // add the resource map
        resourceMap := make(map[string]*spincomm.ResourceStatus)
        for key, value := range taskLog.HostResource {
          resourceMap[key] = value
        }
        newTask.resourceUsage = resourceMap
        // update the task update to task table
        s.taskTable.AddTask(newTask)
    	}
      log.Println("One task request Connection to Spinner terminated")
    }()
  }

  Loop:
    for {
      select {
      case <- reqCtx.Done():
        break Loop
      case <- serverCtx.Done():
        break Loop
      }
    }
  log.Println("Application Manager terminated")
  return nil
}

func (s *AppManagerServer) QueryTaskList(ctx context.Context, query *appcomm.Query) (*appcomm.TaskList, error) {
  log.Println(query.ClientId)
  // get the clientInfo
  newClient := &Client{
    clientId: query.ClientId,
    appId: query.AppId,
    geoLocation: query.GeoLocation,
  }
  s.clientTable.AddClient(newClient)
  // query from task table based on the
  result, err := s.taskTable.SelectTask(3, newClient)
  if err != nil {
    log.Println("Task selection fail")
    return nil, errors.New("Task selection fail")
  }
  // return TaskList to requester
  return result, nil
}

////////////// helper function

func CopyFromAppToSpin(re *appcomm.TaskRequest) *spincomm.TaskRequest {
  req := new(spincomm.TaskRequest)
  buffer, _ := json.Marshal(re)
  json.Unmarshal([]byte(buffer), req)
  return req
}

func CopyRequest(tq *spincomm.TaskRequest, taskID string, lat float64, lon float64) *spincomm.TaskRequest {
	req := new(spincomm.TaskRequest)
	buffer, _ := json.Marshal(tq)
	json.Unmarshal([]byte(buffer), req)
	req.TaskId = &spincomm.UUID{Value: taskID}
	req.GetTaskspec().DataSources = &spincomm.Location{Lat: lat, Lon: lon}
	return req
}
