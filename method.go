package appManager

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	// "net"
	// "os"
	// "sync"
	//"time"
	// "os/signal"
	// "syscall"
	// "io"
	"log"
	"math/rand"
	"strings"

	// "fmt"
	appcomm "github.com/armadanet/appManager/appcomm"
	spincomm "github.com/armadanet/appManager/spincomm"
	"google.golang.org/grpc"
)

func (s *AppManagerServer) SubmitApplication(application *appcomm.Application, appStatus appcomm.ApplicationManager_SubmitApplicationServer) error {
	// Get server & request context
	reqCtx := appStatus.Context()
	serverCtx, cancel := context.WithCancel(s.ctx) // serverCtx derived from spinner server
	defer cancel()
	// store the application
	newApplication := &Application{
		appId:            application.AppId,
		numOfDuplication: int(application.NumOfDuplication),
		taskRequest:      application.TaskRequest,
	}
	if err := s.appTable.AddApplication(newApplication); err != nil {
		log.Println("rpc SubmitApplication(): AppId already exists in the system")
		return errors.New("duplicate AppId in the system")
	}
	// build connection to Spinner
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(s.spinnerURL, opts...)
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
	lat := originalRequest.Taskspec.DataSources.Lat + rand.Float64()*0.2
	lon := originalRequest.Taskspec.DataSources.Lon + rand.Float64()*0.2
	for i := 0; i < int(application.NumOfDuplication); i++ {
		// lat := rand.Float64() * 100
		// lon := rand.Float64() * 100
		tid := i + 1
		request := CopyRequest(originalRequest, "t"+strconv.Itoa(tid), lat, lon)
		// use a new routine to send out this request
		go s.SendTaskRequest(client, request, tid, originalRequest.AppId)
	}

	go s.repeatSendRequest(client, int(application.NumOfDuplication), originalRequest)

	// // TO BE MODIFIED: auto scaling should happen on the fly
	// // Here we just try to deploy more replicas on the same
	// for i := tid + 1; i <= tid+s.scaleN; i++ {
	// 	lat := originalRequest.Taskspec.DataSources.Lat + rand.Float64()*0.2
	// 	lon := originalRequest.Taskspec.DataSources.Lon + rand.Float64()*0.2
	// 	log.Print(lat)
	// 	log.Print(lat)
	// 	log.Println("=========")

	// 	request := CopyRequest(originalRequest, "t"+strconv.Itoa(i), lat, lon)
	// 	// Remove the "FirstDeployment" filter
	// 	request.Taskspec.Filters = []string{}
	// 	go func() {
	// 		log.Println("Submitting Duplicated task " + strconv.Itoa(tid) + " to Spinner")
	// 		spinnnerReqCtx := context.Background()
	// 		stream, err := client.Request(spinnnerReqCtx, request)
	// 		if err != nil {
	// 			log.Println("rpc SubmitApplication(): Send task request to Spinner fail")
	// 		}
	// 		for {
	// 			taskLog, err := stream.Recv()
	// 			if err != nil {
	// 				// Selected node already has this task
	// 				if strings.Contains(err.Error(), "task is present") {
	// 					log.Println("Selected node already has this task")
	// 					// no resource
	// 				} else if strings.Contains(err.Error(), "no resource") {
	// 					log.Println("No resource ==> task deployment fails")
	// 					// task fail ==> remove it
	// 				} else {
	// 					log.Println("rpc SubmitApplication(): Response from task request fail")
	// 					s.taskTable.RemoveTask("t" + strconv.Itoa(tid))
	// 				}
	// 				break
	// 			}
	// 			// Based on the taskLog, update the task table in application manager
	// 			newTask := &Task{
	// 				taskId: taskLog.TaskId,
	// 				appId:  originalRequest.AppId,
	// 				// Status of this task: created, scheduled, running, interrupt, finish, cancel, noResource
	// 				status:   "running", // assume the task must be running if response sent back
	// 				ip:       taskLog.Ip,
	// 				port:     taskLog.Port,
	// 				tag:      taskLog.Tag,
	// 				nodeType: int(taskLog.NodeType),
	// 				// add geoLocation of this task
	// 				geoLocation: &spincomm.Location{
	// 					Lat: taskLog.Location.Lat,
	// 					Lon: taskLog.Location.Lon,
	// 				},
	// 				cpuUtilization: taskLog.CpuUtilization,
	// 				assignedCpu:    int(taskLog.AssignedCpu),
	// 			}
	// 			log.Println("New task update: " + newTask.taskId.Value)
	// 			// add the resource map
	// 			resourceMap := make(map[string]*spincomm.ResourceStatus)
	// 			for key, value := range taskLog.HostResource {
	// 				resourceMap[key] = value
	// 			}
	// 			newTask.resourceUsage = resourceMap
	// 			// update the task update to task table
	// 			s.taskTable.AddTask(newTask)
	// 		}
	// 	}()
	// }

Loop:
	for {
		select {
		case <-reqCtx.Done():
			break Loop
		case <-serverCtx.Done():
			break Loop
		}
	}
	log.Println("Application Manager terminated")
	return nil
}

func (s *AppManagerServer) QueryTaskList(ctx context.Context, query *appcomm.Query) (*appcomm.TaskList, error) {

	// get the clientInfo
	newClient := &Client{
		clientId:    query.ClientId,
		appId:       query.AppId,
		tag:         query.Tag,
		geoLocation: query.GeoLocation,
	}
	s.clientTable.AddClient(newClient)
	// query from task table based on the
	result, err := s.taskTable.SelectTask(s.topN, newClient)
	if err != nil {
		log.Println("Task selection fail")
		return nil, errors.New("Task selection fail")
	}
	// return TaskList to requester
	return result, nil
}

////////////// helper function

func (s *AppManagerServer) SendTaskRequest(client spincomm.SpinnerClient, request *spincomm.TaskRequest, tid int, appId *spincomm.UUID) {
	log.Println("Submitting task " + request.TaskId.Value + " to Spinner")
	spinnnerReqCtx := context.Background()
	stream, err := client.Request(spinnnerReqCtx, request)
	if err != nil {
		log.Println("rpc SubmitApplication(): Send task request to Spinner fail")
	}
	var howMany int
	// Periodically receive task deployment update from Spinner
	for {
		taskLog, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "task is present") {
				// Selected node already has this task
				// log.Println("Selected node already has this task")
			} else if strings.Contains(err.Error(), "no resource") {
				// no resource
				// log.Println("No resource ==> task deployment fails")
			} else {
				// task fail ==> remove it
				howMany = s.taskTable.RemoveTask("t" + strconv.Itoa(tid))
				log.Println("[" + strconv.Itoa(howMany) + "] tasks in the system: REMOVE the task " + strconv.Itoa(tid))
			}
			break
		}
		// Based on the taskLog, update the task table in application manager
		newTask := &Task{
			taskId: taskLog.TaskId,
			appId:  appId,
			// Status of this task: created, scheduled, running, interrupt, finish, cancel, noResource
			status:   "running", // assume the task must be running if response sent back
			ip:       taskLog.Ip,
			port:     taskLog.Port,
			tag:      taskLog.Tag,
			nodeType: int(taskLog.NodeType),
			// add geoLocation of this task
			geoLocation: &spincomm.Location{
				Lat: taskLog.Location.Lat,
				Lon: taskLog.Location.Lon,
			},
			// task cpu utilization
			cpuUtilization: taskLog.CpuUtilization,
			assignedCpu:    int(taskLog.AssignedCpu),
		}
		// add the resource map
		resourceMap := make(map[string]*spincomm.ResourceStatus)
		for key, value := range taskLog.HostResource {
			resourceMap[key] = value
		}
		newTask.resourceUsage = resourceMap
		// update the task update to task table
		howMany = s.taskTable.AddTask(newTask)
		log.Println("[" + strconv.Itoa(howMany) + "] tasks in the system: Task UPDATE from " + newTask.taskId.Value)
	}
	// log.Println("One task request Connection to Spinner terminated")
}

func (s *AppManagerServer) repeatSendRequest(client spincomm.SpinnerClient, tid int, oldRequest *spincomm.TaskRequest) {
	lat := oldRequest.Taskspec.DataSources.Lat + rand.Float64()*0.2
	lon := oldRequest.Taskspec.DataSources.Lon + rand.Float64()*0.2
	for {
		tid = tid + 1
		request := CopyRequest(oldRequest, "t"+strconv.Itoa(tid), lat, lon)
		// use a new routine to send out this request
		go s.SendTaskRequest(client, request, tid, oldRequest.AppId)
		time.Sleep(1000 * time.Millisecond)
	}
}

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
