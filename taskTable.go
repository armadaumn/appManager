package appManager

import (
	"errors"
	"log"
	"sort"
	"sync"

	appcomm "github.com/armadanet/appManager/appcomm"
	spincomm "github.com/armadanet/appManager/spincomm"
	"github.com/mmcloughlin/geohash"
)

// Define task table
type TaskTable struct {
	mutex *sync.Mutex
	tasks map[string]*Task
}

type Task struct {
	// id of the task
	taskId *spincomm.UUID
	// app id of this task
	appId *spincomm.UUID
	// Status of this task: created, scheduled, running, interrupt, finish, cancel, noResource
	status string
	// IP address of this task (with default port)
	ip string
	// port
	port string
	// info about LAN
	tag []string
	// public server or PC
	nodeType int
	// the geolocation of this task
	geoLocation *spincomm.Location
	// real-time resource usuage
	resourceUsage map[string]*spincomm.ResourceStatus

	// real-time task cpu usuage
	cpuUtilization float64
	// task assigned cpu
	assignedCpu int
}

// Add new task into task table
func (t *TaskTable) AddTask(ta *Task) int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	// if _, ok := t.tasks[ta.taskId]; ok {
	// 	return errors.New("Task id already exists in the task table")
	// }
	t.tasks[ta.taskId.Value] = ta
	return len(t.tasks)
}

func (t *TaskTable) RemoveTask(taskId string) int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	log.Println("Removing task: " + taskId)
	delete(t.tasks, taskId)
	return len(t.tasks)
}

// Leak: lock holds too long if there are a lot of tasks
func (t *TaskTable) SelectTask(numOfTasks int, clientInfo *Client) (*appcomm.TaskList, error) {

	type info struct {
		task     *appcomm.Task
		distance int
		score    float64
	}

	// Candidate task without specified tag
	regularList := make([]info, 0)
	// Candidate task with specified tag
	tagList := make([]info, 0)
	// Selected Task list
	finalResult := make([]*appcomm.Task, 0)
	// Client geo info
	sourceGeoID := geohash.Encode(clientInfo.geoLocation.Lat, clientInfo.geoLocation.Lon)
	// Client tag
	useLAN := false
	var tag string
	if len(clientInfo.tag) != 0 {
		useLAN = true
		tag = clientInfo.tag[0]
	}

	t.mutex.Lock()
	if numOfTasks > len(t.tasks) {
		t.mutex.Unlock()
		return nil, errors.New("not enough tasks in the system")
	}

	// Traverse all tasks in task table
	for _, task := range t.tasks {

		// Check (1) appId (2) running status
		if task.appId.Value != clientInfo.appId.Value || task.status != "running" {
			continue
		}

		// Calculate the average cpu usage during a period T
		// Total cpu on this node
		totalCpuOnNode := task.resourceUsage["CPU"].Total
		// Used cpu for this task
		used := task.cpuUtilization / 100.0 * float64(totalCpuOnNode)
		// availale cpu for this task
		availCpu := float64(task.assignedCpu) - used

		// availCpu := float64(task.resourceUsage["CPU"].Total) * task.resourceUsage["CPU"].Available / 100.0
		// availMem := float64(task.resourceUsage["Memory"].Total) * task.resourceUsage["Memory"].Available / 100.0

		///////////////////////////////// DEBUG ///////////////////////////////////
		// fmt.Printf("Task %s: CPU %f Memory %f\n", task.taskId.Value, availCpu, availMem)
		log.Printf("	[Task selction log] Task %s -- Assigned: %v -- Total: %v -- Used: %v\n", task.taskId.Value, task.assignedCpu, totalCpuOnNode, used)
		///////////////////////////////////////////////////////////////////////////

		// (1) tag (2) geo-locality (3) resource-availability (cpu + *memory + *gpu) (4) node type (5) *bandwidth

		// Calculate the relative distance
		taskGeoID := geohash.Encode(task.geoLocation.GetLat(), task.geoLocation.GetLon())
		distance := proximityComparison([]rune(sourceGeoID), []rune(taskGeoID))
		// calculate score based on resource and node type
		var typeScore float64
		if task.nodeType == 2 {
			typeScore = 4
		} else {
			typeScore = 2
		}

		candidate := info{
			task:     &appcomm.Task{Ip: task.ip, Port: task.port},
			distance: distance,
			score:    0.5*availCpu + 0.5*typeScore,
		}
		log.Printf("	[Task selction log] %s:%s distance %d, score %f", task.ip, task.port, distance, candidate.score)

		if len(task.tag) == 0 || !(useLAN && tag == task.tag[0]) {
			regularList = append(regularList, candidate)
		} else {
			tagList = append(tagList, candidate)
		}
	}
	t.mutex.Unlock()

	if len(tagList)+len(regularList) < numOfTasks {
		return nil, errors.New("not enough tasks for this application in the system")
	}
	// Calculate the candidate score
	// (1) Geo-proximity
	// (2) resource + node type

	// First add lan node
	numberOfLANServer := len(tagList)
	for i := 0; i < numberOfLANServer; i++ {
		// if there is a task of this node already exists
		if nodeAlreadyExist(finalResult, tagList[i].task) {
			continue
		}
		finalResult = append(finalResult, tagList[i].task)
		if len(finalResult) == numOfTasks {
			return &appcomm.TaskList{
				TaskList: finalResult,
			}, nil
		}
	}

	// Second add regular node
	// Sort the regular list - sort by distance and then sort by (resource + node_type)
	sort.Slice(regularList, func(i, j int) bool {
		if regularList[i].distance > regularList[j].distance {
			return true
		} else if regularList[i].distance < regularList[j].distance {
			return false
		} else {
			return regularList[i].score > regularList[j].score
		}
	})

	numberOfRegularServer := len(regularList)
	for i := 0; i < numberOfRegularServer; i++ {

		if nodeAlreadyExist(finalResult, regularList[i].task) {
			continue
		}
		finalResult = append(finalResult, regularList[i].task)
		if len(finalResult) == numOfTasks {
			return &appcomm.TaskList{
				TaskList: finalResult,
			}, nil
		}
	}

	// not enough nodes
	return nil, errors.New("not enough tasks in the system after scanning everything")

	// // First check if LAN already has 3 nodes
	// numberOfLANServer := len(tagList)
	// enoughLAN := false
	// if numberOfLANServer >= numOfTasks {
	// 	numberOfLANServer = numOfTasks
	// 	enoughLAN = true
	// }

	// // TODO: if we have multiple LAN node, sort them with score
	// for i := 0; i < numberOfLANServer; i++ {
	// 	finalResult[i] = tagList[i].task
	// }

	// // If LAN node < 3, then fill out the rest with WAN node
	// if !enoughLAN {
	// 	// sort by distance and then sort by (resource + node_type)
	// 	sort.Slice(regularList, func(i, j int) bool {
	// 		if regularList[i].distance > regularList[j].distance {
	// 			return true
	// 		} else if regularList[i].distance < regularList[j].distance {
	// 			return false
	// 		} else {
	// 			return regularList[i].score > regularList[j].score
	// 		}
	// 	})

	// 	for i := numberOfLANServer; i < numOfTasks; i++ {
	// 		finalResult[i] = regularList[i-numberOfLANServer].task
	// 	}
	// }

	// return &appcomm.TaskList{
	// 	TaskList: finalResult,
	// }, nil
}

// Helper function
func nodeAlreadyExist(finalResult []*appcomm.Task, task *appcomm.Task) bool {
	for i := 0; i < len(finalResult); i++ {
		if finalResult[i].Ip == task.Ip {
			return true
		}
	}
	return false
}

// Helper function
func proximityComparison(ghSrc, ghDst []rune) int {
	ghSrcLen := len(ghSrc)

	prefixMatchCount := 0

	for i := 0; i < ghSrcLen; i++ {
		if ghSrc[i] == ghDst[i] {
			prefixMatchCount++
		} else {
			break
		}
	}
	return prefixMatchCount
}
