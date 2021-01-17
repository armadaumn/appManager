package appManager

import (
  "errors"
  "log"
  // "sort"
  "sync"
  // "github.com/mmcloughlin/geohash"
  spincomm "github.com/armadanet/appManager/spincomm"
  appcomm "github.com/armadanet/appManager/appcomm"
)

// Define task table
type TaskTable struct {
  mutex           *sync.Mutex
  tasks           map[string]*Task
}

type Task struct {
  // id of the task
  taskId              *spincomm.UUID
  // app id of this task
  appId               *spincomm.UUID
  // Status of this task: created, scheduled, running, interrupt, finish, cancel, noResource
  status              string
  // IP address of this task (with default port)
  ip                  string
  // port
  port                string
  // the geolocation of this task
  geoLocation         *spincomm.Location
  // real-time resource usuage
  resourceUsage       map[string]*spincomm.ResourceStatus
}

// Add new task into task table
func (t *TaskTable) AddTask(ta *Task) error {
  t.mutex.Lock()
  defer t.mutex.Unlock()
  // if _, ok := t.tasks[ta.taskId]; ok {
	// 	return errors.New("Task id already exists in the task table")
	// }
	t.tasks[ta.taskId.Value] = ta
	return nil
}

func (t *TaskTable) RemoveTask(taskId string) error {
  t.mutex.Lock()
  defer t.mutex.Unlock()
  log.Println("Removing task: "+taskId)
  t.tasks[taskId].status = "failed"
  return nil
}

// Leak: lock holds too long if there are a lot of tasks
func (t *TaskTable) SelectTask(numOfTasks int, clientInfo *Client) (*appcomm.TaskList, error) {
  t.mutex.Lock()
  defer t.mutex.Unlock()
  if numOfTasks > len(t.tasks) {
    return nil, errors.New("Not enough tasks in the system")
  }


  var finalResult []*appcomm.Task
  for _, value := range t.tasks {
    finalResult = append(finalResult, &appcomm.Task{
      Ip: value.ip,
      Port: value.port,
    })
  }
  //log.Print(finalResult[0].Ip)
  //
  // type info struct {
  //   task  *appcomm.Task
  //   score float64
  // }
  // bestResult := make([]info, 0)
  // subResult := make([]info, 0)
  // finalResult := make([]*appcomm.Task, numOfTasks, numOfTasks)
  // sourceGeoID := geohash.Encode(clientInfo.geoLocation.Lat, clientInfo.geoLocation.Lon)
  // // Select task in taskTable
  // for _, task := range t.tasks {
  //   // Check (1) appId (2) geo-locality (3) resource-availability (4) bandwidth (no way now)
  //
  //   // Status check
  //   if task.status != "running" {
  //     continue
  //   }
  //
  //   // Resource Check
  //   availCpu := float64(task.resourceUsage["CPU"].Total) * task.resourceUsage["CPU"].Available
  //   availMem := float64(task.resourceUsage["Memory"].Total) * task.resourceUsage["Memory"].Available
  //   requireCpu := 2.0
  //   requireMem := 1000000000.0
  //   if availCpu < requireCpu || availMem < requireMem{
  //     subResult = append(subResult, info{
  //       task: &appcomm.Task{Ip: task.ip, Port: task.port},
  //       score: 0.5 * availCpu / requireCpu + 0.5 * availMem / requireMem,
  //     })
  //   } else {
  //     taskGeoID := geohash.Encode(task.geoLocation.GetLat(), task.geoLocation.GetLon())
  //     distance := proximityComparison([]rune(sourceGeoID), []rune(taskGeoID))
  //     bestResult = append(bestResult, info{
  //       task: &appcomm.Task{Ip: task.ip, Port: task.port},
  //       score: float64(distance),
  //     })
  //   }
  // }
  //
  // if len(bestResult) >= numOfTasks {
  //   // Select tasks with least distance
  //   sort.Slice(bestResult, func(i, j int) bool { return bestResult[i].score < bestResult[j].score })
  //   for i:=0; i < numOfTasks; i++ {
  //     finalResult[i] = bestResult[i].task
  //   }
  // } else {
  //   // Select tasks from less optimal list
  //   sort.Slice(subResult, func(i, j int) bool { return subResult[i].score < subResult[j].score })
  //   i := 0
  //   for i < len(bestResult) {
  //     finalResult[i] = bestResult[i].task
  //     i++
  //   }
  //   for i < numOfTasks {
  //     finalResult[i] = subResult[i - len(bestResult)].task
  //     i++
  //   }
  // }

  return &appcomm.TaskList{
    TaskList: finalResult,
  }, nil
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
