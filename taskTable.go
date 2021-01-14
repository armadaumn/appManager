package appManager

import (
  "sync"
  "errors"
  "log"
  spincomm "github.com/lei6669/appManager/spincomm"
  appcomm "github.com/lei6669/appManager/appcomm"
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
  var result []*appcomm.Task
  // Select task in taskTable
  for _, task := range t.tasks {
    // Check (1) appId (2) geo-locality (3) resource-availability (4) bandwidth (no way now)
    // 
    if task.status == "running"{
      result = append(result, &appcomm.Task{
        Ip: task.ip,
      })
      if len(result) >= numOfTasks {
        break
      }
    }
  }
  return &appcomm.TaskList{
    TaskList: result,
  }, nil
}
