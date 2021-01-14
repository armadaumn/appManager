package appManager

import (
  "sync"
  "errors"
  //"log"
  appcomm "github.com/lei6669/appManager/appcomm"
)

// Define application table
type AppTable struct {
  mutex           *sync.Mutex
  applications    map[string]*Application
}

type Application struct {
  // Application id
  appId               *appcomm.UUID
  // number of duplications
  numOfDuplication    int
  // requirement of the task
  taskRequest         *appcomm.TaskRequest
}

// add an application into the table
func (a *AppTable) AddApplication(app *Application) error {
  a.mutex.Lock()
  defer a.mutex.Unlock()
  if _, ok := a.applications[app.appId.Value]; ok {
		return errors.New("Application id already exists in the application table")
	}
	a.applications[app.appId.Value] = app
	return nil
}
