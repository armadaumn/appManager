package appManager

import (
  "sync"
  //"errors"
  //"log"
  appcomm "github.com/lei6669/appManager/appcomm"
)

// Define client table
type ClientTable struct {
  mutex           *sync.Mutex
  clients         map[string]*Client
}

type Client struct {
  // Unique id of client
  clientId            *appcomm.UUID
  // Application id of this task
  appId               *appcomm.UUID
  // geoLocation of the client
  geoLocation         *appcomm.Location
}

// add an application into the table
func (c *ClientTable) AddClient(cli *Client) error {
  c.mutex.Lock()
  defer c.mutex.Unlock()
  // if _, ok := c.clients[cli.clientId]; ok {
	// 	return errors.New("Client id already exists in the client table")
	// }
	c.clients[cli.clientId.Value] = cli
	return nil
}
