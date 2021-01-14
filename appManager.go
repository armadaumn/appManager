package appManager

import (
  "context"
  "net"
  "os"
  "sync"
  //"time"
  "os/signal"
  "syscall"
  // "io"
  "log"
  "fmt"
  "google.golang.org/grpc"
  // spincomm "github.com/lei6669/simulateArmada/appManager/spincomm"
  appcomm "github.com/lei6669/appManager/appcomm"
)

type AppManagerServer struct {
	appcomm.UnimplementedApplicationManagerServer
  // appManager root context
  ctx             context.Context
  // Application table
  appTable        *AppTable
  // Task table
  taskTable       *TaskTable
  // Client table
  clientTable     *ClientTable
}

func NewAppManagerServer() (*AppManagerServer) {
  // Init appManager server
  appManagerServer := &AppManagerServer{
    appTable:    &AppTable{
      mutex:        &sync.Mutex{},
      applications: make(map[string]*Application),
    },
    taskTable:    &TaskTable{
      mutex:        &sync.Mutex{},
      tasks:        make(map[string]*Task),
    },
    clientTable:    &ClientTable{
      mutex:        &sync.Mutex{},
      clients:      make(map[string]*Client),
    },
  }
  return appManagerServer
}

func (server *AppManagerServer) Run() {
  // Set to accept os signal channel
  signalChan := make(chan os.Signal, 1)
  signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

  // Initializing application manager server
  port := 8888
  lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
  var opts []grpc.ServerOption
  grpcServer := grpc.NewServer(opts...)

  // Create spinner base context
  ctx, cancel := context.WithCancel(context.Background())
  server.ctx = ctx

  // appManager WaitGroup used to track 2 subroutines in spinner: server, adaptator
  var appManagerWaitGroup sync.WaitGroup
  // 2) Start appManager Server routine
  appcomm.RegisterApplicationManagerServer(grpcServer, server)
  go func() {
    appManagerWaitGroup.Add(1)
    defer appManagerWaitGroup.Done()
    if err:=grpcServer.Serve(lis); err!=nil {
      cancel()
    }
  }()
  log.Println("AppManager server up and running...")

  // Two cases Spinner will shut down: 1) Self shut down 2) Server start fails
  select {
  case <- signalChan:
    log.Println("\nSelf shutting down...")
    // Notify all rpc routines to stop though context
    cancel()
    // GracefulStop() make server stop accepting new requests and wait current ones finish
    grpcServer.GracefulStop()
    // Wait server, scheduler, monitor routines terminate
    appManagerWaitGroup.Wait()
    log.Println("AppManager successfully shut down")
    return
  case <- ctx.Done():
    log.Println("Server start fails")
    return
  }
}

// Count Wait Group
type CountWaitGroup struct {
  mutex           *sync.Mutex
  counter         int
  // change to false after schedulerChan is closed
  chanAvailable   bool
}

func (g *CountWaitGroup) Size() int {
  g.mutex.Lock()
  defer g.mutex.Unlock()
  return g.counter
}

func (g *CountWaitGroup) Add() {
  g.mutex.Lock()
  defer g.mutex.Unlock()
  g.counter++
}

func (g *CountWaitGroup) Done() {
  g.mutex.Lock()
  defer g.mutex.Unlock()
  g.counter--
}

func (g *CountWaitGroup) Close() {
  g.mutex.Lock()
  defer g.mutex.Unlock()
  g.chanAvailable = false
}

func (g *CountWaitGroup) AddWhenAvailable() bool {
  g.mutex.Lock()
  defer g.mutex.Unlock()
  // Add into group if channel haven't closed
  if g.chanAvailable {
    g.counter++
    return true
  } else {
    return false
  }
}
