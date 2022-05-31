package main

import (
	"context"
	"github.com/fsnotify/fsnotify"
	"github.com/go-dmux/logging"
	"github.com/slok/reload"
	"log"
)

//maintain controller and dmuxlogging separately as they won't be reloaded, they will just get updated
//dmuxSize represents the number of dmux connections
//prevChs are used to stop the connections that are running whenever a reload is triggered
var (
	controller Controller
	dmuxLogging *logging.DMuxLogging
	dmuxSize int
	prevChs interface{}
)

//loadDmux uses the dmux config and starts the application
func (conf DmuxConf) loadDmux(isReload bool) {
	//In case of reload updating the controller and restarting logging is sufficient
	if isReload{
		controller.config = conf
		dmuxLogging.Start(conf.Logging)

		chs := prevChs.([]chan interface{})

		//send signal to stop dmux connections that are running
		for _, ch := range chs{
			ch <- struct {}{}
		}
		log.Printf("reloaded config: %v", conf)
	} else {
		controller = Controller{config: conf}
		go controller.start()

		dmuxLogging = new(logging.DMuxLogging)
		dmuxLogging.Start(conf.Logging)

		log.Printf("config: %v", conf)
	}

	//update dmux size with new config
	dmuxSize = len(conf.DMuxItems)

	//create a new set of channels to communicate with new dmux connections
	newChs := make([]chan interface{}, dmuxSize)

	//start dmux connections
	for i, item := range conf.DMuxItems {
		newChs[i] = make(chan interface{})
		go func(connType ConnectionType, connConf interface{}, logDebug bool, ind int) {
			dmux := connType.Start(connConf, logDebug)
			for{
				<- newChs[ind]
				dmux.Stop()
				break
			}
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug, i)
	}

	//update the last set of channels
	prevChs = newChs
}

//watchFile checks for any write operations on the config file and signals the reloader
func (s DMuxConfigSetting) watchFile(watcherCh chan<- interface{}){
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("modified file:", event.Name)
					watcherCh<- struct{}{}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()
	err = watcher.Add(s.FilePath)
	if err != nil {
		log.Fatal(err)
	}
}

//startReloader adds components that are to be reloaded and the notifier gets triggered by a signal
//from file watcher which starts the reload process
func (s DMuxConfigSetting) startReloader(watcherCh<- chan interface{}) {
	//create manager
	reloadSvc := reload.NewManager()

	//Add the component to be reloaded
	reloadSvc.Add(0, reload.ReloaderFunc(func(ctx context.Context, id string) error {
		conf := s.GetDmuxConf()
		conf.loadDmux(true)
		return nil
	}))

	//check for signals from watcher and trigger notifier
	reloadSvc.On(reload.NotifierFunc(func(ctx context.Context) (string, error) {
		select{
		case <-ctx.Done():
		case <-watcherCh:
			return "reload", nil
		}
		return "", nil
	}))

	//Run manager to check the notifiers and initiate the reload process accordingly
	er := reloadSvc.Run(context.Background())
	if er != nil {
		log.Panic(er.Error())
	}
}