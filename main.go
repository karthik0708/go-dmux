package main

import (
	"os"
)

//

// **************** Bootstrap ***********

func main() {
	args := os.Args[1:]
	sz := len(args)

	var path string

	if sz == 1 {
		path = args[0]
	}

	dconf := DMuxConfigSetting{
		FilePath: path,
	}
	//get configuration
	conf := dconf.GetDmuxConf()

	//start dmux for the first time
	conf.loadDmux(false)

	watcherCh := make(chan interface{})
	//start the file watcher
	go dconf.watchFile(watcherCh)

	//run the reloader in the background
	dconf.startReloader(watcherCh)
	//TODO make changes to listen to kill and reboot
}
