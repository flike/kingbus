// Copyright 2018 The kingbus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/server"
)

var (
	//BuildDate used for generating build date in make command
	BuildDate string
	//BuildVersion used for generating kingbus version in make command
	BuildVersion string
)

const banner string = `
   __   _             __
  / /__(_)___  ____ _/ /_  __  _______
 / //_/ / __ \/ __ '/ __ \/ / / / ___/
/ ,< / / / / / /_/ / /_/ / /_/ (__  )
/_/|_/_/_/ /_/\__, /_.___/\__,_/____/
            /____/
`

func main() {
	configFile := flag.String("config", "./kingbus.yaml", "kingbus config file")
	printVersion := flag.Bool("version", false, "print kingbus version info")
	flag.Parse()

	//only print version
	if *printVersion {
		fmt.Printf("version is %s, build at %s\n", BuildVersion, BuildDate)
		return
	}

	//fmt.Print(banner)
	fmt.Printf("version is %s, build at %s\n", BuildVersion, BuildDate)

	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}
	serverCfg, err := config.NewKingbusServerConfig(*configFile)
	if err != nil {
		fmt.Printf("NewKingbusServerConfig error,err:%s\n", err.Error())
		return
	}

	//init log
	log.InitLoggers(serverCfg.LogDir, serverCfg.LogLevel)
	defer log.UnInitLoggers()

	ks, err := server.NewKingbusServer(serverCfg)
	if err != nil {
		log.Log.Fatalf("main:NewKingbusServer error,err:%s", err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	go func() {
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				ks.Stop()
				os.Exit(0)
			}
		}
	}()

	ks.Run()
}
