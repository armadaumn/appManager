package main

import (
	"log"
	"os"
	"strconv"

	"github.com/armadanet/appManager"
)

func main() {

	if len(os.Args) != 3 {
		log.Println("[spinner url] [#duplicated task] ")
		return
	}
	spinnerURL := os.Args[1]
	scaleN, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Println("input wrong")
		return
	}

	appManager.NewAppManagerServer(spinnerURL, scaleN).Run()
}
