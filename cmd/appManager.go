package main

import (
	"log"
	"os"
	"strconv"

	"github.com/armadanet/appManager"
)

func main() {

	if len(os.Args) != 2 {
		log.Println("Need input # of additional task deployment requests")
		return
	}
	scaleN, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Println("input wrong")
		return
	}

	appManager.NewAppManagerServer(scaleN).Run()
}
