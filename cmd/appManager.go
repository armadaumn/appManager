package main

import (
	"log"
	"os"
	"strconv"

	"github.com/armadanet/appManager"
)

func main() {

	if len(os.Args) != 3 {
		log.Println("[spinner url] [#topN] ")
		return
	}
	spinnerURL := os.Args[1]
	topN, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Println("topN value format wrong")
		return
	}
	if topN < 1 || topN > 10 {
		log.Println("topN should be in the range of [1, 10]")
		return
	}

	// scaleN, err := strconv.Atoi(os.Args[2])
	// if err != nil {
	// 	log.Println("input wrong")
	// 	return
	// }

	// appManager.NewAppManagerServer(spinnerURL, scaleN).Run()
	appManager.NewAppManagerServer(spinnerURL, 0, topN).Run()
}
