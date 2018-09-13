package main

import (
	"fmt"

	"github.com/johnshiver/plankton/config"
)

func main() {
	c := config.GetConfig()
	fmt.Println(c)
}
