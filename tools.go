package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println(time.Now().Add(2 * time.Minute).Unix())
}
