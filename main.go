package main

import (
	"fmt"
	"io"
	"os"
)

func main() {

	var w io.Writer
	w = os.Stdout

	fmt.Printf("%v, %T\n", w, w)
	rw := w.(io.ReadWriter)
	fmt.Printf("%v, %T\n", rw, rw)
}
