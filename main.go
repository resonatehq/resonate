package main

import (
	"github.com/resonatehq/resonate/cmd"
)

func main() {
	m := map[string]string{"a": "a", "b": "b"}

	for k, v := range m {
		println(k, v)
	}

	cmd.Execute()
}
