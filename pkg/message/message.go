package message

import (
	"fmt"
)

type Mesg struct {
	Type Type   `json:"type"`
	Root string `json:"root"`
	Leaf string `json:"leaf"`
}

func (m *Mesg) String() string {
	return fmt.Sprintf("Mesg(type=%s, root=%s, leaf=%s)", m.Type, m.Root, m.Leaf)
}

type Type string

const (
	Invoke = "invoke"
	Resume = "resume"
)
