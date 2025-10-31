package message

import (
	"fmt"
)

type Mesg struct {
	Type Type              `json:"type"`
	Head map[string]string `json:"head,omitempty"`
	Root string            `json:"root"`
	Leaf string            `json:"leaf"`
}

func (m *Mesg) String() string {
	return fmt.Sprintf("Mesg(type=%s, root=%s, leaf=%s)", m.Type, m.Root, m.Leaf)
}

type Type string

const (
	Invoke = "invoke"
	Resume = "resume"
	Notify = "notify"
)
