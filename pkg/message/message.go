package message

import (
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Mesg struct {
	Type     Type                        `json:"type"`
	Root     string                      `json:"root,omitempty"`
	Leaf     string                      `json:"leaf,omitempty"`
	Promises map[string]*promise.Promise `json:"promises,omitempty"`
}

func (m *Mesg) String() string {
	return string(m.Type)
}

func (m *Mesg) SetPromises(root *promise.Promise, leaf *promise.Promise) {
	util.Assert(root == nil || root.Id == m.Root, "root id must match")
	util.Assert(leaf == nil || leaf.Id == m.Leaf, "leaf id must match")

	// hack, unset root and leaf
	m.Root = ""
	m.Leaf = ""

	// set promises
	m.Promises = map[string]*promise.Promise{
		"root": root,
		"leaf": leaf,
	}
}

type Type string

const (
	Invoke = "invoke"
	Resume = "resume"
)
