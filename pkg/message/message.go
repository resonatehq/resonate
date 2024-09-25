package message

import (
	"fmt"

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
	return fmt.Sprintf("Mesg(type=%s, root=%s, leaf=%s)", m.Type, m.Root, m.Leaf)
}

func (m *Mesg) SetPromises(root *promise.Promise, leaf *promise.Promise) {
	util.Assert(root == nil || root.Id == m.Root, "root id must match")
	util.Assert(leaf == nil || leaf.Id == m.Leaf, "leaf id must match")

	// set promises
	m.Promises = map[string]*promise.Promise{}

	if root != nil {
		m.Promises["root"] = root
	}
	if leaf != nil {
		m.Promises["leaf"] = leaf
	}
}

type Type string

const (
	Invoke = "invoke"
	Resume = "resume"
)
