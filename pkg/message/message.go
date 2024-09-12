package message

import (
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Mesg struct {
	Type Type   `json:"type"`
	Root string `json:"root"`
	Leaf string `json:"leaf"`
}

func (m *Mesg) String() string {
	return string(m.Type)
}

func (m *Mesg) WithPromises(root *promise.Promise, leaf *promise.Promise) *MesgWithPromises {
	util.Assert(root == nil || root.Id == m.Root, "root id must match")
	util.Assert(leaf == nil || leaf.Id == m.Leaf, "leaf id must match")

	return &MesgWithPromises{
		Type:     m.Type,
		Promises: map[string]*promise.Promise{"root": root, "leaf": leaf},
	}
}

type MesgWithPromises struct {
	Type     Type                        `json:"type"`
	Promises map[string]*promise.Promise `json:"promises"`
}

type Type string

const (
	Invoke = "invoke"
	Resume = "resume"
)
