package model

import "strings"

func originOf(id string) string {
	if i := strings.IndexByte(id, ':'); i >= 0 {
		return id[:i]
	}
	return id
}

func sameOrigin(a, b string) bool { return originOf(a) == originOf(b) }

func originTagOK(id string, tags Tags) bool {
	origin, ok := tags.Get("resonate:origin")
	if !ok {
		return true
	}
	if strings.ContainsRune(origin, ':') {
		return false
	}
	return id == origin || strings.HasPrefix(id, origin+":")
}

func rejectedBySchema(o Op) bool {
	switch o.Kind {
	case "promise.create":
		return !originTagOK(o.ID, o.Tags)
	case "task.create", "task.fence":
		return o.Action != nil && o.Action.Kind == "promise.create" &&
			!originTagOK(o.Action.ID, o.Action.Tags)
	case "promise.register_callback":
		return o.ID == o.Awaiter || !sameOrigin(o.ID, o.Awaiter)
	case "task.suspend":
		for _, a := range o.Awaited {
			if a == o.ID || !sameOrigin(a, o.ID) {
				return true
			}
		}
	case "task.heartbeat":
		for _, ref := range o.Refs {
			if len(o.Refs) > 0 && !sameOrigin(ref.ID, o.Refs[0].ID) {
				return true
			}
		}
	}
	return false
}
