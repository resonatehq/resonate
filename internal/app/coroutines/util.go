package coroutines

type inflight map[string]bool

func (i inflight) get(id string) bool {
	return i[id]
}

func (i inflight) add(id string) {
	i[id] = true
}

func (i inflight) remove(id string) {
	delete(i, id)
}
