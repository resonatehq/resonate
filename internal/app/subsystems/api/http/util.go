package http

import "github.com/resonatehq/resonate/internal/util"

func ExtractId(id string) string {
	util.Assert(len(id) > 0 && id[0] == '/', "invalid id, gin trailing ids should start with '/'")
	return id[1:]
}
