package util

import (
	"fmt"
	"math/rand" // nosemgrep
	"strconv"
	"strings"

	"github.com/resonatehq/resonate/test/dst"
)

type RangeIntFlag struct {
	Min int
	Max int
}

func (f *RangeIntFlag) String() string {
	if f.Max == f.Min+1 {
		return fmt.Sprintf("%d", f.Min)
	}

	return fmt.Sprintf("%d:%d", f.Min, f.Max)
}

func (f *RangeIntFlag) Type() string {
	return "range"
}

func (f *RangeIntFlag) Set(s string) error {
	r := strings.Split(s, ":")
	if len(r) != 1 && len(r) != 2 {
		return fmt.Errorf("range flag can contain 1 or 2 values")
	}

	var err error

	f.Min, err = strconv.Atoi(r[0])
	if err != nil {
		return err
	}

	if len(r) == 2 {
		f.Max, err = strconv.Atoi(r[1])
		if err != nil {
			return err
		}
	} else {
		f.Max = f.Min + 1
	}

	return nil
}

func (f *RangeIntFlag) UnmarshalText(text []byte) error {
	return f.Set(string(text))
}

func (f *RangeIntFlag) Resolve(r *rand.Rand) int {
	return dst.RangeIntn(r, f.Min, f.Max)
}
