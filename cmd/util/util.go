package util

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

func Unwrap[T comparable](value T) *T {
	var zero T
	if value == zero {
		return nil
	}
	return &value
}

func Errorf(format string, v ...any) {
	out := fmt.Sprintf(format, v...)
	fmt.Println(out)
	os.Exit(1)
}

func Write(cmd *cobra.Command, writer io.Writer, format string, v ...any) {
	out := fmt.Sprintf(format, v...)
	_, _ = cmd.OutOrStdout().Write([]byte(out))
}
