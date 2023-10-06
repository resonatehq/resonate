package t_aio

import "fmt"

type EchoSubmission struct {
	Data string
}

func (s *EchoSubmission) String() string {
	return fmt.Sprintf("Echo(data=%s)", s.Data)
}

type EchoCompletion struct {
	Data string
}

func (c *EchoCompletion) String() string {
	return fmt.Sprintf("Echo(data=%s)", c.Data)
}
