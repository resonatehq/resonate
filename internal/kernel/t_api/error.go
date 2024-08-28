package t_api

type Error struct {
	code          StatusCode
	originalError error
}

func NewError(code StatusCode, error error) *Error {
	return &Error{
		code:          code,
		originalError: error,
	}
}

func (e *Error) Code() StatusCode {
	return e.code
}

func (e *Error) Error() string {
	return e.code.String()
}

func (e *Error) Unwrap() error {
	return e.originalError
}

func (e *Error) Is(target error) bool {
	_, ok := target.(*Error)
	return ok
}
