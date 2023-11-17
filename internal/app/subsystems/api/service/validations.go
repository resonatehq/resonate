package service

import (
	"strings"

	"github.com/go-playground/validator/v10"
)

var OneOfCaseInsensitive validator.Func = func(fl validator.FieldLevel) bool {
	fieldValue := fl.Field().String()
	allowedValues := strings.Split(fl.Param(), " ")

	for _, allowedValue := range allowedValues {
		if strings.EqualFold(fieldValue, allowedValue) {
			return true
		}
	}

	return false
}
