package types

import (
	"encoding/json"

	"github.com/golang-jwt/jwt"
)

var (
	signingMethod = jwt.SigningMethodHS256
	secretKey     = []byte("resonate") // TODO
)

type Cursor[T any] struct {
	Next *T
}

type Claims[T any] struct {
	Next *T
}

func (c *Claims[T]) Valid() error {
	return nil
}

func NewCursor[T any](tokenString string) (*Cursor[T], error) {
	cursor := &Cursor[T]{}

	if err := cursor.Decode(tokenString); err != nil {
		return nil, err
	}

	return cursor, nil
}

func (c *Cursor[T]) Encode() (string, error) {
	token := jwt.NewWithClaims(signingMethod, &Claims[T]{
		Next: c.Next,
	})

	return token.SignedString(secretKey)
}

func (c *Cursor[T]) Decode(tokenString string) error {
	claims := &Claims[T]{}
	_, err := jwt.ParseWithClaims(tokenString, claims, func(*jwt.Token) (interface{}, error) {
		return secretKey, nil
	})

	if err != nil {
		return err
	}

	c.Next = claims.Next
	return nil
}

func (c *Cursor[T]) MarshalJSON() ([]byte, error) {
	tokenString, err := c.Encode()
	if err != nil {
		return nil, err
	}

	return json.Marshal(tokenString)
}

func (c *Cursor[T]) UnmarshalJSON(data []byte) error {
	var tokenString string
	if err := json.Unmarshal(data, &tokenString); err != nil {
		return err
	}

	return c.Decode(tokenString)
}
