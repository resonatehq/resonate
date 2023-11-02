package checker

// a consistency model specifying the contract between
type Linearizable struct {
	Model     string
	Algorithm string
}

func NewLinearizable() Linearizable {
	return Linearizable{
		Model:     "",
		Algorithm: "linear",
	}
}
