package metadata

import "github.com/mitchellh/mapstructure"

// Metadata is the generic struct used to initialize the vendor specific queue.
type Metadata struct {
	Properties map[string]interface{} `json:",inline"`
}

// Decode is a helper function to decode the metadata into the vendor specific struct.
func Decode(input interface{}, result interface{}) error {
	return mapstructure.Decode(input, result)
}
