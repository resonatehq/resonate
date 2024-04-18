package metadata

import "testing"

type MockMetadataImpl struct {
	Name  string `mapstructure:"name"`
	Value int    `mapstructure:"value"`
}

func TestMetadata(t *testing.T) {
	tcs := []struct {
		name     string
		metadata Metadata
		expected MockMetadataImpl
	}{
		{
			name: "Test decoding metadata",
			metadata: Metadata{
				Properties: map[string]interface{}{
					"name":  "Test",
					"value": 42,
				},
			},
			expected: MockMetadataImpl{
				Name:  "Test",
				Value: 42,
			},
		},
		{
			name: "Test decoding empty metadata",
			metadata: Metadata{
				Properties: map[string]interface{}{},
			},
			expected: MockMetadataImpl{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var result MockMetadataImpl
			err := Decode(tc.metadata.Properties, &result)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != tc.expected {
				t.Errorf("Expected %+v, but got %+v", tc.expected, result)
			}
		})
	}
}
