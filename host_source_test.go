package gocql

import "testing"

func TestUnmarshalCassVersion(t *testing.T) {
	tests := [...]struct {
		data    string
		version cassVersion
	}{
		{"3.2", cassVersion{3, 2, 0}},
		{"2.10.1-SNAPSHOT", cassVersion{2, 10, 1}},
		{"1.2.3", cassVersion{1, 2, 3}},
	}

	for i, test := range tests {
		v := &cassVersion{}
		if err := v.UnmarshalCQL(nil, []byte(test.data)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if *v != test.version {
			t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
		}
	}
}
