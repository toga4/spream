package spream

import "testing"

func TestPartitionMetadata_IsRootPartition(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  bool
	}{
		{
			name:  "root partition",
			token: RootPartitionToken,
			want:  true,
		},
		{
			name:  "non-root partition",
			token: "child-partition-1",
			want:  false,
		},
		{
			name:  "empty token",
			token: "",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PartitionMetadata{PartitionToken: tt.token}
			if got := p.IsRootPartition(); got != tt.want {
				t.Errorf("IsRootPartition() = %v, want %v", got, tt.want)
			}
		})
	}
}
