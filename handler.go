package spream

import (
	"context"
	"time"
)

type ChangeHandler func(ctx context.Context, change *Change) error
type Watermarker func(ctx context.Context, partitionToken string, timestamp time.Time) error
