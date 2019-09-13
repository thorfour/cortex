package querier

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// BlockQuerier is a querier of thanos blocks
type BlockQuerier struct {
	proxy storepb.StoreServer
}

// NewBlockQuerier returns a client to query a s3 block store
func NewBlockQuerier() *BlockQuerier {
	b := &BlockQuerier{}

	//query.NewQueryableCreator(util.Logger, nil, "")

	return b
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, mathcer ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, fmt.Errorf("Unimplemented")
}
