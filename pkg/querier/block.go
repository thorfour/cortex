package querier

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// BlockQuerier is a querier of thanos blocks
type BlockQuerier struct {
	proxy    storepb.StoreServer
	qcreator query.QueryableCreator
}

// NewBlockQuerier returns a client to query a s3 block store
func NewBlockQuerier(s3cfg s3.Config) (*BlockQuerier, error) {
	bkt, err := s3.NewBucketWithConfig(util.Logger, s3cfg, "cortex")
	if err != nil {
		return nil, err
	}
	b := &BlockQuerier{}

	b.qcreator = query.NewQueryableCreator(util.Logger, bkt, "")

	return b, nil
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, mathcer ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, fmt.Errorf("Unimplemented")
}
