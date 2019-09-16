package querier

import (
	"context"
	"fmt"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
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

	indexCacheSizeBytes := uint64(250 * units.Mebibyte)
	maxItemSizeBytes := indexCacheSizeBytes / 2

	indexCache, err := storecache.NewIndexCache(util.Logger, nil, storecache.Opts{
		MaxSizeBytes:     indexCacheSizeBytes,
		MaxItemSizeBytes: maxItemSizeBytes,
	})
	if err != nil {
		return nil, err
	}

	store, err := store.NewBucketStore(util.Logger, nil, bkt, "cache", indexCache, uint64(2*units.Gibibyte), 0, 20, false, 20, nil)
	if err != nil {
		return nil, err
	}

	b.proxy = store

	return b, nil
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, mathcer ...*labels.Matcher) ([]chunk.Chunk, error) {
	return nil, fmt.Errorf("Unimplemented")
}
