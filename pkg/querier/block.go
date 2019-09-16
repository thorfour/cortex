package querier

import (
	"context"
	"net"
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// BlockQuerier is a querier of thanos blocks
type BlockQuerier struct {
	store  storepb.StoreServer
	Client storepb.StoreClient
}

// NewBlockQuerier returns a client to query a s3 block store
func NewBlockQuerier(s3cfg s3.Config) (*BlockQuerier, error) {
	bkt, err := s3.NewBucketWithConfig(util.Logger, s3cfg, "cortex")
	if err != nil {
		return nil, err
	}
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

	if err := store.InitialSync(context.Background()); err != nil {
		return nil, errors.Wrap(err, "bucket store sync failed")
	}
	level.Info(util.Logger).Log("msg", "bucket store ready")

	stopc := make(chan struct{})
	go runutil.Repeat(30*time.Second, stopc, func() error {
		if err := store.SyncBlocks(context.Background()); err != nil {
			level.Warn(util.Logger).Log("msg", "block sync failed", "err", err)
		}
		return nil
	})

	// FIXME Starting a GRPC server is gross
	s := grpc.NewServer()
	storepb.RegisterStoreServer(s, store)

	l, err := net.Listen("tcp", "")
	if err != nil {
		return nil, errors.Wrap(err, "listen failed")
	}
	go s.Serve(l)

	cc, err := grpc.Dial(l.Addr().String())
	if err != nil {
		return nil, errors.Wrap(err, "dial failed")
	}

	return &BlockQuerier{
		store:  store,
		Client: storepb.NewStoreClient(cc),
	}, nil
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {

	// TODO userID needs to be prefixed

	// Convert matchers to LabelMatcher
	var converted []storepb.LabelMatcher
	for _, m := range matchers {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}

		converted = append(converted, storepb.LabelMatcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		})
	}

	b.Client.Series(ctx, nil)
}
