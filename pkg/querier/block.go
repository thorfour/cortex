package querier

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// BlockQuerier is a querier of thanos blocks
type BlockQuerier struct {
	us *UserStore
}

// NewBlockQuerier returns a client to query a s3 block store
func NewBlockQuerier(s3cfg s3.Config) (*BlockQuerier, error) {

	us, err := NewUserStore(util.Logger, s3cfg)
	if err != nil {
		return nil, err
	}

	stopc := make(chan struct{})
	go runutil.Repeat(30*time.Second, stopc, func() error {
		// FIXME some jitter between calls to syncblocks underneath is probably ideal
		if err := us.SyncStores(context.Background()); err != nil {
			level.Warn(util.Logger).Log("msg", "sync stores failed", "err", err)
		}
		return nil
	})

	return &BlockQuerier{
		us: us,
	}, nil
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {

	// lookup the user client
	client, ok := b.us.clients[userID]
	if !ok {
		return nil, fmt.Errorf("user not found")
	}

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

	seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:  int64(from),
		MaxTime:  int64(through),
		Matchers: converted,
	})
	if err != nil {
		return nil, err
	}

	var chunks []chunk.Chunk
	for {
		resp, err := seriesClient.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		chunks = append(chunks, seriesToChunks(userID, resp.GetSeries())...)
	}

	// TODO sort chunks
	return chunks, nil
}

func seriesToChunks(userID string, series *storepb.Series) []chunk.Chunk {

	var lbls labels.Labels
	for i := range series.Labels {
		lbls = append(lbls, labels.Label{
			Name:  series.Labels[i].Name,
			Value: series.Labels[i].Value,
		})
	}

	var chunks []chunk.Chunk
	for _, c := range series.Chunks {
		ch := encoding.New()

		enc, err := chunkenc.FromData(chunkenc.Encoding(c.Raw.Type), c.Raw.Data)
		if err != nil {
			level.Warn(util.Logger).Log("msg", "failed to converted raw encoding to chunk", "err", err)
			continue
		}

		it := enc.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			_, err := ch.Add(model.SamplePair{ // TODO handle overflow chunks
				Timestamp: model.Time(ts),
				Value:     model.SampleValue(v),
			})
			if err != nil {
				level.Warn(util.Logger).Log("msg", "failed adding sample to chunk", "err", err)
				continue
			}
		}

		chunks = append(chunks, chunk.NewChunk(userID, client.Fingerprint(lbls), lbls, ch, model.Time(c.MinTime), model.Time(c.MaxTime)))
	}
	return chunks
}
