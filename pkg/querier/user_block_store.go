package querier

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// UserStore is a multi-tenant version of Thanos BucketStore
type UserStore struct {
	logger  log.Logger
	cfg     s3.Config
	bucket  objstore.BucketReader
	stores  map[string]*store.BucketStore
	clients map[string]storepb.StoreClient
}

// NewUserStore returns a new UserStore
func NewUserStore(logger log.Logger, s3cfg s3.Config) (*UserStore, error) {
	bkt, err := s3.NewBucketWithConfig(logger, s3cfg, "cortex-userstore")
	if err != nil {
		return nil, err
	}

	return &UserStore{
		logger:  logger,
		cfg:     s3cfg,
		bucket:  bkt,
		stores:  make(map[string]*store.BucketStore),
		clients: make(map[string]storepb.StoreClient),
	}, nil
}

// FIXME should also handle deleting users that no longer exist
// TODO add an InitialSync that deletes users that aren't found
// TODO have SyncStores call SyncBlocks on each of the users instead
// TODO maybe have some jitter in between syncing?

// SyncStores iterates over the s3 bucket creating user bucket stores
func (u *UserStore) SyncStores(ctx context.Context) error {
	startTS := time.Now()
	if err := u.userStoreSync(ctx); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(u.stores))
	for user, s := range u.stores {
		go func(userID string) {
			defer wg.Done()
			if err := s.SyncStores(ctx); err != nil {
				level.Warn(u.logger).Log("msg", "SyncStores failed", "user", userID)
			}
		}(user)
	}

	wg.Wait()
	level.Info(util.Logger).Log("msg", "SyncStores finished", "time", time.Since(startTS))
}

// InitialSync iteratIs over the s3 bucket creating user bucket stores, calling InitialSync on each of them.
func (u *UserStore) InitialSync(ctx context.Context) error {
	startTS := time.Now()
	if err := u.userStoreSync(ctx); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(u.stores))
	for user, s := range u.stores {
		go func(userID string) {
			defer wg.Done()
			if err := s.InitialSync(ctx); err != nil {
				level.Warn(u.logger).Log("msg", "initial sync failed", "user", userID)
			}
		}(user)
	}

	wg.Wait()
	level.Info(util.Logger).Log("msg", "InitialSync finished", "time", time.Since(startTS))
}

func (u *UserStore) userStoreSync(ctx context.Context) error {
	startTS := time.Now()

	mint, maxt := &model.TimeOrDurationValue{}, &model.TimeOrDurationValue{}
	mint.Set("0000-01-01T00:00:00Z")
	maxt.Set("9999-12-31T23:59:59Z")

	err := u.bucket.Iter(ctx, "", func(s string) error {
		user := strings.TrimSuffix(s, "/")

		// If bucket store already exists for user, do nothing
		if _, ok := u.stores[user]; ok {
			return nil
		}

		level.Info(u.logger).Log("msg", "creating user bucket store", "user", user)
		bkt, err := s3.NewBucketWithConfig(u.logger, u.cfg, fmt.Sprintf("cortex-%s", user))
		if err != nil {
			return err
		}

		// Bucket with the user wrapper
		userBkt := &ingester.Bucket{
			UserID: user,
			Bucket: bkt,
		}

		indexCacheSizeBytes := uint64(250 * units.Mebibyte)
		maxItemSizeBytes := indexCacheSizeBytes / 2
		indexCache, err := storecache.NewIndexCache(u.logger, nil, storecache.Opts{
			MaxSizeBytes:     indexCacheSizeBytes,
			MaxItemSizeBytes: maxItemSizeBytes,
		})
		if err != nil {
			return err
		}
		bs, err := store.NewBucketStore(u.logger,
			nil,
			userBkt,
			user,
			indexCache,
			uint64(2*units.Gibibyte),
			0,
			20,
			false,
			20,
			&store.FilterConfig{
				MinTime: *mint,
				MaxTime: *maxt,
			},
		)
		if err != nil {
			return err
		}

		u.stores[user] = bs

		// Create a server with the bucket store
		// TODO this is gross. It should be one grpc server that routes based on userID
		serv := grpc.NewServer()
		storepb.RegisterStoreServer(serv, bs)
		l, err := net.Listen("tcp", "")
		if err != nil {
			return nil
		}
		go serv.Serve(l)

		cc, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
		if err != nil {
			return err
		}

		u.clients[user] = storepb.NewStoreClient(cc)
		return nil
	})
}
