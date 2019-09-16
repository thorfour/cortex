package querier

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

// UserStore is a multi-tenant version of Thanos BucketStore
type UserStore struct {
	logger log.Logger
	cfg    s3.Config
	bucket objstore.BucketReader
	stores map[string]*store.BucketStore
}

// NewUserStore returns a new UserStore
func NewUserStore(logger log.Logger, s3cfg s3.Config) (*UserStore, error) {
	bkt, err := s3.NewBucketWithConfig(logger, s3cfg, "cortex-userstore")
	if err != nil {
		return nil, err
	}

	return &UserStore{
		logger: logger,
		cfg:    s3cfg,
		bucket: bkt,
		stores: make(map[string]*store.BucketStore),
	}, nil
}

// SyncStores iterates over the s3 bucket and creating/deleting user bucket stores
func (u *UserStore) SyncStores(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	err := u.bucket.Iter(ctx, "", func(s string) error {
		user := strings.TrimSuffix(s, "/")

		// If bucket store already exists for user, do nothing
		if _, ok := u.stores[user]; ok {
			return nil
		}
		// FIXME should also handle deleting users that no longer exist

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
			nil,
		)
		if err != nil {
			return err
		}

		u.stores[user] = bs

		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			if err := bs.InitialSync(ctx); err != nil {
				level.Warn(u.logger).Log("msg", "initial sync failed", "user", userID)
			}
		}(user)

		return nil
	})

	wg.Wait()

	return err
}
