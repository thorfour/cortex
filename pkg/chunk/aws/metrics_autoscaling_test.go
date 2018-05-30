package aws

import (
	"context"
	"testing"
	"time"

	promApi "github.com/prometheus/client_golang/api"
	promV1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/mtime"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

func TestTableManagerMetricsAutoScaling(t *testing.T) {
	var debugLevel util.AllowedLevel
	debugLevel.Set("debug")
	util.InitLogger(debugLevel)

	dynamoDB := newMockDynamoDB(0, 0)
	//mockProm := mockPrometheus{}
	promClient, err := promApi.NewClient(promApi.Config{Address: "http://127.0.0.1:8080/api/prom"})
	require.NoError(t, err)
	promAPI := promV1.NewAPI(promClient)

	client := dynamoTableClient{
		DynamoDB: dynamoDB,
		promAPI:  promAPI,
	}

	autoScalingConfig := chunk.AutoScalingConfig{
		Enabled:     true,
		MinCapacity: 10,
		MaxCapacity: 20,
	}
	periodicTableConfig := chunk.PeriodicTableConfig{
		Prefix: tablePrefix,
		Period: tablePeriod,
		From:   util.NewDayValue(model.TimeFromUnix(0)),
		ProvisionedWriteThroughput: write,
		ProvisionedReadThroughput:  read,
		WriteScale:                 autoScalingConfig,
	}
	chunkTableConfig := periodicTableConfig
	chunkTableConfig.Prefix = chunkTablePrefix

	cfg := chunk.SchemaConfig{
		OriginalTableName:   "a",
		UsePeriodicTables:   true,
		IndexTables:         periodicTableConfig,
		ChunkTables:         chunkTableConfig,
		CreationGracePeriod: gracePeriod,
	}

	tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
	if err != nil {
		t.Fatal(err)
	}

	// Create tables
	ctx := context.Background()
	mtime.NowForce(time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod))
	defer mtime.NowReset()
	err = tableManager.SyncTables(ctx)
	require.NoError(t, err)
	tables, err := client.ListTables(ctx)
	require.NoError(t, err)
	require.Equal(t, len(tables), 3)

	//mockProm.rangeVal = model.Vector{model.Sample{}}
	//mockProm.queryVal = model.Vector{}

	err = tableManager.SyncTables(ctx)
	require.NoError(t, err)
}

type mockPrometheus struct {
	queryVal  model.Value
	rangeVal  model.Value
	labelVal  model.LabelValues
	seriesVal []model.LabelSet
}

func (m *mockPrometheus) Query(ctx context.Context, query string, ts time.Time) (model.Value, error) {
	return m.queryVal, nil
}

func (m *mockPrometheus) QueryRange(ctx context.Context, query string, r promV1.Range) (model.Value, error) {
	return m.rangeVal, nil
}

func (m *mockPrometheus) LabelValues(ctx context.Context, label string) (model.LabelValues, error) {
	return m.labelVal, nil
}

func (m *mockPrometheus) Series(ctx context.Context, matches []string, startTime time.Time, endTime time.Time) ([]model.LabelSet, error) {
	return m.seriesVal, nil
}
