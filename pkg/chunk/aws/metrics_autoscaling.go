package aws

import (
	"context"
	"fmt"
	"time"

	promV1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/cortex/pkg/chunk"
)

func (d dynamoTableClient) metricsAutoScale(ctx context.Context, current, expected chunk.TableDesc) error {
	qlVector, err := promQuery(ctx, d.promAPI, `sum(cortex_ingester_flush_queue_length)`, 2*time.Minute)
	if err != nil {
		return err
	}
	if len(qlVector) == 0 {
		return fmt.Errorf("No data for queue length")
	}

	deVector, err := promQuery(ctx, d.promAPI, `sum(rate(cortex_dynamo_failures_total{error="ProvisionedThroughputExceededException",operation=~".*Write.*"}[1m])) by (table) > 0`, 0*time.Minute)
	if err != nil {
		return err
	}

	fmt.Printf("qlVector: %#v\ndeVector: %#v\n", qlVector, deVector)

	return nil
}

func promQuery(ctx context.Context, promAPI promV1.API, query string, duration time.Duration) (model.Vector, error) {
	queryRange := promV1.Range{
		Start: time.Now(),
		End:   time.Now().Add(-duration),
		Step:  time.Minute,
	}

	value, err := promAPI.QueryRange(ctx, query, queryRange)
	if err != nil {
		return nil, err
	}
	vector, ok := value.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("Unable to convert value to vector: %#v", value)
	}
	return vector, nil
}
