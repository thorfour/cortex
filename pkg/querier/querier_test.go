package querier

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/wire"
)

func TestRemoteReadHandler(t *testing.T) {
	q := Queryable{
		Queriers: []Querier{
			mockQuerier{
				iters: []local.SeriesIterator{
					util.NewSampleStreamIterator(&model.SampleStream{
						Metric: model.Metric{"foo": "bar"},
						Values: []model.SamplePair{
							{0, 0}, {1, 1}, {2, 2}, {3, 3},
						},
					}),
				},
			},
		},
	}

	requestBody, err := proto.Marshal(&client.ReadRequest{
		Queries: []*client.QueryRequest{
			{StartTimestampMs: 0, EndTimestampMs: 10},
		},
	})
	require.NoError(t, err)
	requestBody = snappy.Encode(nil, requestBody)
	request, err := http.NewRequest("GET", "/query", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	recorder := httptest.NewRecorder()
	q.RemoteReadHandler(recorder, request)

	require.Equal(t, 200, recorder.Result().StatusCode)
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	responseBody, err = snappy.Decode(nil, responseBody)
	require.NoError(t, err)
	var response client.ReadResponse
	err = proto.Unmarshal(responseBody, &response)
	require.NoError(t, err)

	expected := client.ReadResponse{
		Results: []*client.QueryResponse{
			{
				Timeseries: []client.TimeSeries{
					{
						Labels: []client.LabelPair{
							{wire.Bytes([]byte("foo")), wire.Bytes([]byte("bar"))},
						},
						Samples: []client.Sample{
							{0, 0}, {1, 1}, {2, 2}, {3, 3},
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, response)
}

type mockQuerier struct {
	iters []local.SeriesIterator
}

func (m mockQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]local.SeriesIterator, error) {
	return m.iters, nil
}

func (mockQuerier) LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error) {
	return nil, nil
}

func (mockQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...[]*labels.Matcher) ([]metric.Metric, error) {
	return nil, nil
}
