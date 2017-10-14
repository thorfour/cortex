// TODO(prom2): add attribution, this is copied from Prometheus.

package querier

import (
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/prom1/storage/metric"
)

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() storage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

type metadataSeriesSet struct {
	cur    int
	series []storage.Series
}

func (s metadataSeriesSet) Next() bool {
	panic("not implemented")
}

func (s metadataSeriesSet) At() storage.Series {
	panic("not implemented")
}

func (s metadataSeriesSet) Err() error {
	panic("not implemented")
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implements storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []model.SamplePair
}

func (c *concreteSeries) Labels() labels.Labels {
	return c.labels
}

func (c *concreteSeries) Iterator() storage.SeriesIterator {
	return newConcreteSeriesIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriesIterator(series *concreteSeries) storage.SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= model.Time(t)
	})
	return c.cur < len(c.series.samples)
}

func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return int64(s.Timestamp), float64(s.Value)
}

func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

func (c *concreteSeriesIterator) Err() error {
	return nil
}

func metricsToSeriesSet(ms []metric.Metric) storage.SeriesSet {
	series := make([]storage.Series, 0, len(ms))
	for _, m := range ms {
		labels := make(labels.Labels, 0, len(m.Metric))
		series = append(series, &concreteSeries{
			labels:  labels,
			samples: nil,
		})
	}
	return &concreteSeriesSet{
		series: series,
	}
}

func matrixToSeriesSet(m model.Matrix) storage.SeriesSet {
	series := make([]storage.Series, 0, len(m))
	for _, ss := range m {
		labels := make(labels.Labels, 0, len(ss.Metric))
		series = append(series, &concreteSeries{
			labels:  labels,
			samples: ss.Values,
		})
	}
	return &concreteSeriesSet{
		series: series,
	}
}
