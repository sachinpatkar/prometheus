// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestMergeStringSlices(t *testing.T) {
	for _, tc := range []struct {
		input    [][]string
		expected []string
	}{
		{},
		{[][]string{{"foo"}}, []string{"foo"}},
		{[][]string{{"foo"}, {"bar"}}, []string{"bar", "foo"}},
		{[][]string{{"foo"}, {"bar"}, {"baz"}}, []string{"bar", "baz", "foo"}},
	} {
		testutil.Equals(t, tc.expected, mergeStringSlices(tc.input))
	}
}

func TestMergeTwoStringSlices(t *testing.T) {
	for _, tc := range []struct {
		a, b, expected []string
	}{
		{[]string{}, []string{}, []string{}},
		{[]string{"foo"}, nil, []string{"foo"}},
		{nil, []string{"bar"}, []string{"bar"}},
		{[]string{"foo"}, []string{"bar"}, []string{"bar", "foo"}},
		{[]string{"foo"}, []string{"bar", "baz"}, []string{"bar", "baz", "foo"}},
		{[]string{"foo"}, []string{"foo"}, []string{"foo"}},
	} {
		testutil.Equals(t, tc.expected, mergeTwoStringSlices(tc.a, tc.b))
	}
}

type mockSeries struct {
	labels   func() labels.Labels
	iterator func() chunkenc.Iterator
	chunkIterator func() ChunkIterator
}

func newSeries(l map[string]string, s [][]tsdbutil.Sample) Series {
	var mergedSamples []tsdbutil.Sample
	cs := make([]chunks.Meta, 0, len(s))
	for _, samples := range s {
		cs = append(cs, tsdbutil.ChunkFromSamples(samples))
		mergedSamples = append(mergedSamples, samples...)
	}

	return &mockSeries{
		labels:   func() labels.Labels { return labels.FromMap(l) },
		iterator: func() chunkenc.Iterator { return newListSeriesIterator(mergedSamples) },
		chunkIterator: func() ChunkIterator { return NewChunkIterator(cs) },
	}
}

func (m *mockSeries) Labels() labels.Labels        { return m.labels() }
func (m *mockSeries) Iterator() chunkenc.Iterator  { return m.iterator() }
func (m *mockSeries) ChunkIterator() ChunkIterator  { return m.chunkIterator() }

func TestMergeSeriesSet(t *testing.T) {
	for _, c := range []struct {
		// The input sets in order (samples in series in b are strictly
		// after those in a).
		a, b SeriesSet
		// The composition of a and b in the partition series set must yield
		// results equivalent to the result series set.
		exp SeriesSet
	}{
		{
			a: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{},
				),
			),
			b: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{},
				),
				newSeries(
					map[string]string{"b": "b"},
					[][]tsdbutil.Sample{},
				),
			),
			exp: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{},
				),
				newSeries(
					map[string]string{"b": "b"},
					[][]tsdbutil.Sample{},
				),
			),
		},
		{
			a: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{
						{sample{t: 1, v: 1},sample{t: 2, v: 2}}, {sample{t: 3, v: 3}},
					}),
			),
			b: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{
						{sample{t: 4, v: 4}},
					}),
				newSeries(
					map[string]string{"b": "b"},
					[][]tsdbutil.Sample{
						{sample{t: 1, v: 1}},
					}),
			),
			exp: newMockSeriesSet(
				newSeries(
					map[string]string{"a": "a"},
					[][]tsdbutil.Sample{
						{sample{t: 1, v: 1},sample{t: 2, v: 2}}, {sample{t: 3, v: 3}}, {sample{t: 4, v: 4}},
					}),
				newSeries(
					map[string]string{"b": "b"},
					[][]tsdbutil.Sample{
						{sample{t: 1, v: 1}},
					}),
			),
		},
		{
			a: newMockSeriesSet(
				newSeries(
					map[string]string{"handler":  "prometheus","instance": "127.0.0.1:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 1, v: 1},
					}}),
				newSeries(
					map[string]string{"handler":  "prometheus","instance": "localhost:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 1, v: 2},
					}}),
			),
			b: newMockSeriesSet(
				newSeries(
					map[string]string{"handler":  "prometheus","instance": "127.0.0.1:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 2, v: 1},
					}}),
				newSeries(
					map[string]string{"handler":  "query","instance": "localhost:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 2, v: 2},
					}}),
			),
			exp: newMockSeriesSet(
				newSeries(
					map[string]string{"handler":  "prometheus", "instance": "127.0.0.1:9090"},
					[][]tsdbutil.Sample{
						{sample{t: 1, v: 1}},
						{sample{t: 2, v: 1}},
					}),
				newSeries(
					map[string]string{"handler": "prometheus", "instance": "localhost:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 1, v: 2},
					}}),
				newSeries(
					map[string]string{"handler": "query", "instance": "localhost:9090"},
					[][]tsdbutil.Sample{{
						sample{t: 2, v: 2},
					}}),
			),
		},
	} {
		t.Run("", func(t *testing.T) {
			res := NewMergeSeriesSet(c.a, c.b)

			for {
				eok, rok := c.exp.Next(), res.Next()
				testutil.Equals(t, eok, rok)

				if !eok {
					return
				}

				sexp := c.exp.At()
				sres := res.At()
				testutil.Equals(t, sexp.Labels(), sres.Labels())
				fmt.Println(sres.Labels())

				smplExp, errExp := expandSeriesIterator(sexp.Iterator())
				smplRes, errRes := expandSeriesIterator(sres.Iterator())
				testutil.Equals(t, errExp, errRes)
				testutil.Equals(t, smplExp, smplRes)

				chkExp, errExp := expandChunkIterator(sexp.ChunkIterator())
				chkRes, errRes := expandChunkIterator(sres.ChunkIterator())
				testutil.Equals(t, errExp, errRes)
				testutil.Equals(t, chkExp, chkRes)
			}
		})
	}
}

func expandSeriesIterator(iter chunkenc.Iterator) ([]tsdbutil.Sample, error) {
	var result []tsdbutil.Sample
	for iter.Next() {
		t, v := iter.At()
		// NaNs can't be compared normally, so substitute for another value.
		if math.IsNaN(v) {
			v = -42
		}
		result = append(result, sample{t, v})
	}
	return result, iter.Err()
}

func expandChunkIterator(it ChunkIterator) (chks []chunks.Meta, err error) {
	for it.Next() {
		chks = append(chks, it.At())
	}
	return chks, it.Err()
}

type mockSeriesSet struct {
	idx    int
	series []Series
}

func newMockSeriesSet(series ...Series) SeriesSet {
	return &mockSeriesSet{
		idx:    -1,
		series: series,
	}
}

func (m *mockSeriesSet) Next() bool {
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockSeriesSet) At() Series {
	return m.series[m.idx]
}

func (m *mockSeriesSet) Err() error {
	return nil
}

var result []tsdbutil.Sample

func makeSeriesSet(numSeries, numSamples int) SeriesSet {
	series := []Series{}
	for j := 0; j < numSeries; j++ {
		labels := labels.Labels{{Name: "foo", Value: fmt.Sprintf("bar%d", j)}}
		samples := []tsdbutil.Sample{}
		for k := 0; k < numSamples; k++ {
			samples = append(samples, sample{t: int64(k), v: float64(k)})
		}
		series = append(series, newSeries(labels.Map(), [][]tsdbutil.Sample{samples}))
	}
	return newMockSeriesSet(series...)
}

func makeMergeSeriesSet(numSeriesSets, numSeries, numSamples int) SeriesSet {
	seriesSets := []SeriesSet{}
	for i := 0; i < numSeriesSets; i++ {
		seriesSets = append(seriesSets, makeSeriesSet(numSeries, numSamples))
	}
	return NewMergeSeriesSet(seriesSets...)
}

func benchmarkDrain(seriesSet SeriesSet, b *testing.B) {
	for n := 0; n < b.N; n++ {
		for seriesSet.Next() {
			result, _ = expandSeriesIterator(seriesSet.At().Iterator())
		}
	}
}

func BenchmarkNoMergeSeriesSet_100_100(b *testing.B) {
	seriesSet := makeSeriesSet(100, 100)
	benchmarkDrain(seriesSet, b)
}

func BenchmarkMergeSeriesSet(b *testing.B) {
	for _, bm := range []struct {
		numSeriesSets, numSeries, numSamples int
	}{
		{1, 100, 100},
		{10, 100, 100},
		{100, 100, 100},
	} {
		seriesSet := makeMergeSeriesSet(bm.numSeriesSets, bm.numSeries, bm.numSamples)
		b.Run(fmt.Sprintf("%d_%d_%d", bm.numSeriesSets, bm.numSeries, bm.numSamples), func(b *testing.B) {
			benchmarkDrain(seriesSet, b)
		})
	}
}

type iteratorCase struct {
	a, b, c, expected []tsdbutil.Sample

	// Only relevant for some iterators that filters by min and max time.
	mint, maxt int64

	// Seek being zero means do not test seek.
	seek        int64
	seekSuccess bool
}

func (tc iteratorCase) test(t *testing.T, it chunkenc.Iterator) {
	tv, v := it.At()
	testutil.Equals(t, int64(math.MinInt64), tv)
	testutil.Equals(t, float64(0), v)

	var r []tsdbutil.Sample
	if tc.seek != 0 {
		testutil.Equals(t, tc.seekSuccess, it.Seek(tc.seek))
		testutil.Equals(t, tc.seekSuccess, it.Seek(tc.seek)) // Next one should be noop.

		if tc.seekSuccess {
			// After successful seek iterator is ready. Grab the value.
			t, v := it.At()
			r = append(r, sample{t: t, v: v})
		}
	}
	expandedResult, err := expandSeriesIterator(it)
	testutil.Ok(t, err)

	r = append(r, expandedResult...)
	testutil.Equals(t, tc.expected, r)
}

func TestSeriesIterators(t *testing.T) {
	cases := []iteratorCase{
		{
			a:    []tsdbutil.Sample{},
			b:    []tsdbutil.Sample{},
			c:    []tsdbutil.Sample{},
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			expected: nil,
		},
		{
			a: []tsdbutil.Sample{
				sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
			},
			b: []tsdbutil.Sample{},
			c: []tsdbutil.Sample{
				sample{7, 89}, sample{9, 8},
			},
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			expected: []tsdbutil.Sample{
				sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1}, sample{7, 89}, sample{9, 8},
			},
		},
		{
			a: []tsdbutil.Sample{
				sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
			},
			b: []tsdbutil.Sample{
				sample{7, 89}, sample{9, 8},
			},
			c: []tsdbutil.Sample{
				sample{10, 22}, sample{203, 3493},
			},
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			expected: []tsdbutil.Sample{
				sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1}, sample{7, 89}, sample{9, 8}, sample{10, 22}, sample{203, 3493},
			},
		},
		// Seek cases.
		{
			a:    []tsdbutil.Sample{},
			b:    []tsdbutil.Sample{},
			c:    []tsdbutil.Sample{},
			seek: 1,

			seekSuccess: false,
			expected:    nil,
		},
		{
			a: []tsdbutil.Sample{
				sample{2, 3},
			},
			b: []tsdbutil.Sample{},
			c: []tsdbutil.Sample{
				sample{7, 89}, sample{9, 8},
			},
			seek: 10,
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			seekSuccess: false,
			expected:    nil,
		},
		{
			a: []tsdbutil.Sample{},
			b: []tsdbutil.Sample{
				sample{1, 2}, sample{3, 5}, sample{6, 1},
			},
			c: []tsdbutil.Sample{
				sample{7, 89}, sample{9, 8},
			},
			seek: 2,
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			seekSuccess: true,
			expected: []tsdbutil.Sample{
				sample{3, 5}, sample{6, 1}, sample{7, 89}, sample{9, 8},
			},
		},
		{
			a: []tsdbutil.Sample{
				sample{6, 1},
			},
			b: []tsdbutil.Sample{
				sample{9, 8},
			},
			c: []tsdbutil.Sample{
				sample{10, 22}, sample{203, 3493},
			},
			seek: 10,
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			seekSuccess: true,
			expected: []tsdbutil.Sample{
				sample{10, 22}, sample{203, 3493},
			},
		},
		{
			a: []tsdbutil.Sample{
				sample{6, 1},
			},
			b: []tsdbutil.Sample{
				sample{9, 8},
			},
			c: []tsdbutil.Sample{
				sample{10, 22}, sample{203, 3493},
			},
			seek: 203,
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			seekSuccess: true,
			expected: []tsdbutil.Sample{
				sample{203, 3493},
			},
		},
		{
			a: []tsdbutil.Sample{
				sample{6, 1},
			},
			b: []tsdbutil.Sample{
				sample{9, 8},
			},
			c: []tsdbutil.Sample{
				sample{10, 22}, sample{203, 3493},
			},
			seek: -120,
			mint: math.MinInt64,
			maxt: math.MaxInt64,

			seekSuccess: true,
			expected: []tsdbutil.Sample{
				sample{6, 1}, sample{9, 8}, sample{10, 22}, sample{203, 3493},
			},
		},
	}

	// To make sure we can properly test things, listSeriesIterator has to work properly as well, even if created
	// for testing purposes.
	t.Run("TestList", func(t *testing.T) {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				tc.test(t, newListSeriesIterator(append(append(append([]tsdbutil.Sample{}, tc.a...), tc.b...), tc.c...)))
			})
		}
	})

	t.Run("Chain", func(t *testing.T) {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				tc.test(t, newChainedSeriesIterator(
					newSeries(nil, [][]tsdbutil.Sample{tc.a}),
					newSeries(nil, [][]tsdbutil.Sample{tc.b}),
					newSeries(nil, [][]tsdbutil.Sample{tc.c}),
				))
			})
		}
	})
}

type chunkIteratorCase struct {
	a, b, c, expected []chunks.Meta

	// Seek being zero means do not test seek.
	seek        int64
	seekSuccess bool
}

func (tc chunkIteratorCase) test(t *testing.T, it ChunkIterator) {
	testutil.Equals(t, chunks.Meta{}, it.At())
	var r []chunks.Meta
	expandedResult, err := expandChunkIterator(it)
	testutil.Ok(t, err)

	r = append(r, expandedResult...)
	testutil.Equals(t, tc.expected, r)
}

func TestChunkIterators(t *testing.T) {
	cases := []chunkIteratorCase{
		{
			a: []chunks.Meta{},
			b: []chunks.Meta{},
			c: []chunks.Meta{},

			expected: nil,
		},
		{
			a: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
				}),
			},
			b: []chunks.Meta{},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},

			expected: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},
		},
		{
			a: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
				}),
			},
			b: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},

			expected: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{2, 3}, sample{3, 5}, sample{6, 1},
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},
		},
		// Seek cases.
		{
			a:    []chunks.Meta{},
			b:    []chunks.Meta{},
			c:    []chunks.Meta{},
			seek: 1,

			seekSuccess: false,
			expected:    nil,
		},
		{
			a: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{2, 3},
				}),
			},
			b: []chunks.Meta{},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},
			seek: 10,

			seekSuccess: false,
			expected:    nil,
		},
		{
			a: []chunks.Meta{},
			b: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{3, 5}, sample{6, 1},
				}),
			},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},
			seek: 2,

			seekSuccess: true,
			expected: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{1, 2}, sample{3, 5}, sample{6, 1},
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{7, 89}, sample{9, 8},
				}),
			},
		},
		{
			a: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{6, 1},
				}),
			},
			b: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{9, 8},
				}),
			},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},
			seek: 10,

			seekSuccess: true,
			expected: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},
		},
		{
			a: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{6, 1},
				}),
			},
			b: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{9, 8},
				}),
			},
			c: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},
			seek: 203,

			seekSuccess: true,
			expected: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 22}, sample{203, 3493},
				}),
			},
		},
	}

	t.Run("Simple", func(t *testing.T) {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				tc.test(t, NewChunkIterator(append(append(append([]chunks.Meta{}, tc.a...), tc.b...), tc.c...)))
			})
		}
	})

	t.Run("Chained", func(t *testing.T) {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
				tc.test(t, newChainedSeriesChunkIterator(
					&mockSeries{chunkIterator: func() ChunkIterator { return NewChunkIterator(tc.a)}},
					&mockSeries{chunkIterator: func() ChunkIterator { return NewChunkIterator(tc.b)}},
					&mockSeries{chunkIterator: func() ChunkIterator { return NewChunkIterator(tc.c)}},
				))
			})
		}
	})
}
