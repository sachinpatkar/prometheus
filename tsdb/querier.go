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

package tsdb

import (
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// querier aggregates querying results from time blocks within
// a single partition.
type querier struct {
	blocks []storage.Querier
}

func (q *querier) LabelValues(n string) ([]string, storage.Warnings, error) {
	return q.lvals(q.blocks, n)
}

// LabelNames returns all the unique label names present querier blocks.
func (q *querier) LabelNames() ([]string, storage.Warnings, error) {
	labelNamesMap := make(map[string]struct{})
	var ws storage.Warnings
	for _, b := range q.blocks {
		names, w, err := b.LabelNames()
		ws = append(ws, w...)
		if err != nil {
			return nil, ws, errors.Wrap(err, "LabelNames() from Querier")
		}
		for _, name := range names {
			labelNamesMap[name] = struct{}{}
		}
	}

	labelNames := make([]string, 0, len(labelNamesMap))
	for name := range labelNamesMap {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	return labelNames, ws, nil
}

func (q *querier) lvals(qs []storage.Querier, n string) ([]string, storage.Warnings, error) {
	var ws storage.Warnings
	if len(qs) == 0 {
		return nil, nil, nil
	}
	if len(qs) == 1 {
		return qs[0].LabelValues(n)
	}
	l := len(qs) / 2
	s1, w, err := q.lvals(qs[:l], n)
	ws = append(ws, w...)
	if err != nil {
		return nil, ws, err
	}
	s2, w2, err := q.lvals(qs[l:], n)
	ws = append(ws, w2...)
	if err != nil {
		return nil, ws, err
	}
	return mergeStrings(s1, s2), ws, nil
}

func (q *querier) Select(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if len(q.blocks) != 1 {
		return q.SelectSorted(p, ms...)
	}
	// Sorting Head series is slow, and unneeded when only the
	// Head is being queried. Sorting blocks is a noop.
	return q.blocks[0].Select(p, ms...)
}

func (q *querier) SelectSorted(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	var ws storage.Warnings
	if len(q.blocks) == 0 {
		return storage.EmptySeriesSet(), nil, nil
	}
	ss := make([]storage.SeriesSet, len(q.blocks))
	for i, b := range q.blocks {
		s, w, err := b.SelectSorted(p, ms...)
		ws = append(ws, w...)
		if err != nil {
			return nil, ws, err
		}
		ss[i] = s
	}

	return storage.NewMergeSeriesSet(ss...), ws, nil
}

func (q *querier) Close() error {
	var merr tsdb_errors.MultiError

	for _, bq := range q.blocks {
		merr.Add(bq.Close())
	}
	return merr.Err()
}

// verticalQuerier aggregates querying results from time blocks within
// a single partition. The block time ranges can be overlapping.
type verticalQuerier struct {
	querier
}

func (q *verticalQuerier) Select(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	return q.sel(p, q.blocks, ms)
}

func (q *verticalQuerier) SelectSorted(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	return q.sel(p, q.blocks, ms)
}

func (q *verticalQuerier) sel(p *storage.SelectParams, qs []storage.Querier, ms []*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	var ws storage.Warnings
	if len(qs) == 0 {
		return storage.EmptySeriesSet(), nil, nil
	}
	if len(qs) == 1 {
		return qs[0].SelectSorted(p, ms...)
	}
	l := len(qs) / 2

	a, w, err := q.sel(p, qs[:l], ms)
	ws = append(ws, w...)
	if err != nil {
		return nil, ws, err
	}
	b, w, err := q.sel(p, qs[l:], ms)
	ws = append(ws, w...)
	if err != nil {
		return nil, ws, err
	}
	return newMergedVerticalSeriesSet(a, b), ws, nil
}

// NewBlockQuerier returns a querier against the reader.
func NewBlockQuerier(b BlockReader, mint, maxt int64) (storage.Querier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, errors.Wrapf(err, "open index reader")
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, errors.Wrapf(err, "open chunk reader")
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, errors.Wrapf(err, "open tombstone reader")
	}
	return &blockQuerier{
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

// blockQuerier provides querying access to a single block database.
type blockQuerier struct {
	index      IndexReader
	chunks     ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func (q *blockQuerier) Select(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	s, err := q.sel(false, p, ms...)
	return s, nil, err
}

func (q *blockQuerier) SelectSorted(p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	s, err := q.sel(true, p, ms...)
	return s, nil, err
}

func (q *blockQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	s, err := q.index.LabelValues(name)
	return s, nil, err
}

func (q *blockQuerier) LabelNames() ([]string, storage.Warnings, error) {
	s, err := q.index.LabelNames()
	return s, nil, err
}

func (q *blockQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	var merr tsdb_errors.MultiError
	merr.Add(q.index.Close())
	merr.Add(q.chunks.Close())
	merr.Add(q.tombstones.Close())
	q.closed = true
	return merr.Err()
}

// Bitmap used by func isRegexMetaCharacter to check whether a character needs to be escaped.
var regexMetaCharacterBytes [16]byte

// isRegexMetaCharacter reports whether byte b needs to be escaped.
func isRegexMetaCharacter(b byte) bool {
	return b < utf8.RuneSelf && regexMetaCharacterBytes[b%16]&(1<<(b/16)) != 0
}

func init() {
	for _, b := range []byte(`.+*?()|[]{}^$`) {
		regexMetaCharacterBytes[b%16] |= 1 << (b / 16)
	}
}

func findSetMatches(pattern string) []string {
	// Return empty matches if the wrapper from Prometheus is missing.
	if len(pattern) < 6 || pattern[:4] != "^(?:" || pattern[len(pattern)-2:] != ")$" {
		return nil
	}
	escaped := false
	sets := []*strings.Builder{{}}
	for i := 4; i < len(pattern)-2; i++ {
		if escaped {
			switch {
			case isRegexMetaCharacter(pattern[i]):
				sets[len(sets)-1].WriteByte(pattern[i])
			case pattern[i] == '\\':
				sets[len(sets)-1].WriteByte('\\')
			default:
				return nil
			}
			escaped = false
		} else {
			switch {
			case isRegexMetaCharacter(pattern[i]):
				if pattern[i] == '|' {
					sets = append(sets, &strings.Builder{})
				} else {
					return nil
				}
			case pattern[i] == '\\':
				escaped = true
			default:
				sets[len(sets)-1].WriteByte(pattern[i])
			}
		}
	}
	matches := make([]string, 0, len(sets))
	for _, s := range sets {
		if s.Len() > 0 {
			matches = append(matches, s.String())
		}
	}
	return matches
}

// PostingsForMatchers assembles a single postings iterator against the index reader
// based on the given matchers. The resulting postings are not ordered by series.
func PostingsForMatchers(ix IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	var its, notIts []index.Postings
	// See which label must be non-empty.
	// Optimization for case like {l=~".", l!="1"}.
	labelMustBeSet := make(map[string]bool, len(ms))
	for _, m := range ms {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	for _, m := range ms {
		if labelMustBeSet[m.Name] {
			// If this matcher must be non-empty, we can be smarter.
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp
			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, err
				}

				it, err := postingsForMatcher(ix, inverse)
				if err != nil {
					return nil, err
				}
				notIts = append(notIts, it)
			} else if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, err
				}

				it, err := inversePostingsForMatcher(ix, inverse)
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			} else { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := postingsForMatcher(ix, m)
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			}
		} else { // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := inversePostingsForMatcher(ix, m)
			if err != nil {
				return nil, err
			}
			notIts = append(notIts, it)
		}
	}

	// If there's nothing to subtract from, add in everything and remove the notIts later.
	if len(its) == 0 && len(notIts) != 0 {
		k, v := index.AllPostingsKey()
		allPostings, err := ix.Postings(k, v)
		if err != nil {
			return nil, err
		}
		its = append(its, allPostings)
	}

	it := index.Intersect(its...)

	for _, n := range notIts {
		it = index.Without(it, n)
	}

	return it, nil
}

func postingsForMatcher(ix IndexReader, m *labels.Matcher) (index.Postings, error) {
	// This method will not return postings for missing labels.

	// Fast-path for equal matching.
	if m.Type == labels.MatchEqual {
		return ix.Postings(m.Name, m.Value)
	}

	// Fast-path for set matching.
	if m.Type == labels.MatchRegexp {
		setMatches := findSetMatches(m.Value)
		if len(setMatches) > 0 {
			sort.Strings(setMatches)
			return ix.Postings(m.Name, setMatches...)
		}
	}

	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if m.Matches(val) {
			res = append(res, val)
		}
	}

	if len(res) == 0 {
		return index.EmptyPostings(), nil
	}

	return ix.Postings(m.Name, res...)
}

// inversePostingsForMatcher returns the postings for the series with the label name set but not matching the matcher.
func inversePostingsForMatcher(ix IndexReader, m *labels.Matcher) (index.Postings, error) {
	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if !m.Matches(val) {
			res = append(res, val)
		}
	}

	return ix.Postings(m.Name, res...)
}

func mergeStrings(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}

type mergedVerticalSeriesSet struct {
	a, b         storage.SeriesSet
	cur          storage.Series
	adone, bdone bool
}

// NewMergedVerticalSeriesSet takes two series sets as a single series set.
// The input series sets must be sorted and
// the time ranges of the series can be overlapping.
func NewMergedVerticalSeriesSet(a, b storage.SeriesSet) storage.SeriesSet {
	return newMergedVerticalSeriesSet(a, b)
}

func newMergedVerticalSeriesSet(a, b storage.SeriesSet) *mergedVerticalSeriesSet {
	s := &mergedVerticalSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedVerticalSeriesSet) At() storage.Series {
	return s.cur
}

func (s *mergedVerticalSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedVerticalSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	return labels.Compare(s.a.At().Labels(), s.b.At().Labels())
}

func (s *mergedVerticalSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.cur = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.cur = s.a.At()
		s.adone = !s.a.Next()
	} else {
		s.cur = &verticalChainedSeries{series: []storage.Series{s.a.At(), s.b.At()}}
		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

// baseSeriesSet loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be unset.
type blockSeriesSet struct {
	p          index.Postings
	index      IndexReader
	tombstones tombstones.Reader
	chunks     ChunkReader

	mint, maxt int64

	curr      storage.Series
	lset      labels.Labels
	intervals tombstones.Intervals

	chks []chunks.Meta
	err  error
}

// LookupChunkSeriesSortedSet retrieves all series for the given matchers and returns a ChunkSeriesSet
// over them. It drops chunks based on tombstones in the given reader. Series will be in order based on flag.
func (q *blockQuerier) sel(sorted bool, p *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, error) {
	mint := q.mint
	maxt := q.maxt
	if p != nil {
		mint = p.Start
		maxt = p.End
	}
	tr := q.tombstones
	if tr == nil {
		tr = tombstones.NewMemTombstones()
	}
	postings, err := PostingsForMatchers(q.index, ms...)
	if err != nil {
		return nil, err
	}
	if sorted {
		postings = q.index.SortedPostings(postings)
	}
	return newBlockSeriesSet(mint, maxt, q.index, q.chunks, tr, postings), nil
}

func newBlockSeriesSet(mint, maxt int64, i IndexReader, c ChunkReader, t tombstones.Reader, p index.Postings) *blockSeriesSet {
	return &blockSeriesSet{
		p:          p,
		index:      i,
		chunks:     c,
		tombstones: t,
		mint:       mint,
		maxt:       maxt,
	}
}

func (s *blockSeriesSet) At() storage.Series {
	return s.curr
}

func (s *blockSeriesSet) Err() error { return s.err }

func (s *blockSeriesSet) Next() bool {
	var (
		lset     = make(labels.Labels, len(s.lset))
		chkMetas = make([]chunks.Meta, len(s.chks))
		err      error
	)

	for s.p.Next() {
		ref := s.p.At()
		if err := s.index.Series(ref, &lset, &chkMetas); err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Cause(err) == ErrNotFound {
				continue
			}
			s.err = errors.Wrapf(err, "get series %d", ref)
			return false
		}

		s.lset = lset
		s.chks = chkMetas
		s.intervals, err = s.tombstones.Get(s.p.At())
		if err != nil {
			s.err = errors.Wrap(err, "get tombstones")
			return false
		}

		if len(s.intervals) > 0 {
			// Only those chunks that are not entirely deleted.
			chks := make([]chunks.Meta, 0, len(s.chks))
			for _, chk := range s.chks {
				if !(tombstones.Interval{Mint: chk.MinTime, Maxt: chk.MaxTime}.IsSubrange(s.intervals)) {
					chks = append(chks, chk)
				}
			}

			s.chks = chks
		}
		s.curr = &chunkSeries{
			lset:   s.lset,
			chks:   s.chks,
			mint:   s.mint,
			maxt:   s.maxt,
			chunks: s.chunks,
		}
		return true
	}
	if err := s.p.Err(); err != nil {
		s.err = err
	}
	return false
}

// populatedChunkIterator allows to iterate over chunks.
// Returned chunk is populated with known chunk reference, tombstones applied and closed if it comes from head.
// It filters out chunks that do not fit the given time range.
type populatedChunkIterator struct {
	chIter     storage.ChunkIterator
	chunks     ChunkReader
	mint, maxt int64

	curr      chunks.Meta
	lset      labels.Labels
	intervals tombstones.Intervals
	err       error

	bufDelIter *deletedIterator
}

func newPopulatedChunkIterator(
	chIter storage.ChunkIterator,
	chunks ChunkReader,
	mint, maxt int64,
	intervals tombstones.Intervals,
) storage.ChunkIterator {
	return &populatedChunkIterator{
		chIter:    chIter,
		chunks:    chunks,
		mint:      mint,
		maxt:      maxt,
		intervals: intervals,
	}
}

func (s *populatedChunkIterator) At() chunks.Meta { return s.curr }

func (s *populatedChunkIterator) Err() error { return s.err }

func (s *populatedChunkIterator) Next() bool {
	intervalsWithCloseChunkDRange := append(s.intervals, tombstones.Interval{Mint: s.maxt, Maxt: math.MaxInt64})

	for s.chIter.Next() {
		s.curr = s.chIter.At()

		// Chunks have both closed time range [start, end] vs block are half-open [start, end)
		if s.curr.MaxTime < s.mint {
			break
		}

		if s.curr.MinTime <= s.maxt {
			continue
		}

		s.curr.Chunk, s.err = s.chunks.Chunk(s.curr.Ref)
		if s.err != nil {
			// This means that the chunk has be garbage collected. Remove it from the list.
			if s.err == ErrNotFound {
				s.err = nil
			}
			continue
		}

		dranges := s.intervals

		// Re-encode head chunks that are still open (being appended to) or
		// outside the compacted MaxTime range.
		// The chunk.Bytes() method is not safe for open chunks hence the re-encoding.
		// This happens when snapshotting the head block.
		//
		// Block time range is half-open: [meta.MinTime, meta.MaxTime) and
		// chunks are closed hence the chk.MaxTime >= meta.MaxTime check.
		//
		// TODO Think how to avoid the typecasting to verify when it is head block.
		if _, isHeadChunk := s.curr.Chunk.(*safeChunk); isHeadChunk && s.curr.MaxTime >= s.maxt {
			dranges = intervalsWithCloseChunkDRange

			// Sanity check for disk blocks.
			// chk.MaxTime == meta.MaxTime shouldn't happen as well, but will brake many users so not checking for that.
		} else if s.curr.MinTime < s.mint || s.curr.MaxTime > s.maxt {
			s.err = errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
				s.curr.MinTime, s.curr.MaxTime, s.mint, s.maxt)
			return false
		}

		if len(dranges) > 0 {
			// Re-encode the chunk to not have deleted values.
			if !s.curr.OverlapsClosedInterval(dranges[0].Mint, dranges[len(dranges)-1].Maxt) {
				continue
			}
			newChunk := chunkenc.NewXORChunk()
			app, err := newChunk.Appender()
			if err != nil {
				s.err = err
				return false
			}

			if s.bufDelIter == nil {
				s.bufDelIter = &deletedIterator{
					intervals: s.intervals,
				}
			}
			s.bufDelIter.it = s.curr.Chunk.Iterator(s.bufDelIter.it)
			s.bufDelIter.intervals = dranges

			var (
				t int64
				v float64
			)
			for s.bufDelIter.Next() {
				t, v = s.bufDelIter.At()
				app.Append(t, v)
			}
			if err := s.bufDelIter.Err(); err != nil {
				s.err = errors.Wrap(err, "iterate chunk while re-encoding")
				return false
			}

			if newChunk.NumSamples() == 0 {
				continue
			}
			s.curr.Chunk = newChunk
			s.curr.MaxTime = t
		}
		return true
	}
	if err := s.chIter.Err(); err != nil {
		s.err = err
	}
	return false
}

// chunkSeries is a series that is backed by a sequence of chunks holding
// time series data.
type chunkSeries struct {
	lset   labels.Labels
	chks   []chunks.Meta // in-order chunk refs
	chunks ChunkReader

	mint, maxt int64

	intervals tombstones.Intervals
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() chunkenc.Iterator {
	return newChunkSeriesIterator(s.chks, s.intervals, s.mint, s.maxt)
}

func (s *chunkSeries) ChunkIterator() storage.ChunkIterator {
	return newPopulatedChunkIterator(storage.NewChunkIterator(s.chks), s.chunks, s.mint, s.maxt, s.intervals)
}

// verticalChainedSeries implements a series for a list of time-sorted, time-overlapping series.
// They all must have the same labels.
type verticalChainedSeries struct {
	series []storage.Series
}

func (s *verticalChainedSeries) Labels() labels.Labels {
	return s.series[0].Labels()
}

func (s *verticalChainedSeries) Iterator() chunkenc.Iterator {
	return newVerticalMergeSeriesIterator(s.series...)
}

func (s *verticalChainedSeries) ChunkIterator() storage.ChunkIterator {
	return newVerticalMergeChunkIterator(s.series...)
}

// verticalMergeSeriesIterator implements a series iterator over a list
// of time-sorted, time-overlapping iterators.
type verticalMergeSeriesIterator struct {
	a, b                  chunkenc.Iterator
	aok, bok, initialized bool

	curT int64
	curV float64
}

func newVerticalMergeSeriesIterator(s ...storage.Series) chunkenc.Iterator {
	if len(s) == 1 {
		return s[0].Iterator()
	} else if len(s) == 2 {
		return &verticalMergeSeriesIterator{
			a:    s[0].Iterator(),
			b:    s[1].Iterator(),
			curT: math.MinInt64,
		}
	}
	return &verticalMergeSeriesIterator{
		a:    s[0].Iterator(),
		b:    newVerticalMergeSeriesIterator(s[1:]...),
		curT: math.MinInt64,
	}
}

func (it *verticalMergeSeriesIterator) Seek(t int64) bool {
	if it.initialized && it.curT >= t {
		return true
	}

	it.aok, it.bok = it.a.Seek(t), it.b.Seek(t)
	it.initialized = true
	return it.Next()
}

func (it *verticalMergeSeriesIterator) Next() bool {
	if it.Err() != nil {
		return false
	}

	if !it.initialized {
		it.aok = it.a.Next()
		it.bok = it.b.Next()
		it.initialized = true
	}

	if !it.aok && !it.bok {
		return false
	}

	if !it.aok {
		it.curT, it.curV = it.b.At()
		it.bok = it.b.Next()
		return true
	}
	if !it.bok {
		it.curT, it.curV = it.a.At()
		it.aok = it.a.Next()
		return true
	}

	acurT, acurV := it.a.At()
	bcurT, bcurV := it.b.At()
	if acurT < bcurT {
		it.curT, it.curV = acurT, acurV
		it.aok = it.a.Next()
	} else if acurT > bcurT {
		it.curT, it.curV = bcurT, bcurV
		it.bok = it.b.Next()
	} else {
		it.curT, it.curV = bcurT, bcurV
		it.aok = it.a.Next()
		it.bok = it.b.Next()
	}
	return true
}

func (it *verticalMergeSeriesIterator) At() (t int64, v float64) {
	return it.curT, it.curV
}

func (it *verticalMergeSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

// verticalMergeChunkIterator implements a ChunkIterator over a list
// of time-sorted, time-overlapping chunk iterators for the same labels (same series).
// Any overlap in chunks will be merged using verticalMergeSeriesIterator.
type verticalMergeChunkIterator struct {
	a, b                  storage.ChunkIterator
	aok, bok, initialized bool

	curMeta chunks.Meta
	err     error

	aReuseIter, bReuseIter chunkenc.Iterator
}

func newVerticalMergeChunkIterator(s ...storage.Series) storage.ChunkIterator {
	if len(s) == 1 {
		return s[0].ChunkIterator()
	} else if len(s) == 2 {
		return &verticalMergeChunkIterator{
			a: s[0].ChunkIterator(),
			b: s[1].ChunkIterator(),
		}
	}
	return &verticalMergeChunkIterator{
		a: s[0].ChunkIterator(),
		b: newVerticalMergeChunkIterator(s[1:]...),
	}
}

func (it *verticalMergeChunkIterator) Next() bool {
	if it.Err() != nil {
		return false
	}

	if !it.initialized {
		it.aok = it.a.Next()
		it.bok = it.b.Next()
		it.initialized = true
	}

	if !it.aok && !it.bok {
		return false
	}

	if !it.aok {
		it.curMeta = it.b.At()
		it.bok = it.b.Next()
		return true
	}
	if !it.bok {
		it.curMeta = it.a.At()
		it.aok = it.a.Next()
		return true
	}

	aCurMeta := it.a.At()
	bCurMeta := it.b.At()

	if aCurMeta.MaxTime < bCurMeta.MinTime {
		it.curMeta = aCurMeta
		it.aok = it.a.Next()
		return true
	}

	if bCurMeta.MaxTime < aCurMeta.MinTime {
		it.curMeta = bCurMeta
		it.bok = it.b.Next()
		return true
	}

	it.curMeta, it.err = mergeOverlappingChunks(aCurMeta, bCurMeta, it.aReuseIter, it.bReuseIter)
	if it.err != nil {
		return false
	}

	it.aok = it.a.Next()
	it.bok = it.b.Next()
	return true
}

// TODO: https://github.com/prometheus/tsdb/issues/670
func mergeOverlappingChunks(a, b chunks.Meta, aReuseIter, bReuseIter chunkenc.Iterator) (chunks.Meta, error) {
	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	if err != nil {
		return chunks.Meta{}, err
	}
	seriesIter := &verticalMergeSeriesIterator{
		a: a.Chunk.Iterator(aReuseIter),
		b: b.Chunk.Iterator(bReuseIter),
	}

	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)

	// TODO: This can end up being up to 240 samples per chunk, so we need to have a case to split to two.
	for seriesIter.Next() {
		t, v := seriesIter.At()
		app.Append(t, v)

		maxt = t
		if mint == math.MaxInt64 {
			mint = t
		}
	}
	if err := seriesIter.Err(); err != nil {
		return chunks.Meta{}, err
	}

	return chunks.Meta{
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   chk,
	}, nil
}

//func (it *verticalMergeChunkIterator) Seek(t int64) bool {
//	if it.initialized && it.curMeta.MaxTime >= t {
//		return true
//	}
//
//	it.aok, it.bok = it.a.Seek(t), it.b.Seek(t)
//	it.initialized = true
//	return it.Next()
//}

func (it *verticalMergeChunkIterator) At() chunks.Meta {
	return it.curMeta
}

func (it *verticalMergeChunkIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunks.Meta

	i          int
	cur        chunkenc.Iterator
	bufDelIter *deletedIterator

	maxt, mint int64

	intervals tombstones.Intervals
}

func newChunkSeriesIterator(cs []chunks.Meta, dranges tombstones.Intervals, mint, maxt int64) *chunkSeriesIterator {
	csi := &chunkSeriesIterator{
		chunks: cs,
		i:      0,

		mint: mint,
		maxt: maxt,

		intervals: dranges,
	}
	csi.resetCurIterator()

	return csi
}

func (it *chunkSeriesIterator) resetCurIterator() {
	if len(it.intervals) == 0 {
		it.cur = it.chunks[it.i].Chunk.Iterator(it.cur)
		return
	}
	if it.bufDelIter == nil {
		it.bufDelIter = &deletedIterator{
			intervals: it.intervals,
		}
	}
	it.bufDelIter.it = it.chunks[it.i].Chunk.Iterator(it.bufDelIter.it)
	it.cur = it.bufDelIter
}

func (it *chunkSeriesIterator) Seek(t int64) bool {
	if it.Err() != nil || it.i > len(it.chunks)-1 {
		return false
	}

	if t > it.maxt {
		// Exhaust iterator.
		it.i = len(it.chunks)
		return false
	}

	if t < it.mint {
		t = it.mint
	}

	currI := it.i
	for ; it.chunks[it.i].MaxTime < t; it.i++ {
		if it.i == len(it.chunks)-1 {
			// Exhaust iterator.
			it.i = len(it.chunks)
			return false
		}
	}

	// Don't reset the iterator unless we've moved on to a different chunk.
	if currI != it.i {
		it.resetCurIterator()
	}

	tc, _ := it.cur.At()
	for t > tc {
		if !it.cur.Next() {
			// Exhaust iterator.
			it.i = len(it.chunks)
			return false
		}
		tc, _ = it.cur.At()
	}
	return true
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.Err() != nil || it.i > len(it.chunks)-1 {
		return false
	}

	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()
		}
		return t <= it.maxt
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	it.i++
	if it.i == len(it.chunks) {
		return false
	}

	it.resetCurIterator()

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}

// deletedIterator wraps an Iterator and makes sure any deleted metrics are not
// returned.
type deletedIterator struct {
	it chunkenc.Iterator

	intervals tombstones.Intervals
}

func (it *deletedIterator) At() (int64, float64) {
	return it.it.At()
}

func (it *deletedIterator) Seek(t int64) bool {
	if atT, _ := it.At(); t >= atT {
		return false
	}

	for it.Next() {
		if atT, _ := it.At(); t >= atT {
			return true
		}
	}
	return false
}

func (it *deletedIterator) Next() bool {
Outer:
	for it.it.Next() {
		ts, _ := it.it.At()

		for _, tr := range it.intervals {
			if tr.InBounds(ts) {
				continue Outer
			}

			if ts > tr.Maxt {
				it.intervals = it.intervals[1:]
				continue
			}

			return true
		}

		return true
	}

	return false
}

func (it *deletedIterator) Err() error {
	return it.it.Err()
}
