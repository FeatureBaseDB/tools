package bench

import "time"

// Stats object helps track timing stats.
type Stats struct {
	Min            time.Duration `json:"min"`
	Max            time.Duration `json:"max"`
	Mean           time.Duration `json:"mean"`
	sumSquareDelta float64
	Total          time.Duration   `json:"total-time"`
	Num            int64           `json:"num"`
	All            []time.Duration `json:"all"`
	SaveAll        bool            `json:"-"`
}

// NewStats gets a Stats object.
func NewStats() *Stats {
	return &Stats{
		Min: 1<<63 - 1,
		All: make([]time.Duration, 0),
	}
}

// Add adds a new time to the stats object.
func (s *Stats) Add(td time.Duration) {
	if s.SaveAll {
		s.All = append(s.All, td)
	}
	s.Num += 1
	s.Total += td
	if td < s.Min {
		s.Min = td
	}
	if td > s.Max {
		s.Max = td
	}

	// online variance calculation
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
	delta := td - s.Mean
	s.Mean += delta / time.Duration(s.Num)
	s.sumSquareDelta += float64(delta * (td - s.Mean))
}
