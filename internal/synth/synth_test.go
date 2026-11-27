package synth

import (
	"context"
	"math"
	"testing"
	"time"

	"gomuse/internal/hub"
	"gomuse/internal/sample"
)

// fakeClock is a virtual clock: Sleep advances it instead of blocking, so a
// test can cover minutes of simulated streaming in microseconds.
type fakeClock struct {
	t      float64
	sleeps int
	limit  int
	cancel context.CancelFunc
}

func (c *fakeClock) now() float64 { return c.t }

func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	if d > 0 {
		c.t += d.Seconds()
	}
	c.sleeps++
	if c.limit > 0 && c.sleeps >= c.limit && c.cancel != nil {
		c.cancel()
	}
	return ctx.Err()
}

// runSim runs a source against a virtual clock until the clock has been asked
// to sleep limit times, and returns every chunk that reached the hub.
func runSim(t *testing.T, streams map[sample.StreamID]bool, limit int) []sample.Chunk {
	t.Helper()

	h := hub.New()
	defer h.Close()
	ch, cancelSub := h.Subscribe(16384)
	defer cancelSub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clk := &fakeClock{t: 1_700_000_000, limit: limit, cancel: cancel}
	s := NewWithOptions(h, streams, Options{
		Seed:  7,
		Start: clk.t,
		Now:   clk.now,
		Sleep: clk.sleep,
	})

	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
	cancelSub()

	var got []sample.Chunk
	for c := range ch {
		got = append(got, c)
	}
	return got
}

func allStreams() map[sample.StreamID]bool {
	m := map[sample.StreamID]bool{}
	for _, id := range sample.All() {
		m[id] = true
	}
	return m
}

func TestChunkShapes(t *testing.T) {
	chunks := runSim(t, allStreams(), 400)
	if len(chunks) == 0 {
		t.Fatal("no chunks published")
	}
	seen := map[sample.StreamID]int{}
	for _, c := range chunks {
		m := sample.MetaFor(c.Stream)
		seen[c.Stream]++
		if c.NumChannels() != m.NumChannels() {
			t.Fatalf("%s: %d channels, want %d", m.Name, c.NumChannels(), m.NumChannels())
		}
		if c.NumSamples() != m.SamplesPerPacket {
			t.Fatalf("%s: %d samples, want %d", m.Name, c.NumSamples(), m.SamplesPerPacket)
		}
		for ch, d := range c.Data {
			if len(d) != len(c.Timestamps) {
				t.Fatalf("%s ch%d: %d samples, timestamps %d", m.Name, ch, len(d), len(c.Timestamps))
			}
			for _, v := range d {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					t.Fatalf("%s ch%d: non-finite sample %v", m.Name, ch, v)
				}
			}
		}
	}
	for _, id := range sample.All() {
		if seen[id] == 0 {
			t.Errorf("stream %s produced no chunks", sample.MetaFor(id).Name)
		}
	}
}

func TestTimestampsMonotonicAndAtNominalRate(t *testing.T) {
	chunks := runSim(t, allStreams(), 600)

	last := map[sample.StreamID]float64{}
	first := map[sample.StreamID]float64{}
	count := map[sample.StreamID]int{}

	for _, c := range chunks {
		m := sample.MetaFor(c.Stream)
		for i := 1; i < len(c.Timestamps); i++ {
			if c.Timestamps[i] <= c.Timestamps[i-1] {
				t.Fatalf("%s: timestamps not increasing within chunk: %v", m.Name, c.Timestamps)
			}
			if d := c.Timestamps[i] - c.Timestamps[i-1]; math.Abs(d-m.Period()) > 1e-6 {
				t.Fatalf("%s: intra-chunk period %v, want %v", m.Name, d, m.Period())
			}
		}
		if prev, ok := last[c.Stream]; ok {
			if c.Timestamps[0] <= prev {
				t.Fatalf("%s: chunk start %v not after previous end %v", m.Name, c.Timestamps[0], prev)
			}
			if d := c.Timestamps[0] - prev; math.Abs(d-m.Period()) > 1e-6 {
				t.Fatalf("%s: inter-chunk period %v, want %v", m.Name, d, m.Period())
			}
		} else {
			first[c.Stream] = c.Timestamps[0]
		}
		last[c.Stream] = c.Timestamps[len(c.Timestamps)-1]
		count[c.Stream] += c.NumSamples()
	}

	// Samples delivered must match the elapsed span at the nominal rate.
	for id, n := range count {
		m := sample.MetaFor(id)
		span := last[id] - first[id]
		want := span*m.Rate + 1
		if math.Abs(float64(n)-want) > 1e-3 {
			t.Errorf("%s: %d samples over %.3fs, want %.3f at %g Hz", m.Name, n, span, want, m.Rate)
		}
	}
}

func TestOnlySelectedStreams(t *testing.T) {
	chunks := runSim(t, map[sample.StreamID]bool{sample.EEG: true, sample.ACC: false}, 200)
	if len(chunks) == 0 {
		t.Fatal("no chunks published")
	}
	for _, c := range chunks {
		if c.Stream != sample.EEG {
			t.Fatalf("got stream %v, want only EEG", c.Stream)
		}
	}
}

func TestRelativeRates(t *testing.T) {
	chunks := runSim(t, allStreams(), 1000)
	n := map[sample.StreamID]int{}
	for _, c := range chunks {
		n[c.Stream] += c.NumSamples()
	}
	// EEG runs at 256 Hz vs ACC's 52: expect roughly 4.9x the samples.
	ratio := float64(n[sample.EEG]) / float64(n[sample.ACC])
	if ratio < 4.4 || ratio > 5.4 {
		t.Errorf("EEG/ACC sample ratio = %.2f, want ~4.92", ratio)
	}
	// Same rate and packet size, so they can differ by at most the one packet
	// that the cancellation happened to land between.
	if d := n[sample.ACC] - n[sample.Gyro]; d < 0 || d > sample.MetaFor(sample.ACC).SamplesPerPacket {
		t.Errorf("ACC %d samples vs Gyro %d: same rate, expected within one packet", n[sample.ACC], n[sample.Gyro])
	}
}

// TestEEGLooksLikeEEG pins the properties the viewer depends on: an alpha peak
// worth some tens of microvolts, and a large slow drift on at least one channel
// so that toggling the DC blocker visibly changes the trace.
func TestEEGLooksLikeEEG(t *testing.T) {
	chunks := runSim(t, map[sample.StreamID]bool{sample.EEG: true}, 4000)

	nch := sample.MetaFor(sample.EEG).NumChannels()
	series := make([][]float64, nch)
	for _, c := range chunks {
		for ch := range c.Data {
			series[ch] = append(series[ch], c.Data[ch]...)
		}
	}
	if len(series[0]) < 2560 {
		t.Fatalf("only %d samples, need at least 10 s", len(series[0]))
	}

	var maxDrift float64
	for ch, x := range series {
		if a := amplitudeAt(x, 10, 256); a < 5 || a > 200 {
			t.Errorf("ch%d: 10 Hz amplitude %.1f uV, want a plausible alpha (5-200)", ch, a)
		}
		// Drift shows up as a large swing in the per-second means.
		lo, hi := math.Inf(1), math.Inf(-1)
		for i := 0; i+256 <= len(x); i += 256 {
			var s float64
			for _, v := range x[i : i+256] {
				s += v
			}
			m := s / 256
			lo, hi = math.Min(lo, m), math.Max(hi, m)
		}
		if hi-lo > maxDrift {
			maxDrift = hi - lo
		}
	}
	if maxDrift < 100 {
		t.Errorf("largest slow drift is %.1f uV peak-to-peak, want at least 100", maxDrift)
	}
}

func TestPPGHasLargePedestal(t *testing.T) {
	chunks := runSim(t, map[sample.StreamID]bool{sample.PPG: true}, 500)
	for _, c := range chunks {
		for ch, d := range c.Data {
			for _, v := range d {
				if v < 50_000 || v > 3_000_000 {
					t.Fatalf("ppg ch%d sample %v outside the plausible 5e4..3e6 count range", ch, v)
				}
			}
		}
	}
}

func TestACCRestsAtOneG(t *testing.T) {
	chunks := runSim(t, map[sample.StreamID]bool{sample.ACC: true}, 500)
	var sums [3]float64
	var n float64
	for _, c := range chunks {
		for ch := 0; ch < 3; ch++ {
			for _, v := range c.Data[ch] {
				sums[ch] += v
			}
		}
		n += float64(c.NumSamples())
	}
	want := [3]float64{0, 0, 1}
	for ch := range sums {
		if got := sums[ch] / n; math.Abs(got-want[ch]) > 0.1 {
			t.Errorf("acc ch%d mean = %.3f g, want ~%.0f", ch, got, want[ch])
		}
	}
}

func TestGyroNearZero(t *testing.T) {
	chunks := runSim(t, map[sample.StreamID]bool{sample.Gyro: true}, 500)
	var sum, n float64
	for _, c := range chunks {
		for _, d := range c.Data {
			for _, v := range d {
				sum += v
				n++
			}
		}
	}
	if got := sum / n; math.Abs(got) > 5 {
		t.Errorf("gyro mean = %.2f dps, want near zero", got)
	}
}

func TestDeterministicForASeed(t *testing.T) {
	a := runSim(t, allStreams(), 300)
	b := runSim(t, allStreams(), 300)
	if len(a) != len(b) {
		t.Fatalf("run lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Stream != b[i].Stream {
			t.Fatalf("chunk %d: stream %v vs %v", i, a[i].Stream, b[i].Stream)
		}
		for ch := range a[i].Data {
			for j := range a[i].Data[ch] {
				if a[i].Data[ch][j] != b[i].Data[ch][j] {
					t.Fatalf("chunk %d ch%d sample %d: %v vs %v",
						i, ch, j, a[i].Data[ch][j], b[i].Data[ch][j])
				}
			}
		}
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	h := hub.New()
	defer h.Close()
	s := New(h, allStreams())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
}

// TestRunWithRealClock proves the default (unfaked) path publishes something,
// without sleeping for anything a human would notice.
func TestRunWithRealClock(t *testing.T) {
	h := hub.New()
	defer h.Close()
	ch, cancelSub := h.Subscribe(256)
	defer cancelSub()

	s := New(h, map[sample.StreamID]bool{sample.EEG: true})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	select {
	case c := <-ch:
		if c.Stream != sample.EEG {
			t.Fatalf("got stream %v, want EEG", c.Stream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no chunk arrived within 2s")
	}
	select {
	case err := <-done:
		if err != context.DeadlineExceeded {
			t.Fatalf("Run returned %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the deadline")
	}
}

// amplitudeAt estimates the amplitude of the freq component of x.
func amplitudeAt(x []float64, freq, fs float64) float64 {
	var re, im float64
	for i, v := range x {
		ph := 2 * math.Pi * freq * float64(i) / fs
		re += v * math.Cos(ph)
		im += v * math.Sin(ph)
	}
	return 2 * math.Hypot(re, im) / float64(len(x))
}

// The demo battery exists to drive the viewer's indicator, so what matters is
// that it passes through every state the real thing does: unknown at first,
// then a level that falls, then flat.
func TestBatteryDrainsFromUnknownToFlat(t *testing.T) {
	clock := 1000.0
	src := NewWithOptions(hub.New(), map[sample.StreamID]bool{}, Options{
		Start: clock,
		Now:   func() float64 { return clock },
	})

	if _, ok := src.Battery(); ok {
		t.Error("battery is known immediately on connect; " +
			"the viewer's unknown state would be unreachable in the demo")
	}

	clock += batteryUnknownSec
	pct, ok := src.Battery()
	if !ok {
		t.Fatal("battery still unknown after the unknown window")
	}
	if pct != 100 {
		t.Errorf("battery starts at %f, want 100", pct)
	}

	clock += 10 * batterySecPerPct
	if pct, ok = src.Battery(); !ok || pct != 90 {
		t.Errorf("battery after 10 percent's worth of drain = (%f, %v), want (90, true)", pct, ok)
	}

	// Well past empty: it stays flat rather than going negative or wrapping
	// back to full, either of which the viewer would render as nonsense.
	clock += 1000 * batterySecPerPct
	if pct, ok = src.Battery(); !ok || pct != 0 {
		t.Errorf("battery long after empty = (%f, %v), want (0, true)", pct, ok)
	}
}
