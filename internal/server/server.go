// Package server serves the live viewer: the embedded single-page app over
// HTTP, and the sample stream over a WebSocket.
//
// Everything the browser needs is compiled into the binary with //go:embed, so
// gomuse is a single file with no runtime network access — the uPlot build in
// web/vendor is deliberately vendored rather than pulled from a CDN.
//
// Each WebSocket client is independent. It gets its own hub subscription (so a
// slow browser drops its own frames and nobody else's) and its own filter
// state: display filtering is a per-viewer preference, and the DSP banks carry
// per-channel history that cannot be shared. Because the same sample.Chunk is
// handed to every subscriber, filtering must never write into the chunk it was
// given; dsp.Bank.Process and dsp.DCBlockerBank.Process both return fresh
// slices, which is what makes that safe.
package server

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"gomuse/internal/dsp"
	"gomuse/internal/hub"
	"gomuse/internal/pulse"
	"gomuse/internal/sample"
	"gomuse/internal/wire"
)

//go:embed web
var webFS embed.FS

const (
	// subscriberBuffer is how many chunks may queue for one browser before it
	// starts dropping. Roughly a second of all four streams: enough to ride
	// out a GC pause or a tab switch, short enough that a viewer that falls
	// behind catches up quickly instead of showing stale data.
	subscriberBuffer = 96

	// writeTimeout bounds a single frame write. A browser that has stopped
	// reading without closing the TCP connection must not pin a goroutine.
	writeTimeout = 10 * time.Second

	// shutdownTimeout bounds the graceful shutdown.
	shutdownTimeout = 5 * time.Second

	// Display filter parameters, matching muse-lsl's viewer: a 32-tap
	// 1-40 Hz bandpass at the EEG rate, preceded by a 0.5 Hz DC blocker
	// because that FIR has a DC gain of 0.87 and does not remove drift on
	// its own. See internal/dsp.
	filterTaps = 32
	filterLow  = 1.0
	filterHigh = 40.0

	// pulseIntervalSec throttles the pulse message. Four a second is far more
	// often than a heart beats; it exists so the readout feels live rather
	// than to carry new beats.
	pulseIntervalSec = 0.25

	// pulseStaleSec is how long other live streams may advance without a pulse
	// chunk before the last rate is withdrawn. Normal PPG packets are less than
	// 100 ms apart; a full second tolerates routine subscriber drops while
	// ensuring a stopped notification cannot leave a locked BPM on screen.
	pulseStaleSec = 1.0

	// batteryInterval is how often the battery provider is polled. A headband
	// reports its level every five to ten seconds and a cell takes hours to
	// move a percent, so this is about noticing a fresh reading promptly, not
	// about resolution. Polling is cheap — one mutex-guarded read — and a
	// message only goes out when the value actually changes, so the interval
	// costs nothing when nothing is happening.
	batteryInterval = 2 * time.Second
)

// Config configures a Server.
type Config struct {
	// Addr is the TCP address to listen on, e.g. ":1111".
	Addr string
	// Hub is the source of sample chunks. Required.
	Hub *hub.Hub
	// Streams describes the connected headband's streams. The viewer builds
	// its entire UI from this, so it must match what the source publishes: an
	// Athena's EEG is 4 channels where a classic headband's is 5. Nil means
	// the classic layout.
	Streams *sample.Set

	// Battery reports the source's battery level as a percentage, and whether
	// one has been reported yet. It is polled from the WebSocket writer
	// goroutines, so it must be safe for concurrent use.
	//
	// Nil means this source has no battery at all, which is a different thing
	// from a source whose level is not known yet: the viewer hides the
	// indicator entirely for the first and shows it as unknown for the second.
	Battery func() (float64, bool)
}

// Server serves the viewer app and the sample WebSocket.
type Server struct {
	cfg     Config
	mux     *http.ServeMux
	codec   *wire.Codec
	clients atomic.Int64

	// shutdown is closed when ListenAndServe's context is cancelled, which
	// ends the WebSocket loops. http.Server.Shutdown cannot do that itself:
	// a hijacked connection is invisible to it.
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// StreamInfo is the per-stream metadata sent to a client on connect, so that
// the browser does not hardcode channel names or rates.
type StreamInfo struct {
	ID               int      `json:"id"`
	Name             string   `json:"name"`
	Channels         []string `json:"channels"`
	Unit             string   `json:"unit"`
	Rate             float64  `json:"rate"`
	SamplesPerPacket int      `json:"samplesPerPacket"`
}

// Hello is the first message on a WebSocket, sent as JSON text. Every later
// message is a binary wire frame.
type Hello struct {
	Type    string       `json:"type"`
	Streams []StreamInfo `json:"streams"`
	// Battery says whether this source reports a battery level at all, so the
	// viewer can leave the indicator out rather than showing a permanently
	// blank one. It is not the level itself: that arrives in a BatteryMessage
	// once the headband has sent one.
	Battery bool `json:"battery"`
}

// PulseMessage is the derived heart-rate message, sent as a WebSocket text
// frame alongside the binary sample frames.
//
// The numeric fields are pointers so that "not known yet" marshals as null
// rather than as zero: a bpm of 0 and an unknown bpm must not be confusable in
// the browser.
type PulseMessage struct {
	Type      string    `json:"type"`
	Channel   *string   `json:"channel"`
	State     string    `json:"state"`
	BPM       *float64  `json:"bpm"`
	RMSSD     *float64  `json:"rmssd"`
	IBIMs     *float64  `json:"ibiMs"`
	Beats     []float64 `json:"beats"`
	BeatCount int       `json:"beatCount"`
}

// BatteryMessage carries the headband's charge level, as a WebSocket text
// frame like PulseMessage.
//
// Percent is a pointer for the same reason the pulse fields are: a headband
// sends no battery block for the first several seconds after connecting, and an
// unknown level must not arrive in the browser as a flat 0%.
type BatteryMessage struct {
	Type    string   `json:"type"`
	Percent *float64 `json:"percent"`
}

// Health is the /healthz body.
type Health struct {
	Status  string `json:"status"`
	Clients int64  `json:"clients"`
	Dropped uint64 `json:"dropped"`
}

// New returns a Server for cfg. It panics if cfg.Hub is nil, which is a
// programming error rather than a runtime condition.
func New(cfg Config) *Server {
	if cfg.Hub == nil {
		panic("server: Config.Hub is required")
	}
	if cfg.Streams == nil {
		cfg.Streams = sample.Classic()
	}
	s := &Server{
		cfg:      cfg,
		mux:      http.NewServeMux(),
		shutdown: make(chan struct{}),
		codec:    wire.NewCodec(cfg.Streams),
	}

	root, err := fs.Sub(webFS, "web")
	if err != nil {
		// Impossible: the directory is embedded at build time.
		panic("server: embedded web assets missing: " + err.Error())
	}

	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /ws", s.handleWS)
	s.mux.Handle("GET /", http.FileServerFS(root))
	return s
}

// Handler returns the HTTP handler, for httptest or for mounting elsewhere.
func (s *Server) Handler() http.Handler { return s.mux }

// Clients returns the number of connected WebSocket clients.
func (s *Server) Clients() int64 { return s.clients.Load() }

// ListenAndServe serves until ctx is cancelled, then shuts down gracefully and
// returns nil. A listen or serve failure is returned as-is.
func (s *Server) ListenAndServe(ctx context.Context) error {
	hs := &http.Server{
		Addr:    s.cfg.Addr,
		Handler: s.mux,
		// No WriteTimeout: it would cap the lifetime of every WebSocket.
		ReadHeaderTimeout: 10 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	s.shutdownOnce.Do(func() { close(s.shutdown) })

	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := hs.Shutdown(sctx)
	if serveErr := <-errc; err == nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return err
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(Health{
		Status:  "ok",
		Clients: s.clients.Load(),
		Dropped: s.cfg.Hub.Dropped(),
	})
}

// hello builds the metadata message for a new client.
func hello(streams *sample.Set, hasBattery bool) Hello {
	h := Hello{Type: "hello", Battery: hasBattery}
	for _, id := range streams.All() {
		m := streams.MetaFor(id)
		h.Streams = append(h.Streams, StreamInfo{
			ID:               int(id),
			Name:             m.Name,
			Channels:         m.Channels,
			Unit:             m.Unit,
			Rate:             m.Rate,
			SamplesPerPacket: m.SamplesPerPacket,
		})
	}
	return h
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The payload is already-compact float32; deflate would burn CPU 21
		// times a second per stream for nothing.
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return // Accept has already written the error response.
	}
	defer func() {
		_ = conn.CloseNow()
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// A server shutdown must end this connection too.
	go func() {
		select {
		case <-s.shutdown:
			cancel()
		case <-ctx.Done():
		}
	}()

	s.clients.Add(1)
	defer s.clients.Add(-1)

	msg, err := json.Marshal(hello(s.cfg.Streams, s.cfg.Battery != nil))
	if err != nil {
		return
	}
	wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
	err = conn.Write(wctx, websocket.MessageText, msg)
	wcancel()
	if err != nil {
		return
	}

	chunks, unsubscribe := s.cfg.Hub.Subscribe(subscriberBuffer)
	defer unsubscribe()

	cl := newClient(s.cfg.Streams, s.cfg.Battery)
	go cl.readLoop(ctx, cancel, conn)

	for {
		select {
		case <-ctx.Done():
			return
		case c, ok := <-chunks:
			if !ok {
				// The hub closed: the capture is over.
				_ = conn.Close(websocket.StatusNormalClosure, "stream ended")
				return
			}
			wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageBinary, s.codec.EncodeChunk(cl.prepare(c)))
			wcancel()
			if err != nil {
				return
			}

			if msg, due := cl.pulseUpdate(c); due {
				if !writeJSON(ctx, conn, msg) {
					return
				}
			}

			// Battery is polled here rather than on its own ticker so that the
			// connection keeps a single writer and needs no extra
			// synchronisation. The cost is that the level stops updating when
			// no chunks arrive at all — but that is a stream that is seconds
			// away from the auto-disconnect watchdog, and a stale battery
			// reading is the least of what the viewer has to show then.
			if msg, due := cl.batteryUpdate(time.Now()); due {
				if !writeJSON(ctx, conn, msg) {
					return
				}
			}
		}
	}
}

// writeJSON sends v as a text frame and reports whether the connection is still
// usable. A value that cannot be encoded is skipped rather than fatal — that is
// a bug in the message, not a reason to drop the viewer — while a failed write
// means the connection is gone.
func writeJSON(ctx context.Context, conn *websocket.Conn, v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return true
	}
	wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
	defer wcancel()
	return conn.Write(wctx, websocket.MessageText, b) == nil
}

// client is the per-connection state: the requested settings, and the DSP
// banks that implement them.
type client struct {
	// want is set by the reader goroutine and read by the writer goroutine.
	want atomic.Bool
	// window is the viewer's display window in seconds. The server does not
	// use it — the ring buffers live in the browser — but accepting it keeps
	// the control message single-purpose and lets a future recorder or
	// downsampler act on it.
	window atomic.Uint64

	// Everything below belongs to the writer goroutine alone.
	on   bool
	bank *dsp.Bank
	dc   *dsp.DCBlockerBank

	// Pulse tracking, writer-goroutine-owned like the DSP banks above.
	// tracker is nil when the device has no pulse stream at all. pulseID's
	// zero value is sample.EEG (StreamID 0), which would wrongly match an EEG
	// chunk — but pulseUpdate checks tracker == nil first, and tracker stays
	// nil on exactly the devices where pulseID is never set, so the
	// coincidence is harmless. Kept as a plain zero value rather than a
	// sentinel because the guard already makes it correct.
	tracker       *pulse.Tracker
	pulseID       sample.StreamID
	pulseNames    []string
	pulseRate     float64
	pulseChannels int
	nextPulse     float64
	lastPulse     float64
	havePulse     bool
	pulseStale    bool

	// Battery polling, also writer-goroutine-owned. battery is nil when the
	// source has none. sentBattery guards the first send: without it, a source
	// whose level is not known yet would never produce a message at all, and
	// the viewer could not tell "waiting for the first reading" from "the
	// server forgot about me".
	battery     func() (float64, bool)
	nextBattery time.Time
	sentBattery bool
	lastPct     int
	lastKnown   bool
}

// newClient sizes the per-viewer DSP to the connected headband's EEG stream:
// the filter banks carry one history per channel, so a 4-channel Athena and a
// 5-channel classic headband need differently shaped state.
func newClient(streams *sample.Set, battery func() (float64, bool)) *client {
	m, ok := streams.Meta(sample.EEG)
	if !ok {
		// No EEG on this device: the banks are never used, but they must not
		// be nil, and a zero-channel bank is valid.
		m = sample.Meta{Rate: 256}
	}
	nch, rate := m.NumChannels(), m.Rate
	c := &client{
		bank:    dsp.NewBank(dsp.FirwinBandpass(filterTaps, filterLow, filterHigh, rate), nch),
		dc:      dsp.NewDCBlockerBank(0.5, rate, nch),
		battery: battery,
	}

	// Optics and PPG are alternatives — an Athena has the first, a classic
	// headband the second, and no device produces both — so the first one
	// present is this device's pulse stream.
	for _, id := range []sample.StreamID{sample.Optics, sample.PPG} {
		pm, ok := streams.Meta(id)
		if !ok {
			continue
		}
		c.tracker = pulse.NewTracker(pm.Rate, pm.NumChannels())
		c.pulseID = id
		c.pulseNames = pm.Channels
		c.pulseRate = pm.Rate
		c.pulseChannels = pm.NumChannels()
		break
	}
	return c
}

// pulseUpdate feeds pulse chunks to the tracker and uses every other stream as
// a freshness clock. It normally returns false for non-pulse chunks, except for
// the one that proves pulse delivery has gone stale and withdraws the readout.
func (c *client) pulseUpdate(chunk sample.Chunk) (PulseMessage, bool) {
	if c.tracker == nil || chunk.NumSamples() == 0 {
		return PulseMessage{}, false
	}
	now := chunk.Timestamps[chunk.NumSamples()-1]
	if chunk.Stream != c.pulseID {
		if !c.havePulse || c.pulseStale || now < c.lastPulse || now-c.lastPulse <= pulseStaleSec {
			return PulseMessage{}, false
		}

		// Rebuild the whole chain, including selector history. Merely breaking
		// HRV would leave the old accepted intervals available to BPM(), allowing
		// a resumed stream to present the pre-outage rate as locked.
		c.tracker = pulse.NewTracker(c.pulseRate, c.pulseChannels)
		c.pulseStale = true
		c.nextPulse = now + pulseIntervalSec
		return c.pulseMessage(), true
	}

	c.tracker.Process(chunk.Data, chunk.Timestamps)
	c.lastPulse, c.havePulse, c.pulseStale = now, true, false

	// In normal operation now advances by roughly one sample step per call,
	// so it sits within one interval below nextPulse while throttled. If it
	// is further behind than that, nextPulse was not set from a plausible
	// now: either a corrupted timestamp jumped far into the future (the
	// classic path's Dejitter tolerates large counter steps, see
	// internal/muse/timestamp.go) or the clock stepped backwards by more
	// than an interval. Either way, waiting for the stale nextPulse to
	// arrive would wedge the readout silent forever, so resync to now and
	// send immediately instead.
	if now < c.nextPulse && now >= c.nextPulse-pulseIntervalSec {
		return PulseMessage{}, false
	}
	c.nextPulse = now + pulseIntervalSec

	return c.pulseMessage(), true
}

// pulseMessage serialises the current tracker snapshot. Keeping this separate
// lets a stale transition publish StateNoPulse without feeding unrelated data
// into the tracker.
func (c *client) pulseMessage() PulseMessage {
	snap := c.tracker.Snapshot()
	beats := snap.Beats
	if beats == nil {
		// Snapshot leaves Beats nil when no channel is selected. Normalise to
		// an empty slice so the JSON shape (an array) is consistent across
		// states rather than sometimes null, sometimes [] — the browser
		// should not have to special-case that.
		beats = []float64{}
	}
	msg := PulseMessage{
		Type:      "pulse",
		State:     string(snap.State),
		Beats:     beats,
		BeatCount: snap.BeatCount,
	}
	if snap.Channel >= 0 && snap.Channel < len(c.pulseNames) {
		name := c.pulseNames[snap.Channel]
		msg.Channel = &name
	}
	if snap.HasBPM {
		v := snap.BPM
		msg.BPM = &v
	}
	if snap.HasRMSSD {
		v := snap.RMSSD
		msg.RMSSD = &v
	}
	if snap.HasIBI {
		v := snap.IBIMs
		msg.IBIMs = &v
	}
	return msg
}

// batteryUpdate polls the source's battery and reports whether a message is
// due. It returns false for sources with no battery, between poll intervals,
// and — the common case by far — when the level has not moved since the last
// message: a cell that drains a percent an hour would otherwise generate
// thousands of identical frames for every one that carries news.
//
// The level is compared as a whole percent because that is what the viewer
// displays. Below that, the Athena's 1/512 quantisation and a classic
// headband's fuel gauge both jitter in the low bits, which would defeat the
// change check without changing anything anyone can see.
func (c *client) batteryUpdate(now time.Time) (BatteryMessage, bool) {
	if c.battery == nil {
		return BatteryMessage{}, false
	}
	if c.sentBattery && now.Before(c.nextBattery) {
		return BatteryMessage{}, false
	}
	c.nextBattery = now.Add(batteryInterval)

	pct, known := c.battery()
	rounded := int(math.Round(pct))
	if c.sentBattery && known == c.lastKnown && (!known || rounded == c.lastPct) {
		return BatteryMessage{}, false
	}
	c.sentBattery, c.lastKnown, c.lastPct = true, known, rounded

	msg := BatteryMessage{Type: "battery"}
	if known {
		// The raw percentage goes out, not the rounded one, and it is not
		// clamped to 100: a level outside the plausible range means the decode
		// is wrong, and hiding that in the viewer would make it much harder to
		// find. The browser clamps the bar it draws, not the number it shows.
		v := pct
		msg.Percent = &v
	}
	return msg, true
}

// readLoop handles control messages until the connection fails, then cancels
// the connection's context so the writer stops too.
func (c *client) readLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var ctrl wire.Control
		if err := json.Unmarshal(data, &ctrl); err != nil {
			continue // Ignore junk rather than dropping the viewer.
		}
		if ctrl.Filter != nil {
			c.want.Store(*ctrl.Filter)
		}
		if ctrl.Window != nil {
			c.window.Store(uint64(*ctrl.Window * 1000))
		}
	}
}

// prepare applies this client's filter settings to a chunk, returning either
// the chunk unchanged or a new one. It never writes into c's slices: the same
// chunk is being delivered to every other subscriber.
//
// Only EEG is filtered. A 1-40 Hz bandpass designed for 256 Hz would be
// meaningless on 52 Hz accelerometer data, and the pedestal in the pulse
// streams (classic PPG, Athena optics) is signal, not drift.
func (c *client) prepare(chunk sample.Chunk) sample.Chunk {
	want := c.want.Load()
	if want != c.on {
		if want {
			// Start from a settled filter rather than resuming mid-transient
			// from whenever the viewer last turned it off.
			c.bank.Reset()
			c.dc.Reset()
		}
		c.on = want
	}
	if !c.on || chunk.Stream != sample.EEG {
		return chunk
	}
	if chunk.NumChannels() != c.bank.Channels() {
		return chunk // Not the shape the banks were built for; pass it through.
	}
	return sample.Chunk{
		Stream:     chunk.Stream,
		Timestamps: chunk.Timestamps,
		Data:       c.bank.Process(c.dc.Process(chunk.Data)),
	}
}
