// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Command loadgen is a fragment-splitting OTLP trace generator (ADR-011 r7).
// It replaces telemetrygen for the multi-instance compose gate, which needs
// something telemetrygen cannot do: put the spans of ONE trace on DIFFERENT
// collectors, with the keep marker on only one of them. Every other
// endpoint then holds a fragment that looks healthy in isolation, so the
// only way those spans reach the exporter is the keep bus.
//
// The summary on stdout is a contract (see summary in gen.go); everything
// else — logs, flag errors — goes to stderr, so a consumer can pipe stdout
// straight into jq.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// batchTraces is the nominal traces per Export. Small enough that a
	// --duration run reacts to its deadline promptly, large enough that
	// per-RPC overhead does not dominate the byte rate.
	batchTraces = 100

	// maxBatchBytes keeps one OTLP request under the collector's 4 MiB
	// default receive limit once --span-bytes grows. spanOverheadBytes is
	// the ids, name, kind, timestamps and protobuf framing riding along
	// with each span's padding.
	maxBatchBytes     = 2 << 20
	spanOverheadBytes = 128

	// exportRetries is the number of retries AFTER the first attempt.
	exportRetries = 3
	backoffBase   = 100 * time.Millisecond
	exportTimeout = 30 * time.Second

	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// clock is the two wall-clock effects the driver needs. main wires the real
// ones; tests substitute a synthetic clock, which keeps pacing assertions
// exact and instant. Generation itself is clock-free — generate takes the
// base time as a value.
type clock struct {
	now   func() time.Time
	sleep func(time.Duration)
}

// usageError is a bad flag VALUE, reported by run. Flag syntax errors are
// reported by the FlagSet itself, so they are not wrapped in this.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return usageError{msg: fmt.Sprintf(format, args...)}
}

type options struct {
	endpoints          []string
	traces             int
	spansPerTrace      int
	errorTraces        int
	slowSpanTraces     int
	traceLatencyTraces int
	errorPct           float64
	slowSpanMS         int
	elapsedMS          int
	seed               uint64
	rate               int
	duration           time.Duration
	targetMbps         float64
	spanBytes          int
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		o         options
		endpoints string
	)
	fs.StringVar(&endpoints, "endpoints", "", "comma-separated OTLP/gRPC host:port targets; span j of every trace goes to endpoint j%N")
	fs.IntVar(&o.traces, "traces", 0, "total traces to send (mutually exclusive with --duration)")
	fs.IntVar(&o.spansPerTrace, "spans-per-trace", 4, "spans per trace, split round-robin across the endpoints")
	fs.IntVar(&o.errorTraces, "error-traces", 0, "traces whose root span carries an error status")
	fs.IntVar(&o.slowSpanTraces, "slow-span-traces", 0, "traces whose root span lasts --slow-span-ms")
	fs.IntVar(&o.traceLatencyTraces, "trace-latency-traces", 0, "traces whose root span carries "+elapsedMSAttribute)
	fs.Float64Var(&o.errorPct, "error-pct", 0, "percentage of traces carrying an error status; the --duration-mode alternative to --error-traces")
	fs.IntVar(&o.slowSpanMS, "slow-span-ms", 2000, "root span duration for the slow-span class")
	fs.IntVar(&o.elapsedMS, "elapsed-ms", 10000, "value of the "+elapsedMSAttribute+" attribute for the trace-latency class")
	fs.Uint64Var(&o.seed, "seed", 1, "generator seed; each batch uses seed+batchIndex, so ids stay fresh and the run stays reproducible")
	fs.IntVar(&o.rate, "rate", 0, "traces per second (0 = unpaced)")
	fs.DurationVar(&o.duration, "duration", 0, "run until this much time has passed (mutually exclusive with --traces)")
	fs.Float64Var(&o.targetMbps, "target-mbps", 0, "hold this OTLP payload rate in megabits per second; overrides --rate")
	fs.IntVar(&o.spanBytes, "span-bytes", 128, "bytes of attribute padding per span")

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	for e := range strings.SplitSeq(endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			o.endpoints = append(o.endpoints, e)
		}
	}
	return o, o.validate()
}

func (o options) validate() error {
	switch {
	case len(o.endpoints) == 0:
		return usagef("--endpoints is required")
	case o.traces > 0 && o.duration > 0:
		return usagef("--traces and --duration are mutually exclusive")
	case o.traces <= 0 && o.duration <= 0:
		return usagef("one of --traces or --duration is required")
	case o.spansPerTrace < 1:
		return usagef("--spans-per-trace must be at least 1")
	case o.spanBytes < 0:
		return usagef("--span-bytes must not be negative")
	case o.rate < 0:
		return usagef("--rate must not be negative")
	case o.targetMbps < 0:
		return usagef("--target-mbps must not be negative")
	case o.slowSpanMS < 0 || o.elapsedMS < 0:
		return usagef("--slow-span-ms and --elapsed-ms must not be negative")
	case o.errorTraces < 0 || o.slowSpanTraces < 0 || o.traceLatencyTraces < 0:
		return usagef("class counts must not be negative")
	case o.errorPct < 0 || o.errorPct > 100:
		return usagef("--error-pct must be between 0 and 100")
	// paramsFor derives the whole class mix from --error-pct alone, so any
	// absolute count set alongside it would be silently discarded.
	case o.errorPct > 0 && o.errorTraces+o.slowSpanTraces+o.traceLatencyTraces > 0:
		return usagef("--error-pct and the absolute class counts (--error-traces/--slow-span-traces/--trace-latency-traces) are mutually exclusive")
	}

	// Absolute class counts name a share of a known total, which --duration
	// does not have. --error-pct is the proportional form; refusing here
	// beats silently reinterpreting the number as per-batch.
	if o.duration > 0 && o.errorTraces+o.slowSpanTraces+o.traceLatencyTraces > 0 {
		return usagef("--error-traces/--slow-span-traces/--trace-latency-traces need --traces; use --error-pct with --duration")
	}
	if n := o.errorTraces + o.slowSpanTraces + o.traceLatencyTraces; n > o.traces {
		return usagef("keep-class traces (%d) exceed --traces (%d)", n, o.traces)
	}
	return nil
}

// tracesPerBatch caps a batch at batchTraces and shrinks it further when
// --span-bytes would push one OTLP request past maxBatchBytes.
func (o options) tracesPerBatch() int {
	perTrace := o.spansPerTrace * (o.spanBytes + spanOverheadBytes)
	return max(1, min(batchTraces, maxBatchBytes/max(perTrace, 1)))
}

// paramsFor slices the run-wide class assignment onto the batch covering
// traces [sent, sent+n). The classes occupy the leading indices of the run
// — [0,E), [E,E+S), [E+S,E+S+L) — so every batch takes its overlap and the
// totals come out exact regardless of where the batch boundaries fall.
func (o options) paramsFor(sent, n int, batch uint64) genParams {
	p := genParams{
		Endpoints:     len(o.endpoints),
		Traces:        n,
		SpansPerTrace: o.spansPerTrace,
		SlowSpanMS:    o.slowSpanMS,
		ElapsedMS:     o.elapsedMS,
		SpanBytes:     o.spanBytes,
		Seed:          o.seed + batch,
	}
	lo, hi := sent, sent+n
	if o.errorPct > 0 {
		p.ErrorTraces = cumPct(o.errorPct, hi) - cumPct(o.errorPct, lo)
		return p
	}
	errEnd := o.errorTraces
	slowEnd := errEnd + o.slowSpanTraces
	latEnd := slowEnd + o.traceLatencyTraces
	p.ErrorTraces = overlap(lo, hi, 0, errEnd)
	p.SlowSpanTraces = overlap(lo, hi, errEnd, slowEnd)
	p.TraceLatencyTraces = overlap(lo, hi, slowEnd, latEnd)
	return p
}

// cumPct rounds the cumulative total rather than each batch, so --error-pct
// holds the percentage over the run instead of drifting by a fraction of a
// trace per batch.
func cumPct(pct float64, n int) int {
	return int(math.Round(float64(n) * pct / 100))
}

func overlap(lo, hi, start, end int) int {
	return max(0, min(hi, end)-max(lo, start))
}

// paceDelay is how long to wait after a batch to hold the configured pace.
// --target-mbps wins over --rate. Zero means send now: either nothing is
// configured, or the run is already behind and cannot catch up by waiting.
func paceDelay(rate int, targetMbps float64, sentTraces int, sentBytes int64, elapsed time.Duration) time.Duration {
	var want time.Duration
	switch {
	case targetMbps > 0:
		bytesPerSec := targetMbps * 1e6 / 8
		want = time.Duration(float64(sentBytes) / bytesPerSec * float64(time.Second))
	case rate > 0:
		want = time.Duration(float64(sentTraces) / float64(rate) * float64(time.Second))
	default:
		return 0
	}
	return max(want-elapsed, 0)
}

// exporter is one endpoint's OTLP/gRPC client.
type exporter struct {
	target string
	conn   *grpc.ClientConn
	client ptraceotlp.GRPCClient
}

func (e *exporter) export(ctx context.Context, td ptrace.Traces) error {
	_, err := e.client.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
	return err
}

func dialAll(endpoints []string) ([]*exporter, error) {
	exps := make([]*exporter, 0, len(endpoints))
	for _, target := range endpoints {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeAll(exps, nil)
			return nil, fmt.Errorf("dialing %s: %w", target, err)
		}
		exps = append(exps, &exporter{target: target, conn: conn, client: ptraceotlp.NewGRPCClient(conn)})
	}
	return exps, nil
}

func closeAll(exps []*exporter, logger *log.Logger) {
	for _, e := range exps {
		if err := e.conn.Close(); err != nil && logger != nil {
			logger.Printf("closing %s: %v", e.target, err)
		}
	}
}

// exportWithRetry sends one batch, retrying a failure exportRetries times
// with exponential backoff. A batch that still fails ends the run: a load
// generator that quietly drops what it claims to have sent would make every
// downstream conservation assert meaningless.
func exportWithRetry(exp *exporter, td ptrace.Traces, clk clock, logger *log.Logger) error {
	var err error
	for attempt := range exportRetries + 1 {
		if attempt > 0 {
			clk.sleep(backoffBase * (1 << (attempt - 1)))
		}
		err = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), exportTimeout)
			defer cancel()
			return exp.export(ctx, td)
		}()
		if err == nil {
			return nil
		}
		logger.Printf("export to %s failed (attempt %d/%d): %v", exp.target, attempt+1, exportRetries+1, err)
	}
	return fmt.Errorf("export to %s failed after %d attempts: %w", exp.target, exportRetries+1, err)
}

// drive runs the batch loop and returns the summary. On an export failure
// it returns the summary accumulated so far alongside the error, so the
// caller can report how far the run got.
func drive(o options, exps []*exporter, clk clock, logger *log.Logger) (summary, error) {
	total := summary{
		SpansPerEndpoint:             make([]int, len(exps)),
		ExpectedKeptSpansPerEndpoint: make([]int, len(exps)),
		KeptTraceIDs:                 []string{},
	}
	sizer := &ptrace.ProtoMarshaler{}
	start := clk.now()
	deadline := start.Add(o.duration)
	per := o.tracesPerBatch()
	sent := 0

	for batch := uint64(0); ; batch++ {
		n := per
		if o.traces > 0 {
			if n = min(per, o.traces-sent); n <= 0 {
				break
			}
		} else if !clk.now().Before(deadline) {
			break
		}

		res := generate(o.paramsFor(sent, n, batch), clk.now())
		for e, exp := range exps {
			td := res.perEndpoint[e]
			if err := exportWithRetry(exp, td, clk, logger); err != nil {
				total.ElapsedSeconds = clk.now().Sub(start).Seconds()
				return total, err
			}
			// Counted after the Export returns, so bytes_sent is bytes the
			// collector accepted, not bytes we hoped to send.
			total.BytesSent += int64(sizer.TracesSize(td))
		}
		total.add(res.summary)
		sent += n

		if d := paceDelay(o.rate, o.targetMbps, sent, total.BytesSent, clk.now().Sub(start)); d > 0 {
			clk.sleep(d)
		}
	}

	total.ElapsedSeconds = clk.now().Sub(start).Seconds()
	return total, nil
}

func run(args []string, stdout, stderr io.Writer, clk clock) int {
	logger := log.New(stderr, "loadgen: ", 0)

	o, err := parseFlags(args, stderr)
	if err != nil {
		var ue usageError
		if errors.As(err, &ue) {
			logger.Printf("%v", err)
		}
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}

	exps, err := dialAll(o.endpoints)
	if err != nil {
		logger.Printf("%v", err)
		return exitError
	}
	defer closeAll(exps, logger)

	total, err := drive(o, exps, clk, logger)
	if err != nil {
		logger.Printf("%v", err)
		return exitError
	}
	// Only a complete run prints a summary, so "stdout parses" and "the run
	// succeeded" are the same statement for a consumer.
	if err := json.NewEncoder(stdout).Encode(total); err != nil {
		logger.Printf("writing summary: %v", err)
		return exitError
	}
	return exitOK
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, clock{now: time.Now, sleep: time.Sleep}))
}
