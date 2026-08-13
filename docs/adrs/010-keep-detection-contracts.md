# ADR-010: Keep-detection contracts — baggage, baseline, OTTL policies

- **Date:** 2026-08-13
- **Status:** Accepted
- **Related:** ADR-003, ADR-004, ADR-008

## Context

Stage 4 lands the detection chain ADR-008 rule 1 and rule 2 named but did not
mechanize: the guaranteed-alloc-free built-ins, the deterministic baseline,
and OTTL user policies. Building it fixed the transport for baggage, the
baseline's exact bit source and its out-of-range hazards, the OTTL error
policy, and one accepted gap in the keep lifecycle. It also amends ADR-004
rule 2's description of the gated benchmark set and closes the stage-3
future-stamped-segment carry-over on the ingest side.

## Decision

1. **Baggage rides span attributes under configurable keys.**
   `t0_attribute` / `elapsed_ms_attribute`, defaulting to `baggage.t0` and
   `baggage.elapsed_ms`, both epoch-milliseconds. Values are accepted as
   int64 or as a decimal-digit string, because baggage is a string on the
   wire — a generic baggage→attribute copier yields strings, a custom
   stamper may write ints, and refusing either half would refuse half the
   deployments. The string grammar is digits only: `strconv`'s error path
   allocates, and anything outside that grammar is misconfiguration rather
   than data.
   **Missing baggage means the condition cannot fire** — never a keep, never
   an error. Baggage is a platform property, and a propagation gap is
   already ADR-003 rule 5's exported health signal, not this processor's
   fault to escalate. **Malformed-but-present is counted separately**
   (`baggage.malformed`) from the propagation gap: a wrong-typed or
   non-numeric value is an upstream bug, whereas an absent one is an
   un-instrumented hop, and one number covering both would hide whichever
   is rarer.
   **A positive threshold never silently disables itself.** Milliseconds are
   the finest granularity baggage carries, so a sub-millisecond threshold
   floors to 1ms instead of truncating to 0 — truncation would turn
   `trace_latency_threshold: 500us` into a condition that is configured,
   documented, exported, and dead.

2. **The baseline verdict is the trace id's trailing 56 bits compared
   against a threshold precomputed from `baseline_rate`.** Those bits are
   the OTel consistent-probability sampling randomness source, random by
   contract under W3C trace-context level 2 ids; the threshold is
   `round(rate × 2^56)`, computed once at Build. No hash and no float on the
   hot path, and identical on every instance by construction — which is the
   whole point (ADR-008 rule 1: complete healthy traces need no broadcast).
   A full-id-hash mode for deployments with non-level-2 ids is a compatible
   later addition, not a supersession: it changes which bits feed the
   compare, not the contract.
   **`Config.Validate`'s `[0, 1]` bound with NaN rejection is a correctness
   bound, not cosmetics.** Each way out of range fails differently, and only
   one of the three is benign:
   - `rate > 1` yields a threshold above the 56-bit mask, so every trace
     keeps. Defined, useless, and probably not what was meant.
   - `rate ≥ 256` overflows the float64→uint64 conversion, whose behaviour
     Go leaves implementation-defined and which therefore differs by
     architecture. A mixed-architecture fleet would compute *different*
     baselines from the same config, destroying the identical-verdict
     property the design rests on.
   - `rate < 0` or NaN is swallowed by `Build`'s `> 0` gate (NaN compares
     false against everything), silently disabling the baseline.
   Validate rejecting the range is what keeps the last two out of a running
   collector.

3. **User policies are a named `{name, condition}` list, one ottlspan
   boolean condition each, OR-composed across policies, first match wins.**
   Names must be unique and non-empty: they key the per-policy telemetry
   attribution and the per-policy-chain benchmark baselines (ADR-004 rule 2,
   ADR-008 rule 2). **Conditions parse twice** — once in `Config.Validate`
   against throwaway nop settings so a typo fails config load loudly, and
   once in `Build` with the component's real telemetry settings. Parsing is
   the only way to validate an OTTL expression, and a startup-time
   `component.TelemetrySettings` does not exist during `Validate`.
   The single ottlfuncs import ADR-005 already authorizes brings a
   **transitive dependency surface that is accepted knowingly**: go-grok,
   xmlquery/xpath, uap-go, murmur3 and others arrive indirect, none imported
   by our code, all reachable through the standard converter set. That
   surface is the price of the one rule language; it is priced here so a
   later `go.mod` reader does not mistake it for scope creep.

4. **An OTTL evaluation error is ignore-and-count, with a warn-once per
   policy**: the span does not match, `policy.eval_errors` moves, the walk
   continues to the next policy. There is deliberately **no `error_mode`
   field** (ADR-005 rule 2 — no knob for a decision the processor should
   make): propagating would fail the batch, and a receiver retrying a poison
   span turns one malformed record into an endlessly retried batch.
   The eval-error path is narrower than it looks, and the tests are shaped
   by a verified v0.158.0 fact: `Int()` over a non-numeric string is a
   **silent no-match, not an eval error** — `StandardIntLikeGetter` swallows
   the `ParseInt` failure. Producing a genuine eval error takes a type
   mismatch. Anyone writing a test that assumes "bad input ⇒ error" will
   write a passing test that proves nothing.

5. **A baseline keep suppresses a later error keep's broadcast. Accepted.**
   Decided-set idempotency (ADR-008 rule 5) marks a trace once and does not
   retain the deciding reason, so a trace already decided as baseline
   absorbs a subsequent error verdict entirely — including the publish that
   would have told peers. Spans still flush locally (decided-arrival
   forward), so this instance's copy is complete; peers expire their
   fragments of a trace this instance kept. The exposure is bounded by
   `baseline_rate` — at a 1% baseline, 1% of error traces lose their
   cross-instance completeness — and escalation would require the decided
   set to retain its reason and re-open for a stronger one, which ADR-008
   rule 5 deliberately does not do. Escalation stays a **compatible future
   extension** of the decided set, not a fix owed now. Pinned by
   `TestKeepAfterBaselineIsDuplicate` so a future change announces itself.

6. **Event stamps are clamped to the worker's own clock at ingest** — the
   ingest half of ADR-008 rule 7's time hygiene. A producer clock running
   ahead used to be unreclaimable: a future `tMax` outlived both `Expire`
   and the watermark rung's floor check, so enough skewed data pinned a
   shard `atFloor` and shed all its ingest until real time passed the skew,
   and a future keep deadline held its decided entry — and, the ring
   evicting in insertion order, every entry behind it — past `W`. One clock
   read per event on the worker goroutine, off the ADR-004 producer path,
   counted in `Stats.ClampedStamps` so upstream skew reads as a number
   instead of vanishing.
   **This closes the ingest side only, and it does not make deadlines
   monotone.** Deadlines are wall-clock derived: a local clock stepping
   backwards still inserts a smaller deadline behind a larger one, and a
   `tMax` already on disk from a pre-clamp build can still be replayed ahead
   of `now` on restart. The tick's `max(now − tMax, 0)` guard therefore
   stays load-bearing, the buffer-side residual stays a recorded carry-over,
   and no claim of monotonicity-by-construction is made here.
   `ClampedStamps` is deliberately **not exported through telemetry this
   stage**: the counter earns a metric when there is an operator question it
   answers, and `skew.clamped` already carries the detector's clamps.

7. **The built-in/OTTL cost split is documented in `doc.go` and the config
   field comments.** ADR-008's consequence asked for it in the generated
   `README.md` / `documentation.md`, and mdatagen v0.158.0 offers no
   free-text hook there — the generated files are regenerated wholesale, so
   text added to them is either lost or a harness silencing (ADR-001). The
   godoc *is* the generated-adjacent surface: `doc.go` states the split,
   and each config field states its own cost beside its own knob, where an
   operator reading `span_latency_threshold` will actually meet it. This
   interprets ADR-008's consequence; it does not supersede it.

8. **ADR-004 rule 2's OTTL benchmark landed as a standalone
   `BenchmarkPolicyEval` in `internal/detect`, not as a `BenchmarkIngest`
   variant**, and it carries its own committed baseline rows. The gated set
   is now **six** (`bench_gate.sh`'s `gated=6` and the Makefile `-bench`
   regex, both edits still required together per ADR-009 rule 7). A separate
   benchmark isolates the policy tail from ingest's fragmenting and
   appending, so the allocs/op number attributes to the expression rather
   than to everything around it.
   **Scope, stated so the row is not over-read:** it prices *one*
   simple-compare shape over a non-matching span. Chained conditions,
   converting functions, and regex remain unpriced by the gate — deliberately,
   because ADR-008 rule 2's whole point is that OTTL cost is
   expression-shape-dependent, and a gate on one shape must not be mistaken
   for a bound on all of them.

9. **`bus.ReasonBaseline` is the highest reason byte, and that is a shared
   invariant.** Two independent things size themselves from it:
   `detect.nReasons` allocates the per-reason counter array as
   `ReasonBaseline + 1`, and `telemetry.go`'s `detReasons` table claims to
   name every byte in `1..ReasonBaseline`. A new constant declared above
   `ReasonBaseline` silently breaks both — an out-of-range counter index
   swallowed by a bounds check, and a metric label that never appears — with
   no compile error in either. Recorded here because the coupling is
   invisible at both definition sites; `TestDetReasonsPinsEveryLabel` is the
   guard.

## Consequences

- Operators get one rule language and a small set of scalar built-ins, per
  ADR-008's "the config surface must not present two rule systems". The
  built-ins' alloc-free guarantee and OTTL's ~30–110 ns / 0–3 allocs per
  span per condition are now stated where they are read: `doc.go` and the
  field comments, with span latency called out as the case where the
  setting must be used instead of an OTTL arithmetic expression.
- A deployment that configures no policies keeps a provably alloc-free hot
  path: the OTTL tail is skipped by a length check, and
  `TestDetectBuiltinsZeroAllocs` gates the armed built-in chain including
  its string-parsing arm.
- The baseline grows `kept.local` without growing `published.keeps`, which
  widens the ADR-009 rule 6 crash-window gap monitor by design. That gap is
  no longer readable as "lost publishes" alone once `baseline_rate > 0`;
  the `detected.keeps{reason=baseline}` series is what separates them.
- Rule 5's accepted gap means cross-instance completeness for error traces
  degrades by roughly `baseline_rate`. Operators running a large baseline
  should know they are trading trace completeness for it.
- Rule 2's out-of-range analysis makes `Config.Validate` load-bearing for
  fleet-wide determinism, not merely for input hygiene. A future config
  path that bypasses `Validate` would reintroduce an arch-divergent
  baseline.
