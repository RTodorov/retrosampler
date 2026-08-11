// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// spanRef addresses one span in a batch; next chains refs of one trace
// (used by Fragmenter for arena chaining; encoder receives flat slice).
type spanRef struct{ rs, ss, sp, next int32 }

func sizeStatus(st ptrace.Status) int {
	return sizeStr(2, st.Message()) + sizeVarintF(3, i64wire(int64(st.Code())))
}

func putStatus(e *enc, st ptrace.Status) {
	e.str(2, st.Message())
	e.varintF(3, i64wire(int64(st.Code())))
}

func sizeEvent(ev ptrace.SpanEvent) int {
	return sizeFixed64F(1, uint64(ev.Timestamp())) +
		sizeStr(2, ev.Name()) +
		sizeAttrs(3, ev.Attributes()) +
		sizeVarintF(4, uint64(ev.DroppedAttributesCount()))
}

func putEvent(e *enc, ev ptrace.SpanEvent) {
	e.fixed64F(1, uint64(ev.Timestamp()))
	e.str(2, ev.Name())
	putAttrs(e, 3, ev.Attributes())
	e.varintF(4, uint64(ev.DroppedAttributesCount()))
}

func sizeLink(lk ptrace.SpanLink) int {
	ltid, lsid := lk.TraceID(), lk.SpanID()
	n := 0
	if !ltid.IsEmpty() {
		n += sizeLen(1, 16)
	}
	if !lsid.IsEmpty() {
		n += sizeLen(2, 8)
	}
	return n + sizeStr(3, lk.TraceState().AsRaw()) +
		sizeAttrs(4, lk.Attributes()) +
		sizeVarintF(5, uint64(lk.DroppedAttributesCount())) +
		sizeFixed32F(6, lk.Flags())
}

func putLink(e *enc, lk ptrace.SpanLink) {
	if ltid := lk.TraceID(); !ltid.IsEmpty() {
		e.bytesF(1, ltid[:])
	}
	if lsid := lk.SpanID(); !lsid.IsEmpty() {
		e.bytesF(2, lsid[:])
	}
	e.str(3, lk.TraceState().AsRaw())
	putAttrs(e, 4, lk.Attributes())
	e.varintF(5, uint64(lk.DroppedAttributesCount()))
	e.fixed32F(6, lk.Flags())
}

func sizeSpan(sp ptrace.Span) int {
	stid, ssid, spid := sp.TraceID(), sp.SpanID(), sp.ParentSpanID()
	n := 0
	if !stid.IsEmpty() {
		n += sizeLen(1, 16)
	}
	if !ssid.IsEmpty() {
		n += sizeLen(2, 8)
	}
	n += sizeStr(3, sp.TraceState().AsRaw())
	if !spid.IsEmpty() {
		n += sizeLen(4, 8)
	}
	n += sizeStr(5, sp.Name()) +
		sizeVarintF(6, i64wire(int64(sp.Kind()))) +
		sizeFixed64F(7, uint64(sp.StartTimestamp())) +
		sizeFixed64F(8, uint64(sp.EndTimestamp())) +
		sizeAttrs(9, sp.Attributes()) +
		sizeVarintF(10, uint64(sp.DroppedAttributesCount()))
	for i := 0; i < sp.Events().Len(); i++ {
		n += sizeLen(11, sizeEvent(sp.Events().At(i)))
	}
	n += sizeVarintF(12, uint64(sp.DroppedEventsCount()))
	for i := 0; i < sp.Links().Len(); i++ {
		n += sizeLen(13, sizeLink(sp.Links().At(i)))
	}
	n += sizeVarintF(14, uint64(sp.DroppedLinksCount()))
	if s := sizeStatus(sp.Status()); s > 0 {
		n += sizeLen(15, s)
	}
	n += sizeFixed32F(16, sp.Flags())
	return n
}

func putSpan(e *enc, sp ptrace.Span) {
	if stid := sp.TraceID(); !stid.IsEmpty() {
		e.bytesF(1, stid[:])
	}
	if ssid := sp.SpanID(); !ssid.IsEmpty() {
		e.bytesF(2, ssid[:])
	}
	e.str(3, sp.TraceState().AsRaw())
	if spid := sp.ParentSpanID(); !spid.IsEmpty() {
		e.bytesF(4, spid[:])
	}
	e.str(5, sp.Name())
	e.varintF(6, i64wire(int64(sp.Kind())))
	e.fixed64F(7, uint64(sp.StartTimestamp()))
	e.fixed64F(8, uint64(sp.EndTimestamp()))
	putAttrs(e, 9, sp.Attributes())
	e.varintF(10, uint64(sp.DroppedAttributesCount()))
	for i := 0; i < sp.Events().Len(); i++ {
		e.msg(11, sizeEvent(sp.Events().At(i)))
		putEvent(e, sp.Events().At(i))
	}
	e.varintF(12, uint64(sp.DroppedEventsCount()))
	for i := 0; i < sp.Links().Len(); i++ {
		e.msg(13, sizeLink(sp.Links().At(i)))
		putLink(e, sp.Links().At(i))
	}
	e.varintF(14, uint64(sp.DroppedLinksCount()))
	if s := sizeStatus(sp.Status()); s > 0 {
		e.msg(15, s)
		putStatus(e, sp.Status())
	}
	e.fixed32F(16, sp.Flags())
}

func sizeResource(res pcommon.Resource) int {
	return sizeAttrs(1, res.Attributes()) +
		sizeVarintF(2, uint64(res.DroppedAttributesCount()))
}

func putResource(e *enc, res pcommon.Resource) {
	putAttrs(e, 1, res.Attributes())
	e.varintF(2, uint64(res.DroppedAttributesCount()))
}

func sizeScope(sc pcommon.InstrumentationScope) int {
	return sizeStr(1, sc.Name()) + sizeStr(2, sc.Version()) +
		sizeAttrs(3, sc.Attributes()) +
		sizeVarintF(4, uint64(sc.DroppedAttributesCount()))
}

func putScope(e *enc, sc pcommon.InstrumentationScope) {
	e.str(1, sc.Name())
	e.str(2, sc.Version())
	putAttrs(e, 3, sc.Attributes())
	e.varintF(4, uint64(sc.DroppedAttributesCount()))
}

// ssRun is a contiguous run of refs sharing (rs, ss).
func sizeSSRun(ss ptrace.ScopeSpans, refs []spanRef) int {
	n := sizeLen(1, sizeScope(ss.Scope()))
	for _, r := range refs {
		n += sizeLen(2, sizeSpan(ss.Spans().At(int(r.sp))))
	}
	return n + sizeStr(3, ss.SchemaUrl())
}

func sizeRSRun(rs ptrace.ResourceSpans, refs []spanRef) int {
	n := sizeLen(1, sizeResource(rs.Resource()))
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].ss == refs[i].ss {
			j++
		}
		ss := rs.ScopeSpans().At(int(refs[i].ss))
		n += sizeLen(2, sizeSSRun(ss, refs[i:j]))
		i = j
	}
	return n + sizeStr(3, rs.SchemaUrl())
}

// sizeGroup/putGroup encode a TracesData (resource_spans = 1) holding
// exactly the referenced spans. refs must be in batch iteration order.
func sizeGroup(td ptrace.Traces, refs []spanRef) int {
	n := 0
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].rs == refs[i].rs {
			j++
		}
		rs := td.ResourceSpans().At(int(refs[i].rs))
		n += sizeLen(1, sizeRSRun(rs, refs[i:j]))
		i = j
	}
	return n
}

func putGroup(e *enc, td ptrace.Traces, refs []spanRef) {
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].rs == refs[i].rs {
			j++
		}
		rs := td.ResourceSpans().At(int(refs[i].rs))
		e.msg(1, sizeRSRun(rs, refs[i:j]))
		e.msg(1, sizeResource(rs.Resource()))
		putResource(e, rs.Resource())
		for k := i; k < j; {
			l := k
			for l < j && refs[l].ss == refs[k].ss {
				l++
			}
			ss := rs.ScopeSpans().At(int(refs[k].ss))
			e.msg(2, sizeSSRun(ss, refs[k:l]))
			e.msg(1, sizeScope(ss.Scope()))
			putScope(e, ss.Scope())
			for _, r := range refs[k:l] {
				sp := ss.Spans().At(int(r.sp))
				e.msg(2, sizeSpan(sp))
				putSpan(e, sp)
			}
			e.str(3, ss.SchemaUrl())
			k = l
		}
		e.str(3, rs.SchemaUrl())
		i = j
	}
}
