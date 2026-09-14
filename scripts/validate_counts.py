# produced == consumed + dlq from the dashboard's GET /validate, with each
# group's lag folded in. Called by validate.sh:
#   python3 validate_counts.py <validate json> <lag json> <batch size>
# Exit 0 when every check passes.
import json, sys
v = json.loads(sys.argv[1]); lag = json.loads(sys.argv[2]); batch = int(sys.argv[3])
ok = True
fold = v.get("fold") or {}
if fold and not fold.get("complete", True):
    # The dashboard instance took over mid-stream (a daemon restart or a
    # reclaimed pool replaced it): each partition resumed from its own
    # committed point, so produced and consumed cover different windows.
    print("\033[33mWARN\033[0m  the dashboard's fold is partial (resumed mid-stream on %d partition(s), up %ss): produced == consumed cannot be judged from it — rerun `make up` to reset the fold; lag and the duplicate check below still stand"
          % (len(fold.get("partial_partitions") or []), fold.get("uptime_s")))
    led = v["ledger"]
    print(("\033[32mPASS\033[0m  " if led["duplicates"] == 0 else "\033[31mFAIL\033[0m  ") + f"st06 ledger (read_committed, since the dashboard resumed): {led['records']} records, {led['units']} distinct units, {led['duplicates']} duplicates")
    sys.exit(0 if led["duplicates"] == 0 else 1)
GREEN, RED, OFF = "\033[32m", "\033[31m", "\033[0m"
def report(good, text):
    global ok
    if not good: ok = False
    print(("%sPASS%s  " % (GREEN, OFF) if good else "%sFAIL%s  " % (RED, OFF)) + text)
groups = {"st02": "st02-die-attach", "st03": "st03-wire-bond", "st04": "st04-inspection",
          "st05": "st05-mold-cure", "st06": "st06-final-test", "st06twin": "st06-final-test-twin"}
for st, s in v["stations"].items():
    if st == "st01":
        continue
    produced, consumed, dlq = s["produced"], s["consumed"], s["dlq"]
    inflight = lag.get(groups[st]) or 0
    if st in ("st03", "st06"):
        # The uncommitted partial batch is neither consumed nor lost.
        good = produced == consumed + dlq + inflight and inflight < batch
        report(good, f"{st}: produced {produced} == consumed {consumed} + dlq {dlq} + in-flight {inflight}" +
               ("" if good else f"  (diff {produced - consumed - dlq - inflight})"))
    elif st == "st06twin":
        # Consumed counts what it processed, whether or not it has committed
        # yet — including records it replayed after a restart, which are the
        # twin ledger's duplicates; only the partial batch may be outstanding.
        replayed = v["twin"]["duplicates"]
        pending = produced - (consumed - replayed) - dlq
        good = 0 <= pending < batch
        report(good, f"{st}: produced {produced} == consumed {consumed} − replayed {replayed} + dlq {dlq} + partial batch {pending} (uncommitted lag {inflight})")
    else:
        extra = f" · redelivered {s['redelivered']} · replayed {s['replayed']}" if s["redelivered"] or s["replayed"] else ""
        good = produced == consumed + dlq
        report(good, f"{st}: produced {produced} == consumed {consumed} + dlq {dlq}{extra}" +
               ("" if good else f"  (diff {produced - consumed - dlq})"))
s1 = v["stations"]["st01"]
report(True, f"st01: {s1['lots']} lot(s) via POST /lot → {s1['produced']} wafer records on probe.lots; rejected {s1['rejected']}; simulator put {s1['produced_by_simulator']} more")
led, twin = v["ledger"], v["twin"]
report(led["duplicates"] == 0, f"st06 ledger (read_committed): {led['records']} records, {led['units']} distinct units, {led['duplicates']} duplicates")
print(f"      twin ledger (at-least-once): {twin['records']} records, {twin['units']} distinct units, {twin['duplicates']} duplicates")
sys.exit(0 if ok else 1)
