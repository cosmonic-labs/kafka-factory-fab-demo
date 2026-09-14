#!/usr/bin/env python3
"""Regenerate the static scenario record sets the simulator embeds.

Deterministic (fixed seed) so a regeneration is a no-op diff. Every line is
one Kafka record: `f` is the 1-second frame it plays in, the key field named
in KEYS is the record key (die / bonder / job / press / unit / lot), and the
rest is the synthetic sensor payload the station parses.

Rates are the design doc's, scaled to a laptop (a Redpanda in a container
and a single Desktop host): ~30 die-attach readings/s, 40 bonders at 1
bond/s, one inspection job every 2 s, 6 presses x 8 zones of mold telemetry,
20 final-test units/s, one probe lot per pass.

A scenario directory holds `scenario.json` ({frames, next, overlay}) and one
`<topic>.jsonl` per topic it drives. A loop scenario that omits a topic
falls back to baseline's frames for it; an overlay only ever adds records.
"""
import json
import math
import os
import random

HERE = os.path.dirname(os.path.abspath(__file__))
rng = random.Random(20260914)

KEYS = {
    "probe.lots": "lot",
    "dieattach.readings": "die",
    "wirebond.raw": "bonder",
    "inspect.jobs": "job",
    "mold.telemetry": "press",
    "finaltest.bins": "unit",
}

HEADS = ["H1", "H2", "H3", "H4"]
PRESSES = ["M-1", "M-2", "M-3", "M-4", "M-5", "M-6"]
PROBERS = ["P-03", "P-07", "P-11"]
DEFECTS = ["none", "none", "none", "none", "none", "none", "void>10%", "bridge", "lift"]


def r(x, nd=2):
    return round(x, nd)


def die_reading(f, i, seq):
    head = HEADS[i % 4]
    # H3 runs a little wide so its Cpk is the worst head on the panel.
    sd = 1.6 if head == "H3" else 1.1
    return {
        "f": f,
        "die": f"D-{seq:07d}",
        "head": head,
        "bl_um": r(rng.gauss(25.0, sd)),
        "epoxy_mg": r(rng.gauss(3.2, 0.18)),
        "dx_um": r(rng.gauss(0, 4.0), 1),
        "dy_um": r(rng.gauss(0, 4.0), 1),
        "theta_deg": r(rng.gauss(0, 0.05), 3),
        "stage_c": r(rng.gauss(25.4, 0.2), 1),
    }


def bond(f, bonder, seq, power_scale=1.0):
    base_w = 1.10 + 0.01 * (bonder % 7)
    return {
        "f": f,
        "bonder": bonder,
        "bond": seq,
        "us_w": r(rng.gauss(base_w * power_scale, 0.02), 3),
        "us_khz": r(rng.gauss(120.0, 0.3), 1),
        "force_gf": r(rng.gauss(25.0, 0.6), 1),
        "cap_c": r(rng.gauss(150.0, 0.8), 1),
    }


def job(f, seq, lot):
    return {
        "f": f,
        "job": f"J-{seq:06d}",
        "strip": f"S-{seq // 4:05d}",
        "lot": lot,
        "cost_ms": rng.randint(200, 400),
        "img": f"xray://fab3/strip/S-{seq // 4:05d}/{seq % 4}.tif",
    }


def mold(f, press, zone, temp_offset=0.0):
    return {
        "f": f,
        "press": press,
        "zone": zone,
        "temp_c": r(rng.gauss(175.0 + temp_offset, 0.6), 1),
        "press_mpa": r(rng.gauss(7.0, 0.15)),
        "rh_pct": r(rng.gauss(42.0, 1.0), 1),
        "particles": max(0, int(rng.gauss(110, 25))),
        "vib_g": r(abs(rng.gauss(0.10, 0.03)), 3),
    }


def unit(f, seq, lot):
    b = rng.choices([1, 2, 3, 5, 8, 12, 16], weights=[90, 4, 2, 1, 1, 1, 1])[0]
    return {
        "f": f,
        "unit": f"U-{seq:07d}",
        "lot": lot,
        "bin": b,
        "vout": r(rng.gauss(3.300, 0.004), 4),
        "iq_ua": r(rng.gauss(12.3, 0.5), 1),
        "tester_c": r(rng.gauss(24.8, 0.3), 1),
    }


def lot_wafers(f, lot_seq, wafers=25):
    lot = f"L-{24090 + lot_seq // 40:05d}-{lot_seq % 40:02d}"
    prober = PROBERS[lot_seq % len(PROBERS)]
    return [
        {
            "f": f,
            "lot": lot,
            "prober": prober,
            "wafer": w + 1,
            "contact_mohm": r(rng.gauss(180.0, 12.0), 1),
            "chuck_c": r(rng.gauss(85.0, 0.4), 1),
            "yield_pct": r(rng.gauss(97.4, 0.8), 1),
        }
        for w in range(wafers)
    ]


def write(scenario, topic, lines):
    d = os.path.join(HERE, scenario)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{topic}.jsonl"), "w") as fh:
        for rec in lines:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")


def meta(scenario, frames, nxt=None, overlay=False, note=""):
    with open(os.path.join(HERE, scenario, "scenario.json"), "w") as fh:
        json.dump(
            {"name": scenario, "frames": frames, "next": nxt, "overlay": overlay, "note": note},
            fh,
            indent=2,
        )
        fh.write("\n")


def main():
    # ---- baseline: 60 frames, wraps ------------------------------------
    F = 60
    die, wb, jobs, mt, ft, lots = [], [], [], [], [], []
    dseq = bseq = jseq = useq = 0
    for f in range(F):
        for i in range(30):
            dseq += 1
            die.append(die_reading(f, i, dseq))
        for b in range(1, 41):
            bseq += 1
            wb.append(bond(f, b, bseq))
        if f % 2 == 0:
            jseq += 1
            jobs.append(job(f, jseq, "L-24091-07"))
        for p in PRESSES:
            for z in range(1, 9):
                mt.append(mold(f, p, z))
        for _ in range(20):
            useq += 1
            ft.append(unit(f, useq, f"L-2409{1 + useq // 3000}-{(useq // 500) % 40:02d}"))
    lots = lot_wafers(0, 1)
    os.makedirs(os.path.join(HERE, "baseline"), exist_ok=True)
    write("baseline", "dieattach.readings", die)
    write("baseline", "wirebond.raw", wb)
    write("baseline", "inspect.jobs", jobs)
    write("baseline", "mold.telemetry", mt)
    write("baseline", "finaltest.bins", ft)
    write("baseline", "probe.lots", lots)
    meta("baseline", F, None, False, "quiet Tuesday; wraps")

    # ---- shift-change: 60 frames then back to baseline -------------------
    # 100x lot uploads (2 lots per frame) and 2x mold telemetry; every other
    # topic falls back to baseline's frames.
    lots2, mt2 = [], []
    for f in range(F):
        for k in range(2):
            lots2.extend(lot_wafers(f, 100 + f * 2 + k))
        for p in PRESSES:
            for z in range(1, 9):
                mt2.append(mold(f, p, z))
                mt2.append(mold(f, p, z))
    os.makedirs(os.path.join(HERE, "shift-change"), exist_ok=True)
    write("shift-change", "probe.lots", lots2)
    write("shift-change", "mold.telemetry", mt2)
    meta("shift-change", F, "baseline", False, "06:00 shift change: 100x lots, 2x mold telemetry, then baseline by itself")

    # ---- poison: one-shot, one frame, one record ------------------------
    bad = die_reading(0, 2, 9999999)
    bad["die"] = "D-POISON1"
    bad["bl_um"] = "NaN"
    bad["prober"] = "P-07"
    os.makedirs(os.path.join(HERE, "poison"), exist_ok=True)
    write("poison", "dieattach.readings", [bad])
    meta("poison", 1, None, True, "one dieattach reading with a NaN bond-line thickness")

    # ---- drift: 90-frame overlay, bonder 17 sags 18% ----------------------
    # Overlays only add records, so this adds bonder 17's sagged bonds at 3x
    # baseline's rate: a 5-s window then averages (3 x 0.78 + 1.0) / 4 = 0.835
    # of the bonder's trailing mean, a 16.5% sag -- past ST-03's 15% NSOP line.
    drift = []
    for f in range(90):
        for k in range(3):
            bseq += 1
            drift.append(bond(f, 17, bseq, power_scale=0.78))
    os.makedirs(os.path.join(HERE, "drift"), exist_ok=True)
    write("drift", "wirebond.raw", drift)
    meta("drift", 90, None, True, "bonder 17 transducer power sags 18% for 90 s")

    # ---- excursion: 120-frame overlay, 20x inspection jobs ---------------
    exc = []
    for f in range(120):
        for k in range(10):
            jseq += 1
            exc.append(job(f, jseq, "L-24091-09"))
    os.makedirs(os.path.join(HERE, "excursion"), exist_ok=True)
    write("excursion", "inspect.jobs", exc)
    meta("excursion", 120, None, True, "bond excursion: 20x inspection jobs for 120 s")


if __name__ == "__main__":
    main()
