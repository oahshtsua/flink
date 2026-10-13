# Moving a Flink map/filter chain into C — how it's built, and why

The reference for **how everything in this experiment works and why it was built that way**, in
plain terms. It covers the idea, the test job (with a worked example), the Java version, the
native (C) version, how correctness is checked, and how speed is measured.

- **Results** (what we found): `microbatch-native-summary.md`
- **How to run it**: `benchmark/sensor-bench/README.md`

---

## Contents

1. [The idea](#1-the-idea)
2. [The test job](#2-the-test-job)
3. [The Java version](#3-the-java-version)
4. [The native (C) version](#4-the-native-c-version)
5. [Checking correctness](#5-checking-correctness)
6. [Measuring speed](#6-measuring-speed)
7. [Decisions at a glance](#7-decisions-at-a-glance)
8. [Lessons and pitfalls](#8-lessons-and-pitfalls)
9. [Where everything is](#9-where-everything-is)
10. [Not done yet](#10-not-done-yet)

---

## 1. The idea

### The problem

A Flink job is a chain of small steps, e.g. `map → filter → map → filter`. Flink already runs
such a chain in one thread with no network in between ("chaining"). But each step is still a
separate object, so every record takes a small **hop** from one step to the next: a method call
through Flink's plumbing. A few nanoseconds per hop, paid for every record, at every step.

### What already exists: fusion (Java)

An earlier change in this repo, the **fusion prototype**, spots a run of map/filter steps when
Flink wires up the job and merges them into **one** operator that calls the user's functions
back to back in a loop. It's still Java and still calls the same functions — it just removes the
hops. It made trivial jobs 16–23% faster.

### This experiment: one C function

Go one step further: replace the whole chain with **one C function** that does all the steps for
a record in a single loop. There are no steps any more, so there are no hops. Later, an AI agent
would **write that C function automatically** from the user's Java code — possibly smarter than
the original (for example, doing cheap checks first). For now the C function is written by hand,
as a stand-in.

### The catch: getting into C costs something

Java can't hand its objects to C. Every record has to be **copied** into plain memory that C can
read, and each **call** into C has a fixed cost. An earlier measurement found one call into C
costs about as much as the hops it was supposed to remove — so calling C once per record looked
pointless for trivial work. The fix is **batching**: collect many records, make one call for the
whole batch, so the call's cost is shared.

### The question

> Once each record does real work, is C actually faster than Java — and does that survive inside
> a real Flink job?

To keep it honest, the C version has to be a genuine, optimized effort: a weak C version would
"prove" Java is fine while proving nothing. SIMD (making the CPU process several numbers per
instruction) is deliberately left out: the point is moving the chain into one function, not
hand-vectorizing it.

---

## 2. The test job

### What it pretends to be

Sensor monitoring: many sensors send short bursts of readings, and we want to pick out the bursts
that look like a known pattern — while ignoring sensors that are dead or flat-lining.

```
source ──► map: stats ──► filter: not flat ──► map: score ──► filter: good match ──► sink
```

It's **only map and filter** — no reduce, no grouping, no windows, no state. Every record is
handled on its own. One thread (parallelism 1).

### The record

```java
class SensorReading {
    int      deviceId;              // which sensor
    long     ts;                    // when
    double[] samples;               // its last 8 readings (8 numbers = 64 bytes)
    double   mean, stddev, max;     // filled in by step 1
    double   score;                 // filled in by step 3
}
```

**Why 8 numbers?** 8 doubles are exactly 64 bytes — one "cache line", the chunk of memory the
CPU fetches in one go — and plain numbers are the cheapest thing to copy into C (measured earlier:
~1.8 ns per record, vs ~25 ns for records containing text). It gives C its best fair chance.

### What the source produces

The source counts `0, 1, 2, …` up to the number of records asked for (e.g. 50 million). Each
number `seq` becomes one record:

- `deviceId = seq % 1024` (1024 pretend sensors), `ts = seq`;
- the 8 readings come from a fixed pseudo-random formula seeded by `seq`, so record 2 is **always
  the same record**, in every run and every version.

A random draw decides what kind of record it is:

| kind | share | the 8 readings | what happens to it |
|---|---|---|---|
| flat-lining | ~15% | one value repeated, with tiny jitter (a dead sensor) | dropped at step 2 |
| known pattern | ~30% | one of the reference shapes, stretched and shifted, plus some noise | survives to the end |
| random noise | ~55% | random values | dropped at step 4, almost always |

### The four steps

| step | type | what it does |
|---|---|---|
| 1. **stats** | map | **average** of the 8 readings; **spread** (standard deviation: how far readings typically sit from the average); **peak** (largest reading) |
| 2. **not flat** | filter | keep only if the spread is at least **0.01** |
| 3. **score** | map | **normalize** the readings (subtract the average, divide by the spread) so only the *shape* is left; compare that shape with each of `k` **reference shapes** (sine waves) — a number from −1 (opposite) to 1 (identical); the best match is the **score** |
| 4. **good match** | filter | keep only if the score is at least **0.9** |

The **sink** either throws the results away (when timing) or computes a fingerprint of them (when
checking correctness, section 5).

### A worked example

Real records from the generator (`k = 1`; the reference shape is
`0.00, 1.00, 1.41, 1.00, 0.00, −1.00, −1.41, −1.00`, one sine wave):

| | record 0 | record 1 | record 2 |
|---|---|---|---|
| **samples** | 27.67 ×8 | 23.43, 24.07, 26.90, 25.07, 25.16, 27.10, 23.51, 23.28 | 15.29, 18.98, 21.25, 19.03, 15.33, 9.19, 7.91, 9.67 |
| kind | flat-lining | random noise | known pattern |
| **step 1** stats | average 27.67, spread 0.0000 | average 24.82, spread 1.43, peak 27.10 | average 14.58, spread 4.77, peak 21.25 |
| **step 2** not flat? | **no → dropped** | yes | yes |
| **step 3** normalized | — | −0.97, −0.52, 1.46, 0.18, 0.24, 1.60, −0.91, −1.08 | 0.15, 0.92, 1.40, 0.93, 0.16, −1.13, −1.40, −1.03 |
| **step 3** score | — | 0.31 | **0.995** |
| **step 4** good match? | — | **no → dropped** | **yes → output** |

Record 2's normalized readings are almost exactly the reference wave — because the generator made
it from that wave. Overall, ~85% of records pass step 2 and ~30% survive to the end.

### The difficulty dial: `k`

`k` is the number of reference shapes step 3 compares each record against. Counting the
arithmetic for one record:

| step | operations per record |
|---|---|
| 1. stats | ~45, fixed (8 adds + 8 comparisons for average and peak; 8 subtracts, 8 multiplies, 8 adds, a divide and a square root for the spread) |
| 2. not flat | 1 comparison |
| 3. score | 17 to normalize, **plus 16 per reference shape** (8 multiplies + 8 adds) |
| 4. good match | 1 comparison |

| `k` | operations per record (roughly) | Java time per record (measured) |
|---|---|---|
| 1 | ~65 | 35 ns |
| 4 | ~110 | 46 ns |
| 16 | ~300 | 77 ns |
| 64 | ~1,070 | 157 ns |

At `k=1`, step 1 is most of the work; at `k=64`, step 3's comparisons dominate.

Time grows much slower than the operation count. The 64 comparisons don't depend on each other,
so the CPU works on several at once. Step 1 is the opposite: each addition in a sum must wait for
the previous one, so it costs more time than its operation count suggests.

**Why `k` exists:** the question is about "real work", and with a fixed job "real enough" would
be one guessed point. (The original guess for this job was 80–150 ns per record; at `k=1` it's
really 35 ns.) `k` turns it into a curve. Changing `k` changes neither the record nor the share
of survivors (30.1% at `k=1`, 31.1% at `k=64`), so only the work changes.

Not to be confused with the **batch size** — how many records the native version hands to C in
one call. `k` changes how much work each record costs; the batch size only changes how often
Java crosses into C.

---

## 3. The Java version

### One record object, updated in place

Each step writes its results into the same `SensorReading` and passes it on, instead of creating
a new object. **Why:** the C version reuses its output objects too. If Java created new objects
at every step and C didn't, we'd be measuring Java's garbage collection, not the work.

The record follows Flink's "POJO" rules (public class, public fields, empty constructor), so Flink
knows how to handle it efficiently instead of falling back to a slow generic serializer.

### All the arithmetic in one plain Java file

The four steps live as plain functions in `SensorScoring.java`, which has **no Flink code in
it**. The Flink job's steps are one-line wrappers around them, and the same functions are used by
the correctness check and the speed measurements.

**Why:** it's the single definition of the work. The C version must copy it exactly; if there
were several Java copies, a mismatch could come from a difference between Java copies rather than
between Java and C.

### Written so C can match it to the last bit

Floating-point results depend on the **order** of operations: `(a + b) + c` can differ from
`a + (b + c)` in the last bit. So the Java code is written in a form C can copy exactly:

- sums are added up left to right, starting from 0;
- divide where the code divides (`x / 8`), multiply by the inverse where it does that (`* inv`);
- the peak uses a plain `>` comparison, not `Math.max` (which treats `-0.0` and NaN differently
  from C's equivalent);
- only `+ − × ÷` and square root, which give identical results in Java and C (`sin`, `exp`, `pow`
  are not guaranteed to);
- no "fused multiply-add" (`a × b + c` in one instruction, rounded once) — Java never does it, so
  C must be told not to either.

**Two details worth knowing:**

- Step 2 guarantees step 3 never divides by zero: only records with spread ≥ 0.01 reach the
  normalization. That keeps the arithmetic well-defined — and divide-by-zero and NaN are exactly
  where Java and C tend to differ.
- Step 3 normalizes once into a small scratch array and reuses it for all `k` comparisons. The
  scratch array belongs to the step's object (created once at start-up), not created per record
  and not shared globally.

### The reference shapes

`k` sine waves (1, 2 or 3 cycles across the 8 positions, each shifted a bit), normalized the same
way as the readings. They use `sin`, which breaks the "only `+ − × ÷` √" rule — that's fine
because they're computed **once, in Java**, and handed to C as data. C never recomputes them.

### The data generator

Every random value is computed from the record number with a standard hash (splitmix64), not
from a running random generator. **Why:** any version can produce record N without producing
records 0…N−1 first, and every version sees exactly the same data — which is what makes exact
comparison possible.

**Why the thresholds:**

- **0.01** for "flat": flat records have a spread around 0.00006; real ones typically 0.5 or
  more. No ambiguity.
- **0.9** for "good match": pattern records score ~0.98. Noise records score around 0 with a lot
  of scatter, and the *best* of many comparisons creeps up as `k` grows — 0.9 is high enough that
  noise almost never passes even at `k=64`. That keeps the share of survivors constant across
  `k`, so a change in time means a change in work, not in the amount of output.

Both filters have genuinely unpredictable outcomes, which is realistic: a filter that always goes
the same way would let the CPU predict it and flatter whichever version has it.

### Wiring it into Flink

```java
env.setParallelism(1);
env.getConfig().enableObjectReuse();
env.fromSource(new DataGeneratorSource<>(new GenerateReading(profiles, k), count, ...), ...)
   .map(new ComputeStats())
   .filter(new DropFlat())
   .map(new ScoreAgainstProfiles(profiles, k))
   .filter(new KeepHighScore())
   .sinkTo(checksum ? new ChecksumSink() : new DiscardingSink<>());
```

- **The data is made inside the source** (`DataGeneratorSource`), not by an extra `map` after a
  number source. **Why:** an extra `map` would also be merged by the fusion prototype (5 steps
  fused), while the C version replaces only the 4 real steps — the versions wouldn't be doing the
  same amount of work. Checked: the fusion build logs "Fusing chained run of 4 operators".
- **The source reuses one record object** — safe, because each record is fully consumed down the
  chain before the next is made.
- **Each step is a small named class**, not a lambda: Flink can read their types reliably, and the
  names show up in logs, which makes "which steps got fused" readable.
- **One thread, object reuse on** — same as the other benchmark jobs, and the condition the
  fusion prototype needs.
- **Arguments:** `SensorScoringJob [records] [k] [discard|checksum]`, default 50 million records,
  `k=1`. After the job ends it prints `NET_RUNTIME_MS=…` — the job's own runtime, excluding Java
  and Flink start-up — which the local speed tests read.

---

## 4. The native (C) version

### The big picture

```
 source ──► [ batching operator (Java) ] ──► sink
               │  copy record in      ▲
               │  when batch is full  │ survivors + "came from input #"
               ▼                      │
            [ doorway (JNI) ] ──► [ C function: all 4 steps, one loop ]
```

`SensorScoringNativeJob` is the same job — same source, same data, same sink — with the four steps
replaced by one **batching operator**. Everything outside those four steps is still Java. Three
new pieces; only one knows anything about sensors.

### Piece 1 — the doorway into C (`NativeBridge`)

**Background.** Java can't call C directly. The standard mechanism, JNI, works like this: Java
declares a method as `native` (no body), and a C function with a very specific name — built from
the Java package and class, like `Java_org_apache_flink_accel_NativeBridge_invoke` — gets linked
to it. So normally **every C function needs its own glue**, in Java and in C.

**What we did instead:** write the glue **once**, with four general operations:

| operation | what it does |
|---|---|
| `open` | load a C library file (e.g. `libsensor_kernel.so`) |
| `lookup` | find a function in it **by name** (e.g. `"sensor_score"`), get its address |
| `address` | tell C where a Java memory buffer is |
| `invoke` | call the function at that address on a batch |

The C functions themselves are plain C with **one agreed shape**:

```c
int32_t kernel(const void *settings, const void *records_in, int32_t n,
               void *results_out, int32_t *came_from);   // returns: number of survivors
```

No Java-specific code at all. **Why:** a new C function — such as one an agent generates — needs
no new Java or JNI code. Compile it, give its file and function name, done. Like a universal
adapter instead of a custom cable per device.

**Why it's installed with Flink, not inside the job.** Flink loads each submitted job into its own
separate space (a "class loader"), so jobs don't interfere. Java only lets a C library be
attached to **one** such space at a time. If the doorway were inside the job, the first run would
work and the second submission to the same cluster would fail with "library already loaded in
another classloader". Installed in Flink's `lib/` folder, it's loaded once and shared by every
job. (This follows from the Java rule; so far everything ran in one local process, where it can't
happen — the multi-run cluster benchmark will be the real test.)

### Piece 2 — the batching operator (`MicroBatchingNativeOperator`)

Takes the place of the four steps. It's generic: it works for any job; the sensor-specific part
is a 30-line subclass saying how to copy a reading in and out.

**For every record that comes in:**

1. copy it into a block of memory C can read, at the next free slot;
2. remember its timestamp (and whether it had one) in Java;
3. when the batch is full, make **one** call into C;
4. C writes the survivors' results and a list saying which input slot each survivor came from;
5. rebuild each survivor and send it on, with its own original timestamp.

**Why timestamps stay in Java:** C never needs to know about Flink. The "came from slot N" list
lets Java put each survivor's timestamp back. A record that had **no** timestamp must stay without
one — so a "had a timestamp" flag is kept too (an easy-to-miss detail: otherwise records would
come out with a timestamp they never had).

**Sending records on early.** Records waiting in a half-full batch must never be overtaken by
something that should come after them. So the batch is emptied early when:

| event | why |
|---|---|
| batch full | the normal case |
| a watermark arrives (both the classic and the newer Flink 2.x kind) | buffered records must go out *before* the watermark, or they'd arrive "late" |
| "record attributes" change (a Flink 2.x signal) | same reason: it must not overtake records |
| a checkpoint is about to be taken | then the batch is always empty at checkpoint time, so it never needs saving |
| end of input | otherwise the last partial batch would be lost |

Not done yet: a **time-based** flush. On a quiet stream, a half-full batch would wait forever;
fine for a benchmark that never stops, required for production. It's left as an explicit TODO in
the code.

**The memory blocks** (input, output, settings, "came from" list):

- **off-heap** (outside Java's normal memory), so C can read them directly and Java's garbage
  collector never moves them;
- in the CPU's **native byte order**. Java's default is the opposite of what the CPU (and C) uses
  — forget this and C reads plausible-looking garbage, not an error;
- **aligned to 64 bytes**, so records line up with the CPU's memory chunks;
- their addresses are looked up **once** at start-up, not per batch (each lookup costs ~17 ns).

### Piece 3 — the C function (`sensor_score`)

All four steps written as **one loop**:

```
for each record in the batch:
    work out average, spread, peak        (step 1)
    if spread < 0.01: skip                (step 2)
    normalize; compare with k shapes      (step 3)
    if score < 0.9: skip                  (step 4)
    write average, spread, peak, score; note which input it came from
return how many survived
```

**Built to give exactly Java's answers, while being properly optimized:**

| compiler setting | why |
|---|---|
| `-O3 -march=native` | full optimization for this machine's CPU — a fair, serious C version |
| `-ffp-contract=off` | never fuse `a × b + c` into one instruction (Java doesn't) |
| no `-ffast-math` | never reorder arithmetic (that would change the last bits) |
| `-fno-math-errno` | skip the square root's error bookkeeping; changes no result |

Looking at the compiled code: it's plain one-number-at-a-time instructions, no SIMD.

### The memory layout — and a trick

The Java side and the C side agree on fixed layouts (in `SensorLayout.java` and as C structs):

| block | per record | contents |
|---|---|---|
| input | 80 bytes | device id, timestamp, 8 readings |
| output | 32 bytes, survivors only | average, spread, peak, score |
| settings | once | `k`, the two thresholds, the `k` reference shapes |

**The trick:** the device id, timestamp and raw readings are **never copied back out of C**. They
ride along in the input block; C only says "survivor 3 came from input slot 17", and Java reads
those fields straight from slot 17. C writes back only the four numbers it computed.

**Why not just send the 8 readings to C?** The original plan did exactly that — but the output
record needs the device id too, and Java can't hold on to the original record object (Flink reuses
it for the next record). So every output field must either go through C's memory or be kept aside
in Java. Riding along in the input block is the simplest version.

---

## 5. Checking correctness

A faster number means nothing if the answers are wrong — a version that silently drops records
would also be fast. So correctness is checked separately, and exactly.

### The fingerprint

For every surviving record, compute a hash over **every field** — device id, timestamp, all 8
readings, average, spread, peak, score — using the exact bits of each number. Add up the hashes,
and count the survivors. Two runs print the same `CHECKSUM survivors=… hash=…` line only if they
produced exactly the same records, to the last bit.

- **Every field, including the 8 readings:** the C version rebuilds the readings from its input
  block; a fingerprint over only the computed fields wouldn't notice if it got them wrong.
- **Exact bits, not "close enough":** the whole point is to prove the C version does exactly
  Java's arithmetic.
- **Adding up, not chaining:** adding is order-independent, so the fingerprint stays valid even
  if a future version emits records in a different order (e.g. with more threads).

### The reference

`SensorScoringReference` runs the same four functions in a plain loop with **no Flink at all** and
prints the fingerprint. The rule: **any version, on any Flink build, must print the identical
line.**

### What was verified (2,000,000 records)

| `k` | survivors | fingerprint | matched by |
|---|---|---|---|
| 1 | 602,267 | `3d0044a86f2e5afb` | native (batch 1024, 7, 1) |
| 16 | 617,961 | `83dda26b980dfdf6` | stock Flink, fusion, native (batch 1024, 7, 1) |
| 64 | 621,176 | `a3f1c1b1dad62df3` | native (batch 1024, 7, 1) |

Batch 7 means the last batch is only partly full (2,000,000 isn't divisible by 7) — that tests the
end-of-input flush. Batch 1 means a call into C for every record.

---

## 6. Measuring speed

Two stages, from simple to realistic:

| stage | what runs | tool |
|---|---|---|
| **1 — Java vs C, on their own** | only the four steps, no Flink | JMH (Java's standard micro-benchmark tool) |
| **2 — plugged into Flink** | the whole job: source, Flink, the four steps, sink | Flink's local mode, timing whole runs |

### Stage 1 — on their own

- **`SensorWork`** measures the Java work per record at each `k`.
- **`SensorBatch`** compares Java with the full C path — copy in, call, copy out — at every `k`
  and every batch size (1 to 4096). It's shaped like the real operators: Java handles one record at
  a time (make it, process it, next), the C path makes and copies records one by one and calls C
  each time a batch fills.

Choices and why:

- **A fresh record every time**, never repeated: the data is fresh in the CPU's cache, as in the
  real job, and the filters' outcomes are genuinely unpredictable — replaying the same records
  would let the CPU learn the pattern and flatter the results.
- **Data generation is measured on its own** (`generate`) so it can be accounted for.
- **Java 17**, the same as the Docker setup's image.
- **Pinned to one fast core.** This laptop (Intel i5-12500H) has fast and slow cores; without
  pinning, a run can hop between them.

### Stage 2 — plugged into Flink

`minicluster-sweep.sh` runs stock Flink, the fusion prototype, and the native version at every
batch size, for every `k`: 50 million records per run, 3 runs each, in Flink's built-in local
mode.

- **Runs are interleaved** — run 1 of every combination, then run 2, … — so a slow patch on the
  machine spreads across all combinations instead of hurting whichever ran last.
- **Each run reports the job's own runtime** (`NET_RUNTIME_MS`), excluding Java and Flink
  start-up.
- Pinned to the fast cores.

`report.py` turns both stages' results into tables.

---

## 7. Decisions at a glance

| decision | alternative | why |
|---|---|---|
| One C function for the whole chain | one C function per step | no steps = no hops; one function is what an agent would generate and optimize |
| Batching operator in the job's own code, wired with `.transform()` | build it into Flink | no Flink rebuild (seconds instead of ~20 minutes per change); automatic detection is a separate problem |
| One generic doorway (`NativeBridge`) | JNI glue per C function | new C functions need no Java code; also avoids the "library already loaded" problem |
| Data made inside the source | number source + extra `map` | the fusion prototype would swallow the extra `map`; versions would differ in work |
| A difficulty dial `k` | one fixed job | "real work" becomes a curve, not a guess |
| Update records in place | new object per step | otherwise Java pays for creating objects and C doesn't |
| Arithmetic in one Flink-free Java file | logic inside the Flink steps | a single definition that the job, the reference, the benchmarks and C all follow |
| C keeps Java's exact arithmetic order | let the compiler reorder | answers can be checked bit for bit |
| Timestamps stay in Java, "came from" list from C | send timestamps to C | C never needs Flink concepts |
| Fields ride along in the input block | copy everything back from C | C writes back only what it computed |
| No SIMD | vectorized C | out of scope: the idea is one function, not hand-vectorizing |
| Exact fingerprint over every field | compare with a tolerance | proves the C version does exactly Java's arithmetic |

---

## 8. Lessons and pitfalls

- **Memory alignment cut the buffer short.** Java's `alignedSlice()` rounds the *end* of a buffer
  down as well as the start up, so "size + 64 bytes of slack" could come out up to 63 bytes short
  → crash on large batches. Fixed by rounding the size up first.
- **The fingerprint first missed the 8 readings.** Caught before the C version could rely on it;
  it now covers every field.
- **How a Java loop is arranged changes its speed a lot.** Same work, `k=1`: making a whole batch
  of records first and then processing them = 60 ns per record; one record at a time = 83 ns. So
  "C vs Java" numbers depend on comparing the same arrangement — part of C's lead on its own is
  arrangement, not language.
- **A first, small round of in-Flink runs was misleading.** It showed C 15% slower at `k=1`; a
  larger, interleaved sweep showed a tie. Lesson: interleave runs and repeat before believing a
  difference of a few percent.
- **Running a job locally with Flink's console logging config** creates stray files literally
  named `${sys:log.file}` in the current directory. Leave that flag out, or also set
  `-Dlog.file=…`.
- **Adding the native job broke the old benchmark's build** (it imports the doorway class, which
  isn't in the job). Fixed by compiling against the doorway's source without packaging it.

---

## 9. Where everything is

| path | what |
|---|---|
| `benchmark/job/org/apache/flink/demo/SensorReading.java` | the record |
| `benchmark/job/org/apache/flink/demo/SensorScoring.java` | the four steps, reference shapes, data generator |
| `benchmark/job/org/apache/flink/demo/SensorScoringJob.java` | the Java Flink job |
| `benchmark/job/org/apache/flink/demo/SensorScoringNativeJob.java` | the native Flink job + its 30-line sensor subclass |
| `benchmark/job/org/apache/flink/demo/MicroBatchingNativeOperator.java` | the generic batching operator |
| `benchmark/job/org/apache/flink/demo/SensorLayout.java` | the memory layouts, copy in / copy out |
| `benchmark/job/org/apache/flink/demo/SensorChecksum.java` | the fingerprint |
| `benchmark/job/org/apache/flink/demo/SensorScoringReference.java` | the no-Flink reference |
| `benchmark/native-kernel/bridge/` | the doorway: `NativeBridge.java` + `native_bridge.c` |
| `benchmark/native-kernel/kernels/sensor_kernel.c` | the C function |
| `benchmark/native-kernel/build.sh` | builds the doorway and the C function |
| `benchmark/sensor-bench/` | speed measurements: `SensorWork`, `SensorBatch`, `run.sh`, `minicluster-sweep.sh`, `report.py`, and the README on how to run them |

---

## 10. Not done yet

- **Java arranged like the C path** (batch first, then process) and **hand-tuned Java** (the same
  tricks the C compiler used) — the fair tests of whether C itself is what helps.
- **Real cluster runs**: the Docker benchmark can't run the native version yet (the doorway and
  C library need installing in the image's Flink, built for the image's system libraries).
- **The time-based flush** in the batching operator.
- **Agent-generated C**: have an AI agent write the C function from the Java job, checked with the
  same fingerprint.
