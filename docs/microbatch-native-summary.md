# Moving a Flink map/filter chain into C — summary

A plain-language summary of the experiment: what we built, which versions we compared,
and what we measured. Written to be turned into a presentation; each section is roughly one
slide.

How everything is built and why, with a worked example of the job:
`microbatch-native-implementation.md`.
How to run the benchmarks and reproduce every number here: `benchmark/sensor-bench/README.md`.

---

## 1. The question

A Flink job is a chain of small steps: `map → filter → map → filter`. Each step is a separate
object, and every record takes a small hop from one step to the next.

**Idea:** replace the whole chain with **one C function** that does all the steps in one go.
No steps means no hops. Later, an AI agent would write that C function automatically from the
Java code.

**What we want to know:** once each record does real work, is C actually faster than Java — or
does Java keep up?

---

## 2. The test job

A pretend sensor-monitoring pipeline: many sensors send short bursts of readings, and we want to
pick out the bursts that look like a known pattern — while ignoring sensors that are dead or
flat-lining.

```
source ──► map: stats ──► filter: not flat ──► map: score ──► filter: good match ──► sink
```

```java
env.fromSource(new DataGeneratorSource<>(new GenerateReading(...), count, ...), ...)
   .map(new ComputeStats())            // step 1
   .filter(new DropFlat())             // step 2
   .map(new ScoreAgainstProfiles(...)) // step 3
   .filter(new KeepHighScore())        // step 4
   .sinkTo(new DiscardingSink<>());
```

Runs on one thread (parallelism 1), with Flink's "object reuse" switched on, like the other
benchmark jobs.

### The record

```java
class SensorReading {
    int      deviceId;     // which sensor
    long     ts;           // when
    double[] samples;      // its last 8 readings (8 numbers = 64 bytes)
    double   mean, stddev, max;   // filled in by step 1
    double   score;               // filled in by step 3
}
```

Each step fills in its results on the same record and passes it on, instead of creating a new
one, so no version wastes time creating objects.

### The source — where the records come from

A built-in Flink source (`DataGeneratorSource`) counts 0, 1, 2, … up to the number of records
asked for (e.g. 50 million), and turns each number into a reading:

- `deviceId` = the number modulo 1024 (so 1024 different sensors), `ts` = the number itself;
- the 8 readings are made up with a fixed pseudo-random formula based on the number, so **every
  run and every version sees exactly the same data**.

Each record is one of three kinds:

| kind | share | what the 8 readings look like | what happens to it |
|---|---|---|---|
| flat-lining | ~15% | a constant level with tiny jitter | dropped at step 2 |
| known pattern | ~30% | one of the known shapes, scaled up, plus some noise | survives to the end |
| random noise | ~55% | random values | dropped at step 4 (almost always) |

The data is generated inside the source on purpose: if it were made by an extra `map` step, that
step would also get merged into the chain by the fusion prototype, and the versions would no
longer be doing the same amount of work.

### The four steps

| step | Flink type | what it does, concretely |
|---|---|---|
| 1. **stats** | map | **average** of the 8 readings; **spread** (standard deviation: how far the readings typically sit from the average); **peak** (largest reading) |
| 2. **not flat** | filter | keep the record only if the spread is at least **0.01**; a sensor whose readings barely move is flat-lining |
| 3. **score** | map | **normalize** the readings (subtract the average, divide by the spread), so only the *shape* is left. Compare that shape with each of `k` **known shapes** (sine waves) and measure how alike they are — a number from −1 (opposite) to 1 (identical). The best match is the record's **score** |
| 4. **good match** | filter | keep the record only if its score is at least **0.9** |

The **sink** throws the results away when we're measuring speed, or computes a fingerprint of
them when we're checking correctness (section 5).

### A dial for the amount of work

`k`, the number of known shapes compared in step 3. `k=1` is light work per record, `k=64` is
heavy work (64 comparisons per record). This shows whether the answer changes as the work gets
heavier. The record and the share of survivors (~30%) stay the same at every `k`.

Not to be confused with the **batch size** — how many records the native version collects
before each call into C (we tried 1 to 4096). `k` changes how much work each record
costs; the batch size only changes how often Java crosses into C.

---

## 3. How we compared: two stages

We answer the question in two steps, from simple to realistic:

| stage | question | what runs |
|---|---|---|
| **Stage 1 — Java vs C, on their own** | Is C faster than Java at the actual work? | just the four steps, no Flink around them |
| **Stage 2 — plugged into Flink** | Does that difference survive inside a real Flink job? | the whole job: source, Flink runtime, the four steps, output |

Stage 1 isolates the work; stage 2 adds everything a real job has around it.

---

## 4. Stage 1 — Java vs C, on their own

### The versions

The first sweep compared plain Java with C, and C won by ~1.4×. But the two were not arranged the
same way, so we added three Java versions to separate "C is faster" from "the code is arranged
better":

| version | how the four steps are written | how records flow |
|---|---|---|
| **Java** | four separate functions, as the Flink steps call them | one record at a time: make it, process it, next |
| **tuned Java** | one hand-written function doing all four steps (`scoreFused`) | one record at a time |
| **Java, batch-first** | four separate functions | make a whole batch of records first, then process them in one loop |
| **tuned Java, batch-first** | the one hand-written function | batch-first |
| **C** | one C function doing all four steps (`sensor_score`) | batch-first (it has to be: copy a batch in, one call) |

**Tuned Java** is the Java a person or an agent could write instead of C. It uses the same tricks
the C compiler uses: the 8 readings and the intermediate values live in local variables (the
processor's registers) instead of being re-read from memory, and the differences from the average
are computed once and reused. It gives bit-for-bit the same answers as the other versions.

The C function (`sensor_score`) is written by hand for now; it is the function an AI agent would
generate later. It does the arithmetic in exactly the same order as Java, so it gives exactly the
same answers (section 6).

### How Java calls C

Java can't call C directly, and it can't hand C its own objects. So every call into C involves:

1. **copying** each record into a block of memory C can read,
2. **one call** into C for a whole batch of records,
3. **copying back** the survivors' results.

That copying and the call itself are the price of using C at all, so the C numbers below
**include them** — that's the honest comparison.

The call goes through a small piece we built, **the doorway** (`NativeBridge`). Java can't call
a C function directly; it needs "glue" code (JNI) whose names are tied to specific Java classes.
Normally every C function would need its own glue. Instead we wrote the glue **once**, with four
general operations:

| operation | what it does |
|---|---|
| `open` | load a C library file (e.g. `libsensor_kernel.so`) |
| `lookup` | find a function in it **by name** (e.g. `"sensor_score"`) |
| `address` | tell C where a Java memory buffer is, so C can read and write it |
| `invoke` | call that function on a batch |

The C functions themselves are plain C with one agreed shape (settings, records in, count,
survivors out) and no Java-specific code. So a new C function, such as one an agent generates,
needs no new Java code: compile it, and give its file and function name. Like a universal adapter
instead of a custom cable per device.

A small trick: the device id, timestamp and raw readings are never copied back out of C. C only
says "survivor 3 came from input 17", and Java reads those fields from slot 17 itself. C only
writes back the four numbers it actually computed.

### Results

Measured with JMH (a Java micro-benchmark tool). Most numbers are accurate to within ±1 ns, but
about a third have error bars of ±2–7 ns (one ±14 ns), so differences of a few ns are noise. Every
version pays ~37 ns per record for making up the test data; it's included in all numbers.

**Time per record (ns)** — batch-first versions and C at batch 1024:

| shapes (`k`) | Java | tuned Java | Java, batch-first | tuned Java, batch-first | C |
|---|---|---|---|---|---|
| 1 (light) | 82 | 73 | 62 | 61 | **60** |
| 4 | 94 | 88 | 86 | 82 | **65** |
| 16 | 127 | 116 | 119 | 115 | **84** |
| 64 (heavy) | 204 | 186 | 200 | 183 | **140** |

**C speed-up** (above 1 = C faster):

| shapes (`k`) | over plain Java | over the best Java (tuned, batch-first) |
|---|---|---|
| 1 | 1.37× | **1.01× — a tie** |
| 4 | 1.45× | **1.26×** |
| 16 | 1.50× | **1.37×** |
| 64 | 1.46× | **1.31×** |

**By batch size (ns per record)** — "batch" = how many records are made before processing them
(Java) or handed to C in one call (C). Batch 1 means one call into C per record.

| shapes (`k`) | version | batch 1 | 16 | 64 | 256 | 1024 | 4096 |
|---|---|---|---|---|---|---|---|
| 1 | Java, batch-first | 80 | 60 | 60 | 61 | 62 | 62 |
| 1 | tuned Java, batch-first | 74 | 61 | 62 | 65 | 61 | 63 |
| 1 | C | 62 | 61 | 63 | 60 | 60 | 66 |
| 4 | Java, batch-first | 91 | 82 | 83 | 84 | 86 | 83 |
| 4 | tuned Java, batch-first | 91 | 80 | 84 | 84 | 82 | 80 |
| 4 | C | 66 | 66 | 67 | 65 | 65 | 71 |
| 16 | Java, batch-first | 124 | 115 | 120 | 116 | 119 | 119 |
| 16 | tuned Java, batch-first | 117 | 110 | 115 | 113 | 115 | 111 |
| 16 | C | 83 | 84 | 84 | 84 | 84 | 89 |
| 64 | Java, batch-first | 214 | 205 | 208 | 205 | 200 | 199 |
| 64 | tuned Java, batch-first | 188 | 190 | 184 | 182 | 183 | 185 |
| 64 | C | 139 | 142 | 140 | 139 | 140 | 146 |

### Stage 1 takeaway

- **Light work: C's lead was entirely how the loop is arranged.** Java that makes a batch first
  and then processes it ties with C (61 vs 60 ns at `k=1`). Nothing about C itself helps here.
- **Heavier work: C really is faster at the work — about 1.3× over the best Java we wrote**,
  even with the copying in and out. The lead grows from `k=1` to `k=16` and then holds.
- **Hand-tuning Java helps less than hoped:** 6–11% one record at a time, 2–9% batch-first.
- **Where C's lead comes from: the comparison loop in step 3.** Each extra comparison against a
  known shape costs C ~1.5 ns (~7 processor cycles) and batch-first Java 2.0–2.2 ns (9–10 cycles;
  tuned to plain). Derived from the `k=4` → `k=64` slope, per record that reaches step 3 (~85%).
  - It is **not** vector instructions: the C machine code is plain one-number-at-a-time
    (scalar) arithmetic, the same operations Java does.
  - It is **not** branches: C picks the best match without a branch, but a branch-free Java
    version (`Math.max`) measured the same as the original.
  - What's left is in the machine code the Java JIT compiler generates for that loop (likely
    array bounds checks and instruction scheduling). Confirming it needs a disassembler plug-in
    for the JVM (`hsdis`) — an open question.
- **Batch size barely matters.** C is as fast with one call per record as with 1024 (one call
  per record adds only ~2–3 ns per record here, small next to 60–200 ns of work). Batch-first Java needs batches of at least 16.
  Very large batches (4096) make C a little slower, probably because the batch no longer fits in
  the processor's fastest memory.

---

## 5. Stage 2 — plugged into Flink

### The four versions

All four run the whole job: the same source, the same data, the same output.

| version | how the four steps run | hops between steps |
|---|---|---|
| **A. Stock Flink** | four separate Flink operators, one per step | 3 per record |
| **B. Flink + fusion prototype** | same job, unchanged; our modified Flink build spots the four steps and merges them into one operator that calls the four Java functions in a loop | none |
| **C. Flink + native operator** | the four steps replaced by one operator that collects records and hands them to the C function from stage 1, one call per batch | none |
| **D. Flink + tuned Java** | the four steps replaced by one filter that calls the tuned Java function from stage 1; stock Flink | none |

A and B run the **exact same job code**; only the Flink build differs. C and D are the same job
with the four steps swapped out. Everything else is the same Java.

### How the C function fits into Flink: the batching operator

The **batching operator** (`MicroBatchingNativeOperator`) takes the place of the four steps. For
every record that comes in:

1. copy it into a block of memory that C can read;
2. once the batch is full, make **one** call into C
   through the doorway;
3. C reports back the survivors and which input record each one came from;
4. send each survivor on, with its original timestamp.

It also sends buffered records on early whenever something must not overtake them: a watermark,
a checkpoint, or the end of the input. Only a 30-line part of it knows about sensors (how to copy
a reading in and out); the rest works for any job.

```
 source ──► [ batching operator (Java) ] ──► sink
               │  copy record in      ▲
               │  when batch is full  │ survivors + "came from input #"
               ▼                      │
            [ doorway (JNI) ] ──► [ C function: all 4 steps, one loop ]
```

The doorway is installed with Flink (in its `lib/` folder), not packed inside the job. Flink loads
each job submission into its own separate space, and Java allows a C library to be attached to
only one such space at a time — so a second submission of the same job would fail with "library
already loaded". Installed with Flink, it's loaded once and shared by every job. (This follows
from the Java rule; it will actually be tested in the multi-run cluster benchmark, which hasn't
run yet.)

Version D needs nothing like this: a Flink filter may also fill in the record it is given, so one
filter can do all four steps in Java.

### Results

50 million records per run, 3 runs per number (median), runs interleaved so machine drift
spreads evenly. Runs typically agree within ~4% (worst cell 9.3%). Every version pays ~40 ns per record for making
up the test data.

**Time per record (ns):**

| shapes (`k`) | A. stock | B. fusion | D. tuned Java | C, batch 1 | C, 16 | C, 64 | C, 256 | C, 1024 | C, 4096 |
|---|---|---|---|---|---|---|---|---|---|
| 1 (light) | 124 | 128 | **120** | 130 | 125 | 124 | 133 | 132 | 127 |
| 4 | 141 | 136 | 133 | 134 | **130** | 136 | 134 | 137 | 134 |
| 16 | 169 | 161 | 159 | **148** | 150 | 155 | 160 | 152 | 158 |
| 64 (heavy) | 263 | 247 | 232 | **203** | **203** | 213 | 211 | 211 | 214 |

("C" = C. Flink + native operator, at each batch size.)

**Speed-up over the fusion prototype** (above 1 = faster than fusion):

| shapes (`k`) | D. tuned Java | C, batch 1 | C, 16 | C, 64 | C, 256 | C, 1024 | C, 4096 |
|---|---|---|---|---|---|---|---|
| 1 | **1.07×** | 0.98× | 1.02× | 1.03× | 0.96× | 0.97× | 1.01× |
| 4 | 1.03× | 1.02× | **1.05×** | 1.00× | 1.02× | 0.99× | 1.01× |
| 16 | 1.01× | **1.09×** | 1.07× | 1.04× | 1.01× | 1.06× | 1.02× |
| 64 | 1.07× | **1.22×** | **1.22×** | 1.16× | 1.17× | 1.17× | 1.16× |

Differences under ~4% are within run-to-run noise.

### Stage 2 takeaway: how stage 1 translates

| shapes (`k`) | stage 1: C vs best Java | stage 2: C (batch 1) vs tuned Java | fastest in Flink |
|---|---|---|---|
| 1 (light) | 1.01× | 0.92× | **tuned Java** |
| 4 | 1.26× | 0.99× | all within noise |
| 16 | 1.37× | 1.07× | **C** |
| 64 (heavy) | 1.31× | 1.14× | **C** |

- **Light work: tuned Java is the fastest version of all** — 120 ns vs 124–133 ns for everything
  else, 1.07× over fusion. The native operator only ties with fusion: copying records in and out
  eats whatever C saves.
- **Heavy work: C wins — 1.16–1.22× over fusion, 1.09–1.14× over tuned Java.** The crossover is
  somewhere between `k=4` and `k=16`.
- **Stage 1 predicts tuned Java closely.** Tuning saved 9 ns per record at `k=1` and 18 ns at
  `k=64` on its own, and 8 and 16 ns inside Flink.
- **C's stage 1 lead shrinks inside Flink** (1.3× → ~1.1× over the best Java): Flink's own work
  and the test-data generation are paid by every version and dilute the difference.
- **Batch size doesn't matter inside Flink either** — batch 1 is as good as any.

---

## 6. How we know the results are correct

A faster number means nothing if the answers are wrong.

- A small program runs the four steps in a plain Java loop, with no Flink at all, and prints a
  **fingerprint** of every surviving record: how many there are, plus a hash of every field,
  exact to the last bit.
- Every version must print the **identical** fingerprint.

Checked on 2,000,000 records in the final run (2026-10-21, `benchmark/run-all.sh`):

| shapes (`k`) | survivors | fingerprint | reference, tuned | A. stock | B. fusion | C. native | D. tuned Java |
|---|---|---|---|---|---|---|---|
| 1 | 602,267 | `3d0044a86f2e5afb` | ✓ | ✓ | ✓ | ✓ | ✓ |
| 16 | 617,961 | `83dda26b980dfdf6` | ✓ | ✓ | ✓ | ✓ | ✓ |
| 64 | 621,176 | `a3f1c1b1dad62df3` | ✓ | ✓ | ✓ | ✓ | ✓ |

The native version was checked with batches of 1024 and 1 (a call into C for every record), and
earlier also with 7 (so the last batch is only half full): identical every time.

The C function and the tuned Java function give **bit-for-bit the same answers** as the plain
Java because they do the arithmetic in exactly the same order.

---

## 7. What it means

- **Correctness was never the problem:** every version gives identical answers, bit for bit.
- **"Is it C, or how the code is arranged?" — both, depending on the work.**
  - Light work: only arrangement. Java written the right way matches C, and inside Flink the
    tuned Java version is the fastest of all.
  - Heavier work: C is genuinely ~1.3× faster at the work itself, which becomes ~1.1–1.2× for
    the whole Flink job.
- **Native pays off only when each record does a lot of work.** Here that means 16 or more
  comparisons per record (~80+ ns of Java work). Below that, a single well-written Java function
  is as good or better — with none of native's costs (a C toolchain, per-platform builds, a crash
  in C taking down the whole TaskManager).
- **Hand-tuned Java captures part of the gain,** not all: at `k=64` it gets 1.07× over fusion,
  where C gets 1.16–1.22×.
- **Batching isn't what makes it work here.** The cost of calling C is small for this job, so even
  one call per record performs as well as large batches.

---

## 8. Caveats

- All timings are from **one final run on one laptop** (2026-10-21, `benchmark/run-all.sh`; Intel
  i5-12500H, Java 17), pinned to its fast cores, on AC power at full clock speed. The 4.5 GHz
  ceiling was logged before every step; AC power and the performance profile were set but the
  script's check for them printed `?` (fixed since). An earlier run with the clock accidentally capped at
  2.5 GHz was ~1.8× slower everywhere but gave the same ratios, so the comparisons are robust.
- Stage 2 uses Flink's built-in local mode, not a real cluster.
- 3 runs per number in stage 2; runs typically agree within ~4%. The worst cell spread 9.3%
  (stock at `k=1`); medians are unaffected.
- About 40 ns of every record's time is spent making up the test data, which shrinks the visible
  differences between versions.
- The tuned Java and the C function are both hand-written, by us. A better Java version may exist
  — the comparison loop is where to look (section 4).
- The arithmetic is deliberately kept in Java's order in every version, so the answers can be
  compared exactly. C compiled with `-O3 -march=native`, but no fused multiply-add and no
  "fast-math" reordering.

---

## 9. Next steps

1. **Find out what the Java compiler does differently in the comparison loop** (install `hsdis`,
   read the generated machine code). This tells us whether Java can close the remaining 1.3×.
2. **Real cluster runs** with the Docker benchmark setup.
3. **Agent-generated code**: have an AI agent write the C function *and* an optimized Java
   function from the Java job, check both with the same fingerprint, and compare them — the
   results here say the Java route is enough for light work.
4. **The time-based flush** in the batching operator, so a quiet stream doesn't hold records.
