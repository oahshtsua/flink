# Moving a Flink map/filter chain into C — summary

A plain-language summary of the experiment so far: what we built, which versions we compared,
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

### The two versions

| version | what it is |
|---|---|
| **Java** | the four steps written as one plain Java loop: for each record, stats → filter → score → filter |
| **C** | the same four steps written as one plain C function, one loop over a batch of records |

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

Measured with JMH (a Java micro-benchmark tool), each number accurate to about ±1 ns. Every
version pays ~37 ns per record for making up the test data; it's included in all numbers.

**Time per record (ns)** — Java vs C at every work level and batch size:

| shapes (`k`) | Java | C, batch 1 | C, batch 16 | C, batch 64 | C, batch 256 | C, batch 1024 | C, batch 4096 |
|---|---|---|---|---|---|---|---|
| 1 (light) | 83 | 62 | 61 | 60 | 60 | 59 | 65 |
| 4 | 94 | 66 | 66 | 65 | 65 | 65 | 71 |
| 16 | 124 | 82 | 84 | 84 | 83 | 83 | 90 |
| 64 (heavy) | 203 | 140 | 142 | 141 | 139 | 140 | 147 |

**C speed-up over Java** (above 1 = C faster):

| shapes (`k`) | batch 1 | batch 16 | batch 64 | batch 256 | batch 1024 | batch 4096 |
|---|---|---|---|---|---|---|
| 1 | 1.35× | 1.37× | 1.39× | 1.40× | 1.40× | 1.27× |
| 4 | 1.42× | 1.42× | 1.44× | 1.45× | 1.45× | 1.32× |
| 16 | 1.51× | 1.47× | 1.47× | 1.49× | 1.49× | 1.38× |
| 64 | 1.45× | 1.43× | 1.44× | 1.46× | 1.45× | 1.38× |

"Batch" = how many records are handed to C in one call. Batch 1 means one call into C per record.

### Stage 1 takeaway

- **C is about 1.4× faster at every work level**, including the copying in and out.
- **Batch size barely matters.** Even one call into C per record (batch 1) is nearly as fast as
  batch 1024: here, a call into C costs only ~3 ns, small next to 60–200 ns of work. Very large
  batches (4096) get a little slower, probably because the batch no longer fits in the
  processor's fastest memory.
- **Careful: part of this is not "C vs Java", it's "how the loop is arranged".** The Java version
  handles one record at a time: make it, process it, next. The C path makes a batch of records
  first, then processes them all in one tight loop. An earlier Java version that also made the
  batch first took **60 ns instead of 83 ns** at `k=1` — the same as C. So at light work, C's
  lead comes mostly from batching, which Java could do too.
- At heavy work there is a second, real code effect: the C compiler kept the 8 normalized
  readings in the processor's registers while comparing them to all 64 shapes; Java fetches them
  from memory again for every shape. Java written the same way could likely do the same.

---

## 5. Stage 2 — plugged into Flink

### The three versions

All three run the whole job: the same source, the same data, the same output.

| version | how the four steps run | hops between steps |
|---|---|---|
| **A. Stock Flink** | four separate Flink operators, one per step | 3 per record |
| **B. Flink + fusion prototype** | same job, unchanged; our modified Flink build spots the four steps and merges them into one operator that calls the four Java functions in a loop | none |
| **C. Flink + native operator** | the four steps replaced by one operator that collects records and hands them to the C function from stage 1, one call per batch | none |

A and B run the **exact same job code**; only the Flink build differs. C is the same job with
the four steps swapped for the native operator. Everything else is the same Java.

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

### Results

50 million records per run, 3 runs per number (median), runs interleaved so machine drift
spreads evenly. Runs typically agree within ~3%. Every version pays ~40 ns per record for making
up the test data.

**Time per record (ns):**

| shapes (`k`) | A. stock Flink | B. Flink + fusion | C, batch 1 | C, batch 16 | C, batch 64 | C, batch 256 | C, batch 1024 | C, batch 4096 |
|---|---|---|---|---|---|---|---|---|
| 1 (light) | 128 | 127 | 129 | 127 | 127 | 128 | 129 | 130 |
| 4 | 139 | 136 | 132 | 131 | 132 | 135 | 138 | 132 |
| 16 | 169 | 161 | 144 | 149 | 151 | 157 | 150 | 157 |
| 64 (heavy) | 256 | 244 | 201 | 204 | 204 | 210 | 209 | 207 |

("C" = C. Flink + native operator, at each batch size.)

**Native speed-up over the fusion prototype** (above 1 = native faster):

| shapes (`k`) | batch 1 | batch 16 | batch 64 | batch 256 | batch 1024 | batch 4096 |
|---|---|---|---|---|---|---|
| 1 | 0.99× | 1.00× | 1.00× | 0.99× | 0.98× | 0.98× |
| 4 | 1.03× | 1.05× | 1.04× | 1.01× | 0.99× | 1.03× |
| 16 | 1.12× | 1.08× | 1.07× | 1.03× | 1.08× | 1.03× |
| 64 | 1.21× | 1.20× | 1.20× | 1.17× | 1.17× | 1.18× |

Differences under ~3% (light-work rows) are within run-to-run noise.


### Stage 2 takeaway: how stage 1 translates

| shapes (`k`) | stage 1: C vs Java | stage 2: native vs fusion (batch 1024) |
|---|---|---|
| 1 (light) | 1.40× | 0.98× — no difference |
| 4 | 1.45× | 0.99× — no difference |
| 16 | 1.49× | 1.08× |
| 64 (heavy) | 1.45× | 1.17× |

- **Light work: the stage 1 lead disappears inside Flink.** Consistent with stage 1's caveat:
  at light work C's lead came mostly from loop arrangement, and inside Flink the Java version
  (fusion) is already arranged well enough.
- **Heavy work: a real but smaller win, ~1.2×.** The saving per record is similar in both stages
  (~40–60 ns at `k=64`), but inside Flink it's a smaller share of a bigger total — Flink's own
  work and the test-data generation are paid by every version.
- **Batch size doesn't matter inside Flink either** — batch 1 is as good as any.

---

## 6. How we know the results are correct

A faster number means nothing if the answers are wrong.

- A small program runs the four steps in a plain Java loop, with no Flink at all, and prints a
  **fingerprint** of every surviving record: how many there are, plus a hash of every field,
  exact to the last bit.
- Every version must print the **identical** fingerprint.

| shapes (`k`) | survivors (of 2,000,000) | fingerprint | A. stock | B. fusion | C. native |
|---|---|---|---|---|---|
| 1 | 602,267 | `3d0044a86f2e5afb` | — | — | ✓ |
| 16 | 617,961 | `83dda26b980dfdf6` | ✓ | ✓ | ✓ |
| 64 | 621,176 | `a3f1c1b1dad62df3` | — | — | ✓ |

The native version was also checked with batches of 1024, 7 (so the last batch is only half
full) and 1 (a call into C for every record): identical every time.

The C function gives **bit-for-bit the same answers as Java** because it does the arithmetic in
exactly the same order.

---

## 7. What it means

- **Correctness was never the problem:** every version gives identical answers, bit for bit.
- **Light work: moving the chain into C doesn't help** inside Flink.
- **Heavier work: it helps, up to ~1.2× for the whole job** (vs the fusion prototype; ~1.25× vs
  stock Flink) — and the benefit grows with the amount of work per record.
- **Batching isn't what makes it work here.** The cost of calling C is small for this job, so even
  one call per record performs as well as large batches.
- **Is it C, or how the code is arranged?** Both effects we found — processing a batch in one
  tight loop, and keeping values in registers — are things Java code could also do. The fair next
  test is Java written the same way.

---

## 8. Caveats

- All timings are from **one laptop** (Intel i5-12500H, Java 17), pinned to its fast cores.
- Stage 2 uses Flink's built-in local mode, not a real cluster.
- 3 runs per number in stage 2; runs typically agree within ~3%, worst case 7.5%.
- About 40 ns of every record's time is spent making up the test data, which shrinks the visible
  differences between versions.
- Stage 1's Java number depends on how its loop is arranged (60 vs 83 ns at `k=1`); the table
  uses the one-record-at-a-time arrangement, which is how the Flink job works.
- The C function is hand-written, and the arithmetic is deliberately kept in Java's order so the
  answers can be compared exactly.

---

## 9. Next steps

1. **Java arranged like the C path**: batch the records first, then process them in one loop —
   tells us how much of stage 1's lead is batching rather than C.
2. **Hand-tuned Java**: write the four steps as one Java function, using the same tricks the C
   compiler used (values kept in local variables). If it matches C, the language isn't what made
   the difference.
3. **Real cluster runs** with the Docker benchmark setup.
4. **Agent-generated C**: have an AI agent write the C function from the Java job, and check its
   answers with the same fingerprint.
