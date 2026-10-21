# The JNI Crossing Benchmark

Measures what it costs to call from Java into native C code, and whether that cost is worth
paying.

Everything needed is in this directory. One command runs it — no build system, no Maven.

---

## 1. Why this exists

Flink pipelines are chains of small operations: transform each record, drop the ones that don't
match, transform again. We already merge chained stages into one operator, which removed the
per-record hand-off between them and saved **13–25%** of runtime.

The next idea under evaluation: **rewrite the user's operations in C ahead of time, compile them
to a binary, and have the operator call into that instead.**

The catch is that Java and C don't share a world. Java objects live in memory the runtime owns
and rearranges, carry hidden bookkeeping, and store text in a different encoding than C expects.
C cannot read a Java object directly. So every call into C is a **border crossing** with two
distinct costs:

| cost | what it is | paid |
|---|---|---|
| **the toll** | getting across: arranging arguments, telling the runtime this thread is leaving managed code, coming back | once per **call** |
| **repacking** | copying data out into a flat layout C can read, and results back into Java objects | once per **record** |

This benchmark answers one question:

> **Is the border crossing cheaper than the Java work it would replace?**

Note the second half. **A cost in isolation means nothing** — "calling into C takes 6 ns" is
unusable without knowing 6 ns compared to *what*. So the benchmark always measures **the new cost
and the cost it would replace side by side, in the same JVM, in the same run, on the same
machine.** That is its single most important design decision; quoting a JNI figure from a blog
post and comparing it against a locally measured Java figure would be worthless, because machines
and JVMs differ by more than the effect we are looking for.

---

## 2. Running it

```bash
./benchmark/jni-crossing/run.sh
```

Takes about 12 minutes. On first run it downloads JMH into `deps/`. Then it compiles the C,
compiles the Java, runs everything, and writes `results/jni-crossing.{txt,json}`.

```bash
# quick smoke test, ~1 min — NO error bars, do not quote these numbers
./benchmark/jni-crossing/run.sh -f 0 -wi 1 -i 1

# only the two headline benchmarks (trailing argument is a regex filter)
./benchmark/jni-crossing/run.sh "JniCrossing.(jniVoid|javaMegamorphicCall)"

# only the repacking half, with memory-allocation figures
./benchmark/jni-crossing/run.sh -prof gc Marshalling

# measure on a different JVM
JAVA_HOME=/path/to/jdk ./benchmark/jni-crossing/run.sh
```

**Requirements:** `gcc`, `curl`, a JDK. `run.sh` picks one in this order: an explicit
`JAVA_HOME`, then a Homebrew or system OpenJDK 17, then whatever `javac` is on `PATH`. It prints
which one it used. Use 17: it is what the Flink jobs, the Docker image (`eclipse-temurin:17`) and
the sensor benchmark run on, and **numbers from different JDKs are not comparable.**

What `run.sh` does:

```
curl JMH 1.37 from Maven Central            →  deps/
gcc -O2 -fPIC -shared  native/*.c           →  build/libjnicrossing.so
javac (with JMH's annotation processor)     →  build/classes
java org.openjdk.jmh.Main                   →  results/
```

`deps/`, `build/` and `results/` are gitignored; only sources are tracked.

---

## 3. Reading the output

```
Benchmark                        Mode  Cnt   Score   Error  Units
JniCrossing.baselineEmpty        avgt   10   0.308 ± 0.010  ns/op
JniCrossing.javaMonomorphicCall  avgt   10   0.449 ± 0.003  ns/op
JniCrossing.javaMegamorphicCall  avgt   10   6.910 ± 0.269  ns/op
JniCrossing.jniVoid              avgt   10   6.698 ± 0.002  ns/op
```

Each line ran one small piece of code billions of times and reports the **average cost of one
repetition in nanoseconds**, plus a confidence interval.

**`baselineEmpty` is the measuring instrument's own thickness** — it benchmarks a method that does
nothing, and even nothing costs 0.31 ns to measure. **Subtract it from every other number.**

So the two headline figures are **`jniVoid` ≈ 6.4 ns** (one call into an empty C function) and
**`javaMegamorphicCall` ≈ 6.6 ns** (three Java function calls).

### The three names worth understanding

**`jniVoid` — the pure border crossing.** The C function it calls is completely empty: no
arguments, no return, no body. There is nothing to measure except the crossing. **This is the
design principle of the entire C file** — every function in it is empty or does the single
cheapest thing its arguments allow. If the C did real work, we'd be measuring the work instead of
the border.

**`javaMonomorphicCall` / `javaMegamorphicCall` — pure Java, no C involved.** These measure how
much it costs just to *get to* a function in Java, separate from what the function does. The
bodies are trivial (`v + 1`), so nearly everything measured is call overhead. The jargon is only
asking: **can the runtime predict which function is about to be called?**

- *Monomorphic* — yes, always the same one. After a few thousand executions the runtime copies the
  function body straight into the calling code, so no call remains. **0.45 ns.** Effectively free.
- *Megamorphic* — no, a different one each time. The runtime can't predict, can't paste anything
  in, and must do a real lookup-and-jump every time. **6.91 ns for three such calls ≈ 2.2 ns
  each** (net) — roughly **15×** the predictable kind.

Megamorphic is the one that matters, because the current fused operator has exactly that shape:

```java
for (Step step : steps) {
    value = ((MapFunction<Object, Object>) step.function).map(value);
```

That `.map(value)` is **one spot in the code handling every user function in the chain** — a
different class at each step, so the call site is megamorphic.

> **Caveat before quoting it.** This is a *proxy*, not a measurement of the real operator. It
> reproduces the dispatch shape (3 steps, one shared call site, multiple types) but omits the
> `kind` branch, the record plumbing (`getValue()` / `replace()` / `collect()`), and uses trivial
> bodies. The defensible claim is *"~6.6 ns is what the current operator's dispatch pattern
> costs"*, not *"the operator costs 6.6 ns per record."* The omissions make the Java side look **better** than
> it is, so the comparison errs in native's favour — the safe direction. To remove the asterisk,
> benchmark `MapFilterFusedOperator` directly; it is a plain object constructible outside Flink.

---

## 4. Results

Final run 2026-10-21 (by `../run-all.sh`): JDK 17.0.20.1, `gcc -O2`, 2 forks × 5 iterations; raw
output in `results/jni-crossing.txt`, allocation figures from a separate `-prof gc Marshalling`
run (`results/marshalling-gc.txt`). The original run (2026-10-08, JDK 21.0.12.1) is kept in
`results/archive/` and shown alongside for comparison.

### The crossing (`JniCrossing`, ns per call)

| benchmark | score | JDK 21 original | what it isolates |
|---|---|---|---|
| `baselineEmpty` | 0.308 ± 0.010 | 0.324 | measurement floor — subtract from everything |
| `javaMonomorphicCall` | 0.449 ± 0.003 | 0.468 | a Java call the runtime can predict and inline |
| `javaMegamorphicCall` | 6.910 ± 0.269 | 7.339 | three it cannot — the cost a native call must beat |
| `jniVoid` | 6.698 ± 0.002 | 6.512 | the pure crossing: no args, no return, empty body |
| `jniLong` | 6.694 ± 0.004 | 6.705 | crossing with one number argument |
| `jniLong3` | 6.718 ± 0.048 | 6.802 | crossing with three |
| `jniPojo` | 7.958 ± 0.592 | 7.824 | crossing plus reading two fields off a Java object |
| `jniString` | 52.464 ± 0.151 | 51.671 | crossing plus converting text between encodings |
| `jniDirectBufAddr` | 23.512 ± 0.027 | 23.589 | crossing plus looking up a flat buffer's address |

### Batching (`JniBatchCrossing`, ns per call, carrying N records)

| N | `jniBatchNoop` | `jniArrayCritical` | `jniArrayRegion` |
|---|---|---|---|
| 1 | 6.919 | 39.827 | 28.302 |
| 16 | 6.938 | 39.660 | 29.380 |
| 64 | 6.937 | 39.791 | 34.618 |
| 256 | 6.918 | 39.611 | 57.062 |
| 1024 | 6.917 | 39.626 | 143.078 |
| 4096 | 6.923 ± 0.006 | 39.637 | 739.927 |

`jniBatchNoop` passes a direct buffer and a record count and returns without touching the data.
**It is perfectly flat from 1 to 4096 records** — the toll does not care how much you carry.
`jniArrayCritical` reaches a Java array by pinning it (also flat, but freezes the memory
manager); `jniArrayRegion` copies it out, and the cost grows linearly at ~0.19 ns/element.

### Repacking (`Marshalling`, ns per record, batches of 1024)

| benchmark | score | JDK 21 original | allocated |
|---|---|---|---|
| `baselineTouch` / `baselineTouchString` | 1.072 / 1.453 | 1.095 / 1.500 | ≈0 |
| `encodeLongArray` / `encodeLongDirect` | 0.073 / 0.122 | 0.076 / 0.127 | ≈0 |
| `encodeFixedDirect` / `encodeFixedLongArray` | 1.893 / 1.462 | 1.129 / 1.141 | ≈0 |
| `encodeStringGetBytes` | 8.109 | 8.523 | 56 B |
| `encodeStringGetBytesUnicode` | 28.196 | 28.165 | 144 B |
| `encodeStringEncoder` | 50.009 | 85.415 | 56 B |
| `encodeStringPojo` | 10.004 | 10.477 | 56 B |
| `decodeFixedReuse` / `decodeFixedConsume` | 1.270 / 1.012 | 0.237 / 0.472 | ≈0 |
| `decodeFixedAlloc` | 7.253 | 7.000 | 32 B |
| `decodeStringAlloc` | 14.786 | 14.908 | 136 B |
| **`roundTripFixed`** | **3.409 ± 0.023** | 1.785 | ≈0 |
| **`roundTripStringPojo`** | **25.210 ± 0.670** | 25.622 | 192 B |

The two round trips are encode + a real C call + decode — the honest end-to-end repacking cost.
Internal consistency check: each round trip ≈ the sum of its parts (3.16 vs 3.41; 24.79 vs 25.21).

### What changed from the original run

Almost everything agrees within ~0.5 ns. The exceptions are the fixed-width encode and decode
steps (`encodeFixedDirect` 1.13 → 1.89, `decodeFixedReuse` 0.24 → 1.27, so `roundTripFixed` 1.8 →
3.4 ns), `encodeStringEncoder` (85 → 50) and `jniArrayCritical` (34 → 40). The original run was on
JDK 21, this one on JDK 17, so these shifts may be the JDK; whether the original 0.24 ns (about one
CPU cycle for four fields) also involved the JIT removing work, as happened once before (§6), is
not established. No conclusion below changes, and the sensor experiment (`../sensor-bench/`)
times the real copy-in, call and copy-out end to end, so it does not depend on these figures.

---

## 5. Observations

**1. Per-record native offload is dead.** One empty crossing (6.4 ns net) costs about as much as
the entire 3-step Java dispatch chain it would replace (6.6 ns net). Writing out the per-record
budget, where `W` is the user's actual work:

```
today:            6.6 + W_java       dispatch, then the work in Java
per-record C:     6.4 + 3.4 + W_c    toll + repacking, then the work in C   =  9.8 + W_c
```

Paying 9.8 to avoid paying 6.6 — behind before the work even starts. Even infinitely fast C
does not break even.

**2. Batching is mandatory, not an optimisation.** Because the toll is perfectly flat in batch
size, batching divides it away entirely:

```
batch of 1000:    0.007 + 3.4 + W_c  =  3.4 + W_c
```

3.4 instead of 6.6 — **3.2 ns ahead per record** before the work starts, enough to win even if C
is slightly *slower* at the work. This is the difference between losing and winning.

**3. The shape of the data matters more than the complexity of the operation.** Numeric
fixed-width records round-trip in 3.4 ns; records containing text cost 25.2 ns and 192 bytes of
garbage — ~7× worse, which drags the whole thing back underwater. Keep payloads flat and numeric.

**4. There is a Java-only alternative with cheaper plumbing.** Monomorphic calls are nearly free,
so generating specialised code — one dedicated call site per step instead of one shared loop —
would make the calls predictable and inlinable:

```
generated Java:   1.4 + W_java       3 monomorphic calls at 0.45
batched C:        3.4 + W_c
```

**The plumbing then favours Java** (1.4 vs 3.4 ns), and the comparison comes down to one
question: **is `W_c` enough smaller than `W_java`?** Native gets no help from the plumbing and has
to justify itself purely on the work — and `W` is the one thing this benchmark does not measure
(the sensor benchmark does).

**Smaller findings**

- `GetDirectBufferAddress` costs ~17 ns — resolve the address once at setup, never per call.
- Per-record String marshalling (+46 ns) is disqualifying on its own.
- Reusing one output object instead of allocating per record is worth **~6×** (7.3 vs 1.3 ns).
- A reused `CharsetEncoder` is **~6× worse** than naive `getBytes(UTF_8)`, which is intrinsified.
- Non-ASCII text costs **3.5×** more than ASCII — the fast path is Latin-1 only.
- A heap `long[]` is slightly faster to write than a direct `ByteBuffer` (1.46 vs 1.89 ns per
  record).

---

## 6. If you modify this

**Make sure allocated objects genuinely escape.** The first version of `decodeFixedAlloc` reported
0.233 ns/record — *faster than not allocating at all*. The runtime had noticed the object never
left the loop and **deleted the allocation entirely**. Parking the objects in an array, as really
happens downstream, moved the number to 7.000 — a **30× difference**. At nanosecond scale,
mistakes run in the optimistic direction: a number that looks too good usually is.

**Keep the C functions empty.** The moment one does real work, it stops measuring the border.

**Keep the Java baselines.** Without something to compare against, the C numbers mean nothing.

**Don't quote smoke-test numbers** (`-f 0 -wi 1 -i 1`) — no forks, no warm-up, no error bars.
