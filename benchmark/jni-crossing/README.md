# The JNI Crossing Benchmark

Measures what it costs to call from Java into native C code, and whether that cost is worth
paying.

Everything needed is in this directory. One command runs it — no build system, no Maven.

---

## 1. Why this exists

Flink pipelines are chains of small operations: transform each record, drop the ones that don't
match, transform again. We already merge chained stages into one operator, which removed the
per-record hand-off between them and bought **16–23%**.

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

Takes about 9 minutes. On first run it downloads JMH into `deps/`. Then it compiles the C,
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
`JAVA_HOME`, then a Homebrew or system OpenJDK 21, then whatever `javac` is on `PATH`. It prints
which one it used. Prefer 21 — Flink 2.x targets Java 17, so 21 is closer to a real TaskManager
than a bleeding-edge JDK, and **numbers from different JDKs are not comparable.**

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
JniCrossing.baselineEmpty        avgt   10   0.324 ± 0.009  ns/op
JniCrossing.javaMonomorphicCall  avgt   10   0.468 ± 0.001  ns/op
JniCrossing.javaMegamorphicCall  avgt   10   7.339 ± 0.088  ns/op
JniCrossing.jniVoid              avgt   10   6.512 ± 0.032  ns/op
```

Each line ran one small piece of code billions of times and reports the **average cost of one
repetition in nanoseconds**, plus a confidence interval.

**`baselineEmpty` is the measuring instrument's own thickness** — it benchmarks a method that does
nothing, and even nothing costs 0.32 ns to measure. **Subtract it from every other number.**

So the two headline figures are **`jniVoid` ≈ 6.2 ns** (one call into an empty C function) and
**`javaMegamorphicCall` ≈ 7.0 ns** (three Java function calls).

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
  function body straight into the calling code, so no call remains. **0.47 ns.** Effectively free.
- *Megamorphic* — no, a different one each time. The runtime can't predict, can't paste anything
  in, and must do a real lookup-and-jump every time. **7.34 ns for three such calls ≈ 2.3 ns
  each** — roughly **14×** the predictable kind.

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
> bodies. The defensible claim is *"~7 ns is what the current operator's dispatch pattern costs"*,
> not *"the operator costs 7 ns per record."* The omissions make the Java side look **better** than
> it is, so the comparison errs in native's favour — the safe direction. To remove the asterisk,
> benchmark `MapFilterFusedOperator` directly; it is a plain object constructible outside Flink.

---

## 4. Results

Measured on JDK 21, `gcc -O2`, 2 forks × 5 iterations. Raw output in `results/`.

### The crossing (`JniCrossing`, ns per call)

| benchmark | score | what it isolates |
|---|---|---|
| `baselineEmpty` | 0.324 ± 0.009 | measurement floor — subtract from everything |
| `javaMonomorphicCall` | 0.468 ± 0.001 | a Java call the runtime can predict and inline |
| `javaMegamorphicCall` | 7.339 ± 0.088 | three it cannot — the cost a native call must beat |
| `jniVoid` | 6.512 ± 0.032 | the pure crossing: no args, no return, empty body |
| `jniLong` | 6.705 ± 0.005 | crossing with one number argument |
| `jniLong3` | 6.802 ± 0.177 | crossing with three |
| `jniPojo` | 7.824 ± 0.015 | crossing plus reading two fields off a Java object |
| `jniString` | 51.671 ± 0.188 | crossing plus converting text between encodings |
| `jniDirectBufAddr` | 23.589 ± 0.036 | crossing plus looking up a flat buffer's address |

### Batching (`JniBatchCrossing`, ns per call, carrying N records)

| N | `jniBatchNoop` | `jniArrayCritical` | `jniArrayRegion` |
|---|---|---|---|
| 1 | 6.930 | 34.791 | 27.939 |
| 16 | 6.931 | 34.303 | 29.237 |
| 64 | 6.953 | 34.222 | 34.631 |
| 256 | 6.933 | 34.621 | 56.946 |
| 1024 | 6.932 | 34.129 | 143.326 |
| 4096 | 6.937 ± 0.022 | 34.167 | 772.292 |

`jniBatchNoop` passes a direct buffer and a record count and returns without touching the data.
**It is perfectly flat from 1 to 4096 records** — the toll does not care how much you carry.
`jniArrayCritical` reaches a Java array by pinning it (also flat, but freezes the memory
manager); `jniArrayRegion` copies it out, and the cost grows linearly at ~0.18 ns/element.

### Repacking (`Marshalling`, ns per record, batches of 1024)

| benchmark | score | allocated |
|---|---|---|
| `baselineTouch` / `baselineTouchString` | 1.095 / 1.500 | ≈0 |
| `encodeLongArray` / `encodeLongDirect` | 0.076 / 0.127 | ≈0 |
| `encodeFixedDirect` / `encodeFixedLongArray` | 1.129 / 1.141 | ≈0 |
| `encodeStringGetBytes` | 8.523 | 56 B |
| `encodeStringGetBytesUnicode` | 28.165 | 144 B |
| `encodeStringEncoder` | 85.415 | 56 B |
| `encodeStringPojo` | 10.477 | 56 B |
| `decodeFixedReuse` / `decodeFixedConsume` | 0.237 / 0.472 | ≈0 |
| `decodeFixedAlloc` | 7.000 | 32 B |
| `decodeStringAlloc` | 14.908 | 136 B |
| **`roundTripFixed`** | **1.785 ± 0.176** | ≈0 |
| **`roundTripStringPojo`** | **25.622 ± 0.854** | 192 B |

The two round trips are encode + a real C call + decode — the honest end-to-end repacking cost.
Internal consistency check: each round trip ≈ the sum of its parts (1.38 vs 1.79; 25.39 vs 25.62).

---

## 5. Observations

**1. Per-record native offload is dead.** One empty crossing (6.2 ns net) costs about as much as
the entire 3-step Java dispatch chain it would replace (7.0 ns net). Writing out the per-record
budget, where `W` is the user's actual work:

```
today:            7.0 + W_java       dispatch, then the work in Java
per-record C:     6.3 + 1.8 + W_c    toll + repacking, then the work in C   =  8.1 + W_c
```

Paying 8.1 to avoid paying 7.0 — behind before the work even starts. Even infinitely fast C
barely breaks even.

**2. Batching is mandatory, not an optimisation.** Because the toll is perfectly flat in batch
size, batching divides it away entirely:

```
batch of 1000:    0.006 + 1.8 + W_c  =  1.8 + W_c
```

1.8 instead of 7.0 — **5.2 ns ahead per record** before the work starts, enough to win even if C
is somewhat *slower* at the work. This is the difference between losing and winning.

**3. The shape of the data matters more than the complexity of the operation.** Numeric
fixed-width records round-trip in 1.8 ns; records containing text cost 25.6 ns and 192 bytes of
garbage — 14× worse, which drags the whole thing back underwater. Keep payloads flat and numeric.

**4. There is a Java-only alternative that gets the same plumbing cost.** Monomorphic calls are
nearly free, so generating specialised code — one dedicated call site per step instead of one
shared loop — would make the calls predictable and inlinable:

```
generated Java:   1.4 + W_java       3 monomorphic calls at 0.47
batched C:        1.8 + W_c
```

**The plumbing costs are then the same** (1.4 vs 1.8 is a rounding error), and the entire
comparison collapses to one question: **is `W_c` smaller than `W_java`?** Native gets no help from
the plumbing and has to justify itself purely on the work — and `W` is the one thing this
benchmark does not measure.

**Smaller findings**

- `GetDirectBufferAddress` costs ~17 ns — resolve the address once at setup, never per call.
- Per-record String marshalling (+45 ns) is disqualifying on its own.
- Reusing one output object instead of allocating per record is worth **30×**.
- A reused `CharsetEncoder` is **10× worse** than naive `getBytes(UTF_8)`, which is intrinsified.
- Non-ASCII text costs **3.3×** more than ASCII — the fast path is Latin-1 only.
- Direct `ByteBuffer` and heap `long[]` are equally fast to write.

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
