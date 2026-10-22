## Stage 1 — Java vs C, on their own (JMH; C includes copying + call)

ns per record, including data generation (column `generate`).

**Every version side by side** (batch-first versions and C at B=1024):

| k | generate | Java | tuned Java | Java, batch-first | tuned Java, batch-first | C |
|---|---|---|---|---|---|---|
| 1 | 37 | 82 | 73 | 62 | 61 | 60 |
| 4 | 37 | 94 | 88 | 86 | 82 | 65 |
| 16 | 37 | 127 | 116 | 119 | 115 | 84 |
| 64 | 37 | 204 | 186 | 200 | 183 | 140 |

**C speed-up over each Java version** (>1 = C faster; C at B=1024):

| k | vs Java | vs tuned Java | vs Java, batch-first | vs tuned Java, batch-first |
|---|---|---|---|---|
| 1 | 1.37× | 1.23× | 1.04× | 1.01× |
| 4 | 1.45× | 1.36× | 1.32× | 1.26× |
| 16 | 1.50× | 1.38× | 1.42× | 1.37× |
| 64 | 1.46× | 1.33× | 1.43× | 1.31× |

**By batch size** (records generated per batch / per call into C):

| k | version | B=1 | B=16 | B=64 | B=256 | B=1024 | B=4096 |
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

## Stage 2 — plugged into Flink (local MiniCluster, whole job)

ns per record, median of 3 runs. Includes ~40 ns/record of data generation.

| k | stock | fusion | tuned Java | native B=1 | native B=16 | native B=64 | native B=256 | native B=1024 | native B=4096 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 124 | 128 | 120 | 130 | 125 | 124 | 133 | 132 | 127 |
| 4 | 141 | 136 | 133 | 134 | 130 | 136 | 134 | 137 | 134 |
| 16 | 169 | 161 | 159 | 148 | 150 | 155 | 160 | 152 | 158 |
| 64 | 263 | 247 | 232 | 203 | 203 | 213 | 211 | 211 | 214 |

**Native speed-up over the fusion prototype** (>1 = native faster):

| k | B=1 | B=16 | B=64 | B=256 | B=1024 | B=4096 |
|---|---|---|---|---|---|---|
| 1 | 0.98× | 1.02× | 1.03× | 0.96× | 0.97× | 1.01× |
| 4 | 1.02× | 1.05× | 1.00× | 1.02× | 0.99× | 1.01× |
| 16 | 1.09× | 1.07× | 1.04× | 1.01× | 1.06× | 1.02× |
| 64 | 1.22× | 1.22× | 1.16× | 1.17× | 1.17× | 1.16× |

**Speed-up over the fusion prototype: tuned Java vs native** (>1 = faster than fusion):

| k | tuned Java | native B=1 | native B=16 | native B=64 | native B=256 | native B=1024 | native B=4096 |
|---|---|---|---|---|---|---|---|
| 1 | 1.07× | 0.98× | 1.02× | 1.03× | 0.96× | 0.97× | 1.01× |
| 4 | 1.03× | 1.02× | 1.05× | 1.00× | 1.02× | 0.99× | 1.01× |
| 16 | 1.01× | 1.09× | 1.07× | 1.04× | 1.01× | 1.06× | 1.02× |
| 64 | 1.07× | 1.22× | 1.22× | 1.16× | 1.17× | 1.17× | 1.16× |

Run-to-run spread (max-min over median): typical 3.9%, worst 9.3% (stock, k=1).

